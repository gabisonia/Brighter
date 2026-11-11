#region Licence

/* The MIT License (MIT)
Copyright © 2026 gabisonia

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

#nullable enable

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Paramore.Brighter.AWS.V4.Tests.Helpers;
using Paramore.Brighter.AWS.V4.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;
using Xunit;

namespace Paramore.Brighter.AWS.V4.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsRejectionWriterSubscriptionIsolationTests
{
    [Theory]
    [InlineData(false, false, 0)]
    [InlineData(false, false, 1)]
    [InlineData(false, false, 2)]
    [InlineData(false, true, 0)]
    [InlineData(false, true, 1)]
    [InlineData(false, true, 2)]
    [InlineData(true, false, 0)]
    [InlineData(true, false, 1)]
    [InlineData(true, false, 2)]
    [InlineData(true, true, 0)]
    [InlineData(true, true, 1)]
    [InlineData(true, true, 2)]
    public async Task When_subscriptions_select_rejection_writers_should_keep_each_consumers_mapping_isolated(
        bool useAsync, bool customFirst, int route)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var invalidName = reaper.TrackQueue($"Rejection-Isolation-Invalid-{Guid.NewGuid():N}");
        var deadName = reaper.TrackQueue($"Rejection-Isolation-Dead-{Guid.NewGuid():N}");
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var consumers = new List<IAmAMessageConsumerAsync>();
        var sourceNames = new List<string>();
        var sourceUrls = new List<string>();
        var customSelections = new[] { customFirst, !customFirst };
        var factory = new SqsMessageConsumerFactory(connection);

        try
        {
            var invalid = await client.CreateQueueAsync(new CreateQueueRequest { QueueName = invalidName }, timeout.Token);
            var dead = await client.CreateQueueAsync(new CreateQueueRequest { QueueName = deadName }, timeout.Token);
            for (var index = 0; index < 2; index++)
            {
                var sourceName = reaper.TrackQueue($"Rejection-Isolation-Source-{Guid.NewGuid():N}");
                var source = await client.CreateQueueAsync(new CreateQueueRequest
                {
                    QueueName = sourceName,
                    Attributes = new Dictionary<string, string> { ["VisibilityTimeout"] = "120" }
                }, timeout.Token);
                sourceNames.Add(sourceName);
                sourceUrls.Add(source.QueueUrl);
                await client.SendMessageAsync(new SendMessageRequest
                {
                    QueueUrl = source.QueueUrl,
                    MessageBody = $"independent-payload-{index}",
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        ["id"] = new() { DataType = "String", StringValue = $"independent-id-{index}" },
                        ["topic"] = new() { DataType = "String", StringValue = sourceName }
                    }
                }, timeout.Token);
                var subscription = new SqsSubscription(
                    new SubscriptionName(sourceName), new ChannelName(source.QueueUrl),
                    ChannelType.PointToPoint, new RoutingKey(sourceName), requestType: typeof(Command),
                    messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                    findQueueBy: QueueFindBy.Url,
                    deadLetterRoutingKey: new RoutingKey(deadName),
                    invalidMessageRoutingKey: route == 2 ? null : new RoutingKey(invalidName),
                    makeChannels: OnMissingChannel.Validate)
                {
                    InvalidMessageWireMessageWriter = customSelections[index]
                        ? new RejectionProfileSqsWireMessageWriter("custom-invalid", invalid.QueueUrl, compose: true) : null,
                    DeadLetterWireMessageWriter = customSelections[index]
                        ? new RejectionProfileSqsWireMessageWriter("custom-dead", dead.QueueUrl, compose: true) : null
                };
                consumers.Add(useAsync ? factory.CreateAsync(subscription)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription)));
            }

            //Act
            for (var index = 0; index < 2; index++)
            {
                var consumer = consumers[index];
                var message = Assert.Single(useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10)));
                message.Header.HandledCount = 3;
                await client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                {
                    QueueUrl = sourceUrls[index],
                    ReceiptHandle = Assert.IsType<string>(message.Header.Bag["ReceiptHandle"]),
                    VisibilityTimeout = 1
                }, timeout.Token);
                var reason = new MessageRejectionReason(
                    route == 1 ? RejectionReason.DeliveryError : RejectionReason.Unacceptable, "Isolated rejection");
                var rejected = useAsync
                    ? await consumer.RejectAsync(message, reason, timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(message, reason);
                var forwarded = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = route == 0 ? invalid.QueueUrl : dead.QueueUrl,
                    WaitTimeSeconds = 5, MaxNumberOfMessages = 1, MessageAttributeNames = ["All"]
                }, timeout.Token);

                //Assert
                Assert.True(rejected);
                var copy = Assert.Single(forwarded.Messages ?? []);
                Assert.Equal(customSelections[index], copy.MessageAttributes.ContainsKey("rejection-profile"));
                if (customSelections[index])
                    Assert.Equal(route == 0 ? "custom-invalid" : "custom-dead",
                        copy.MessageAttributes["rejection-profile"].StringValue);
                Assert.Equal($"independent-id-{index}", copy.MessageAttributes[HeaderNames.Id].StringValue);
                Assert.Equal($"independent-payload-{index}", copy.Body);
                using var bag = JsonDocument.Parse(copy.MessageAttributes[HeaderNames.Bag].StringValue);
                Assert.Equal(sourceNames[index], bag.RootElement.GetProperty(RejectionMetadataKeyNames.OriginalTopic).GetString());
                Assert.Equal("3", bag.RootElement.GetProperty(HeaderNames.HandledCount).GetString());
            }
            var otherRoute = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = route == 0 ? dead.QueueUrl : invalid.QueueUrl,
                WaitTimeSeconds = 1, MaxNumberOfMessages = 1
            }, timeout.Token);
            Assert.Empty(otherRoute.Messages ?? []);
            await Task.Delay(TimeSpan.FromSeconds(2), timeout.Token);
            foreach (var url in sourceUrls)
            {
                var remaining = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = url, WaitTimeSeconds = 1, MaxNumberOfMessages = 1
                }, timeout.Token);
                Assert.Empty(remaining.Messages ?? []);
            }
        }
        finally
        {
            try
            {
                foreach (var consumer in consumers)
                    await consumer.DisposeAsync();
            }
            finally
            {
                await reaper.ReapAsync();
            }
        }
    }
}
