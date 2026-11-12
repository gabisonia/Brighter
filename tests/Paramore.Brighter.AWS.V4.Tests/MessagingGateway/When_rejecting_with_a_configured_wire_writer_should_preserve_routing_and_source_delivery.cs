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
public class SqsRejectionWireWriterTests
{
    [Theory]
    [InlineData(false, 0, false, 0)]
    [InlineData(false, 0, false, 1)]
    [InlineData(false, 0, false, 2)]
    [InlineData(false, 0, true, 0)]
    [InlineData(false, 0, true, 1)]
    [InlineData(false, 0, true, 2)]
    [InlineData(false, 1, false, 0)]
    [InlineData(false, 1, false, 1)]
    [InlineData(false, 1, false, 2)]
    [InlineData(false, 1, true, 0)]
    [InlineData(false, 1, true, 1)]
    [InlineData(false, 1, true, 2)]
    [InlineData(false, 2, false, 0)]
    [InlineData(false, 2, false, 1)]
    [InlineData(false, 2, false, 2)]
    [InlineData(false, 2, true, 0)]
    [InlineData(false, 2, true, 1)]
    [InlineData(false, 2, true, 2)]
    [InlineData(true, 0, false, 0)]
    [InlineData(true, 0, false, 1)]
    [InlineData(true, 0, false, 2)]
    [InlineData(true, 0, true, 0)]
    [InlineData(true, 0, true, 1)]
    [InlineData(true, 0, true, 2)]
    [InlineData(true, 1, false, 0)]
    [InlineData(true, 1, false, 1)]
    [InlineData(true, 1, false, 2)]
    [InlineData(true, 1, true, 0)]
    [InlineData(true, 1, true, 1)]
    [InlineData(true, 1, true, 2)]
    [InlineData(true, 2, false, 0)]
    [InlineData(true, 2, false, 1)]
    [InlineData(true, 2, false, 2)]
    [InlineData(true, 2, true, 0)]
    [InlineData(true, 2, true, 1)]
    [InlineData(true, 2, true, 2)]
    public async Task When_rejecting_with_a_configured_wire_writer_should_preserve_routing_and_source_delivery(
        bool useAsync, int route, bool compose, int failureMode)
    {
        //Arrange
        const string APPLICATION_ID = "rejected-order-17";
        const string PAYLOAD = "{\"orderId\":\"order-17\"}";
        const string DESCRIPTION = "Cannot process order-17";
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Rejection-Writer-Source-{Guid.NewGuid():N}");
        var invalidName = reaper.TrackQueue($"Rejection-Writer-Invalid-{Guid.NewGuid():N}");
        var deadName = reaper.TrackQueue($"Rejection-Writer-Dead-{Guid.NewGuid():N}");
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        IAmAMessageConsumerAsync? consumer = null;

        try
        {
            var source = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new Dictionary<string, string> { ["VisibilityTimeout"] = "120" }
            }, timeout.Token);
            var invalid = await client.CreateQueueAsync(new CreateQueueRequest { QueueName = invalidName }, timeout.Token);
            var dead = await client.CreateQueueAsync(new CreateQueueRequest { QueueName = deadName }, timeout.Token);
            var invalidWriter = new RejectionProfileSqsWireMessageWriter("invalid-profile", invalid.QueueUrl, compose);
            var deadWriter = new RejectionProfileSqsWireMessageWriter("dead-profile", dead.QueueUrl, compose);
            var selectedWriter = route == 0 ? invalidWriter : deadWriter;
            selectedWriter.FailureMode = failureMode;
            var sent = await client.SendMessageAsync(new SendMessageRequest
            {
                QueueUrl = source.QueueUrl,
                MessageBody = PAYLOAD,
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    ["id"] = new() { DataType = "String", StringValue = APPLICATION_ID },
                    ["topic"] = new() { DataType = "String", StringValue = queueName },
                    ["message-type"] = new() { DataType = "String", StringValue = "MT_EVENT" }
                }
            }, timeout.Token);
            var subscription = new SqsSubscription(
                new SubscriptionName(queueName), new ChannelName(source.QueueUrl),
                ChannelType.PointToPoint, new RoutingKey(queueName), requestType: typeof(Command),
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                findQueueBy: QueueFindBy.Url,
                deadLetterRoutingKey: new RoutingKey(deadName),
                invalidMessageRoutingKey: route == 2 ? null : new RoutingKey(invalidName),
                makeChannels: OnMissingChannel.Validate)
            {
                InvalidMessageWireMessageWriter = invalidWriter,
                DeadLetterWireMessageWriter = deadWriter
            };
            var factory = new SqsMessageConsumerFactory(connection);
            consumer = useAsync ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));
            var message = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10)));
            Assert.Equal(APPLICATION_ID, message.Id.Value);
            Assert.Equal(new RoutingKey(queueName), message.Header.Topic);
            Assert.Equal(PAYLOAD, message.Body.Value);
            message.Header.HandledCount = 3;
            var receipt = Assert.IsType<string>(message.Header.Bag["ReceiptHandle"]);
            await client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
            {
                QueueUrl = source.QueueUrl, ReceiptHandle = receipt, VisibilityTimeout = 1
            }, timeout.Token);
            var reason = new MessageRejectionReason(
                route == 1 ? RejectionReason.DeliveryError : RejectionReason.Unacceptable, DESCRIPTION);
            var before = DateTimeOffset.UtcNow;

            //Act
            var rejected = useAsync
                ? await consumer.RejectAsync(message, reason, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(message, reason);

            //Assert
            Assert.True(rejected);
            if (failureMode != 0)
            {
                var redelivery = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = source.QueueUrl, WaitTimeSeconds = 5, MaxNumberOfMessages = 1,
                    MessageAttributeNames = ["All"]
                }, timeout.Token);
                var original = Assert.Single(redelivery.Messages ?? []);
                Assert.Equal(sent.MessageId, original.MessageId);
                Assert.Equal(PAYLOAD, original.Body);
                Assert.Equal(APPLICATION_ID, original.MessageAttributes["id"].StringValue);
                Assert.Equal(queueName, original.MessageAttributes["topic"].StringValue);
                var invalidAfterFailure = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = invalid.QueueUrl, WaitTimeSeconds = 1, MaxNumberOfMessages = 1
                }, timeout.Token);
                var deadAfterFailure = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = dead.QueueUrl, WaitTimeSeconds = 1, MaxNumberOfMessages = 1
                }, timeout.Token);
                Assert.Empty(invalidAfterFailure.Messages ?? []);
                Assert.Empty(deadAfterFailure.Messages ?? []);

                //Arrange
                selectedWriter.FailureMode = 0;
                await client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                {
                    QueueUrl = source.QueueUrl, ReceiptHandle = original.ReceiptHandle, VisibilityTimeout = 0
                }, timeout.Token);
                message = Assert.Single(useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10)));
                Assert.Equal(APPLICATION_ID, message.Id.Value);
                Assert.Equal(PAYLOAD, message.Body.Value);
                Assert.Equal(new RoutingKey(queueName), message.Header.Topic);
                var recoveredReceipt = Assert.IsType<string>(message.Header.Bag["ReceiptHandle"]);
                Assert.NotEqual(receipt, recoveredReceipt);
                message.Header.HandledCount = 3;
                await client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                {
                    QueueUrl = source.QueueUrl, ReceiptHandle = recoveredReceipt, VisibilityTimeout = 1
                }, timeout.Token);

                //Act
                rejected = useAsync
                    ? await consumer.RejectAsync(message, reason, timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(message, reason);

                //Assert
                Assert.True(rejected);
            }

            var destination = route == 0 ? invalid : dead;
            var other = route == 0 ? dead : invalid;
            var forwarded = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = destination.QueueUrl, WaitTimeSeconds = 5, MaxNumberOfMessages = 10,
                MessageAttributeNames = ["All"]
            }, timeout.Token);
            var copy = Assert.Single(forwarded.Messages ?? []);
            Assert.True(copy.MessageAttributes.TryGetValue("rejection-profile", out var profile));
            Assert.Equal(route == 0 ? "invalid-profile" : "dead-profile", profile.StringValue);
            using var proof = JsonDocument.Parse(compose
                ? copy.MessageAttributes["rejection-proof"].StringValue : copy.Body);
            var metadata = proof.RootElement;
            Assert.Equal(APPLICATION_ID, metadata.GetProperty("id").GetString());
            Assert.Equal(PAYLOAD, metadata.GetProperty("payload").GetString());
            Assert.Equal(3, metadata.GetProperty("handledCount").GetInt32());
            Assert.Equal(queueName, metadata.GetProperty("originalTopic").GetString());
            Assert.Equal("MT_EVENT", metadata.GetProperty("originalType").GetString());
            Assert.Equal(route == 1 ? "DeliveryError" : "Unacceptable", metadata.GetProperty("reason").GetString());
            Assert.Equal(DESCRIPTION, metadata.GetProperty("description").GetString());
            Assert.InRange(metadata.GetProperty("timestamp").GetDateTimeOffset(), before, DateTimeOffset.UtcNow);
            if (compose)
            {
                Assert.Equal(PAYLOAD, copy.Body);
                Assert.Equal(APPLICATION_ID, copy.MessageAttributes[HeaderNames.Id].StringValue);
                using var bag = JsonDocument.Parse(copy.MessageAttributes[HeaderNames.Bag].StringValue);
                Assert.Equal("3", bag.RootElement.GetProperty(HeaderNames.HandledCount).GetString());
                Assert.False(bag.RootElement.TryGetProperty("ReceiptHandle", out _));
            }
            var misrouted = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = other.QueueUrl, WaitTimeSeconds = 1, MaxNumberOfMessages = 1
            }, timeout.Token);
            Assert.Empty(misrouted.Messages ?? []);
            await Task.Delay(TimeSpan.FromSeconds(2), timeout.Token);
            var remaining = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = source.QueueUrl, WaitTimeSeconds = 1, MaxNumberOfMessages = 1
            }, timeout.Token);
            Assert.Empty(remaining.Messages ?? []);
        }
        finally
        {
            try
            {
                if (consumer != null)
                    await consumer.DisposeAsync();
            }
            finally
            {
                await reaper.ReapAsync();
            }
        }
    }
}
