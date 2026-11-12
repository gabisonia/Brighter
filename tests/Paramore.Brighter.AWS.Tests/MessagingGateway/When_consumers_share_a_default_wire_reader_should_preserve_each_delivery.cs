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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Paramore.Brighter.AWS.Tests.Helpers;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Xunit;

namespace Paramore.Brighter.AWS.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsDefaultWireReaderReuseTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_consumers_share_a_default_wire_reader_should_preserve_each_delivery(
        bool useAsync, bool rawMessageDelivery)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var reader = new DefaultSqsWireMessageReader(rawMessageDelivery);
        var factory = new SqsMessageConsumerFactory(connection);
        var subscriptions = new List<SqsSubscription>();
        var consumers = new List<IAmAMessageConsumerAsync>();
        var received = new List<Message>();

        try
        {
            foreach (var index in new[] { 0, 1 })
            {
                var name = reaper.TrackQueue($"Wire-Reader-Reuse-{Guid.NewGuid():N}");
                var queue = await client.CreateQueueAsync(new CreateQueueRequest
                {
                    QueueName = name,
                    Attributes = new() { ["VisibilityTimeout"] = "1" }
                }, timeout.Token);
                var attributes = new Dictionary<string, MessageAttributeValue>
                {
                    ["id"] = new() { DataType = "String", StringValue = $"application-{index}" },
                    ["topic"] = new() { DataType = "String", StringValue = name },
                    ["content-type"] = new() { DataType = "String", StringValue = "text/plain" },
                    ["message-type"] = new() { DataType = "String", StringValue = "MT_EVENT" },
                    ["subject"] = new() { DataType = "String", StringValue = $"subject-{index}" },
                    ["correlation-id"] = new() { DataType = "String", StringValue = $"correlation-{index}" },
                    ["bag"] = new()
                    {
                        DataType = "String",
                        StringValue = JsonSerializer.Serialize(new Dictionary<string, string>
                        {
                            ["tenant_id"] = $"tenant-{index}"
                        })
                    }
                };
                var body = $"payload-{index}";
                await client.SendMessageAsync(new SendMessageRequest
                {
                    QueueUrl = queue.QueueUrl,
                    MessageBody = rawMessageDelivery ? body : JsonSerializer.Serialize(new
                    {
                        Type = "Notification",
                        MessageId = $"notification-{index}",
                        TopicArn = $"arn:aws:sns:us-east-1:123456789012:{name}",
                        Message = body,
                        Subject = $"subject-{index}",
                        MessageAttributes = attributes.ToDictionary(
                            entry => entry.Key,
                            entry => new { Type = entry.Value.DataType, Value = entry.Value.StringValue })
                    }),
                    MessageAttributes = rawMessageDelivery ? attributes : new()
                }, timeout.Token);
                var subscription = new SqsSubscription(
                    new SubscriptionName(name),
                    new ChannelName(queue.QueueUrl),
                    ChannelType.PointToPoint,
                    new RoutingKey(name),
                    requestType: typeof(Command),
                    messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                    findQueueBy: QueueFindBy.Url,
                    queueAttributes: new SqsAttributes(rawMessageDelivery: rawMessageDelivery),
                    makeChannels: OnMissingChannel.Validate)
                {
                    WireMessageReader = reader
                };
                subscriptions.Add(subscription);
                consumers.Add(useAsync
                    ? factory.CreateAsync(subscription)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription)));
            }

            //Act
            foreach (var consumer in consumers)
            {
                var messages = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
                received.Add(Assert.Single(messages));
            }

            //Assert
            Assert.Equal(2, received.Select(message => message.Id.Value).Distinct().Count());
            Assert.Equal(2, received.Select(message => message.Header.Bag["ReceiptHandle"]).Distinct().Count());
            for (var index = 0; index < received.Count; index++)
            {
                var message = received[index];
                Assert.Equal($"application-{index}", message.Id.Value);
                Assert.Equal($"payload-{index}", message.Body.Value);
                Assert.Equal(subscriptions[index].RoutingKey, message.Header.Topic);
                Assert.Equal($"subject-{index}", message.Header.Subject);
                Assert.Equal($"correlation-{index}", message.Header.CorrelationId.Value);
                Assert.Equal($"tenant-{index}", message.Header.Bag["tenant_id"]);
                Assert.Equal(MessageType.MT_EVENT, message.Header.MessageType);
                Assert.Equal(0, message.Header.HandledCount);
                Assert.False(string.IsNullOrEmpty(Assert.IsType<string>(message.Header.Bag["ReceiptHandle"])));
                Assert.Same(reader, subscriptions[index].WireMessageReader);
                if (useAsync)
                    await consumers[index].AcknowledgeAsync(message, timeout.Token);
                else
                    Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumers[index]).Acknowledge(message);
            }
            await Task.Delay(TimeSpan.FromSeconds(2), timeout.Token);
            foreach (var subscription in subscriptions)
            {
                var remaining = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = subscription.ChannelName,
                    WaitTimeSeconds = 1,
                    MaxNumberOfMessages = 1
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
