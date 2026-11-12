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
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Paramore.Brighter.AWS.V4.Tests.Helpers;
using Paramore.Brighter.AWS.V4.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;
using Xunit;

namespace Paramore.Brighter.AWS.V4.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsCustomWireReaderTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_receiving_a_legacy_wire_message_should_map_headers_and_preserve_acknowledgement(bool useAsync)
    {
        //Arrange
        const string body = "{\"orderId\":\"order-17\"}";
        const string traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Wire-Reader-{Guid.NewGuid():N}");
        var topic = new RoutingKey(queueName);
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        IAmAMessageConsumerAsync? consumer = null;

        try
        {
            var queue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new Dictionary<string, string> { ["VisibilityTimeout"] = "1" }
            }, timeout.Token);
            await client.SendMessageAsync(new SendMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                MessageBody = body,
                MessageAttributes = new()
                {
                    ["legacy-id"] = new() { DataType = "String", StringValue = "legacy-order-17" },
                    ["tenant-id"] = new() { DataType = "String", StringValue = "tenant-42" },
                    ["traceparent"] = new() { DataType = "String", StringValue = traceParent }
                }
            }, timeout.Token);

            var subscription = new SqsSubscription(
                new SubscriptionName(queueName),
                new ChannelName(queue.QueueUrl),
                ChannelType.PointToPoint,
                topic,
                requestType: typeof(Command),
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                findQueueBy: QueueFindBy.Url,
                makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = new LegacyTenantSqsWireMessageReader(topic)
            };
            var factory = new SqsMessageConsumerFactory(connection);
            consumer = useAsync
                ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));

            //Act
            var messages = useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
            var message = Assert.Single(messages);
            if (useAsync)
            {
                await consumer.AcknowledgeAsync(message, timeout.Token);
            }
            else
            {
                Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Acknowledge(message);
            }

            await Task.Delay(TimeSpan.FromSeconds(2), timeout.Token);
            var remaining = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                WaitTimeSeconds = 1,
                MaxNumberOfMessages = 1
            }, timeout.Token);

            //Assert
            Assert.Equal("legacy-order-17", message.Id.Value);
            Assert.Equal("tenant-42", message.Header.Bag["tenant_id"]);
            Assert.Equal(body, message.Body.Value);
            Assert.Equal(traceParent, message.Header.TraceParent?.Value);
            Assert.Equal(0, message.Header.HandledCount);
            Assert.Empty(remaining.Messages ?? []);
            Assert.True(message.Header.Bag.TryGetValue("ReceiptHandle", out var receiptHandle));
            Assert.False(string.IsNullOrEmpty(Assert.IsType<string>(receiptHandle)));
        }
        finally
        {
            try
            {
                if (consumer != null)
                {
                    await consumer.DisposeAsync();
                }
            }
            finally
            {
                await reaper.ReapAsync();
            }
        }
    }
}
