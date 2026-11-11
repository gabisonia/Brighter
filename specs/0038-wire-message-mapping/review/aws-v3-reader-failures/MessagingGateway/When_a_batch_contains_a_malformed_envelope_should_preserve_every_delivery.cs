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
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Paramore.Brighter.AWS.Tests.Helpers;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Xunit;
using Xunit.Abstractions;

namespace Paramore.Brighter.AWS.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsMixedBatchEnvelopeWireReaderTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_batch_contains_a_malformed_envelope_should_preserve_every_delivery(bool useAsync, bool configureReader)
    {
        //Arrange
        const string MALFORMED_BODY = "{invalid SNS envelope";
        const string TRACE_PARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Wire-Envelope-Batch-{Guid.NewGuid():N}");
        var invalidQueueName = reaper.TrackQueue($"Wire-Envelope-Batch-Invalid-{Guid.NewGuid():N}");
        var topic = new RoutingKey(queueName);
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        IAmAMessageConsumerAsync? consumer = null;

        try
        {
            var queue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new Dictionary<string, string> { ["VisibilityTimeout"] = "5" }
            }, timeout.Token);
            var invalidQueue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = invalidQueueName
            }, timeout.Token);
            var sent = await client.SendMessageBatchAsync(new SendMessageBatchRequest
            {
                QueueUrl = queue.QueueUrl,
                Entries =
                [
                    Entry("first", "application-first", "first payload", "tenant-one", TRACE_PARENT, queueName),
                    new SendMessageBatchRequestEntry { Id = "invalid", MessageBody = MALFORMED_BODY },
                    Entry("last", "application-last", "last payload", "tenant-two", TRACE_PARENT, queueName)
                ]
            }, timeout.Token);
            Assert.Empty(sent.Failed ?? []);
            Assert.Equal(3, sent.Successful.Count);
            var invalidNativeId = Assert.Single(sent.Successful, entry => entry.Id == "invalid").MessageId;

            var subscription = new SqsSubscription(
                new SubscriptionName(queueName),
                new ChannelName(queue.QueueUrl),
                ChannelType.PointToPoint,
                topic,
                requestType: typeof(Command),
                bufferSize: 3,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                findQueueBy: QueueFindBy.Url,
                queueAttributes: new SqsAttributes(rawMessageDelivery: false),
                invalidMessageRoutingKey: new RoutingKey(invalidQueueName),
                makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = configureReader ? new DefaultSqsWireMessageReader(rawMessageDelivery: false) : null
            };
            var factory = new SqsMessageConsumerFactory(connection);
            consumer = useAsync
                ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));

            //Act
            var received = new List<Message>();
            var receiveException = await Record.ExceptionAsync(async () =>
            {
                var elapsed = Stopwatch.StartNew();
                while (received.Count < 3 && elapsed.Elapsed < TimeSpan.FromSeconds(3))
                {
                    var batch = useAsync
                        ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                        : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(1));
                    output.WriteLine($"Receive returned {batch.Count(message => !message.IsEmpty)} deliveries");
                    received.AddRange(batch.Where(message => !message.IsEmpty));
                }
            });

            //Assert
            Assert.Null(receiveException);
            Assert.Equal(3, received.Count);
            var first = Assert.Single(received, message => message.Id.Value == "application-first");
            var last = Assert.Single(received, message => message.Id.Value == "application-last");
            var failed = Assert.Single(received, message => message.Header.MessageType == MessageType.MT_UNACCEPTABLE);
            Assert.Equal(MessageType.MT_EVENT, first.Header.MessageType);
            Assert.Equal(MessageType.MT_EVENT, last.Header.MessageType);
            Assert.Equal(topic, first.Header.Topic);
            Assert.Equal(topic, last.Header.Topic);
            Assert.Equal("first payload", first.Body.Value);
            Assert.Equal("last payload", last.Body.Value);
            Assert.Equal("tenant-one", first.Header.Bag["tenant_id"]);
            Assert.Equal("tenant-two", last.Header.Bag["tenant_id"]);
            Assert.Equal(TRACE_PARENT, first.Header.TraceParent?.Value);
            Assert.Equal(TRACE_PARENT, last.Header.TraceParent?.Value);
            Assert.Equal(MALFORMED_BODY, failed.Body.Value);
            Assert.Equal(invalidNativeId, failed.Id.Value);
            Assert.All(received, message =>
            {
                Assert.Equal(0, message.Header.HandledCount);
                Assert.False(string.IsNullOrEmpty(Assert.IsType<string>(message.Header.Bag["ReceiptHandle"])));
            });
            Assert.Equal(3, received.Select(message => message.Header.Bag["ReceiptHandle"]).Distinct().Count());

            //Act
            foreach (var message in new[] { first, last })
            {
                if (useAsync)
                    await consumer.AcknowledgeAsync(message, timeout.Token);
                else
                    Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Acknowledge(message);
            }
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "Malformed SNS envelope");
            var rejected = useAsync
                ? await consumer.RejectAsync(failed, reason, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(failed, reason);
            var forwarded = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = invalidQueue.QueueUrl,
                WaitTimeSeconds = 1,
                MaxNumberOfMessages = 1,
                MessageAttributeNames = ["All"]
            }, timeout.Token);
            await Task.Delay(TimeSpan.FromSeconds(6), timeout.Token);
            var remaining = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                WaitTimeSeconds = 1,
                MaxNumberOfMessages = 3
            }, timeout.Token);

            //Assert
            Assert.True(rejected);
            var invalidMessage = Assert.Single(forwarded.Messages ?? []);
            Assert.Equal(MALFORMED_BODY, invalidMessage.Body);
            Assert.Equal(invalidNativeId, invalidMessage.MessageAttributes[HeaderNames.Id].StringValue);
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

    private static SendMessageBatchRequestEntry Entry(string entryId, string applicationId, string body,
        string tenant, string traceParent, string topic)
    {
        var attributes = new Dictionary<string, string>
        {
            ["id"] = applicationId,
            ["topic"] = topic,
            ["content-type"] = "text/plain",
            ["message-type"] = "MT_EVENT",
            ["tenant_id"] = tenant,
            ["cloudeventheaders"] = JsonSerializer.Serialize(new Dictionary<string, string>
            {
                ["traceparent"] = traceParent, ["specversion"] = "1.0"
            })
        };
        return new SendMessageBatchRequestEntry
        {
            Id = entryId,
            MessageBody = JsonSerializer.Serialize(new
            {
                Type = "Notification",
                MessageId = $"sns-{entryId}",
                TopicArn = "arn:aws:sns:us-east-1:123456789012:orders",
                Message = body,
                MessageAttributes = attributes.ToDictionary(
                    attribute => attribute.Key,
                    attribute => new { Type = "String", Value = attribute.Value })
            })
        };
    }
}
