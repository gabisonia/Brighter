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
using Paramore.Brighter.AWS.V4.Tests.Helpers;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;
using Xunit;

namespace Paramore.Brighter.AWS.V4.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsMalformedRejectionCountTests
{
    [Theory]
    [InlineData(false, false, false, false)]
    [InlineData(false, false, false, true)]
    [InlineData(false, false, true, false)]
    [InlineData(false, false, true, true)]
    [InlineData(false, true, false, false)]
    [InlineData(false, true, false, true)]
    [InlineData(false, true, true, false)]
    [InlineData(false, true, true, true)]
    [InlineData(true, false, false, false)]
    [InlineData(true, false, false, true)]
    [InlineData(true, false, true, false)]
    [InlineData(true, false, true, true)]
    [InlineData(true, true, false, false)]
    [InlineData(true, true, false, true)]
    [InlineData(true, true, true, false)]
    [InlineData(true, true, true, true)]
    public async Task When_default_parsing_fails_should_preserve_rejection_metadata_and_count_precedence(
        bool useAsync, bool configureReader, bool rawMessageDelivery, bool routedCopy)
    {
        //Arrange
        const string APPLICATION_ID = "malformed-routed-order-17";
        const string PAYLOAD = "{\"orderId\":\"order-17\"}";
        const int STAMPED_COUNT = 7;
        var metadata = new Dictionary<string, string>
        {
            [RejectionMetadataKeyNames.RejectionReason] = "Unacceptable",
            [RejectionMetadataKeyNames.RejectionMessage] = "Original rejection description",
            [RejectionMetadataKeyNames.RejectionTimestamp] = "2026-10-01T12:00:00.0000000+00:00",
            [RejectionMetadataKeyNames.OriginalTopic] = "original-orders",
            [RejectionMetadataKeyNames.OriginalMessageType] = "MT_EVENT"
        };
        var bag = routedCopy ? new Dictionary<string, string>(metadata) : new Dictionary<string, string>();
        bag[HeaderNames.HandledCount] = "7";
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Malformed-Rejection-Count-{Guid.NewGuid():N}");
        var topic = new RoutingKey(queueName);
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        IAmAMessageConsumerAsync? consumer = null;

        try
        {
            var queue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new Dictionary<string, string> { ["VisibilityTimeout"] = "120" }
            }, timeout.Token);
            var attributes = new Dictionary<string, MessageAttributeValue>
            {
                ["id"] = new() { DataType = "String", StringValue = APPLICATION_ID },
                ["topic"] = new() { DataType = "String", StringValue = queueName },
                ["content-type"] = new() { DataType = "String", StringValue = "not-a-media-type" },
                ["handled-count"] = new() { DataType = "String", StringValue = "7" },
                ["bag"] = new() { DataType = "String", StringValue = JsonSerializer.Serialize(bag) }
            };
            var nativeBody = rawMessageDelivery ? PAYLOAD : JsonSerializer.Serialize(new
            {
                Type = "Notification",
                MessageId = "sns-notification-17",
                TopicArn = "arn:aws:sns:us-east-1:123456789012:orders",
                Message = PAYLOAD,
                MessageAttributes = attributes.ToDictionary(
                    entry => entry.Key,
                    entry => new { Type = entry.Value.DataType, Value = entry.Value.StringValue })
            });
            var sent = await client.SendMessageAsync(new SendMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                MessageBody = nativeBody,
                MessageAttributes = rawMessageDelivery ? attributes : new Dictionary<string, MessageAttributeValue>()
            }, timeout.Token);
            var subscription = new SqsSubscription(
                new SubscriptionName(queueName), new ChannelName(queue.QueueUrl),
                ChannelType.PointToPoint, topic, requestType: typeof(Command),
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                findQueueBy: QueueFindBy.Url,
                queueAttributes: new SqsAttributes(rawMessageDelivery: rawMessageDelivery),
                makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = configureReader ? new DefaultSqsWireMessageReader(rawMessageDelivery) : null
            };
            var factory = new SqsMessageConsumerFactory(connection);
            consumer = useAsync ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));

            //Act
            var first = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10)));

            //Assert
            Assert.Equal(MessageType.MT_UNACCEPTABLE, first.Header.MessageType);
            Assert.Equal(nativeBody, first.Body.Value);
            Assert.Equal(APPLICATION_ID, first.Id.Value);
            Assert.NotEqual(sent.MessageId, first.Id.Value);
            Assert.Equal(topic, first.Header.Topic);
            Assert.Equal(routedCopy ? STAMPED_COUNT : 0, first.Header.HandledCount);
            AssertMetadata(first, routedCopy, metadata);
            var firstReceipt = Assert.IsType<string>(first.Header.Bag["ReceiptHandle"]);
            Assert.False(string.IsNullOrEmpty(firstReceipt));

            //Act
            var requeued = useAsync
                ? await consumer.RequeueAsync(first, TimeSpan.Zero, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Requeue(first, TimeSpan.Zero);

            //Assert
            Assert.True(requeued);
            var second = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10)));
            Assert.Equal(MessageType.MT_UNACCEPTABLE, second.Header.MessageType);
            Assert.Equal(nativeBody, second.Body.Value);
            Assert.Equal(APPLICATION_ID, second.Id.Value);
            Assert.Equal(topic, second.Header.Topic);
            Assert.Equal(routedCopy ? STAMPED_COUNT : 1, second.Header.HandledCount);
            AssertMetadata(second, routedCopy, metadata);
            var secondReceipt = Assert.IsType<string>(second.Header.Bag["ReceiptHandle"]);
            Assert.False(string.IsNullOrEmpty(secondReceipt));
            Assert.NotEqual(firstReceipt, secondReceipt);
            Assert.Equal(firstReceipt, first.Header.Bag["ReceiptHandle"]);
            Assert.Equal(routedCopy ? STAMPED_COUNT : 0, first.Header.HandledCount);
            AssertMetadata(first, routedCopy, metadata);

            //Act
            await client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
            {
                QueueUrl = queue.QueueUrl, ReceiptHandle = secondReceipt, VisibilityTimeout = 1
            }, timeout.Token);
            if (useAsync)
                await consumer.AcknowledgeAsync(second, timeout.Token);
            else
                Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Acknowledge(second);
            await Task.Delay(TimeSpan.FromSeconds(2), timeout.Token);
            var remaining = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl, WaitTimeSeconds = 1, MaxNumberOfMessages = 1
            }, timeout.Token);

            //Assert
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

    private static void AssertMetadata(Message message, bool routedCopy, Dictionary<string, string> expected)
    {
        foreach (var entry in expected)
        {
            Assert.Equal(routedCopy, message.Header.Bag.ContainsKey(entry.Key));
            if (routedCopy)
            {
                if (entry.Key == RejectionMetadataKeyNames.RejectionTimestamp)
                    Assert.Equal(DateTimeOffset.Parse(entry.Value).UtcDateTime,
                        Assert.IsType<DateTime>(message.Header.Bag[entry.Key]).ToUniversalTime());
                else
                    Assert.Equal(entry.Value, message.Header.Bag[entry.Key]);
            }
        }
    }
}
