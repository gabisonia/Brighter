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
using Paramore.Brighter.AWS.Tests.Helpers;
using Paramore.Brighter.AWS.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Xunit;

namespace Paramore.Brighter.AWS.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsNullWireReaderResultTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_a_wire_reader_returns_null_should_preserve_rejection_and_native_payload(bool useAsync)
    {
        //Arrange
        const string body = "invalid legacy payload";
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Wire-Reader-Null-{Guid.NewGuid():N}");
        var invalidQueueName = reaper.TrackQueue($"Wire-Reader-Invalid-{Guid.NewGuid():N}");
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
            var invalidQueue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = invalidQueueName
            }, timeout.Token);
            var sent = await client.SendMessageAsync(new SendMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                MessageBody = body
            }, timeout.Token);

            var subscription = new SqsSubscription(
                new SubscriptionName(queueName),
                new ChannelName(queue.QueueUrl),
                ChannelType.PointToPoint,
                topic,
                requestType: typeof(Command),
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                findQueueBy: QueueFindBy.Url,
                invalidMessageRoutingKey: new RoutingKey(invalidQueueName),
                makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = new NullSqsWireMessageReader()
            };
            var factory = new SqsMessageConsumerFactory(connection);
            consumer = useAsync
                ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));

            //Act
            Message[] messages = [];
            var receiveException = await Record.ExceptionAsync(async () =>
            {
                messages = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
            });

            //Assert
            Assert.Null(receiveException);
            var message = Assert.Single(messages);
            Assert.Equal(MessageType.MT_UNACCEPTABLE, message.Header.MessageType);
            Assert.True(message.Header.Bag.TryGetValue("ReceiptHandle", out var receiptHandle));
            Assert.False(string.IsNullOrEmpty(Assert.IsType<string>(receiptHandle)));
            Assert.Equal(0, message.Header.HandledCount);
            Assert.Equal(body, message.Body.Value);
            Assert.Equal(sent.MessageId, message.Id.Value);

            //Act
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "Invalid legacy payload");
            var rejected = useAsync
                ? await consumer.RejectAsync(message, reason, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(message, reason);

            var forwarded = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = invalidQueue.QueueUrl,
                WaitTimeSeconds = 5,
                MaxNumberOfMessages = 1,
                MessageAttributeNames = ["All"]
            }, timeout.Token);

            await Task.Delay(TimeSpan.FromSeconds(2), timeout.Token);
            var remaining = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                WaitTimeSeconds = 1,
                MaxNumberOfMessages = 1
            }, timeout.Token);

            //Assert
            Assert.True(rejected);
            var invalidMessage = Assert.Single(forwarded.Messages ?? []);
            Assert.Equal(body, invalidMessage.Body);
            Assert.Equal(sent.MessageId, invalidMessage.MessageAttributes[HeaderNames.Id].StringValue);
            Assert.Empty(remaining.Messages ?? []);
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
