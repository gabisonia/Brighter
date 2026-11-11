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
using Paramore.Brighter.AWS.Tests.Helpers;
using Paramore.Brighter.AWS.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Xunit;

namespace Paramore.Brighter.AWS.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsWireReaderSubscriptionIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_subscriptions_select_different_wire_readers_should_keep_their_mapping_isolated(
        bool useAsync, bool customFirst)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var subscriptions = new List<SqsSubscription>();
        var consumers = new Dictionary<SqsSubscription, IAmAMessageConsumerAsync>();
        var received = new Dictionary<SqsSubscription, Message>();
        var customReader = new TenantHeaderSqsWireMessageReader(new DefaultSqsWireMessageReader());
        var factory = new SqsMessageConsumerFactory(connection);
        bool[] creationOrder = customFirst ? [true, false] : [false, true];

        try
        {
            foreach (var isCustom in creationOrder)
            {
                var name = reaper.TrackQueue($"Wire-Reader-Isolation-{Guid.NewGuid():N}");
                var queue = await client.CreateQueueAsync(new CreateQueueRequest
                {
                    QueueName = name,
                    Attributes = new() { ["VisibilityTimeout"] = "1" }
                }, timeout.Token);
                var kind = isCustom ? "custom" : "default";
                await client.SendMessageAsync(new SendMessageRequest
                {
                    QueueUrl = queue.QueueUrl,
                    MessageBody = $"{kind} payload",
                    MessageAttributes = new()
                    {
                        ["id"] = new() { DataType = "String", StringValue = $"{kind}-application" },
                        ["topic"] = new() { DataType = "String", StringValue = name },
                        ["message-type"] = new() { DataType = "String", StringValue = "MT_EVENT" },
                        ["bag"] = new()
                        {
                            DataType = "String",
                            StringValue = JsonSerializer.Serialize(new Dictionary<string, string>
                            {
                                ["tenant-id"] = $"{kind}-tenant", ["order_kind"] = kind
                            })
                        }
                    }
                }, timeout.Token);
                var subscription = new SqsSubscription(
                    new SubscriptionName(name),
                    new ChannelName(queue.QueueUrl),
                    ChannelType.PointToPoint,
                    new RoutingKey(name),
                    requestType: typeof(Command),
                    messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                    findQueueBy: QueueFindBy.Url,
                    makeChannels: OnMissingChannel.Validate);
                if (isCustom)
                    subscription.WireMessageReader = customReader;
                subscriptions.Add(subscription);
                consumers.Add(subscription, useAsync
                    ? factory.CreateAsync(subscription)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription)));
            }

            //Act
            foreach (var subscription in subscriptions)
            {
                var consumer = consumers[subscription];
                var messages = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
                var message = Assert.Single(messages);
                received.Add(subscription, message);
                if (useAsync)
                    await consumer.AcknowledgeAsync(message, timeout.Token);
                else
                    Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Acknowledge(message);
            }
            await Task.Delay(TimeSpan.FromSeconds(2), timeout.Token);

            //Assert
            foreach (var subscription in subscriptions)
            {
                var isCustom = subscription == subscriptions[customFirst ? 0 : 1];
                var kind = isCustom ? "custom" : "default";
                var message = received[subscription];
                Assert.Equal(isCustom, message.Header.Bag.ContainsKey("tenant_id"));
                Assert.Equal(!isCustom, message.Header.Bag.ContainsKey("tenant-id"));
                Assert.Equal($"{kind}-tenant", message.Header.Bag[isCustom ? "tenant_id" : "tenant-id"]);
                Assert.Equal(kind, message.Header.Bag["order_kind"]);
                Assert.Equal($"{kind}-application", message.Id.Value);
                Assert.Equal($"{kind} payload", message.Body.Value);
                Assert.Equal(subscription.RoutingKey, message.Header.Topic);
                Assert.Equal(MessageType.MT_EVENT, message.Header.MessageType);
                Assert.Equal(0, message.Header.HandledCount);
                Assert.True(message.Header.Bag.TryGetValue("ReceiptHandle", out var receiptHandle));
                Assert.False(string.IsNullOrEmpty(Assert.IsType<string>(receiptHandle)));
                if (isCustom)
                    Assert.Same(customReader, subscription.WireMessageReader);
                else
                    Assert.Null(subscription.WireMessageReader);
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
                foreach (var consumer in consumers.Values)
                    await consumer.DisposeAsync();
            }
            finally
            {
                await reaper.ReapAsync();
            }
        }
    }
}
