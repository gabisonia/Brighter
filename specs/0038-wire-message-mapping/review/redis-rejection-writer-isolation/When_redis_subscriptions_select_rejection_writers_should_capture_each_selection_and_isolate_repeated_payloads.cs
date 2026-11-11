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
using Paramore.Brighter.MessagingGateway.Redis;
using Paramore.Brighter.Redis.Tests.TestDoubles;
using ServiceStack.Redis;
using Xunit;

namespace Paramore.Brighter.Redis.Tests.MessagingGateway;

[Trait("Category", "Redis")]
[Collection("RedisMessagingGateway")]
public class RedisRejectionWriterIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_redis_subscriptions_select_rejection_writers_should_capture_each_selection_and_isolate_repeated_payloads(
        bool useAsync, bool legacyFirst)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var order = legacyFirst ? new[] { "legacy", "custom", "composed" } : new[] { "custom", "composed", "legacy" };
        var factory = new RedisMessageConsumerFactory(new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port
        });
        var customInvalid = new RedisRejectionWireWriter("I-custom:" + token + ":", "custom");
        var customDlq = new RedisRejectionWireWriter("D-custom:" + token + ":", "custom");
        var composedInvalid = new RedisRejectionWireWriter("I-composed:" + token + ":", "composed");
        var composedDlq = new RedisRejectionWireWriter("D-composed:" + token + ":", "composed");
        var decoy = new RedisRejectionWireWriter("wrong:" + token + ":", "custom");
        var subscriptions = new List<Paramore.Brighter.MessagingGateway.Redis.RedisSubscription>();
        var consumers = new List<RedisMessageConsumer>();
        var ownedKeys = new List<string>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using IRedisClient observer = new RedisClient(host, port);
        try
        {
            foreach (var mode in order)
            {
                var topic = new RoutingKey("brighter-rejection-isolation-" + mode + "-" + token);
                var queue = new ChannelName("brighter-rejection-isolation-queue-" + mode + "-" + token);
                var invalid = new RoutingKey(topic + "-invalid");
                var dlq = new RoutingKey(topic + "-dlq");
                var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
                    new SubscriptionName("subscription-" + mode + "-" + token), queue, topic,
                    requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                    invalidMessageRoutingKey: invalid, deadLetterRoutingKey: dlq)
                {
                    InvalidMessageWireMessageWriter = mode == "legacy" ? null : mode == "custom" ? customInvalid : composedInvalid,
                    DeadLetterWireMessageWriter = mode == "legacy" ? null : mode == "custom" ? customDlq : composedDlq
                };
                subscriptions.Add(subscription);
                consumers.Add(useAsync
                    ? (RedisMessageConsumer)factory.CreateAsync(subscription)
                    : (RedisMessageConsumer)factory.Create(subscription));
                ownedKeys.AddRange(new[] { topic + ".1", topic + ".2", topic + ".3", topic + ".4", topic + ".queues", topic + ".nextid", queue.Value });
                foreach (var route in new[] { invalid, dlq })
                    ownedKeys.AddRange(new[] { route + ".1", route + ".2", route + ".3", route + ".nextid", route + ".queues", route + "-queue" });
            }
            foreach (var subscription in subscriptions)
            {
                subscription.InvalidMessageWireMessageWriter = decoy;
                subscription.DeadLetterWireMessageWriter = decoy;
            }

            for (var endpoint = 0; endpoint < order.Length; endpoint++)
            {
                var mode = order[endpoint];
                var subscription = subscriptions[endpoint];
                var consumer = consumers[endpoint];
                var topic = subscription.RoutingKey;
                var applicationTopic = new RoutingKey(topic + "-application");
                var queue = subscription.ChannelName!;
                var invalidTopic = new RoutingKey(topic + "-invalid");
                var dlqTopic = new RoutingKey(topic + "-dlq");
                observer.AddItemToSet(invalidTopic + ".queues", invalidTopic + "-queue");
                observer.AddItemToSet(dlqTopic + ".queues", dlqTopic + "-queue");
                var ids = new[] { Id.Random(), Id.Random(), Id.Random(), Id.Random() };
                var originalWires = new List<string>();
                for (var index = 0; index < ids.Length; index++)
                {
                    var initial = new Message(new MessageHeader(ids[index], applicationTopic, MessageType.MT_EVENT),
                        new MessageBody("body-π-" + mode + "-" + index + "-" + token));
                    using var encoder = new RedisMessagePublisher();
                    var wire = encoder.Create(initial);
                    originalWires.Add(wire);
                    observer.SetValue(topic + "." + (index + 1), wire, TimeSpan.FromMinutes(10));
                    observer.AddItemToList(queue.Value, (index + 1).ToString());
                }

                for (var index = 0; index < ids.Length; index++)
                {
                    var source = Assert.Single(useAsync
                        ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                        : consumer.Receive(TimeSpan.FromSeconds(1)));
                    Assert.Equal(ids[index], source.Id);
                    var invalid = index % 2 == 0;
                    var destination = invalid ? invalidTopic : dlqTopic;
                    var other = invalid ? dlqTopic : invalidTopic;
                    var reason = new MessageRejectionReason(invalid ? RejectionReason.Unacceptable : RejectionReason.DeliveryError,
                        "Redis repeated rejection.");

                    //Act
                    var rejected = useAsync
                        ? await consumer.RejectAsync(source, reason, timeout.Token)
                        : consumer.Reject(source, reason);

                    //Assert
                    Assert.True(rejected);
                    using var oracle = new RedisMessagePublisher();
                    var expectedWire = mode == "legacy" ? oracle.Create(source)
                        : (invalid ? "I-" : "D-") + mode + ":" + token + ":"
                            + (mode == "composed" ? oracle.Create(source) : source.Body.Value);
                    var nativeId = (index / 2 + 1).ToString();
                    Assert.Equal(expectedWire, observer.GetValue(destination + "." + nativeId));
                    Assert.Equal(nativeId, observer.GetValue(destination + ".nextid"));
                    Assert.Equal(index < 2 ? new[] { "1" } : new[] { "1", "2" }, observer.GetAllItemsFromList(destination + "-queue"));
                    var otherCount = invalid ? index / 2 : index / 2 + 1;
                    Assert.Equal(otherCount == 0 ? null : otherCount.ToString(), observer.GetValue(other + ".nextid"));
                    Assert.Equal(otherCount == 0 ? Array.Empty<string>() : otherCount == 1 ? new[] { "1" } : new[] { "1", "2" },
                        observer.GetAllItemsFromList(other + "-queue"));
                    Assert.Null(observer.GetValue(destination + ".3"));
                    Assert.Equal(destination, source.Header.Topic);
                    Assert.Equal(applicationTopic.Value, source.Header.Bag["originalTopic"].ToString());
                    Assert.Equal(reason.RejectionReason.ToString(), source.Header.Bag["rejectionReason"].ToString());
                    Assert.Equal(ids[index], source.Id);
                    Assert.Equal("body-π-" + mode + "-" + index + "-" + token, source.Body.Value);
                    var remaining = new List<string>();
                    for (var next = index + 2; next <= ids.Length; next++)
                        remaining.Add(next.ToString());
                    Assert.Equal(remaining, observer.GetAllItemsFromList(queue.Value));
                    Assert.Equal(originalWires[index], observer.GetValue(topic + "." + (index + 1)));
                }
                Assert.Empty(useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : consumer.Receive(TimeSpan.FromSeconds(1)));
            }
            Assert.Equal(2, customInvalid.Calls);
            Assert.Equal(2, customDlq.Calls);
            Assert.Equal(2, composedInvalid.Calls);
            Assert.Equal(2, composedDlq.Calls);
            Assert.Equal(0, decoy.Calls);
        }
        finally
        {
            foreach (var consumer in consumers)
                await consumer.DisposeAsync();
            foreach (var key in ownedKeys)
                observer.Remove(key);
        }
    }
}
