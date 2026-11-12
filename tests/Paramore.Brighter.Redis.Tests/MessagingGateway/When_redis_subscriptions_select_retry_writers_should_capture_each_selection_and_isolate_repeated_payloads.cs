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
public class RedisRequeueWriterIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_redis_subscriptions_select_retry_writers_should_capture_each_selection_and_isolate_repeated_payloads(
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
        var custom = new RedisEndpointRequeueWireWriter("A:" + token + ":", false);
        var composed = new RedisEndpointRequeueWireWriter("B:" + token + ":", true);
        var decoy = new RedisEndpointRequeueWireWriter("wrong:" + token + ":", false);
        var subscriptions = new List<Paramore.Brighter.MessagingGateway.Redis.RedisSubscription>();
        var consumers = new List<RedisMessageConsumer>();
        var ownedKeys = new List<string>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using IRedisClient observer = new RedisClient(host, port);
        try
        {
            foreach (var mode in order)
            {
                var topic = new RoutingKey("brighter-retry-isolation-" + mode + "-" + token);
                var queue = new ChannelName("brighter-retry-isolation-queue-" + mode + "-" + token);
                var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
                    new SubscriptionName("subscription-" + mode + "-" + token), queue, topic,
                    requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
                {
                    RequeueWireMessageWriter = mode == "legacy" ? null : mode == "custom" ? custom : composed
                };
                subscriptions.Add(subscription);
                consumers.Add(useAsync
                    ? (RedisMessageConsumer)factory.CreateAsync(subscription)
                    : (RedisMessageConsumer)factory.Create(subscription));
                ownedKeys.AddRange(new[] { topic + ".1", topic + ".2", topic + ".queues", topic + ".nextid", queue.Value });
            }
            foreach (var subscription in subscriptions)
                subscription.RequeueWireMessageWriter = decoy;

            for (var endpoint = 0; endpoint < order.Length; endpoint++)
            {
                var mode = order[endpoint];
                var subscription = subscriptions[endpoint];
                var consumer = consumers[endpoint];
                var topic = subscription.RoutingKey;
                var queue = subscription.ChannelName!;
                var ids = new[] { Id.Random(), Id.Random() };
                for (var index = 0; index < ids.Length; index++)
                {
                    var initial = new Message(new MessageHeader(ids[index], topic, MessageType.MT_EVENT),
                        new MessageBody("initial-" + mode + "-" + index + "-" + token));
                    using var encoder = new RedisMessagePublisher();
                    observer.SetValue(topic + "." + (index + 1), encoder.Create(initial), TimeSpan.FromMinutes(10));
                    observer.AddItemToList(queue.Value, (index + 1).ToString());
                }

                for (var index = 0; index < ids.Length; index++)
                {
                    var source = Assert.Single(useAsync
                        ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                        : consumer.Receive(TimeSpan.FromSeconds(1)));
                    Assert.Equal(ids[index], source.Id);
                    source.Body = new MessageBody("retry-π-" + mode + "-" + index + "-" + token);
                    using var oracle = new RedisMessagePublisher();
                    var expectedWire = mode switch
                    {
                        "custom" => "A:" + token + ":" + source.Body.Value,
                        "composed" => "B:" + token + ":" + oracle.Create(source),
                        _ => oracle.Create(source)
                    };

                    //Act
                    var requeued = useAsync
                        ? await consumer.RequeueAsync(source, TimeSpan.Zero, timeout.Token)
                        : consumer.Requeue(source, TimeSpan.Zero);

                    //Assert
                    Assert.True(requeued);
                    Assert.Equal(expectedWire, observer.GetValue(topic + "." + (index + 1)));
                    Assert.Equal(index == 0 ? new[] { "2", "1" } : new[] { "2" }, observer.GetAllItemsFromList(queue.Value));
                    Assert.Equal((index + 1).ToString(), observer.RemoveEndFromList(queue.Value));
                    Assert.Null(observer.GetValue(topic + ".nextid"));
                    Assert.Equal(ids[index], source.Id);
                    Assert.Equal(topic, source.Header.Topic);
                    Assert.Equal("retry-π-" + mode + "-" + index + "-" + token, source.Body.Value);
                }
                Assert.Empty(useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : consumer.Receive(TimeSpan.FromSeconds(1)));
            }
            Assert.Equal(2, custom.Calls);
            Assert.Equal(2, composed.Calls);
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
