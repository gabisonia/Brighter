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
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.Redis;
using Paramore.Brighter.Redis.Tests.TestDoubles;
using ServiceStack.Redis;
using Xunit;

namespace Paramore.Brighter.Redis.Tests.MessagingGateway;

[Trait("Category", "Redis")]
[Collection("RedisMessagingGateway")]
public class RedisSubscriptionWireReaderTests
{
    [Theory]
    [InlineData(false, "legacy")]
    [InlineData(false, "custom")]
    [InlineData(false, "composed")]
    [InlineData(true, "legacy")]
    [InlineData(true, "custom")]
    [InlineData(true, "composed")]
    public async Task When_receiving_with_a_redis_wire_reader_should_preserve_mapping_and_acknowledgement(
        bool useAsync, string mode)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var transportTopic = new RoutingKey("brighter-transport-" + token);
        var applicationTopic = new RoutingKey("brighter-application-" + token);
        var queue = new ChannelName("brighter-reader-" + token);
        var ids = new[] { Id.Random(), Id.Random() };
        var bodies = new[] { "first-π-" + token, "second-λ-" + token };
        var markers = new[] { "marker-first-" + token, "marker-second-" + token };
        var wire = new string[2];
        for (var i = 0; i < wire.Length; i++)
        {
            var message = new Message(new MessageHeader(ids[i], applicationTopic, MessageType.MT_EVENT),
                new MessageBody(bodies[i]));
            message.Header.Bag["source-marker"] = markers[i];
            using var legacy = new RedisMessagePublisher();
            wire[i] = mode switch
            {
                "custom" => string.Join("|", ids[i].Value, applicationTopic.Value, bodies[i], markers[i]),
                "composed" => "C:" + legacy.Create(message),
                _ => legacy.Create(message)
            };
        }
        var reader = new RedisSubscriptionWireReader(mode);
        var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
            new SubscriptionName("subscription-" + token), queue, transportTopic,
            requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
        {
            WireMessageReader = mode == "legacy" ? null : reader
        };
        var factory = new RedisMessageConsumerFactory(new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port
        });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using IRedisClient observer = new RedisClient(host, port);
        await using var consumer = useAsync
            ? (RedisMessageConsumer)factory.CreateAsync(subscription)
            : (RedisMessageConsumer)factory.Create(subscription);
        try
        {
            observer.SetValue(transportTopic + ".1", wire[0], TimeSpan.FromMinutes(10));
            observer.SetValue(transportTopic + ".2", wire[1], TimeSpan.FromMinutes(10));
            observer.AddItemToList(queue.Value, "1");
            observer.AddItemToList(queue.Value, "2");

            //Act
            var first = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));

            //Assert
            Assert.Equal(ids[0], first.Id);
            Assert.Equal(bodies[0], first.Body.Value);
            Assert.Equal(applicationTopic, first.Header.Topic);
            Assert.Equal(markers[0], first.Header.Bag["source-marker"].ToString());
            Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(queue.Value));
            var blocked = await Record.ExceptionAsync(async () =>
            {
                if (useAsync)
                    await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token);
                else
                    consumer.Receive(TimeSpan.FromSeconds(1));
            });
            Assert.IsType<ChannelFailureException>(blocked);
            Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(queue.Value));
            if (useAsync)
                await consumer.AcknowledgeAsync(first, timeout.Token);
            else
                consumer.Acknowledge(first);
            var second = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(ids[1], second.Id);
            Assert.Equal(bodies[1], second.Body.Value);
            Assert.Equal(applicationTopic, second.Header.Topic);
            Assert.Equal(markers[1], second.Header.Bag["source-marker"].ToString());
            Assert.Empty(observer.GetAllItemsFromList(queue.Value));
            if (useAsync)
                await consumer.AcknowledgeAsync(second, timeout.Token);
            else
                consumer.Acknowledge(second);
            Assert.Empty(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(wire[0], observer.GetValue(transportTopic + ".1"));
            Assert.Equal(wire[1], observer.GetValue(transportTopic + ".2"));
            Assert.Equal(mode == "legacy" ? Array.Empty<string>() : wire, reader.Inputs);
        }
        finally
        {
            observer.Remove(transportTopic + ".1");
            observer.Remove(transportTopic + ".2");
            observer.Remove(transportTopic + ".queues");
            observer.Remove(queue.Value);
        }
    }
}
