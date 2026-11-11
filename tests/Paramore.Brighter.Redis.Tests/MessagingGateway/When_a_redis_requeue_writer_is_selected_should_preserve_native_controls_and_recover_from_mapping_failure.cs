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
public class RedisImmediateRequeueWireWriterTests
{
    [Theory]
    [InlineData(false, "legacy")]
    [InlineData(true, "legacy")]
    [InlineData(false, "custom")]
    [InlineData(true, "custom")]
    [InlineData(false, "composed")]
    [InlineData(true, "composed")]
    [InlineData(false, "throw")]
    [InlineData(true, "throw")]
    [InlineData(false, "null")]
    [InlineData(true, "null")]
    public async Task When_a_redis_requeue_writer_is_selected_should_preserve_native_controls_and_recover_from_mapping_failure(
        bool useAsync, string mode)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var topic = new RoutingKey("brighter-retry-" + token);
        var applicationTopic = new RoutingKey("brighter-retry-application-" + token);
        var queue = new ChannelName("brighter-retry-queue-" + token);
        var siblingQueue = "brighter-retry-sibling-" + token;
        var sourceId = Id.Random();
        var initial = new Message(new MessageHeader(sourceId, applicationTopic, MessageType.MT_EVENT),
            new MessageBody("original-π-" + token));
        initial.Header.Bag["source-marker"] = "marker-" + token;
        using var legacy = new RedisMessagePublisher();
        var originalWire = legacy.Create(initial);
        var writer = new RedisRecoverableRequeueWireWriter(mode);
        var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
            new SubscriptionName("subscription-" + token), queue, topic,
            requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
        {
            RequeueWireMessageWriter = mode == "legacy" ? null : writer
        };
        var factory = new RedisMessageConsumerFactory(new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port,
            MessageTimeToLive = TimeSpan.FromHours(1)
        });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using IRedisClient observer = new RedisClient(host, port);
        await using var consumer = useAsync
            ? (RedisMessageConsumer)factory.CreateAsync(subscription)
            : (RedisMessageConsumer)factory.Create(subscription);
        try
        {
            observer.SetValue(topic + ".1", originalWire, TimeSpan.FromMinutes(10));
            observer.SetValue(topic + ".nextid", "41", TimeSpan.FromMinutes(10));
            observer.AddItemToList(queue.Value, "1");
            observer.AddItemToSet(topic + ".queues", siblingQueue);
            var source = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(sourceId, source.Id);
            source.Body = new MessageBody("retry-λ-" + token);
            using var retrySerializer = new RedisMessagePublisher();
            var expectedWire = mode switch
            {
                "legacy" => retrySerializer.Create(source),
                "composed" => "C:" + retrySerializer.Create(source),
                _ => "R:" + source.Body.Value
            };
            var failing = mode == "throw" || mode == "null";

            //Act
            if (failing)
            {
                var error = await Record.ExceptionAsync(async () =>
                {
                    if (useAsync)
                        await consumer.RequeueAsync(source, TimeSpan.Zero, timeout.Token);
                    else
                        consumer.Requeue(source, TimeSpan.Zero);
                });

                //Assert
                Assert.NotNull(error);
                if (mode == "throw")
                    Assert.Same(writer.Failure, error);
                else
                    Assert.IsType<InvalidOperationException>(error);
                Assert.Empty(observer.GetAllItemsFromList(queue.Value));
                Assert.Equal(originalWire, observer.GetValue(topic + ".1"));
                Assert.Equal("41", observer.GetValue(topic + ".nextid"));
                Assert.Empty(observer.GetAllItemsFromList(siblingQueue));
                Assert.Equal(1, writer.Calls);
                var blocked = await Record.ExceptionAsync(async () =>
                {
                    if (useAsync)
                        await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token);
                    else
                        consumer.Receive(TimeSpan.FromSeconds(1));
                });
                Assert.IsType<ChannelFailureException>(blocked);
            }

            var requeued = useAsync
                ? await consumer.RequeueAsync(source, TimeSpan.Zero, timeout.Token)
                : consumer.Requeue(source, TimeSpan.Zero);

            //Assert
            Assert.True(requeued);
            Assert.Equal(expectedWire, observer.GetValue(topic + ".1"));
            Assert.Equal(new[] { "1" }, observer.GetAllItemsFromList(queue.Value));
            Assert.Empty(observer.GetAllItemsFromList(siblingQueue));
            Assert.Equal("41", observer.GetValue(topic + ".nextid"));
            Assert.Null(observer.GetValue(topic + ".42"));
            Assert.Null(observer.GetValue(applicationTopic + ".1"));
            Assert.Null(observer.GetValue(applicationTopic + ".nextid"));
            Assert.InRange(observer.GetTimeToLive(topic + ".1")?.TotalSeconds ?? -1, 3000, 3600);
            Assert.Equal(sourceId, source.Id);
            Assert.Equal(applicationTopic, source.Header.Topic);
            Assert.Equal("retry-λ-" + token, source.Body.Value);
            Assert.Equal("marker-" + token, source.Header.Bag["source-marker"].ToString());
            Assert.Equal(mode == "legacy" ? 0 : failing ? 2 : 1, writer.Calls);

            Assert.Equal("1", observer.RemoveStartFromList(queue.Value));
            var nextId = Id.Random();
            var next = new Message(new MessageHeader(nextId, applicationTopic, MessageType.MT_EVENT),
                new MessageBody("next-μ-" + token));
            using var nextSerializer = new RedisMessagePublisher();
            observer.SetValue(topic + ".2", nextSerializer.Create(next), TimeSpan.FromMinutes(10));
            observer.AddItemToList(queue.Value, "2");
            Message[]? received = null;
            var recoveryError = await Record.ExceptionAsync(async () =>
            {
                received = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : consumer.Receive(TimeSpan.FromSeconds(1));
            });
            Assert.Null(recoveryError);
            var recovered = Assert.Single(received!);
            Assert.Equal(nextId, recovered.Id);
            Assert.Equal(next.Body.Value, recovered.Body.Value);
            if (useAsync)
                await consumer.AcknowledgeAsync(recovered, timeout.Token);
            else
                consumer.Acknowledge(recovered);
            Assert.Empty(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(expectedWire, observer.GetValue(topic + ".1"));
            Assert.Empty(observer.GetAllItemsFromList(siblingQueue));
        }
        finally
        {
            observer.Remove(topic + ".1");
            observer.Remove(topic + ".2");
            observer.Remove(topic + ".42");
            observer.Remove(topic + ".nextid");
            observer.Remove(topic + ".queues");
            observer.Remove(applicationTopic + ".1");
            observer.Remove(applicationTopic + ".nextid");
            observer.Remove(queue.Value);
            observer.Remove(siblingQueue);
        }
    }
}
