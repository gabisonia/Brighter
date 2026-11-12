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
public class RedisRejectionFallbackRecoveryTests
{
    [Theory]
    [InlineData(false, "throw")]
    [InlineData(false, "null")]
    [InlineData(true, "throw")]
    [InlineData(true, "null")]
    public async Task When_redis_rejection_falls_back_to_a_dlq_writer_should_preserve_the_source_on_mapping_failure_and_recover(
        bool useAsync, string mode)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var topic = new RoutingKey("brighter-rejection-fallback-source-" + token);
        var applicationTopic = new RoutingKey("brighter-rejection-fallback-app-" + token);
        var sourceQueue = new ChannelName("brighter-rejection-fallback-source-queue-" + token);
        var invalidTopic = new RoutingKey("brighter-rejection-fallback-invalid-" + token);
        var deadLetterTopic = new RoutingKey("brighter-rejection-fallback-dlq-" + token);
        var invalidQueue = "brighter-rejection-fallback-invalid-queue-" + token;
        var deadLetterQueue = "brighter-rejection-fallback-dlq-queue-" + token;
        var destination = deadLetterTopic;
        var destinationQueue = deadLetterQueue;
        var otherTopic = invalidTopic;
        var otherQueue = invalidQueue;
        var prefix = "dlq:";
        var selected = new RedisRejectionWireWriter(prefix, mode);
        var inactive = new RedisRejectionWireWriter("inactive:", "custom");
        var retry = new RedisRejectionWireWriter("retry:", "custom");
        var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
            new SubscriptionName("subscription-" + token), sourceQueue, topic,
            requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            deadLetterRoutingKey: deadLetterTopic, invalidMessageRoutingKey: null)
        {
            InvalidMessageWireMessageWriter = inactive,
            DeadLetterWireMessageWriter = selected,
            RequeueWireMessageWriter = retry
        };
        var factory = new RedisMessageConsumerFactory(new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port,
            MessageTimeToLive = TimeSpan.FromHours(1)
        });
        var sourceId = Id.Random();
        var body = "rejected-π-" + token;
        var original = new Message(new MessageHeader(sourceId, applicationTopic, MessageType.MT_EVENT), new MessageBody(body));
        original.Header.Bag["source-marker"] = "marker-" + token;
        using var originalEncoder = new RedisMessagePublisher();
        var originalWire = originalEncoder.Create(original);
        var nextId = Id.Random();
        var next = new Message(new MessageHeader(nextId, applicationTopic, MessageType.MT_EVENT), new MessageBody("next-λ-" + token));
        using var nextEncoder = new RedisMessagePublisher();
        var nextWire = nextEncoder.Create(next);
        var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "Fallback Redis rejection writer.");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using IRedisClient observer = new RedisClient(host, port);
        await using var consumer = useAsync
            ? (RedisMessageConsumer)factory.CreateAsync(subscription)
            : (RedisMessageConsumer)factory.Create(subscription);
        try
        {
            observer.SetValue(topic + ".1", originalWire, TimeSpan.FromMinutes(10));
            observer.SetValue(topic + ".2", nextWire, TimeSpan.FromMinutes(10));
            observer.AddItemToList(sourceQueue.Value, "1");
            observer.AddItemToList(sourceQueue.Value, "2");
            observer.AddItemToSet(invalidTopic + ".queues", invalidQueue);
            observer.AddItemToSet(deadLetterTopic + ".queues", deadLetterQueue);
            var source = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(sourceId, source.Id);

            //Act
            var error = await Record.ExceptionAsync(async () =>
            {
                if (useAsync)
                    await consumer.RejectAsync(source, reason, timeout.Token);
                else
                    consumer.Reject(source, reason);
            });

            //Assert
            Assert.NotNull(error);
            if (mode == "throw")
                Assert.Same(selected.Failure, error);
            else
                Assert.IsType<InvalidOperationException>(error);
            Assert.Equal(applicationTopic, source.Header.Topic);
            Assert.Equal(sourceId, source.Id);
            Assert.Equal(body, source.Body.Value);
            Assert.Equal("marker-" + token, source.Header.Bag["source-marker"].ToString());
            Assert.Null(observer.GetValue(destination + ".nextid"));
            Assert.Null(observer.GetValue(destination + ".1"));
            Assert.Null(observer.GetValue(otherTopic + ".nextid"));
            Assert.Null(observer.GetValue(otherTopic + ".1"));
            Assert.Empty(observer.GetAllItemsFromList(destinationQueue));
            Assert.Empty(observer.GetAllItemsFromList(otherQueue));
            Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(sourceQueue.Value));
            var blocked = await Record.ExceptionAsync(async () =>
            {
                if (useAsync)
                    await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token);
                else
                    consumer.Receive(TimeSpan.FromSeconds(1));
            });
            Assert.IsType<ChannelFailureException>(blocked);

            var rejected = useAsync
                ? await consumer.RejectAsync(source, reason, timeout.Token)
                : consumer.Reject(source, reason);

            //Assert
            Assert.True(rejected);
            Assert.Equal("1", observer.GetValue(destination + ".nextid"));
            Assert.Equal(new[] { "1" }, observer.GetAllItemsFromList(destinationQueue));
            var expectedWire = prefix + body;
            Assert.Equal(expectedWire, observer.GetValue(destination + ".1"));
            Assert.InRange(observer.GetTimeToLive(destination + ".1")?.TotalSeconds ?? -1, 3000, 3600);
            Assert.Null(observer.GetValue(destination + ".2"));
            Assert.Null(observer.GetValue(otherTopic + ".nextid"));
            Assert.Null(observer.GetValue(otherTopic + ".1"));
            Assert.Empty(observer.GetAllItemsFromList(otherQueue));
            Assert.Equal(destination, source.Header.Topic);
            Assert.Equal(sourceId, source.Id);
            Assert.Equal(body, source.Body.Value);
            Assert.Equal("marker-" + token, source.Header.Bag["source-marker"].ToString());
            Assert.Equal(applicationTopic.Value, source.Header.Bag["originalTopic"].ToString());
            Assert.Equal(reason.RejectionReason.ToString(), source.Header.Bag["rejectionReason"].ToString());
            Assert.Equal(reason.Description, source.Header.Bag["rejectionMessage"].ToString());
            Assert.True(DateTimeOffset.TryParse(source.Header.Bag["rejectionTimestamp"].ToString(), out _));
            Assert.Equal(2, selected.Calls);
            Assert.Equal(0, inactive.Calls);
            Assert.Equal(0, retry.Calls);
            Assert.Equal(destination, selected.LastTopic);
            Assert.Equal(applicationTopic.Value, selected.LastOriginalTopic);
            Assert.Equal(reason.RejectionReason.ToString(), selected.LastReason);
            Assert.Equal(reason.Description, selected.LastDescription);
            Assert.True(DateTimeOffset.TryParse(selected.LastTimestamp, out _));
            Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(sourceQueue.Value));
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
            Assert.Equal(originalWire, observer.GetValue(topic + ".1"));
            Assert.Equal(nextWire, observer.GetValue(topic + ".2"));
            Assert.Null(observer.GetValue(topic + ".nextid"));
        }
        finally
        {
            foreach (var route in new[] { topic, invalidTopic, deadLetterTopic })
            {
                observer.Remove(route + ".1");
                observer.Remove(route + ".2");
                observer.Remove(route + ".nextid");
                observer.Remove(route + ".queues");
            }
            observer.Remove(sourceQueue.Value);
            observer.Remove(invalidQueue);
            observer.Remove(deadLetterQueue);
        }
    }
}
