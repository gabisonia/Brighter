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
public class RedisRejectionBoundaryTests
{
    [Theory]
    [InlineData(false, "fallback-default")]
    [InlineData(false, "fallback-custom")]
    [InlineData(false, "no-channels")]
    [InlineData(false, "no-dlq")]
    [InlineData(false, "native-invalid")]
    [InlineData(false, "native-dlq")]
    [InlineData(true, "fallback-default")]
    [InlineData(true, "fallback-custom")]
    [InlineData(true, "no-channels")]
    [InlineData(true, "no-dlq")]
    [InlineData(true, "native-invalid")]
    [InlineData(true, "native-dlq")]
    public async Task When_redis_rejection_uses_fallback_or_encounters_native_failure_should_preserve_routing_and_release_the_consumer(
        bool useAsync, string mode)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var topic = new RoutingKey("brighter-rejection-boundary-" + token);
        var applicationTopic = new RoutingKey("brighter-rejection-boundary-app-" + token);
        var sourceQueue = new ChannelName("brighter-rejection-boundary-queue-" + token);
        var invalidTopic = new RoutingKey("brighter-rejection-boundary-invalid-" + token);
        var deadLetterTopic = new RoutingKey("brighter-rejection-boundary-dlq-" + token);
        var invalidQueue = "brighter-rejection-boundary-invalid-queue-" + token;
        var deadLetterQueue = "brighter-rejection-boundary-dlq-queue-" + token;
        var fallback = mode.StartsWith("fallback-", StringComparison.Ordinal);
        var nativeFailure = mode.StartsWith("native-", StringComparison.Ordinal);
        var hasInvalid = !fallback && mode != "no-channels";
        var hasDlq = fallback || nativeFailure;
        var shouldRoute = fallback || nativeFailure;
        var destination = mode == "native-invalid" ? invalidTopic : deadLetterTopic;
        var destinationQueue = mode == "native-invalid" ? invalidQueue : deadLetterQueue;
        var otherTopic = mode == "native-invalid" ? deadLetterTopic : invalidTopic;
        var otherQueue = mode == "native-invalid" ? deadLetterQueue : invalidQueue;
        var invalidWriter = new RedisRejectionWireWriter("I:", "custom");
        var dlqWriter = new RedisRejectionWireWriter("D:", "custom");
        var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
            new SubscriptionName("subscription-" + token), sourceQueue, topic,
            requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            deadLetterRoutingKey: hasDlq ? deadLetterTopic : null,
            invalidMessageRoutingKey: hasInvalid ? invalidTopic : null)
        {
            InvalidMessageWireMessageWriter = invalidWriter,
            DeadLetterWireMessageWriter = mode == "fallback-default" ? null : dlqWriter
        };
        var factory = new RedisMessageConsumerFactory(new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port
        });
        var sourceId = Id.Random();
        var body = "boundary-π-" + token;
        var original = new Message(new MessageHeader(sourceId, applicationTopic, MessageType.MT_EVENT), new MessageBody(body));
        using var originalEncoder = new RedisMessagePublisher();
        var originalWire = originalEncoder.Create(original);
        var nextId = Id.Random();
        var next = new Message(new MessageHeader(nextId, applicationTopic, MessageType.MT_EVENT), new MessageBody("next-λ-" + token));
        using var nextEncoder = new RedisMessagePublisher();
        var nextWire = nextEncoder.Create(next);
        var reason = new MessageRejectionReason(mode == "no-dlq" || mode == "native-dlq"
            ? RejectionReason.DeliveryError : RejectionReason.Unacceptable, "Redis rejection boundary.");
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
            if (nativeFailure)
                observer.SetValue(destination + ".nextid", "not-an-integer", TimeSpan.FromMinutes(10));
            var source = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(sourceId, source.Id);
            var rejected = false;

            //Act
            var rejectionError = await Record.ExceptionAsync(async () =>
            {
                rejected = useAsync
                    ? await consumer.RejectAsync(source, reason, timeout.Token)
                    : consumer.Reject(source, reason);
            });

            //Assert
            Assert.Null(rejectionError);
            Assert.True(rejected);
            if (fallback)
            {
                using var oracle = new RedisMessagePublisher();
                var expectedWire = mode == "fallback-default" ? oracle.Create(source) : "D:" + body;
                Assert.Equal(expectedWire, observer.GetValue(deadLetterTopic + ".1"));
                Assert.Equal("1", observer.GetValue(deadLetterTopic + ".nextid"));
                Assert.Equal(new[] { "1" }, observer.GetAllItemsFromList(deadLetterQueue));
            }
            else
            {
                Assert.Null(observer.GetValue(destination + ".1"));
                Assert.Empty(observer.GetAllItemsFromList(destinationQueue));
                Assert.Equal(nativeFailure ? "not-an-integer" : null, observer.GetValue(destination + ".nextid"));
            }
            Assert.Null(observer.GetValue(otherTopic + ".1"));
            Assert.Null(observer.GetValue(otherTopic + ".nextid"));
            Assert.Empty(observer.GetAllItemsFromList(otherQueue));
            Assert.Equal(mode == "native-invalid" ? 1 : 0, invalidWriter.Calls);
            Assert.Equal(mode == "fallback-custom" || mode == "native-dlq" ? 1 : 0, dlqWriter.Calls);
            Assert.Equal(shouldRoute ? destination : applicationTopic, source.Header.Topic);
            Assert.Equal(sourceId, source.Id);
            Assert.Equal(body, source.Body.Value);
            if (mode == "no-channels")
                Assert.False(source.Header.Bag.ContainsKey("originalTopic"));
            else
                Assert.Equal(applicationTopic.Value, source.Header.Bag["originalTopic"].ToString());
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
