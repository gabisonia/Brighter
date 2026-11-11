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
public class RedisReaderFailureRecoveryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_redis_reader_fails_should_preserve_the_native_content_for_rejection_and_continue(
        bool useAsync, bool returnNull)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var sourceTopic = new RoutingKey("brighter-reader-failure-" + token);
        var applicationTopic = new RoutingKey("brighter-application-" + token);
        var invalidTopic = new RoutingKey("brighter-invalid-" + token);
        var sourceQueue = new ChannelName("brighter-source-queue-" + token);
        var invalidQueue = "brighter-invalid-queue-" + token;
        var badWire = "unreadable-π-" + token;
        var goodId = Id.Random();
        var goodBody = "good-λ-" + token;
        var good = new Message(new MessageHeader(goodId, applicationTopic, MessageType.MT_EVENT), new MessageBody(goodBody));
        good.Header.Bag["source-marker"] = "good-marker-" + token;
        using var legacy = new RedisMessagePublisher();
        var goodWire = legacy.Create(good);
        var reader = new RedisRecoverableWireReader(returnNull);
        var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
            new SubscriptionName("subscription-" + token), sourceQueue, sourceTopic,
            requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            invalidMessageRoutingKey: invalidTopic)
        {
            WireMessageReader = reader
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
            observer.SetValue(sourceTopic + ".1", badWire, TimeSpan.FromMinutes(10));
            observer.SetValue(sourceTopic + ".2", goodWire, TimeSpan.FromMinutes(10));
            observer.AddItemToList(sourceQueue.Value, "1");
            observer.AddItemToList(sourceQueue.Value, "2");
            observer.AddItemToSet(invalidTopic + ".queues", invalidQueue);
            Message[]? received = null;

            //Act
            var receiveError = await Record.ExceptionAsync(async () =>
            {
                received = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : consumer.Receive(TimeSpan.FromSeconds(1));
            });

            //Assert
            Assert.Null(receiveError);
            var failed = Assert.Single(received!);
#pragma warning disable CS0618 // Verify the legacy invalid-message control signal.
            Assert.Equal(MessageType.MT_UNACCEPTABLE, failed.Header.MessageType);
#pragma warning restore CS0618
            Assert.Equal(new Id("1"), failed.Id);
            Assert.Equal(sourceTopic, failed.Header.Topic);
            Assert.Equal(badWire, failed.Body.Value);
            Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(sourceQueue.Value));
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "Configured Redis reader failed.");
            var rejected = useAsync
                ? await consumer.RejectAsync(failed, reason, timeout.Token)
                : consumer.Reject(failed, reason);
            Assert.True(rejected);
            Assert.Equal("1", observer.GetValue(invalidTopic + ".nextid"));
            Assert.Equal(new[] { "1" }, observer.GetAllItemsFromList(invalidQueue));
            var forwarded = new DefaultRedisWireMessageReader().ReadMessage(observer.GetValue(invalidTopic + ".1"));
            Assert.Equal(new Id("1"), forwarded.Id);
            Assert.Equal(badWire, forwarded.Body.Value);
            Assert.Equal(invalidTopic, forwarded.Header.Topic);
            Assert.Equal(sourceTopic.Value, forwarded.Header.Bag["originalTopic"].ToString());
            Assert.Equal(RejectionReason.Unacceptable.ToString(), forwarded.Header.Bag["rejectionReason"].ToString());
            Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(sourceQueue.Value));
            var recovered = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(goodId, recovered.Id);
            Assert.Equal(goodBody, recovered.Body.Value);
            Assert.Equal(applicationTopic, recovered.Header.Topic);
            Assert.Equal("good-marker-" + token, recovered.Header.Bag["source-marker"].ToString());
            if (useAsync)
                await consumer.AcknowledgeAsync(recovered, timeout.Token);
            else
                consumer.Acknowledge(recovered);
            Assert.Empty(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Empty(observer.GetAllItemsFromList(sourceQueue.Value));
            Assert.Equal(badWire, observer.GetValue(sourceTopic + ".1"));
            Assert.Equal(goodWire, observer.GetValue(sourceTopic + ".2"));
            Assert.Equal(new[] { badWire, goodWire }, reader.Inputs);
        }
        finally
        {
            observer.Remove(sourceTopic + ".1");
            observer.Remove(sourceTopic + ".2");
            observer.Remove(sourceTopic + ".queues");
            observer.Remove(sourceQueue.Value);
            observer.Remove(invalidTopic + ".1");
            observer.Remove(invalidTopic + ".nextid");
            observer.Remove(invalidTopic + ".queues");
            observer.Remove(invalidQueue);
        }
    }
}
