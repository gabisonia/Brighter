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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.Redis;
using Paramore.Brighter.Redis.Tests.TestDoubles;
using ServiceStack.Redis;
using Xunit;

namespace Paramore.Brighter.Redis.Tests.MessagingGateway;

[Trait("Category", "Redis")]
[Collection("RedisMessagingGateway")]
public class RedisReaderErrorBoundaryTests
{
    [Theory]
    [InlineData(false, "legacy-json")]
    [InlineData(true, "legacy-json")]
    [InlineData(false, "configured-json")]
    [InlineData(true, "configured-json")]
    [InlineData(false, "legacy-sentinel")]
    [InlineData(true, "legacy-sentinel")]
    [InlineData(false, "native-command")]
    [InlineData(true, "native-command")]
    public async Task When_a_redis_read_fails_should_preserve_legacy_and_native_error_boundaries(
        bool useAsync, string mode)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var topic = new RoutingKey("brighter-reader-boundary-" + token);
        var queue = new ChannelName("brighter-reader-boundary-queue-" + token);
        var badWire = mode == "legacy-sentinel" ? "not-an-envelope-π-" + token : "<HEADER\n{\nHEADER/>\n<BODY\nbad\nBODY/>";
        var goodId = Id.Random();
        var goodBody = "recovered-λ-" + token;
        var applicationTopic = new RoutingKey("application-" + token);
        var good = new Message(new MessageHeader(goodId, applicationTopic, MessageType.MT_EVENT), new MessageBody(goodBody));
        using var legacy = new RedisMessagePublisher();
        var goodWire = legacy.Create(good);
        var reader = new RedisRecordingDefaultWireReader();
        var configured = mode == "configured-json" || mode == "native-command";
        var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
            new SubscriptionName("subscription-" + token), queue, topic,
            requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
        {
            WireMessageReader = configured ? reader : null
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
            observer.SetValue(topic + ".1", badWire, TimeSpan.FromMinutes(10));
            observer.SetValue(topic + ".2", goodWire, TimeSpan.FromMinutes(10));
            if (mode == "native-command")
                observer.SetValue(queue.Value, "wrong-type-" + token, TimeSpan.FromMinutes(10));
            else
            {
                observer.AddItemToList(queue.Value, "1");
                observer.AddItemToList(queue.Value, "2");
            }
            Message[]? received = null;

            //Act
            var error = await Record.ExceptionAsync(async () =>
            {
                received = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : consumer.Receive(TimeSpan.FromSeconds(1));
            });

            //Assert
            switch (mode)
            {
                case "legacy-json":
                    Assert.IsAssignableFrom<JsonException>(error);
                    Assert.Null(received);
                    Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(queue.Value));
                    break;
                case "configured-json":
                    Assert.Null(error);
                    var failed = Assert.Single(received!);
#pragma warning disable CS0618 // Verify the legacy invalid-message control signal.
                    Assert.Equal(MessageType.MT_UNACCEPTABLE, failed.Header.MessageType);
#pragma warning restore CS0618
                    Assert.Equal(new Id("1"), failed.Id);
                    Assert.Equal(topic, failed.Header.Topic);
                    Assert.Equal(badWire, failed.Body.Value);
                    Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(queue.Value));
                    break;
                case "legacy-sentinel":
                    Assert.Null(error);
                    var sentinel = Assert.Single(received!);
                    Assert.True(sentinel.IsEmpty);
                    Assert.Equal(string.Empty, sentinel.Body.Value);
                    Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(queue.Value));
                    break;
                case "native-command":
                    var nativeError = Assert.IsType<ChannelFailureException>(error);
                    Assert.IsAssignableFrom<RedisException>(nativeError.InnerException);
                    Assert.Contains("WRONGTYPE", nativeError.InnerException!.Message);
                    Assert.Null(received);
                    Assert.Empty(reader.Inputs);
                    Assert.Equal("wrong-type-" + token, observer.GetValue(queue.Value));
                    observer.Remove(queue.Value);
                    observer.AddItemToList(queue.Value, "2");
                    break;
            }

            var recovered = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(goodId, recovered.Id);
            Assert.Equal(goodBody, recovered.Body.Value);
            Assert.Equal(applicationTopic, recovered.Header.Topic);
            if (useAsync)
                await consumer.AcknowledgeAsync(recovered, timeout.Token);
            else
                consumer.Acknowledge(recovered);
            Assert.Empty(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Empty(observer.GetAllItemsFromList(queue.Value));
            Assert.Equal(badWire, observer.GetValue(topic + ".1"));
            Assert.Equal(goodWire, observer.GetValue(topic + ".2"));
            if (mode == "configured-json")
                Assert.Equal(new[] { badWire, goodWire }, reader.Inputs);
            else if (mode == "native-command")
                Assert.Equal(new[] { goodWire }, reader.Inputs);
            else
                Assert.Empty(reader.Inputs);
        }
        finally
        {
            observer.Remove(topic + ".1");
            observer.Remove(topic + ".2");
            observer.Remove(topic + ".queues");
            observer.Remove(queue.Value);
        }
    }
}
