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
using Paramore.Brighter.Observability;
using Paramore.Brighter.Redis.Tests.TestDoubles;
using ServiceStack.Redis;
using Xunit;

namespace Paramore.Brighter.Redis.Tests.MessagingGateway;

[Trait("Category", "Redis")]
[Collection("RedisMessagingGateway")]
public class RedisWriterFailureRecoveryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_redis_writer_fails_should_preserve_the_error_and_recover_without_native_publication(
        bool useAsync, bool returnNull)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var topic = new RoutingKey("brighter-writer-recovery-" + token);
        var queue = "brighter-recovery-queue-" + token;
        var failedId = Id.Random();
        var failedBody = "failed-π-" + token;
        var failed = new Message(new MessageHeader(failedId, topic, MessageType.MT_EVENT), new MessageBody(failedBody));
        failed.Header.Bag["source-marker"] = "failed-marker-" + token;
        var good = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT), new MessageBody("good-λ-" + token));
        using var legacy = new RedisMessagePublisher();
        var expectedGood = legacy.Create(good);
        var writer = new RedisRecoverableWireWriter(returnNull);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using IRedisClient observer = new RedisClient(host, port);
        await using var producer = new RedisMessageProducer(new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port,
            MessageTimeToLive = TimeSpan.FromHours(1)
        }, new RedisMessagePublication { Topic = topic, WireMessageWriter = writer }, InstrumentationOptions.None);
        try
        {
            observer.AddItemToSet(topic + ".queues", queue);

            //Act
            var error = await Record.ExceptionAsync(async () =>
            {
                if (useAsync)
                    await producer.SendAsync(failed, timeout.Token);
                else
                    producer.Send(failed);
            });

            //Assert
            Assert.NotNull(error);
            if (returnNull)
                Assert.IsType<InvalidOperationException>(error);
            else
                Assert.Same(writer.Failure, error);
            Assert.Null(observer.GetValue(topic + ".nextid"));
            Assert.Null(observer.GetValue(topic + ".1"));
            Assert.Empty(observer.GetAllItemsFromList(queue));
            if (useAsync)
                await producer.SendAsync(good, timeout.Token);
            else
                producer.Send(good);
            Assert.Equal("1", observer.GetValue(topic + ".nextid"));
            Assert.Equal(expectedGood, observer.GetValue(topic + ".1"));
            Assert.Equal(new[] { "1" }, observer.GetAllItemsFromList(queue));
            Assert.Null(observer.GetValue(topic + ".2"));
            Assert.Equal(2, writer.Calls);
            Assert.Equal(failedId, failed.Id);
            Assert.Equal(failedBody, failed.Body.Value);
            Assert.Equal(topic, failed.Header.Topic);
            Assert.Single(failed.Header.Bag);
            Assert.Equal("failed-marker-" + token, failed.Header.Bag["source-marker"]);
        }
        finally
        {
            observer.Remove(topic + ".queues");
            observer.Remove(topic + ".nextid");
            observer.Remove(topic + ".1");
            observer.Remove(topic + ".2");
            observer.Remove(queue);
        }
    }
}
