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
public class RedisPublicationWireWriterTests
{
    [Theory]
    [InlineData(false, "legacy")]
    [InlineData(false, "custom")]
    [InlineData(false, "composed")]
    [InlineData(true, "legacy")]
    [InlineData(true, "custom")]
    [InlineData(true, "composed")]
    public async Task When_sending_with_a_redis_wire_writer_should_preserve_payload_and_native_delivery_controls(
        bool useAsync, string mode)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var topic = new RoutingKey("brighter-source-" + token);
        var publicationTopic = new RoutingKey("brighter-publication-" + token);
        var queueA = "brighter-queue-a-" + token;
        var queueB = "brighter-queue-b-" + token;
        var decoyQueue = "brighter-decoy-" + token;
        var ids = new[] { Id.Random(), Id.Random() };
        var bodies = new[] { "first-π-" + token, "second-λ-" + token };
        var messages = new[]
        {
            new Message(new MessageHeader(ids[0], topic, MessageType.MT_EVENT), new MessageBody(bodies[0])),
            new Message(new MessageHeader(ids[1], topic, MessageType.MT_EVENT), new MessageBody(bodies[1]))
        };
        var expected = new string[2];
        for (var i = 0; i < messages.Length; i++)
        {
            messages[i].Header.Bag["source-marker"] = "marker-" + i + "-" + token;
            using var legacy = new RedisMessagePublisher();
            expected[i] = mode switch
            {
                "custom" => "W:" + bodies[i],
                "composed" => "C:" + legacy.Create(messages[i]),
                _ => legacy.Create(messages[i])
            };
        }
        var writer = new RedisPublicationWireWriter(mode);
        var publication = new RedisMessagePublication
        {
            Topic = publicationTopic,
            WireMessageWriter = mode == "legacy" ? null : writer
        };
        var configuration = new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port,
            MessageTimeToLive = TimeSpan.FromHours(1)
        };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using IRedisClient observer = new RedisClient(host, port);
        var ownedKeys = new[]
        {
            topic + ".queues", topic + ".nextid", topic + ".1", topic + ".2", queueA, queueB,
            publicationTopic + ".queues", publicationTopic + ".nextid",
            publicationTopic + ".1", publicationTopic + ".2", decoyQueue
        };
        try
        {
            observer.AddItemToSet(topic + ".queues", queueA);
            observer.AddItemToSet(topic + ".queues", queueB);
            observer.AddItemToSet(publicationTopic + ".queues", decoyQueue);
            await using var producer = new RedisMessageProducer(configuration, publication, InstrumentationOptions.None);

            //Act
            foreach (var message in messages)
            {
                if (useAsync)
                    await producer.SendAsync(message, timeout.Token);
                else
                    producer.Send(message);
            }

            //Assert
            Assert.Equal("2", observer.GetValue(topic + ".nextid"));
            Assert.Equal(expected[0], observer.GetValue(topic + ".1"));
            Assert.Equal(expected[1], observer.GetValue(topic + ".2"));
            Assert.Equal(new[] { "1", "2" }, observer.GetAllItemsFromList(queueA));
            Assert.Equal(new[] { "1", "2" }, observer.GetAllItemsFromList(queueB));
            Assert.Empty(observer.GetAllItemsFromList(decoyQueue));
            Assert.Null(observer.GetValue(publicationTopic + ".nextid"));
            Assert.InRange(observer.GetTimeToLive(topic + ".1")?.TotalSeconds ?? -1, 3000, 3600);
            Assert.InRange(observer.GetTimeToLive(topic + ".2")?.TotalSeconds ?? -1, 3000, 3600);
            Assert.Equal(mode == "legacy" ? 0 : 2, writer.Calls);
            for (var i = 0; i < messages.Length; i++)
            {
                Assert.Equal(ids[i], messages[i].Id);
                Assert.Equal(bodies[i], messages[i].Body.Value);
                Assert.Equal(topic, messages[i].Header.Topic);
                Assert.Single(messages[i].Header.Bag);
                Assert.Equal("marker-" + i + "-" + token, messages[i].Header.Bag["source-marker"]);
            }
        }
        finally
        {
            foreach (var key in ownedKeys)
                observer.Remove(key);
        }
    }
}
