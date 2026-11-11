#region Licence

/* The MIT License (MIT)
Copyright © 2026 Irakli Gabisonia

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
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Paramore.Brighter.Kafka.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.Kafka;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

[Trait("Category", "Kafka")]
[Collection("Kafka")]
public class KafkaWriterRecoveryTests
{
    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task When_a_kafka_writer_fails_should_surface_the_error_and_recover_publication_and_confirmations(
        bool useAsync, bool returnNull, bool badFirst)
    {
        //Arrange
        var topic = new RoutingKey($"writer-recovery-{Guid.NewGuid():N}");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic.Value, NumPartitions = 1, ReplicationFactor = 1 }]);
        try
        {
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = Guid.NewGuid().ToString(),
                EnableAutoCommit = false, AutoOffsetReset = AutoOffsetReset.Earliest
            }).Build();
            var partition = new TopicPartition(topic.Value, new Partition(0));
            observer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));
            var writer = new KafkaRecoverableWireWriter(returnNull);
            var publication = new KafkaPublication
            {
                Topic = topic, MakeChannels = OnMissingChannel.Validate, NumPartitions = 1, ReplicationFactor = 1,
                MessageTimeoutMs = 10000, RequestTimeoutMs = 5000, WireMessageWriter = writer
            };
            var producers = new KafkaMessageProducerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = "writer-recovery", BootStrapServers = ["localhost:9092"]
            }, [publication]).Create();
            using var registry = new ProducerRegistry(producers);
            var producer = Assert.Single(producers).Value;
            var confirmations = new ConcurrentQueue<PublishConfirmationResult>();
            if (useAsync)
                Assert.IsAssignableFrom<ISupportPublishConfirmationAsync>(producer).OnMessagePublishedAsync += result =>
                {
                    confirmations.Enqueue(result);
                    return Task.CompletedTask;
                };
            else
                Assert.IsAssignableFrom<ISupportPublishConfirmation>(producer).OnMessagePublished += confirmations.Enqueue;
            byte[] badBody = [0, 255, 128, 3];
            byte[] goodBody = [0, 255, 128, 4];
            var bad = new Message(new MessageHeader(new Id("bad-id"), topic, MessageType.MT_EVENT,
                partitionKey: new Paramore.Brighter.PartitionKey("source-key")), new MessageBody(badBody));
            var good = new Message(new MessageHeader(new Id("good-id"), topic, MessageType.MT_EVENT,
                partitionKey: new Paramore.Brighter.PartitionKey("source-key")), new MessageBody(goodBody));

            //Act
            if (!badFirst)
            {
                if (useAsync) await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendAsync(good, timeout.Token);
                else Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(good);
            }
            var mappingError = await Record.ExceptionAsync(async () =>
            {
                if (useAsync) await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendAsync(bad, timeout.Token);
                else Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(bad);
            });
            var failedBodySnapshot = bad.Body.Memory.ToArray();
            var recoveryError = await Record.ExceptionAsync(async () =>
            {
                if (badFirst)
                {
                    if (useAsync) await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendAsync(good, timeout.Token);
                    else Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(good);
                }
                writer.FailBadMessage = false;
                if (useAsync) await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendAsync(bad, timeout.Token);
                else Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(bad);
            });
            registry.Dispose();

            //Assert
            Assert.NotNull(mappingError);
            Assert.Null(recoveryError);
            Assert.Equal(badBody, failedBodySnapshot);
            Assert.Equal(3, writer.WriteCount);
            var successful = confirmations.Where(result => result.Success).ToArray();
            Assert.Equal(2, successful.Length);
            Assert.Equal(new[] { "bad-id", "good-id" }, successful.Select(result => result.MessageId.Value).OrderBy(id => id).ToArray());
            Assert.All(confirmations, result =>
            {
                Assert.Equal(topic, result.Topic);
                Assert.Contains(result.MessageId.Value, new[] { "bad-id", "good-id" });
            });
            var watermarks = observer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10));
            Assert.Equal(2L, watermarks.High.Value);
            var nativeGood = observer.Consume(timeout.Token);
            var nativeRecovered = observer.Consume(timeout.Token);
            Assert.Equal("mapped-good-id", nativeGood.Message.Key);
            Assert.Equal("wire-good-id", Encoding.UTF8.GetString(nativeGood.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(goodBody)), nativeGood.Message.Value);
            Assert.Equal("mapped-bad-id", nativeRecovered.Message.Key);
            Assert.Equal("wire-bad-id", Encoding.UTF8.GetString(nativeRecovered.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(badBody)), nativeRecovered.Message.Value);
            Assert.Equal(new Id("bad-id"), bad.Id);
            Assert.Equal(topic, bad.Header.Topic);
            Assert.Equal(new Paramore.Brighter.PartitionKey("source-key"), bad.Header.PartitionKey);
            Assert.Equal(badBody, bad.Body.Memory.ToArray());
            Assert.Equal(new Id("good-id"), good.Id);
            Assert.Equal(goodBody, good.Body.Memory.ToArray());
        }
        finally
        {
            await admin.DeleteTopicsAsync([topic.Value]);
        }
    }
}
