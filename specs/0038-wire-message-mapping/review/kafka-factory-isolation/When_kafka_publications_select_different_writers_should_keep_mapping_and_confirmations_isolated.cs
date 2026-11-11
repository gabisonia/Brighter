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
public class KafkaPublicationIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_kafka_publications_select_different_writers_should_keep_mapping_and_confirmations_isolated(
        bool useAsync, bool customFirst)
    {
        //Arrange
        var topics = new[] { new RoutingKey($"default-writer-{Guid.NewGuid():N}"), new RoutingKey($"custom-writer-{Guid.NewGuid():N}") };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync(topics.Select(topic => new TopicSpecification
        {
            Name = topic.Value, NumPartitions = 1, ReplicationFactor = 1
        }));
        try
        {
            var writer = new KafkaRecoverableWireWriter(returnNull: false) { FailBadMessage = false };
            var publications = topics.Select(topic => new KafkaPublication
            {
                Topic = topic, MakeChannels = OnMissingChannel.Validate, NumPartitions = 1, ReplicationFactor = 1,
                MessageTimeoutMs = 10000, RequestTimeoutMs = 5000
            }).ToArray();
            publications[1].WireMessageWriter = writer;
            var factory = new KafkaMessageProducerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = "writer-isolation", BootStrapServers = ["localhost:9092"]
            }, customFirst ? publications.Reverse() : publications);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            using var registry = new ProducerRegistry(producers);
            var confirmations = new ConcurrentQueue<PublishConfirmationResult>();
            foreach (var producer in producers.Values)
            {
                if (useAsync)
                    Assert.IsAssignableFrom<ISupportPublishConfirmationAsync>(producer).OnMessagePublishedAsync += result =>
                    {
                        confirmations.Enqueue(result);
                        return Task.CompletedTask;
                    };
                else
                    Assert.IsAssignableFrom<ISupportPublishConfirmation>(producer).OnMessagePublished += confirmations.Enqueue;
            }
            byte[][] bodies = [[0, 255, 128, 1], [0, 255, 128, 2]];
            var messages = new[] { "default-id", "custom-id" }.Select((id, index) => new Message(
                new MessageHeader(new Id(id), topics[index], MessageType.MT_EVENT,
                    partitionKey: new Paramore.Brighter.PartitionKey("source-key")), new MessageBody(bodies[index]))).ToArray();

            //Act
            foreach (var message in messages)
            {
                var producer = producers.Values.Single(candidate => candidate.Publication.Topic == message.Header.Topic);
                if (useAsync) await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendAsync(message, timeout.Token);
                else Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(message);
            }
            registry.Dispose();

            //Assert
            Assert.Equal(1, writer.WriteCount);
            Assert.Equal(2, confirmations.Count);
            Assert.Equal(new[] { "custom-id", "default-id" }, confirmations.Select(result => result.MessageId.Value).OrderBy(id => id).ToArray());
            for (var index = 0; index < messages.Length; index++)
            {
                var source = messages[index];
                var confirmation = Assert.Single(confirmations, result => result.MessageId == source.Id);
                Assert.True(confirmation.Success);
                Assert.Equal(topics[index], confirmation.Topic);
                using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
                {
                    BootstrapServers = "localhost:9092", GroupId = Guid.NewGuid().ToString(),
                    EnableAutoCommit = false, AutoOffsetReset = AutoOffsetReset.Earliest
                }).Build();
                var partition = new TopicPartition(topics[index].Value, new Partition(0));
                observer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));
                Assert.Equal(1L, observer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High.Value);
                var native = observer.Consume(timeout.Token);
                Assert.Equal(index == 0 ? "source-key" : "mapped-custom-id", native.Message.Key);
                Assert.Equal(index == 0 ? "default-id" : "wire-custom-id",
                    Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
                Assert.Equal(index == 0 ? bodies[index] : Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(bodies[index])), native.Message.Value);
                Assert.Equal(new Id(index == 0 ? "default-id" : "custom-id"), source.Id);
                Assert.Equal(topics[index], source.Header.Topic);
                Assert.Equal(new Paramore.Brighter.PartitionKey("source-key"), source.Header.PartitionKey);
                Assert.Equal(bodies[index], source.Body.Memory.ToArray());
            }
        }
        finally
        {
            await admin.DeleteTopicsAsync(topics.Select(topic => topic.Value));
        }
    }
}
