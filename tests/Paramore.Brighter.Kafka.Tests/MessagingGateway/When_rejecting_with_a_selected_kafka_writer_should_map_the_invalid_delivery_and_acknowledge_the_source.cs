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
using System.Diagnostics;
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
public class KafkaInvalidWireWriterTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_rejecting_with_a_selected_kafka_writer_should_map_the_invalid_delivery_and_acknowledge_the_source(
        bool useAsync, bool compose)
    {
        //Arrange
        var topic = new RoutingKey($"invalid-writer-{Guid.NewGuid():N}");
        var invalidTopic = new RoutingKey(topic.Value + "-invalid");
        var group = $"invalid-writer-group-{Guid.NewGuid():N}";
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync(new[] { topic, invalidTopic }.Select(route =>
            new TopicSpecification { Name = route.Value, NumPartitions = 1, ReplicationFactor = 1 }));
        IAmAMessageConsumerSync? syncConsumer = null;
        IAmAMessageConsumerAsync? asyncConsumer = null;
        try
        {
            byte[] body = [0, 255, 128, 3];
            var original = new Message(new MessageHeader(new Id("source-id"), topic, MessageType.MT_EVENT,
                partitionKey: new Paramore.Brighter.PartitionKey("source-key"), handledCount: 3), new MessageBody(body));
            original.Header.Bag["tenant"] = "north";
            using var publisher = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092", Acks = Confluent.Kafka.Acks.All
            }).Build();
            var published = await publisher.ProduceAsync(topic.Value,
                new DefaultKafkaWireMessageWriter().WriteMessage(original), timeout.Token);
            var writer = new KafkaInvalidWireWriter(compose);
            var subscription = new KafkaSubscription(new SubscriptionName(group), new ChannelName(group), topic,
                requestType: typeof(Command), groupId: group, commitBatchSize: 10,
                sweepUncommittedOffsetsInterval: TimeSpan.FromMinutes(5), makeChannels: OnMissingChannel.Validate,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                invalidMessageRoutingKey: invalidTopic)
            {
                InvalidMessageWireMessageWriter = writer
            };
            var factory = new KafkaMessageConsumerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = group, BootStrapServers = ["localhost:9092"]
            });
            if (useAsync) asyncConsumer = factory.CreateAsync(subscription);
            else syncConsumer = factory.Create(subscription);
            Message? received = null;
            var wait = Stopwatch.StartNew();
            while (received == null && wait.Elapsed < TimeSpan.FromSeconds(30))
            {
                var messages = useAsync
                    ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : syncConsumer!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618 // Distinguish empty polls from the source delivery.
                received = messages.SingleOrDefault(message => message.Header.MessageType != MessageType.MT_NONE);
#pragma warning restore CS0618
            }
            Assert.NotNull(received);
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "External validation rejected the delivery.");

            //Act
            if (useAsync) await asyncConsumer!.RejectAsync(received, reason, timeout.Token);
            else Assert.True(syncConsumer!.Reject(received, reason));
            syncConsumer?.Dispose();
            syncConsumer = null;
            if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            asyncConsumer = null;

            //Assert
            Assert.Equal(1, writer.WriteCount);
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = group, EnableAutoCommit = false
            }).Build();
            var partition = new TopicPartition(invalidTopic.Value, new Partition(0));
            observer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));
            Assert.Equal(1L, observer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High.Value);
            var native = observer.Consume(timeout.Token);
            Assert.Equal(invalidTopic.Value, native.Topic);
            Assert.Equal("rejection-key", native.Message.Key);
            Assert.Equal("wire-rejected-id", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(compose ? body : Encoding.UTF8.GetBytes("rejected:" + Convert.ToBase64String(body)), native.Message.Value);
            Assert.Equal("source-id", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes("external-source-id")));
            Assert.Equal(topic.Value, Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes("external-original-topic")));
            Assert.Equal("Unacceptable", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes("external-reason")));
            Assert.Equal("north", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes("external-tenant")));
            Assert.Equal(topic, received.Header.Topic);
            Assert.Equal(new Id("source-id"), received.Id);
            Assert.Equal(new Paramore.Brighter.PartitionKey("source-key"), received.Header.PartitionKey);
            Assert.Equal(3, received.Header.HandledCount);
            Assert.Equal(body, received.Body.Memory.ToArray());
            var committed = Assert.Single(observer.Committed([published.TopicPartition], TimeSpan.FromSeconds(10)));
            Assert.Equal(new TopicPartitionOffset(published.TopicPartition, new Offset(published.Offset.Value + 1)), committed);
        }
        finally
        {
            try
            {
                syncConsumer?.Dispose();
                if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            }
            finally
            {
                await admin.DeleteTopicsAsync([topic.Value, invalidTopic.Value]);
            }
        }
    }
}
