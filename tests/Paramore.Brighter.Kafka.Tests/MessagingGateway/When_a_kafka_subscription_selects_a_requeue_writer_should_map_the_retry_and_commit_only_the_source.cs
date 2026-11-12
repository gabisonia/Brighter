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
public class KafkaImmediateRequeueWriterTests
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
    public async Task When_a_kafka_subscription_selects_a_requeue_writer_should_map_the_retry_and_commit_only_the_source(
        bool useAsync, bool selectWriter, bool useNullDelay)
    {
        //Arrange
        var topic = new RoutingKey($"requeue-writer-{Guid.NewGuid():N}");
        var invalidTopic = new RoutingKey(topic.Value + "-invalid");
        var group = $"requeue-writer-group-{Guid.NewGuid():N}";
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
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
            var partition = new TopicPartition(topic.Value, new Partition(0));
            var published = await publisher.ProduceAsync(partition,
                new DefaultKafkaWireMessageWriter().WriteMessage(original), timeout.Token);
            var retryWriter = new KafkaBrokerRejectedWireWriter { Oversized = false };
            var invalidWriter = new KafkaBrokerRejectedWireWriter { Oversized = false };
            var subscription = new KafkaSubscription(new SubscriptionName(group), new ChannelName(group), topic,
                requestType: typeof(Command), groupId: group, commitBatchSize: 10,
                sweepUncommittedOffsetsInterval: TimeSpan.FromMinutes(5), makeChannels: OnMissingChannel.Validate,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                invalidMessageRoutingKey: invalidTopic)
            {
                RequeueWireMessageWriter = selectWriter ? retryWriter : null,
                InvalidMessageWireMessageWriter = invalidWriter
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
                timeout.Token.ThrowIfCancellationRequested();
                var messages = useAsync
                    ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : syncConsumer!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618
                received = messages.SingleOrDefault(message => message.Header.MessageType != MessageType.MT_NONE);
#pragma warning restore CS0618
            }
            Assert.NotNull(received);
            Assert.Equal(published.TopicPartitionOffset,
                Assert.IsType<TopicPartitionOffset>(received.Header.Bag[HeaderNames.PARTITION_OFFSET]));
            TimeSpan? delay = useNullDelay ? null : TimeSpan.Zero;

            //Act
            var requeued = useAsync
                ? await asyncConsumer!.RequeueAsync(received, delay, timeout.Token)
                : syncConsumer!.Requeue(received, delay);
            syncConsumer?.Dispose();
            syncConsumer = null;
            if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            asyncConsumer = null;

            //Assert
            Assert.True(requeued);
            Assert.Equal(selectWriter ? 1 : 0, retryWriter.WriteCount);
            Assert.Equal(0, invalidWriter.WriteCount);
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = group, EnableAutoCommit = false
            }).Build();
            Assert.Equal(published.Offset.Value + 2,
                observer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5)).High.Value);
            observer.Assign(new TopicPartitionOffset(partition, published.Offset + 1));
            var native = observer.Consume(timeout.Token);
            Assert.Equal(new TopicPartitionOffset(partition, published.Offset + 1), native.TopicPartitionOffset);
            Assert.Equal(selectWriter ? "mapped-source-id" : "source-key", native.Message.Key);
            Assert.Equal(selectWriter ? "wire-source-id" : "source-id",
                Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(selectWriter ? Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(body)) : body,
                native.Message.Value);
            if (!selectWriter)
                Assert.Equal("north", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes("tenant")));
            Assert.Equal(0L, observer.QueryWatermarkOffsets(
                new TopicPartition(invalidTopic.Value, new Partition(0)), TimeSpan.FromSeconds(5)).High.Value);
            Assert.Equal(topic, received.Header.Topic);
            Assert.Equal(original.Id, received.Id);
            Assert.Equal(original.Header.PartitionKey, received.Header.PartitionKey);
            Assert.Equal(3, received.Header.HandledCount);
            Assert.Equal(body, received.Body.Memory.ToArray());
            Assert.Equal("north", received.Header.Bag["tenant"]);
            Assert.Equal(new TopicPartitionOffset(partition, published.Offset + 1),
                Assert.Single(observer.Committed([partition], TimeSpan.FromSeconds(5))));
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
