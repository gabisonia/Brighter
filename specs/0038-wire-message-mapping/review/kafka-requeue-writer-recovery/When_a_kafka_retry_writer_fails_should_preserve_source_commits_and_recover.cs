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
public class KafkaRequeueWriterRecoveryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_kafka_retry_writer_fails_should_preserve_source_commits_and_recover(bool useAsync, bool returnNull)
    {
        //Arrange
        var topic = new RoutingKey($"requeue-writer-recovery-{Guid.NewGuid():N}");
        var group = $"requeue-writer-recovery-group-{Guid.NewGuid():N}";
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification
        {
            Name = topic.Value, NumPartitions = 1, ReplicationFactor = 1
        }]);
        IAmAMessageConsumerSync? sync = null;
        IAmAMessageConsumerAsync? asyncConsumer = null;
        try
        {
            var partition = new TopicPartition(topic.Value, new Partition(0));
            using var publisher = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092", Acks = Confluent.Kafka.Acks.All
            }).Build();
            var originals = new[] { "bad-id", "later" }.Select((id, index) => new Message(
                new MessageHeader(new Id(id), topic, MessageType.MT_EVENT,
                    partitionKey: new Paramore.Brighter.PartitionKey("source-" + id), handledCount: 3),
                new MessageBody(new byte[] { 0, 255, 128, (byte)index }))).ToArray();
            foreach (var original in originals)
                await publisher.ProduceAsync(partition, new DefaultKafkaWireMessageWriter().WriteMessage(original), timeout.Token);
            var writer = new KafkaRecoverableWireWriter(returnNull);
            var subscription = new KafkaSubscription(new SubscriptionName(group), new ChannelName(group), topic,
                requestType: typeof(Command), groupId: group, commitBatchSize: 1,
                sweepUncommittedOffsetsInterval: TimeSpan.FromMinutes(5), makeChannels: OnMissingChannel.Validate,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
            {
                RequeueWireMessageWriter = writer
            };
            var factory = new KafkaMessageConsumerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = group, BootStrapServers = ["localhost:9092"]
            });
            if (useAsync) asyncConsumer = factory.CreateAsync(subscription);
            else sync = factory.Create(subscription);
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = group, EnableAutoCommit = false
            }).Build();
            var first = await Read();
            var later = await Read();
            AssertSource(first, 0);
            AssertSource(later, 1);

            //Act
            var failedRetry = true;
            var failure = await Record.ExceptionAsync(async () =>
            {
                failedRetry = useAsync
                    ? await asyncConsumer!.RequeueAsync(first, TimeSpan.Zero, timeout.Token)
                    : sync!.Requeue(first, TimeSpan.Zero);
            });

            //Assert
            Assert.Null(failure);
            Assert.False(failedRetry);
            AssertSource(first, 0);
            Assert.Equal(1, writer.WriteCount);
            Assert.Equal(2L, observer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5)).High.Value);
            if (useAsync) await asyncConsumer!.AcknowledgeAsync(later, timeout.Token);
            else sync!.Acknowledge(later);
            var wait = Stopwatch.StartNew();
            while (CurrentCommit().Offset == Offset.Unset && wait.Elapsed < TimeSpan.FromSeconds(5))
                await Task.Delay(25, timeout.Token);
            Assert.Equal(new TopicPartitionOffset(partition, new Offset(0)), CurrentCommit());

            //Act
            writer.FailBadMessage = false;
            var recovered = useAsync
                ? await asyncConsumer!.RequeueAsync(first, TimeSpan.Zero, timeout.Token)
                : sync!.Requeue(first, TimeSpan.Zero);
            sync?.Dispose();
            sync = null;
            if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            asyncConsumer = null;

            //Assert
            Assert.True(recovered);
            Assert.Equal(2, writer.WriteCount);
            Assert.Equal(topic, first.Header.Topic);
            Assert.Equal(originals[0].Id, first.Id);
            Assert.Equal(originals[0].Header.PartitionKey, first.Header.PartitionKey);
            Assert.Equal(3, first.Header.HandledCount);
            Assert.Equal(originals[0].Body.Memory.ToArray(), first.Body.Memory.ToArray());
            Assert.Equal(new TopicPartitionOffset(partition, new Offset(2)), CurrentCommit());
            Assert.Equal(3L, observer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5)).High.Value);
            observer.Assign(new TopicPartitionOffset(partition, new Offset(2)));
            var native = observer.Consume(timeout.Token);
            Assert.Equal(new TopicPartitionOffset(partition, new Offset(2)), native.TopicPartitionOffset);
            Assert.Equal("mapped-bad-id", native.Message.Key);
            Assert.Equal("wire-bad-id", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(originals[0].Body.Memory.ToArray())), native.Message.Value);

            TopicPartitionOffset CurrentCommit() => Assert.Single(observer.Committed([partition], TimeSpan.FromSeconds(5)));

            async Task<Message> Read()
            {
                var wait = Stopwatch.StartNew();
                while (wait.Elapsed < TimeSpan.FromSeconds(30))
                {
                    timeout.Token.ThrowIfCancellationRequested();
                    var messages = useAsync
                        ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                        : sync!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618
                    var delivery = messages.SingleOrDefault(message => message.Header.MessageType != MessageType.MT_NONE);
#pragma warning restore CS0618
                    if (delivery != null) return delivery;
                }
                throw new TimeoutException("The source record was not delivered.");
            }

            void AssertSource(Message message, int index)
            {
                Assert.Equal(originals[index].Id, message.Id);
                Assert.Equal(topic, message.Header.Topic);
                Assert.Equal(originals[index].Header.PartitionKey, message.Header.PartitionKey);
                Assert.Equal(3, message.Header.HandledCount);
                Assert.Equal(originals[index].Body.Memory.ToArray(), message.Body.Memory.ToArray());
                Assert.Equal(new TopicPartitionOffset(partition, new Offset(index)),
                    Assert.IsType<TopicPartitionOffset>(message.Header.Bag[HeaderNames.PARTITION_OFFSET]));
            }
        }
        finally
        {
            try
            {
                sync?.Dispose();
                if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            }
            finally
            {
                await admin.DeleteTopicsAsync([topic.Value]);
            }
        }
    }
}
