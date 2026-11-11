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
public class KafkaInflightForwardCommitBarrierTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task When_a_rejection_is_in_flight_should_hold_later_acknowledgements_until_forwarding_succeeds(
        bool useAsync, bool failFirstWrite)
    {
        //Arrange
        var topic = new RoutingKey($"inflight-forward-{Guid.NewGuid():N}");
        var invalidTopic = new RoutingKey(topic.Value + "-invalid");
        var group = $"inflight-forward-group-{Guid.NewGuid():N}";
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        using var writer = new GatedKafkaRejectionWireWriter { FailFirstWrite = failFirstWrite };
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync(new[] { topic, invalidTopic }.Select(route => new TopicSpecification
        {
            Name = route.Value, NumPartitions = 1, ReplicationFactor = 1
        }));
        IAmAMessageConsumerSync? sync = null;
        IAmAMessageConsumerAsync? asyncConsumer = null;
        Task<bool>? rejection = null;
        try
        {
            var partition = new TopicPartition(topic.Value, new Partition(0));
            var invalidPartition = new TopicPartition(invalidTopic.Value, new Partition(0));
            using var publisher = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092", Acks = Confluent.Kafka.Acks.All
            }).Build();
            var originals = new[] { "first", "later" }.Select((id, index) => new Message(
                new MessageHeader(new Id(id), topic, MessageType.MT_EVENT,
                    partitionKey: new Paramore.Brighter.PartitionKey("source-" + id)),
                new MessageBody(new byte[] { 0, 255, 128, (byte)index }))).ToArray();
            foreach (var original in originals)
                await publisher.ProduceAsync(partition, new DefaultKafkaWireMessageWriter().WriteMessage(original), timeout.Token);
            var factory = new KafkaMessageConsumerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = group, BootStrapServers = ["localhost:9092"]
            });
            var subscription = new KafkaSubscription(
                new SubscriptionName(group), new ChannelName(group), topic, requestType: typeof(Command), groupId: group,
                commitBatchSize: 1, numOfPartitions: 1, maxPollInterval: TimeSpan.FromMinutes(2),
                sweepUncommittedOffsetsInterval: TimeSpan.FromMinutes(5), makeChannels: OnMissingChannel.Validate,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                invalidMessageRoutingKey: invalidTopic)
            {
                InvalidMessageWireMessageWriter = writer
            };
            if (useAsync) asyncConsumer = factory.CreateAsync(subscription);
            else sync = factory.Create(subscription);
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = group, EnableAutoCommit = false
            }).Build();
            var first = await Read();
            var later = await Read();
            AssertDelivery(first, 0);
            AssertDelivery(later, 1);
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "External validation rejected the delivery.");

            //Act
            rejection = Task.Run(async () => useAsync
                ? await asyncConsumer!.RejectAsync(first, reason, timeout.Token)
                : sync!.Reject(first, reason));
            Assert.True(writer.Entered.Wait(TimeSpan.FromSeconds(10), timeout.Token), "The rejection writer was not entered.");
            TopicPartitionOffset whileForwarding;
            try
            {
                if (useAsync) await asyncConsumer!.AcknowledgeAsync(later, timeout.Token);
                else sync!.Acknowledge(later);
                var wait = Stopwatch.StartNew();
                do
                {
                    whileForwarding = CurrentCommit();
                    if (whileForwarding.Offset != Offset.Unset) break;
                    await Task.Delay(25, timeout.Token);
                } while (wait.Elapsed < TimeSpan.FromSeconds(3));
                Assert.False(rejection.IsCompleted, "The writer must remain gated during commit observation.");
            }
            finally
            {
                writer.Release.Set();
            }
            var result = await rejection;

            //Assert
            Assert.Equal(new TopicPartitionOffset(partition, new Offset(0)), whileForwarding);
            Assert.Equal(!failFirstWrite, result);
            AssertDelivery(first, 0, requireSourceOffset: failFirstWrite);
            Assert.Equal(1, writer.WriteCount);
            if (failFirstWrite)
            {
                Assert.Equal(new TopicPartitionOffset(partition, new Offset(0)), CurrentCommit());
                Assert.Equal(0L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(5)).High.Value);
                Assert.True(useAsync
                    ? await asyncConsumer!.RejectAsync(first, reason, timeout.Token)
                    : sync!.Reject(first, reason));
                AssertDelivery(first, 0, requireSourceOffset: false);
            }
            sync?.Dispose();
            sync = null;
            if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            asyncConsumer = null;
            Assert.Equal(new TopicPartitionOffset(partition, new Offset(2)), CurrentCommit());
            Assert.Equal(failFirstWrite ? 2 : 1, writer.WriteCount);
            Assert.Equal(1L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(5)).High.Value);
            observer.Assign(new TopicPartitionOffset(invalidPartition, Offset.Beginning));
            var native = observer.Consume(timeout.Token);
            Assert.Equal("mapped-first", native.Message.Key);
            Assert.Equal("wire-first", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(originals[0].Body.Memory.ToArray())), native.Message.Value);

            TopicPartitionOffset CurrentCommit() => Assert.Single(observer.Committed([partition], TimeSpan.FromSeconds(1)));

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

            void AssertDelivery(Message delivery, int index, bool requireSourceOffset = true)
            {
                Assert.Equal(originals[index].Id, delivery.Id);
                Assert.Equal(topic, delivery.Header.Topic);
                Assert.Equal(originals[index].Header.PartitionKey, delivery.Header.PartitionKey);
                Assert.Equal(originals[index].Body.Memory.ToArray(), delivery.Body.Memory.ToArray());
                if (requireSourceOffset)
                    Assert.Equal(new TopicPartitionOffset(partition, new Offset(index)),
                        Assert.IsType<TopicPartitionOffset>(delivery.Header.Bag[HeaderNames.PARTITION_OFFSET]));
            }
        }
        finally
        {
            writer.Release.Set();
            try
            {
                if (rejection != null) await rejection;
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
                    await admin.DeleteTopicsAsync([topic.Value, invalidTopic.Value]);
                }
            }
        }
    }
}
