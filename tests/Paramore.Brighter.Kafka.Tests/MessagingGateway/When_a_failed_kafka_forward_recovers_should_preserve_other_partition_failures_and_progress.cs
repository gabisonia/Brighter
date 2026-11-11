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
using System.Collections.Generic;
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
public class KafkaPartitionForwardIsolationTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(true, 0)]
    [InlineData(true, 1)]
    public async Task When_a_failed_kafka_forward_recovers_should_preserve_other_partition_failures_and_progress(
        bool useAsync, int recoveredPartition)
    {
        //Arrange
        var topic = new RoutingKey($"partition-forward-isolation-{Guid.NewGuid():N}");
        var invalidTopic = new RoutingKey(topic.Value + "-invalid");
        var group = $"partition-forward-isolation-group-{Guid.NewGuid():N}";
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync(new[] { topic, invalidTopic }.Select(route =>
            new TopicSpecification
            {
                Name = route.Value, NumPartitions = route == invalidTopic ? 1 : 2, ReplicationFactor = 1,
                Configs = new Dictionary<string, string> { ["max.message.bytes"] = route == invalidTopic ? "1024" : "1048588" }
            }));
        IAmAMessageConsumerSync? syncConsumer = null;
        IAmAMessageConsumerAsync? asyncConsumer = null;
        try
        {
            var ids = new[] { "p0-failed", "p0-later", "p1-failed", "p1-later" };
            var originals = ids.Select((id, index) => new Message(
                new MessageHeader(new Id(id), topic, MessageType.MT_EVENT,
                    partitionKey: new Paramore.Brighter.PartitionKey("source-" + id), handledCount: 3),
                new MessageBody(new byte[] { 0, 255, 128, (byte)index }))).ToArray();
            using var publisher = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092", Acks = Confluent.Kafka.Acks.All
            }).Build();
            var published = new List<DeliveryResult<string, byte[]>>();
            for (var index = 0; index < originals.Length; index++)
            {
                var original = originals[index];
                original.Header.Bag["tenant"] = "north";
                published.Add(await publisher.ProduceAsync(new TopicPartition(topic.Value, new Partition(index / 2)),
                    new DefaultKafkaWireMessageWriter().WriteMessage(original), timeout.Token));
            }
            for (var partition = 0; partition < 2; partition++)
            {
                Assert.Equal(partition, published[partition * 2].Partition.Value);
                Assert.Equal(published[partition * 2].Offset.Value + 1, published[partition * 2 + 1].Offset.Value);
            }
            var brokerError = await Assert.ThrowsAsync<ProduceException<string, byte[]>>(() =>
                publisher.ProduceAsync(invalidTopic.Value,
                    new Confluent.Kafka.Message<string, byte[]> { Key = "oversize-preflight", Value = new byte[4096] }, timeout.Token));
            Assert.Equal(Confluent.Kafka.ErrorCode.MsgSizeTooLarge, brokerError.Error.Code);
            var writer = new KafkaBrokerRejectedWireWriter();
            var subscription = new KafkaSubscription(new SubscriptionName(group), new ChannelName(group), topic,
                requestType: typeof(Command), groupId: group, commitBatchSize: 1, numOfPartitions: 2,
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
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = group, EnableAutoCommit = false
            }).Build();
            var invalidPartition = new TopicPartition(invalidTopic.Value, new Partition(0));
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "External validation rejected the delivery.");
            if (useAsync) asyncConsumer = factory.CreateAsync(subscription);
            else syncConsumer = factory.Create(subscription);
            var received = new Dictionary<string, Message>();
            for (var index = 0; index < originals.Length; index++)
            {
                Message? delivery = null;
                var wait = Stopwatch.StartNew();
                while (delivery == null && wait.Elapsed < TimeSpan.FromSeconds(30))
                {
                    var messages = useAsync
                        ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                        : syncConsumer!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618 // Distinguish empty polls from a source record.
                    delivery = messages.SingleOrDefault(message => message.Header.MessageType != MessageType.MT_NONE);
#pragma warning restore CS0618
                }
                Assert.NotNull(delivery);
                var sourceIndex = Array.IndexOf(ids, delivery.Id.Value);
                Assert.InRange(sourceIndex, 0, originals.Length - 1);
                Assert.Equal(originals[sourceIndex].Body.Memory.ToArray(), delivery.Body.Memory.ToArray());
                Assert.Equal(published[sourceIndex].TopicPartitionOffset,
                    Assert.IsType<TopicPartitionOffset>(delivery.Header.Bag[HeaderNames.PARTITION_OFFSET]));
                received.Add(ids[sourceIndex], delivery);
            }

            //Act
            foreach (var index in new[] { 0, 2 })
            {
                var rejected = useAsync
                    ? await asyncConsumer!.RejectAsync(received[ids[index]], reason, timeout.Token)
                    : syncConsumer!.Reject(received[ids[index]], reason);

                //Assert
                Assert.False(rejected);
                Assert.Equal(index / 2 + 1, writer.WriteCount);
                Assert.Equal(topic, received[ids[index]].Header.Topic);
                Assert.Equal(originals[index].Id, received[ids[index]].Id);
                Assert.Equal(originals[index].Header.PartitionKey, received[ids[index]].Header.PartitionKey);
                Assert.Equal(3, received[ids[index]].Header.HandledCount);
                Assert.Equal(originals[index].Body.Memory.ToArray(), received[ids[index]].Body.Memory.ToArray());
                Assert.Equal("north", received[ids[index]].Header.Bag["tenant"]);
                Assert.True(received[ids[index]].Header.Bag.TryGetValue(HeaderNames.PARTITION_OFFSET, out var offset));
                Assert.Equal(published[index].TopicPartitionOffset, Assert.IsType<TopicPartitionOffset>(offset));
            }
            Assert.Equal(0L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(10)).High.Value);
            var recoveredIndex = recoveredPartition * 2;
            var remainingIndex = (1 - recoveredPartition) * 2;
            if (useAsync) await asyncConsumer!.AcknowledgeAsync(received[ids[recoveredIndex + 1]], timeout.Token);
            else syncConsumer!.Acknowledge(received[ids[recoveredIndex + 1]]);

            //Act
            writer.Oversized = false;
            var recovered = useAsync
                ? await asyncConsumer!.RejectAsync(received[ids[recoveredIndex]], reason, timeout.Token)
                : syncConsumer!.Reject(received[ids[recoveredIndex]], reason);

            //Assert
            Assert.True(recovered);
            Assert.Equal(topic, received[ids[recoveredIndex]].Header.Topic);
            Assert.Equal(originals[recoveredIndex].Id, received[ids[recoveredIndex]].Id);
            Assert.Equal(originals[recoveredIndex].Body.Memory.ToArray(), received[ids[recoveredIndex]].Body.Memory.ToArray());
            Assert.Equal(3, writer.WriteCount);
            if (useAsync) await asyncConsumer!.AcknowledgeAsync(received[ids[remainingIndex + 1]], timeout.Token);
            else syncConsumer!.Acknowledge(received[ids[remainingIndex + 1]]);
            syncConsumer?.Dispose();
            syncConsumer = null;
            if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            asyncConsumer = null;
            Assert.Equal(1L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(10)).High.Value);
            observer.Assign(new TopicPartitionOffset(invalidPartition, Offset.Beginning));
            var native = observer.Consume(timeout.Token);
            Assert.Equal("mapped-" + ids[recoveredIndex], native.Message.Key);
            Assert.Equal("wire-" + ids[recoveredIndex], Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(originals[recoveredIndex].Body.Memory.ToArray())), native.Message.Value);
            var recoveredCommit = Assert.Single(observer.Committed([published[recoveredIndex].TopicPartition], TimeSpan.FromSeconds(10)));
            Assert.Equal(new TopicPartitionOffset(published[recoveredIndex].TopicPartition, new Offset(published[recoveredIndex + 1].Offset.Value + 1)), recoveredCommit);
            var remainingCommit = Assert.Single(observer.Committed([published[remainingIndex].TopicPartition], TimeSpan.FromSeconds(10)));
            Assert.Equal(published[remainingIndex].TopicPartitionOffset, remainingCommit);

            //Act
            if (useAsync) asyncConsumer = factory.CreateAsync(subscription);
            else syncConsumer = factory.Create(subscription);
            for (var index = remainingIndex; index <= remainingIndex + 1; index++)
            {
                Message? delivery = null;
                var wait = Stopwatch.StartNew();
                while (delivery == null && wait.Elapsed < TimeSpan.FromSeconds(30))
                {
                    var messages = useAsync
                        ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                        : syncConsumer!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618 // Distinguish empty polls from redelivery after partial recovery.
                    delivery = messages.SingleOrDefault(message => message.Header.MessageType != MessageType.MT_NONE);
#pragma warning restore CS0618
                }

                //Assert
                Assert.NotNull(delivery);
                Assert.Equal(originals[index].Id, delivery.Id);
                Assert.Equal(originals[index].Body.Memory.ToArray(), delivery.Body.Memory.ToArray());
                Assert.Equal(published[index].TopicPartitionOffset,
                    Assert.IsType<TopicPartitionOffset>(delivery.Header.Bag[HeaderNames.PARTITION_OFFSET]));
                if (index == remainingIndex)
                {
                    var remainingRecovered = useAsync
                        ? await asyncConsumer!.RejectAsync(delivery, reason, timeout.Token)
                        : syncConsumer!.Reject(delivery, reason);
                    Assert.True(remainingRecovered);
                }
                else if (useAsync) await asyncConsumer!.AcknowledgeAsync(delivery, timeout.Token);
                else syncConsumer!.Acknowledge(delivery);
            }
            Assert.Equal(4, writer.WriteCount);
            syncConsumer?.Dispose();
            syncConsumer = null;
            if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            asyncConsumer = null;
            Assert.Equal(2L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(10)).High.Value);
            var finalNative = observer.Consume(timeout.Token);
            Assert.Equal("mapped-" + ids[remainingIndex], finalNative.Message.Key);
            Assert.Equal("wire-" + ids[remainingIndex], Encoding.UTF8.GetString(finalNative.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(originals[remainingIndex].Body.Memory.ToArray())), finalNative.Message.Value);
            foreach (var index in new[] { 1, 3 })
            {
                var committed = Assert.Single(observer.Committed([published[index].TopicPartition], TimeSpan.FromSeconds(10)));
                Assert.Equal(new TopicPartitionOffset(published[index].TopicPartition, new Offset(published[index].Offset.Value + 1)), committed);
            }
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
