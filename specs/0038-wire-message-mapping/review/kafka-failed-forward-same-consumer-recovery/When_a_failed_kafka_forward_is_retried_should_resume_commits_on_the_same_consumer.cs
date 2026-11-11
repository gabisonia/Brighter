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
public class KafkaFailedForwardSameConsumerRecoveryTests
{
    [Theory]
    [InlineData(false, 1)]
    [InlineData(false, 10)]
    [InlineData(true, 1)]
    [InlineData(true, 10)]
    public async Task When_a_failed_kafka_forward_is_retried_should_resume_commits_on_the_same_consumer(
        bool useAsync, int commitBatchSize)
    {
        //Arrange
        var topic = new RoutingKey($"same-consumer-recovery-{Guid.NewGuid():N}");
        var invalidTopic = new RoutingKey(topic.Value + "-invalid");
        var group = $"same-consumer-recovery-group-{Guid.NewGuid():N}";
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync(new[] { topic, invalidTopic }.Select(route =>
            new TopicSpecification
            {
                Name = route.Value, NumPartitions = 1, ReplicationFactor = 1,
                Configs = new Dictionary<string, string> { ["max.message.bytes"] = route == invalidTopic ? "1024" : "1048588" }
            }));
        IAmAMessageConsumerSync? syncConsumer = null;
        IAmAMessageConsumerAsync? asyncConsumer = null;
        try
        {
            byte[] body = [0, 255, 128, 3];
            var original = new Message(new MessageHeader(new Id("bad-id"), topic, MessageType.MT_EVENT,
                partitionKey: new Paramore.Brighter.PartitionKey("source-key"), handledCount: 3), new MessageBody(body));
            original.Header.Bag["tenant"] = "north";
            using var publisher = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092", Acks = Confluent.Kafka.Acks.All
            }).Build();
            var published = await publisher.ProduceAsync(topic.Value,
                new DefaultKafkaWireMessageWriter().WriteMessage(original), timeout.Token);
            var laterOriginal = new Message(new MessageHeader(new Id("later-id"), topic, MessageType.MT_EVENT,
                partitionKey: new Paramore.Brighter.PartitionKey("later-key")), new MessageBody("later body"));
            var laterPublished = await publisher.ProduceAsync(topic.Value,
                new DefaultKafkaWireMessageWriter().WriteMessage(laterOriginal), timeout.Token);
            Assert.Equal(published.Offset.Value + 1, laterPublished.Offset.Value);
            var brokerError = await Assert.ThrowsAsync<ProduceException<string, byte[]>>(() =>
                publisher.ProduceAsync(invalidTopic.Value,
                    new Confluent.Kafka.Message<string, byte[]> { Key = "oversize-preflight", Value = new byte[4096] }, timeout.Token));
            Assert.Equal(Confluent.Kafka.ErrorCode.MsgSizeTooLarge, brokerError.Error.Code);
            var writer = new KafkaBrokerRejectedWireWriter();
            var subscription = new KafkaSubscription(new SubscriptionName(group), new ChannelName(group), topic,
                requestType: typeof(Command), groupId: group, commitBatchSize: commitBatchSize,
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

            Message? later = null;
            wait.Restart();
            while (later == null && wait.Elapsed < TimeSpan.FromSeconds(30))
            {
                var messages = useAsync
                    ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : syncConsumer!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618 // Distinguish empty polls from the later source record.
                later = messages.SingleOrDefault(message => message.Header.MessageType != MessageType.MT_NONE);
#pragma warning restore CS0618
            }
            Assert.NotNull(later);
            Assert.Equal(new Id("later-id"), later.Id);
            Assert.Equal("later body", later.Body.Value);
            Assert.Equal(laterPublished.TopicPartitionOffset,
                Assert.IsType<TopicPartitionOffset>(later.Header.Bag[HeaderNames.PARTITION_OFFSET]));

            //Act
            var rejected = useAsync
                ? await asyncConsumer!.RejectAsync(received, reason, timeout.Token)
                : syncConsumer!.Reject(received, reason);

            //Assert
            Assert.False(rejected);
            Assert.Equal(1, writer.WriteCount);
            Assert.Equal(topic, received.Header.Topic);
            Assert.Equal(new Id("bad-id"), received.Id);
            Assert.Equal(new Paramore.Brighter.PartitionKey("source-key"), received.Header.PartitionKey);
            Assert.Equal(3, received.Header.HandledCount);
            Assert.Equal(body, received.Body.Memory.ToArray());
            Assert.Equal("north", received.Header.Bag["tenant"]);
            Assert.True(received.Header.Bag.TryGetValue(HeaderNames.PARTITION_OFFSET, out var offset));
            Assert.Equal(published.TopicPartitionOffset, Assert.IsType<TopicPartitionOffset>(offset));
            if (useAsync) await asyncConsumer!.AcknowledgeAsync(later, timeout.Token);
            else syncConsumer!.Acknowledge(later);
            var beforeRecovery = Assert.Single(observer.Committed([published.TopicPartition], TimeSpan.FromSeconds(10)));
            Assert.True(beforeRecovery.Offset == Offset.Unset ||
                (beforeRecovery.Offset.Value >= 0 && beforeRecovery.Offset.Value <= published.Offset.Value));
            Assert.Equal(0L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(10)).High.Value);

            //Act
            writer.Oversized = false;
            var recovered = useAsync
                ? await asyncConsumer!.RejectAsync(received, reason, timeout.Token)
                : syncConsumer!.Reject(received, reason);

            //Assert
            Assert.True(recovered);
            Assert.Equal(2, writer.WriteCount);
            Assert.Equal(topic, received.Header.Topic);
            Assert.Equal(new Id("bad-id"), received.Id);
            Assert.Equal(new Paramore.Brighter.PartitionKey("source-key"), received.Header.PartitionKey);
            Assert.Equal(3, received.Header.HandledCount);
            Assert.Equal(body, received.Body.Memory.ToArray());
            Assert.Equal("north", received.Header.Bag["tenant"]);
            Assert.Equal(1L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(10)).High.Value);
            observer.Assign(new TopicPartitionOffset(invalidPartition, Offset.Beginning));
            var native = observer.Consume(timeout.Token);
            Assert.Equal("mapped-bad-id", native.Message.Key);
            Assert.Equal("wire-bad-id", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(body)), native.Message.Value);
            syncConsumer?.Dispose();
            syncConsumer = null;
            if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            asyncConsumer = null;
            var committed = Assert.Single(observer.Committed([published.TopicPartition], TimeSpan.FromSeconds(10)));
            Assert.Equal(new TopicPartitionOffset(laterPublished.TopicPartition, new Offset(laterPublished.Offset.Value + 1)), committed);

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
