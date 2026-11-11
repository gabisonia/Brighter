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
using System.Collections.Generic;
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
public class KafkaLostPartitionRecoveryTests
{
    [Theory]
    [InlineData(false, PartitionAssignmentStrategy.RoundRobin)]
    [InlineData(true, PartitionAssignmentStrategy.RoundRobin)]
    [InlineData(false, PartitionAssignmentStrategy.CooperativeSticky)]
    [InlineData(true, PartitionAssignmentStrategy.CooperativeSticky)]
    public async Task When_a_lost_partition_returns_after_another_consumer_recovers_should_resume_commits(
        bool useAsync, PartitionAssignmentStrategy assignment)
    {
        //Arrange
        var topic = new RoutingKey($"lost-partition-recovery-{Guid.NewGuid():N}");
        var invalidTopic = new RoutingKey(topic.Value + "-invalid");
        var group = $"lost-partition-recovery-group-{Guid.NewGuid():N}";
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync(new[] { topic, invalidTopic }.Select(route => new TopicSpecification
        {
            Name = route.Value, NumPartitions = 1, ReplicationFactor = 1,
            Configs = new Dictionary<string, string> { ["max.message.bytes"] = route == invalidTopic ? "1024" : "1048588" }
        }));
        IAmAMessageConsumerSync? originalSync = null;
        IAmAMessageConsumerAsync? originalAsync = null;
        IAmAMessageConsumerSync? replacementSync = null;
        IAmAMessageConsumerAsync? replacementAsync = null;
        try
        {
            using var publisher = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092", Acks = Confluent.Kafka.Acks.All
            }).Build();
            var partition = new TopicPartition(topic.Value, new Partition(0));
            var invalidPartition = new TopicPartition(invalidTopic.Value, new Partition(0));
            var originals = new[] { "failed", "later", "after-return" }.Select((id, index) => new Message(
                new MessageHeader(new Id(id), topic, MessageType.MT_EVENT,
                    partitionKey: new Paramore.Brighter.PartitionKey("source-" + id), handledCount: 3),
                new MessageBody(new byte[] { 0, 255, 128, (byte)index }))).ToArray();
            var published = new List<DeliveryResult<string, byte[]>>();
            foreach (var original in originals.Take(2))
                published.Add(await publisher.ProduceAsync(partition,
                    new DefaultKafkaWireMessageWriter().WriteMessage(original), timeout.Token));
            var brokerError = await Assert.ThrowsAsync<ProduceException<string, byte[]>>(() =>
                publisher.ProduceAsync(invalidTopic.Value,
                    new Confluent.Kafka.Message<string, byte[]> { Key = "oversize-preflight", Value = new byte[4096] }, timeout.Token));
            Assert.Equal(ErrorCode.MsgSizeTooLarge, brokerError.Error.Code);
            var failingWriter = new KafkaBrokerRejectedWireWriter();
            var recoveryWriter = new KafkaBrokerRejectedWireWriter { Oversized = false };
            var factory = new KafkaMessageConsumerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = group, BootStrapServers = ["localhost:9092"]
            });
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = group, EnableAutoCommit = false
            }).Build();
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "External validation rejected the delivery.");
            var originalSubscription = Subscription(failingWriter, TimeSpan.FromSeconds(15), group + "-original");
            if (useAsync) originalAsync = factory.CreateAsync(originalSubscription);
            else originalSync = factory.Create(originalSubscription);
            var failed = await Read(originalSync, originalAsync);
            var later = await Read(originalSync, originalAsync);
            AssertDelivery(failed, 0);
            AssertDelivery(later, 1);

            //Act
            var rejected = useAsync
                ? await originalAsync!.RejectAsync(failed, reason, timeout.Token)
                : originalSync!.Reject(failed, reason);

            //Assert
            Assert.False(rejected);
            AssertDelivery(failed, 0);
            Assert.Equal(1, failingWriter.WriteCount);
            if (useAsync) await originalAsync!.AcknowledgeAsync(later, timeout.Token);
            else originalSync!.Acknowledge(later);
            await WaitForCommit(published[0].Offset.Value);
            Assert.Equal(published[0].TopicPartitionOffset, CurrentCommit());
            Assert.Equal(0L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(5)).High.Value);

            //Act
            var replacementSubscription = Subscription(recoveryWriter, TimeSpan.FromMinutes(2), group + "-replacement");
            if (useAsync) replacementAsync = factory.CreateAsync(replacementSubscription);
            else replacementSync = factory.Create(replacementSubscription);
            var redelivered = await Read(replacementSync, replacementAsync);

            //Assert
            AssertDelivery(redelivered, 0);
            var recovered = useAsync
                ? await replacementAsync!.RejectAsync(redelivered, reason, timeout.Token)
                : replacementSync!.Reject(redelivered, reason);
            Assert.True(recovered);
            var replayedLater = await Read(replacementSync, replacementAsync);
            AssertDelivery(replayedLater, 1);
            if (useAsync) await replacementAsync!.AcknowledgeAsync(replayedLater, timeout.Token);
            else replacementSync!.Acknowledge(replayedLater);
            replacementSync?.Dispose();
            replacementSync = null;
            if (replacementAsync != null) await replacementAsync.DisposeAsync();
            replacementAsync = null;
            Assert.Equal(new TopicPartitionOffset(partition, published[1].Offset + 1), CurrentCommit());
            Assert.Equal(1, recoveryWriter.WriteCount);
            Assert.Equal(1L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(5)).High.Value);
            observer.Assign(new TopicPartitionOffset(invalidPartition, Offset.Beginning));
            var native = observer.Consume(timeout.Token);
            Assert.Equal("mapped-failed", native.Message.Key);
            Assert.Equal("wire-failed", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(originals[0].Body.Memory.ToArray())), native.Message.Value);

            //Act
            await WaitForOriginalAssignment();
            published.Add(await publisher.ProduceAsync(partition,
                new DefaultKafkaWireMessageWriter().WriteMessage(originals[2]), timeout.Token));
            var afterReturn = await Read(originalSync, originalAsync, allowPollExceeded: true);

            //Assert
            AssertDelivery(afterReturn, 2);
            if (useAsync) await originalAsync!.AcknowledgeAsync(afterReturn, timeout.Token);
            else originalSync!.Acknowledge(afterReturn);
            originalSync?.Dispose();
            originalSync = null;
            if (originalAsync != null) await originalAsync.DisposeAsync();
            originalAsync = null;
            Assert.Equal(new TopicPartitionOffset(partition, published[2].Offset + 1), CurrentCommit());
            Assert.Equal(1, failingWriter.WriteCount);
            Assert.Equal(1, recoveryWriter.WriteCount);
            Assert.Equal(1L, observer.QueryWatermarkOffsets(invalidPartition, TimeSpan.FromSeconds(5)).High.Value);

            KafkaSubscription Subscription(KafkaBrokerRejectedWireWriter writer, TimeSpan maxPollInterval, string clientId) => new(
                new SubscriptionName(group), new ChannelName(group), topic, requestType: typeof(Command), groupId: group,
                commitBatchSize: 1, numOfPartitions: 1, maxPollInterval: maxPollInterval,
                sweepUncommittedOffsetsInterval: TimeSpan.FromMinutes(5), makeChannels: OnMissingChannel.Validate,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                invalidMessageRoutingKey: invalidTopic, configHook: config => config.ClientId = clientId)
            {
                InvalidMessageWireMessageWriter = writer,
                GroupProtocol = new ClassicGroupProtocol
                {
                    PartitionAssignmentStrategy = assignment,
                    SessionTimeout = TimeSpan.FromSeconds(6),
                    HeartbeatInterval = TimeSpan.FromSeconds(1)
                }
            };

            async Task WaitForOriginalAssignment()
            {
                var wait = Stopwatch.StartNew();
                while (wait.Elapsed < TimeSpan.FromSeconds(45))
                {
                    timeout.Token.ThrowIfCancellationRequested();
                    try
                    {
                        var messages = useAsync
                            ? await originalAsync!.ReceiveAsync(TimeSpan.FromMilliseconds(250), timeout.Token)
                            : originalSync!.Receive(TimeSpan.FromMilliseconds(250));
#pragma warning disable CS0618
                        Assert.All(messages, message => Assert.Equal(MessageType.MT_NONE, message.Header.MessageType));
#pragma warning restore CS0618
                    }
                    catch (ChannelFailureException exception) when (exception.InnerException is ConsumeException consumeException
                        && consumeException.Error.Code == ErrorCode.Local_MaxPollExceeded)
                    {
                    }
                    var description = Assert.Single((await admin.DescribeConsumerGroupsAsync([group],
                        new DescribeConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(5) })).ConsumerGroupDescriptions);
                    if (description.State == ConsumerGroupState.Stable && description.Members.Count == 1
                        && description.Members[0].ClientId == group + "-original"
                        && description.Members[0].Assignment.TopicPartitions.Contains(partition))
                        return;
                }
                Assert.Fail("The broker did not assign the source partition back to the original consumer.");
            }

            async Task<Message> Read(IAmAMessageConsumerSync? sync, IAmAMessageConsumerAsync? asyncConsumer,
                bool allowPollExceeded = false)
            {
                Message? delivery = null;
                var wait = Stopwatch.StartNew();
                while (delivery == null && wait.Elapsed < TimeSpan.FromSeconds(45))
                {
                    timeout.Token.ThrowIfCancellationRequested();
                    try
                    {
                        var messages = useAsync
                            ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                            : sync!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618
                        delivery = messages.SingleOrDefault(message => message.Header.MessageType != MessageType.MT_NONE);
#pragma warning restore CS0618
                    }
                    catch (ChannelFailureException exception) when (allowPollExceeded
                        && exception.InnerException is ConsumeException consumeException
                        && consumeException.Error.Code == ErrorCode.Local_MaxPollExceeded)
                    {
                    }
                }
                Assert.NotNull(delivery);
                return delivery;
            }

            void AssertDelivery(Message delivery, int index)
            {
                Assert.Equal(originals[index].Id, delivery.Id);
                Assert.Equal(topic, delivery.Header.Topic);
                Assert.Equal(originals[index].Header.PartitionKey, delivery.Header.PartitionKey);
                Assert.Equal(originals[index].Body.Memory.ToArray(), delivery.Body.Memory.ToArray());
                Assert.Equal(published[index].TopicPartitionOffset,
                    Assert.IsType<TopicPartitionOffset>(delivery.Header.Bag[HeaderNames.PARTITION_OFFSET]));
            }

            TopicPartitionOffset CurrentCommit() => Assert.Single(observer.Committed([partition], TimeSpan.FromSeconds(5)));

            async Task WaitForCommit(long expected)
            {
                var wait = Stopwatch.StartNew();
                while (CurrentCommit().Offset.Value != expected && wait.Elapsed < TimeSpan.FromSeconds(5))
                    await Task.Delay(25, timeout.Token);
            }
        }
        finally
        {
            try
            {
                replacementSync?.Dispose();
                if (replacementAsync != null) await replacementAsync.DisposeAsync();
            }
            finally
            {
                try
                {
                    originalSync?.Dispose();
                    if (originalAsync != null) await originalAsync.DisposeAsync();
                }
                finally
                {
                    await admin.DeleteTopicsAsync([topic.Value, invalidTopic.Value]);
                }
            }
        }
    }
}
