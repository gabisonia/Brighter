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
using System.Net.Mime;
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
public class KafkaSubscriptionIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_kafka_subscriptions_select_different_readers_should_keep_mapping_and_acknowledgement_isolated(
        bool useAsync, bool customFirst)
    {
        //Arrange
        var topic = new RoutingKey($"reader-isolation-{Guid.NewGuid():N}");
        var groups = new[] { $"default-group-{Guid.NewGuid():N}", $"custom-group-{Guid.NewGuid():N}" };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic.Value, NumPartitions = 1, ReplicationFactor = 1 }]);
        var syncConsumers = new IAmAMessageConsumerSync?[2];
        var asyncConsumers = new IAmAMessageConsumerAsync?[2];
        try
        {
            using var publisher = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092", Acks = Confluent.Kafka.Acks.All
            }).Build();
            byte[] body = [0, 255, 128, 3];
            var original = new Message(new MessageHeader(new Id("native-id"), topic, MessageType.MT_EVENT,
                contentType: new ContentType("application/octet-stream")), new MessageBody(body));
            var headers = new KafkaDefaultMessageHeaderBuilder().Build(original);
            headers.Add("external-id", Encoding.UTF8.GetBytes("native-id"));
            headers.Add("external-parent", Encoding.UTF8.GetBytes("00-11111111111111111111111111111111-2222222222222222-01"));
            headers.Add("external-tenant", Encoding.UTF8.GetBytes("south"));
            var published = await publisher.ProduceAsync(topic.Value, new Confluent.Kafka.Message<string, byte[]>
            {
                Headers = headers, Key = "native-key", Value = body
            }, timeout.Token);
            var reader = new KafkaEndpointWireReader(compose: false);
            var subscriptions = groups.Select(group => new KafkaSubscription(new SubscriptionName(group), new ChannelName(group), topic,
                requestType: typeof(Command), groupId: group, commitBatchSize: 10, messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                sweepUncommittedOffsetsInterval: TimeSpan.FromMinutes(5), makeChannels: OnMissingChannel.Validate)).ToArray();
            subscriptions[1].WireMessageReader = reader;
            var factory = new KafkaMessageConsumerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = "reader-isolation", BootStrapServers = ["localhost:9092"]
            });
            foreach (var index in customFirst ? new[] { 1, 0 } : new[] { 0, 1 })
            {
                if (useAsync) asyncConsumers[index] = factory.CreateAsync(subscriptions[index]);
                else syncConsumers[index] = factory.Create(subscriptions[index]);
            }

            //Act
            var received = new Message?[2];
            for (var index = 0; index < received.Length; index++)
            {
                var wait = Stopwatch.StartNew();
                while (received[index] == null && wait.Elapsed < TimeSpan.FromSeconds(30))
                {
                    var batch = useAsync
                        ? await asyncConsumers[index]!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                        : syncConsumers[index]!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618 // Distinguish empty polls from real deliveries.
                    received[index] = batch.SingleOrDefault(message => message.Header.MessageType != MessageType.MT_NONE);
#pragma warning restore CS0618
                }
                Assert.NotNull(received[index]);
            }

            //Assert
            Assert.Equal(1, reader.ReadCount);
            for (var index = 0; index < received.Length; index++)
            {
                var message = received[index]!;
                Assert.Equal(new Id(index == 0 ? "native-id" : "mapped-native-id"), message.Id);
                Assert.Equal(index == 0 ? topic : new RoutingKey("mapped-topic"), message.Header.Topic);
                Assert.Equal(index == 0 ? body : Encoding.UTF8.GetBytes("decoded:" + Convert.ToBase64String(body)), message.Body.Memory.ToArray());
                Assert.Equal(published.TopicPartitionOffset, Assert.IsType<TopicPartitionOffset>(message.Header.Bag[HeaderNames.PARTITION_OFFSET]));
                if (index == 1)
                {
                    Assert.Equal("south", message.Header.Bag["tenant"]);
                    Assert.Equal("00-11111111111111111111111111111111-2222222222222222-01", message.Header.TraceParent?.Value);
                }
                if (useAsync) await asyncConsumers[index]!.AcknowledgeAsync(message, timeout.Token);
                else syncConsumers[index]!.Acknowledge(message);
            }
            for (var index = 0; index < received.Length; index++)
            {
                syncConsumers[index]?.Dispose();
                syncConsumers[index] = null;
                if (asyncConsumers[index] != null) await asyncConsumers[index]!.DisposeAsync();
                asyncConsumers[index] = null;
                using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
                {
                    BootstrapServers = "localhost:9092", GroupId = groups[index], EnableAutoCommit = false
                }).Build();
                var committed = Assert.Single(observer.Committed([published.TopicPartition], TimeSpan.FromSeconds(10)));
                Assert.Equal(new TopicPartitionOffset(published.TopicPartition, new Offset(published.Offset.Value + 1)), committed);
            }
        }
        finally
        {
            try
            {
                for (var index = 0; index < syncConsumers.Length; index++)
                {
                    syncConsumers[index]?.Dispose();
                    if (asyncConsumers[index] != null) await asyncConsumers[index]!.DisposeAsync();
                }
            }
            finally
            {
                await admin.DeleteTopicsAsync([topic.Value]);
            }
        }
    }
}
