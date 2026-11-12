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
public class KafkaEndpointWireReaderTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_kafka_subscription_selects_a_wire_reader_should_preserve_mapping_and_acknowledge_the_native_offset(
        bool useAsync, bool compose)
    {
        //Arrange
        var topic = new RoutingKey($"wire-reader-{Guid.NewGuid():N}");
        var group = $"wire-reader-group-{Guid.NewGuid():N}";
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic.Value, NumPartitions = 1, ReplicationFactor = 1 }]);
        IAmAMessageConsumerSync? syncConsumer = null;
        IAmAMessageConsumerAsync? asyncConsumer = null;
        try
        {
            using var publisher = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092", Acks = Confluent.Kafka.Acks.All
            }).Build();
            byte[] body = [0, 255, 128, 2];
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
            var reader = new KafkaEndpointWireReader(compose);
            var subscription = new KafkaSubscription(new SubscriptionName(group), new ChannelName(group), topic,
                requestType: typeof(Command), groupId: group, commitBatchSize: 1, makeChannels: OnMissingChannel.Validate,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
            {
                WireMessageReader = reader
            };
            var factory = new KafkaMessageConsumerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = group, BootStrapServers = ["localhost:9092"]
            });
            if (useAsync) asyncConsumer = factory.CreateAsync(subscription);
            else syncConsumer = factory.Create(subscription);

            //Act
            Message? received = null;
            var wait = Stopwatch.StartNew();
            while (received == null && wait.Elapsed < TimeSpan.FromSeconds(30))
            {
                var messages = useAsync
                    ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : syncConsumer!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618 // Distinguish empty polls from real deliveries.
                received = messages.SingleOrDefault(message => message.Header.MessageType != MessageType.MT_NONE);
#pragma warning restore CS0618
            }

            //Assert
            Assert.NotNull(received);
            Assert.Equal(1, reader.ReadCount);
            Assert.Equal(new Id(compose ? "native-id" : "mapped-native-id"), received.Id);
            Assert.Equal(new RoutingKey("mapped-topic"), received.Header.Topic);
            Assert.Equal(compose ? body : Encoding.UTF8.GetBytes($"decoded:{Convert.ToBase64String(body)}"), received.Body.Memory.ToArray());
            Assert.Equal("00-11111111111111111111111111111111-2222222222222222-01", received.Header.TraceParent?.Value);
            Assert.Equal("south", received.Header.Bag["tenant"]);
            Assert.Equal(published.TopicPartitionOffset, Assert.IsType<TopicPartitionOffset>(received.Header.Bag[HeaderNames.PARTITION_OFFSET]));
            if (useAsync) await asyncConsumer!.AcknowledgeAsync(received, timeout.Token);
            else syncConsumer!.Acknowledge(received);
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = group, EnableAutoCommit = false
            }).Build();
            var expectedOffset = new TopicPartitionOffset(published.TopicPartition, new Offset(published.Offset.Value + 1));
            var commitWait = Stopwatch.StartNew();
            var committed = Assert.Single(observer.Committed([published.TopicPartition], TimeSpan.FromSeconds(2)));
            while (committed != expectedOffset && commitWait.Elapsed < TimeSpan.FromSeconds(10))
            {
                await Task.Delay(50, timeout.Token);
                committed = Assert.Single(observer.Committed([published.TopicPartition], TimeSpan.FromSeconds(2)));
            }
            Assert.Equal(expectedOffset, committed);
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
                await admin.DeleteTopicsAsync([topic.Value]);
            }
        }
    }
}
