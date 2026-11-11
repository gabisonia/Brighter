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
using Paramore.Brighter.MessagingGateway.Kafka;
using Paramore.Brighter.Observability;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

[Trait("Category", "Kafka")]
[Collection("Kafka")]
public class KafkaParserExceptionTests
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
    public async Task When_kafka_header_parsing_throws_should_preserve_the_delivery_and_continue_receiving(
        bool useAsync, bool explicitDefault, bool badFirst)
    {
        //Arrange
        var topic = new RoutingKey($"parser-exception-{Guid.NewGuid():N}");
        var group = $"parser-exception-group-{Guid.NewGuid():N}";
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
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
            byte[] badBody = [0, 255, 128, 3];
            byte[] goodBody = [0, 255, 128, 4];
            var ids = badFirst ? new[] { "bad-id", "good-id" } : new[] { "good-id", "bad-id" };
            var published = new List<DeliveryResult<string, byte[]>>();
            foreach (var id in ids)
            {
                var original = new Message(new MessageHeader(new Id(id), topic, MessageType.MT_EVENT,
                    partitionKey: new Paramore.Brighter.PartitionKey("native-key"), handledCount: 3,
                    traceParent: new TraceParent("00-11111111111111111111111111111111-2222222222222222-01")),
                    new MessageBody(id == "bad-id" ? badBody : goodBody));
                var headers = new KafkaDefaultMessageHeaderBuilder().Build(original);
                if (id == "bad-id")
                {
                    headers.Remove(HeaderNames.CLOUD_EVENTS_DATA_CONTENT_TYPE);
                    headers.Add(HeaderNames.CLOUD_EVENTS_DATA_CONTENT_TYPE, Encoding.UTF8.GetBytes("invalid/content/type"));
                }
                headers.Add("tenant", Encoding.UTF8.GetBytes("north"));
                published.Add(await publisher.ProduceAsync(topic.Value, new Confluent.Kafka.Message<string, byte[]>
                {
                    Headers = headers,
                    Key = "native-key", Value = id == "bad-id" ? badBody : goodBody
                }, timeout.Token));
            }
            var subscription = new KafkaSubscription(new SubscriptionName(group), new ChannelName(group), topic,
                requestType: typeof(Command), groupId: group, commitBatchSize: 10,
                sweepUncommittedOffsetsInterval: TimeSpan.FromMinutes(5), makeChannels: OnMissingChannel.Validate,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
            {
                WireMessageReader = explicitDefault ? new DefaultKafkaWireMessageReader() : null
            };
            var factory = new KafkaMessageConsumerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = group, BootStrapServers = ["localhost:9092"]
            });
            if (useAsync) asyncConsumer = factory.CreateAsync(subscription);
            else syncConsumer = factory.Create(subscription);
            var received = new List<Message>();

            //Act
            var receiveError = await Record.ExceptionAsync(async () =>
            {
                var wait = Stopwatch.StartNew();
                while (received.Count < 2 && wait.Elapsed < TimeSpan.FromSeconds(30))
                {
                    var messages = useAsync
                        ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                        : syncConsumer!.Receive(TimeSpan.FromSeconds(1));
#pragma warning disable CS0618 // Distinguish empty polls from real deliveries.
                    received.AddRange(messages.Where(message => message.Header.MessageType != MessageType.MT_NONE));
#pragma warning restore CS0618
                }
            });

            //Assert
            Assert.Null(receiveError);
            Assert.Equal(2, received.Count);
            var bad = received[badFirst ? 0 : 1];
            var good = received[badFirst ? 1 : 0];
#pragma warning disable CS0618 // Mapping failure must be exposed as an unacceptable delivery.
            Assert.Equal(MessageType.MT_UNACCEPTABLE, bad.Header.MessageType);
#pragma warning restore CS0618
            Assert.Equal(badBody, bad.Body.Memory.ToArray());
            Assert.Equal(new Id("bad-id"), bad.Id);
            Assert.Equal(topic, bad.Header.Topic);
            Assert.Equal(new Paramore.Brighter.PartitionKey("native-key"), bad.Header.PartitionKey);
            Assert.Equal(3, bad.Header.HandledCount);
            Assert.Equal("00-11111111111111111111111111111111-2222222222222222-01", bad.Header.TraceParent?.Value);
            Assert.True(bad.Header.Bag.TryGetValue(HeaderNames.PARTITION_OFFSET, out var badOffset));
            Assert.Equal(published[badFirst ? 0 : 1].TopicPartitionOffset, Assert.IsType<TopicPartitionOffset>(badOffset));
            Assert.True(bad.Header.Bag.TryGetValue("tenant", out var tenant));
            Assert.Equal("north", tenant);
            Assert.Equal(new Id("good-id"), good.Id);
            Assert.Equal(topic, good.Header.Topic);
            Assert.Equal(goodBody, good.Body.Memory.ToArray());
            Assert.Equal(published[badFirst ? 1 : 0].TopicPartitionOffset,
                Assert.IsType<TopicPartitionOffset>(good.Header.Bag[HeaderNames.PARTITION_OFFSET]));
            foreach (var message in received)
            {
                if (useAsync) await asyncConsumer!.AcknowledgeAsync(message, timeout.Token);
                else syncConsumer!.Acknowledge(message);
            }
            syncConsumer?.Dispose();
            syncConsumer = null;
            if (asyncConsumer != null) await asyncConsumer.DisposeAsync();
            asyncConsumer = null;
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = group, EnableAutoCommit = false
            }).Build();
            var committed = Assert.Single(observer.Committed([published[1].TopicPartition], TimeSpan.FromSeconds(10)));
            Assert.Equal(new TopicPartitionOffset(published[1].TopicPartition, new Offset(published[1].Offset.Value + 1)), committed);
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
