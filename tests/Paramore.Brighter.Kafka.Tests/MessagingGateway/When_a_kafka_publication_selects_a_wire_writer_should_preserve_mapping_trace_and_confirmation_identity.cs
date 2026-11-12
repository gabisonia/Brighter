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
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.Kafka.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.Kafka;
using Paramore.Brighter.Observability;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

[Trait("Category", "Kafka")]
[Collection("Kafka")]
public class KafkaEndpointWireWriterTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_kafka_publication_selects_a_wire_writer_should_preserve_mapping_trace_and_confirmation_identity(
        bool useAsync, bool compose)
    {
        //Arrange
        var topic = new RoutingKey($"wire-writer-{Guid.NewGuid():N}");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic.Value, NumPartitions = 1, ReplicationFactor = 1 }]);
        try
        {
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = Guid.NewGuid().ToString(),
                EnableAutoCommit = false, AutoOffsetReset = AutoOffsetReset.Earliest
            }).Build();
            observer.Assign(new TopicPartitionOffset(topic.Value, new Partition(0), Offset.Beginning));
            var writer = new KafkaEndpointWireWriter(compose);
            var publication = new KafkaPublication
            {
                Topic = topic, MakeChannels = OnMissingChannel.Validate, NumPartitions = 1, ReplicationFactor = 1,
                MessageTimeoutMs = 10000, RequestTimeoutMs = 5000, WireMessageWriter = writer
            };
            var producers = new KafkaMessageProducerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = "wire-writer", BootStrapServers = ["localhost:9092"]
            }, [publication]).Create();
            using var registry = new ProducerRegistry(producers);
            var producer = Assert.Single(producers).Value;
            using var traceProvider = Sdk.CreateTracerProviderBuilder().Build();
            using var span = new Activity("kafka-wire-dispatch").SetIdFormat(ActivityIdFormat.W3C).Start();
            producer.Span = span;
            var confirmation = new TaskCompletionSource<PublishConfirmationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.IsAssignableFrom<ISupportPublishConfirmation>(producer).OnMessagePublished += result => confirmation.TrySetResult(result);
            byte[] body = [0, 255, 128, 1];
            var source = new Message(new MessageHeader(new Id("source-id"), topic, MessageType.MT_EVENT,
                partitionKey: new Paramore.Brighter.PartitionKey("source-key"),
                traceParent: new TraceParent("00-11111111111111111111111111111111-2222222222222222-01")), new MessageBody(body));
            source.Header.Bag["tenant"] = "north";

            //Act
            if (useAsync)
                await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendAsync(source, timeout.Token);
            else
                Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(source);
            var confirmed = await confirmation.Task.WaitAsync(timeout.Token);
            var native = observer.Consume(timeout.Token);

            //Assert
            Assert.Equal(1, writer.WriteCount);
            Assert.True(confirmed.Success);
            Assert.Equal(new Id("source-id"), confirmed.MessageId);
            Assert.Equal(topic, confirmed.Topic);
            Assert.Equal(topic.Value, native.Topic);
            Assert.Equal("mapped-key", native.Message.Key);
            Assert.Equal("wire-id", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(compose ? body : Encoding.UTF8.GetBytes($"external:{Convert.ToBase64String(body)}"), native.Message.Value);
            Assert.Equal(span.Id, Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes("external-parent")));
            Assert.Equal("north", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes("external-tenant")));
            Assert.DoesNotContain(native.Message.Headers, header => header.Key == HeaderNames.CLOUD_EVENTS_TRACE_PARENT);
            if (compose)
                Assert.Equal("source-id", Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.CLOUD_EVENTS_ID)));
            else
                Assert.Equal(3, native.Message.Headers.Count);
            Assert.Equal(new Id("source-id"), source.Id);
            Assert.Equal(topic, source.Header.Topic);
            Assert.Equal(new Paramore.Brighter.PartitionKey("source-key"), source.Header.PartitionKey);
            Assert.Equal(body, source.Body.Memory.ToArray());
            Assert.Equal("tenant", Assert.Single(source.Header.Bag).Key);
            Assert.Equal("north", source.Header.Bag["tenant"]);
        }
        finally
        {
            await admin.DeleteTopicsAsync([topic.Value]);
        }
    }
}
