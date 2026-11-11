#region Licence

/* The MIT License (MIT)
Copyright © 2026 gabisonia

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
using System.Transactions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.Kafka.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.Kafka;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

/// <summary>Verifies publication selection at actual Kafka retry callback dispatch.</summary>
[Trait("Category", "Kafka")]
[Collection("Kafka")]
public class KafkaScheduledRetrySelectionTests
{
    /// <summary>Retains the logical native destination and source settlement with a selected callback writer.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_kafka_schedules_a_retry_should_select_the_registered_writer_and_preserve_the_destination(bool useAsync, bool custom)
    {
        // Arrange
        var group = "scheduled-selection-" + Guid.NewGuid().ToString("N");
        var topic = new RoutingKey(group);
        var alias = new RoutingKey(group + "-publication");
        var type = new CloudEventsType("scheduled.kafka.event");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync(new[] { topic, alias }.Select(route =>
            new TopicSpecification { Name = route.Value, NumPartitions = 1, ReplicationFactor = 1 }));
        KafkaMessageConsumer? consumer = null;
        try
        {
            var writer = new KafkaScheduledWireWriter();
            var inactiveWriter = new KafkaScheduledWireWriter();
            var config = new KafkaMessagingGatewayConfiguration { Name = group, BootStrapServers = ["localhost:9092"] };
            KafkaPublication Publication(RoutingKey key, IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>? selected)
                => new() { Topic = key, Type = type, NumPartitions = 1, ReplicationFactor = 1,
                    MakeChannels = OnMissingChannel.Validate, WireMessageWriter = selected };
            using var registry = new ProducerRegistry(new KafkaMessageProducerFactory(config,
                [Publication(alias, custom ? writer : null), Publication(topic, custom ? null : inactiveWriter)]).Create());
            using var telemetry = Sdk.CreateTracerProviderBuilder().Build();
            using var tracer = new BrighterTracer();
            var published = new TaskCompletionSource<Activity>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var listener = new ActivityListener
            {
                ShouldListenTo = source => ReferenceEquals(source, tracer.ActivitySource),
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    if (activity.Kind == ActivityKind.Producer
                        && Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "publish"))
                        published.TrySetResult(activity);
                }
            };
            ActivitySource.AddActivityListener(listener);
            using var mappers = new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null);
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(registry,
                new ResiliencePipelineRegistry<string>().AddBrighterDefault(), mappers,
                new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer,
                new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
            var schedulerFactory = new InMemorySchedulerFactory();
            CommandProcessor? processor = null;
            var subscribers = new SubscriberRegistry();
            subscribers.RegisterAsync<FireSchedulerMessage, FireSchedulerMessageHandler>();
            processor = new CommandProcessor(subscribers,
                new SimpleHandlerFactoryAsync(_ => new FireSchedulerMessageHandler(processor!)),
                new InMemoryRequestContextFactory(), new PolicyRegistry(), new ResiliencePipelineRegistry<string>(),
                mediator, schedulerFactory, tracer: tracer);
            await using var scheduler = Assert.IsType<InMemoryScheduler>(schedulerFactory.Create(processor));
            var subscription = new KafkaSubscription(new SubscriptionName(group), new ChannelName(group), topic,
                requestType: typeof(Command), groupId: group, commitBatchSize: 10,
                sweepUncommittedOffsetsInterval: TimeSpan.FromMinutes(5), makeChannels: OnMissingChannel.Validate,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
            { ScheduledRequeuePublicationTopic = alias };
            var factory = new KafkaMessageConsumerFactory(config, scheduler);
            consumer = useAsync ? (KafkaMessageConsumer)factory.CreateAsync(subscription) : (KafkaMessageConsumer)factory.Create(subscription);
            subscription.ScheduledRequeuePublicationTopic = topic;
            var original = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT, type: type), new MessageBody("scheduled-π"));
            original.Header.Bag["tenant"] = "scheduled-λ";
            using var publisher = new ProducerBuilder<string, byte[]>(new ProducerConfig
            { BootstrapServers = "localhost:9092", Acks = Confluent.Kafka.Acks.All }).Build();
            var sent = await publisher.ProduceAsync(topic.Value, new DefaultKafkaWireMessageWriter().WriteMessage(original), timeout.Token);
            Message? source = null;
            while (source == null)
            {
                timeout.Token.ThrowIfCancellationRequested();
                var received = useAsync ? await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(100), timeout.Token) : consumer.Receive(TimeSpan.FromMilliseconds(100));
                source = received.FirstOrDefault(message => message.Id == original.Id);
            }
            var sourceTrace = source.Header.TraceParent;
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            { BootstrapServers = "localhost:9092", GroupId = group, EnableAutoCommit = false }).Build();
            observer.Assign(new TopicPartitionOffset(sent.TopicPartition, new Offset(sent.Offset.Value + 1)));
            var delay = TimeSpan.FromSeconds(2);
            var elapsed = Stopwatch.StartNew();

            // Act
            Assert.True(useAsync ? await consumer.RequeueAsync(source, delay, timeout.Token) : consumer.Requeue(source, delay));

            // Assert
            Assert.False(published.Task.IsCompleted);
            var delivered = observer.Consume(timeout.Token);
            Assert.True(elapsed.Elapsed >= delay);
            var span = await published.Task.WaitAsync(timeout.Token);
            var payload = Encoding.UTF8.GetString(delivered.Message.Value);
            Assert.Equal(custom, payload.StartsWith("selected:", StringComparison.Ordinal));
            Assert.Equal(topic.Value, delivered.Topic);
            Assert.Equal(sent.Partition, delivered.Partition);
            Assert.Equal(sent.Offset.Value + 1, delivered.Offset.Value);
            var decoded = new DefaultKafkaWireMessageReader().ReadMessage(new ConsumeResult<string, byte[]>
            {
                Topic = delivered.Topic, Partition = delivered.Partition, Offset = delivered.Offset,
                Message = new Confluent.Kafka.Message<string, byte[]> { Key = delivered.Message.Key,
                    Headers = delivered.Message.Headers, Value = Encoding.UTF8.GetBytes(custom ? payload[9..] : payload) }
            });
            Assert.Equal(original.Id, decoded.Id);
            Assert.Equal(topic, decoded.Header.Topic);
            Assert.Equal(type, decoded.Header.Type);
            Assert.Equal("scheduled-π", decoded.Body.Value);
            Assert.Equal("scheduled-λ", decoded.Header.Bag["tenant"].ToString());
            Assert.Equal(span.Id, decoded.Header.TraceParent.Value);
            Assert.False(delivered.Message.Headers.TryGetLastBytes(Message.ProducerTopicHeaderName, out _));
            Assert.Equal(0, inactiveWriter.Calls);
            Assert.Equal(custom ? 1 : 0, writer.Calls);
            Assert.Equal(topic, source.Header.Topic);
            Assert.Equal(type, source.Header.Type);
            Assert.Equal("scheduled-π", source.Body.Value);
            Assert.Equal("scheduled-λ", source.Header.Bag["tenant"].ToString());
            Assert.Equal(sourceTrace, source.Header.TraceParent);
            Assert.False(source.Header.Bag.ContainsKey(Message.ProducerTopicHeaderName));
            while (outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()).Any())
                await Task.Delay(10, timeout.Token);
            Assert.Equal(0, observer.QueryWatermarkOffsets(new TopicPartition(alias.Value, new Partition(0)), TimeSpan.FromSeconds(10)).High.Value);
            if (useAsync) await consumer.DisposeAsync(); else consumer.Dispose();
            consumer = null;
            Assert.Equal(sent.Offset.Value + 1, Assert.Single(observer.Committed([sent.TopicPartition], TimeSpan.FromSeconds(10))).Offset.Value);
            Assert.Equal(sent.Offset.Value + 2, observer.QueryWatermarkOffsets(sent.TopicPartition, TimeSpan.FromSeconds(10)).High.Value);
        }
        finally
        {
            try
            {
                if (consumer != null)
                {
                    if (useAsync) await consumer.DisposeAsync(); else consumer.Dispose();
                }
            }
            finally { await admin.DeleteTopicsAsync([topic.Value, alias.Value]); }
        }
    }
}
