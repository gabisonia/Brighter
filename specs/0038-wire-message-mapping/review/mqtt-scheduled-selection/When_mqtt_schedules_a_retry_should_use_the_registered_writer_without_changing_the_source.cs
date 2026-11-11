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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.MQTT;
using Paramore.Brighter.MQTT.Tests.TestDoubles;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.MQTT.Tests.MessagingGateway;

/// <summary>Verifies MQTT scheduled retry selection through real callbacks and native delivery.</summary>
[Trait("Category", "MQTT")]
[Collection("MQTT")]
public class MqttScheduledRetrySelectionTests
{
    /// <summary>Retains publication selection, logical destination and fresh telemetry after delay.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_mqtt_schedules_a_retry_should_use_the_registered_writer_without_changing_the_source(bool useAsync, bool custom)
    {
        // Arrange
        var name = "mqtt-scheduled-" + Guid.NewGuid().ToString("N");
        var topic = new RoutingKey("logical-business");
        var alias = new RoutingKey("retry-publication");
        var type = new CloudEventsType("scheduled.mqtt.event");
        var host = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new MqttConsumerSelectedWireWriter(0);
        var inactiveWriter = new MqttConsumerSelectedWireWriter(0);
        MqttMessageProducer Producer(RoutingKey key, IAmAWireMessageWriter<MqttApplicationMessage>? selected)
            => new(new MqttMessagePublisher(new MqttMessagingGatewayProducerConfiguration
            { Hostname = host, Port = port, TopicPrefix = name, ClientID = name + "-" + key.Value, CleanSession = true }),
            new MqttPublication { Topic = key, Type = type, WireMessageWriter = selected }, InstrumentationOptions.All);
        using var registry = new ProducerRegistry(new Dictionary<ProducerKey, IAmAMessageProducer>
        {
            [new ProducerKey(alias, type)] = Producer(alias, custom ? writer : null),
            [new ProducerKey(topic, type)] = Producer(topic, custom ? null : inactiveWriter)
        });
        using var native = new MqttFactory().CreateMqttClient();
        var deliveries = new ConcurrentQueue<MqttApplicationMessage>();
        var initial = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        native.ApplicationMessageReceivedAsync += args =>
        {
            deliveries.Enqueue(new MqttApplicationMessage { Topic = args.ApplicationMessage.Topic,
                PayloadSegment = args.ApplicationMessage.PayloadSegment.ToArray() });
            initial.TrySetResult(true);
            return Task.CompletedTask;
        };
        await native.ConnectAsync(new MqttClientOptionsBuilder().WithTcpServer(host, port).WithClientId(name + "-native").WithCleanSession().Build(), timeout.Token);
        await native.SubscribeAsync(new MqttTopicFilter { Topic = name + "/#", QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce }).WaitAsync(timeout.Token);
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
            var subscription = new MqttSubscription(new SubscriptionName(name), new ChannelName(name), topic,
                requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
            { ScheduledRequeuePublicationTopic = alias };
            var consumers = new MqttMessageConsumerFactory(new MqttMessagingGatewayConsumerConfiguration
            { Hostname = host, Port = port, TopicPrefix = name, ClientID = name + "-consumer", CleanSession = true }, scheduler);
            await using var consumer = useAsync ? (MqttMessageConsumer)consumers.CreateAsync(subscription) : (MqttMessageConsumer)consumers.Create(subscription);
            subscription.ScheduledRequeuePublicationTopic = topic;
            var original = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT, type: type), new MessageBody("scheduled-π"));
            original.Header.Bag["tenant"] = "scheduled-λ";
            var input = new DefaultMqttWireMessageWriter().WriteMessage(original);
            input.Topic = name + "/" + topic.Value;
            input.QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce;
            await native.PublishAsync(input, timeout.Token);
            await initial.Task.WaitAsync(timeout.Token);
            Message? source = null;
            while (source == null)
            {
                timeout.Token.ThrowIfCancellationRequested();
                var received = useAsync ? await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(100), timeout.Token) : consumer.Receive(TimeSpan.FromMilliseconds(100));
                source = received.FirstOrDefault(message => message.Id == original.Id);
            }
            deliveries.Clear();
            var before = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
            var delay = TimeSpan.FromSeconds(2);
            var elapsed = Stopwatch.StartNew();

            // Act
            Assert.True(useAsync ? await consumer.RequeueAsync(source, delay, timeout.Token) : consumer.Requeue(source, delay));

            // Assert
            Assert.Equal(before, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.False(published.Task.IsCompleted);
            Assert.Empty(deliveries);
            var span = await published.Task.WaitAsync(timeout.Token);
            Assert.True(elapsed.Elapsed >= delay);
            while (deliveries.IsEmpty || outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()).Any())
                await Task.Delay(10, timeout.Token);
            var delivered = Assert.Single(deliveries);
            Assert.Equal(name + "/" + topic.Value, delivered.Topic);
            var payload = Encoding.UTF8.GetString(delivered.PayloadSegment);
            Assert.Equal(custom, payload.StartsWith("selected:", StringComparison.Ordinal));
            var decoded = new DefaultMqttWireMessageReader().ReadMessage(new MqttApplicationMessage
            { Topic = delivered.Topic, PayloadSegment = Encoding.UTF8.GetBytes(custom ? payload[9..] : payload) });
            Assert.Equal(original.Id, decoded.Id);
            Assert.Equal(topic, decoded.Header.Topic);
            Assert.Equal(type, decoded.Header.Type);
            Assert.Equal("scheduled-π", decoded.Body.Value);
            Assert.Equal("scheduled-λ", decoded.Header.Bag["tenant"].ToString());
            Assert.Equal(span.Id, decoded.Header.TraceParent.Value);
            Assert.Equal(0, inactiveWriter.Calls);
            Assert.Equal(custom ? 1 : 0, writer.Calls);
            Assert.Equal(before, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
    }
}
