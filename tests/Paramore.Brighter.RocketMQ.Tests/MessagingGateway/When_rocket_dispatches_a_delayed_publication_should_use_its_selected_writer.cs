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
using System.Diagnostics;
using System.Text;
using System.Transactions;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Org.Apache.Rocketmq;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.MessagingGateway.RocketMQ;
using Paramore.Brighter.Observability;
using Paramore.Brighter.RocketMQ.Tests.TestDoubles;
using Paramore.Brighter.RocketMQ.Tests.Utils;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.RocketMQ.Tests.MessagingGateway;

/// <summary>Verifies writer selection through native delay and real scheduler callbacks.</summary>
[Trait("Category", "RocketMQ")]
[Collection("RocketMQMessagingGateway")]
public class RocketScheduledPublicationSelectionTests
{
    /// <summary>Retains selected body, telemetry, native destination, keys and tag on delayed sends.</summary>
    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task When_rocket_dispatches_a_delayed_publication_should_use_its_selected_writer(bool useAsync, bool nativeDelay, bool custom)
    {
        // Arrange
        var connection = GatewayFactory.CreateConnection();
        var topic = new RoutingKey(nativeDelay
            ? useAsync ? "gen_p_delayed_msg" : "gen_r_delayed_msg"
            : useAsync ? "rmq_mapping_p" : "rmq_mapping_r");
        var tag = "scheduled-" + Guid.NewGuid().ToString("N");
        var type = new CloudEventsType("scheduled.rocket.event");
        var writer = new RocketSelectedWireWriter(true);
        var inactive = new RocketSelectedWireWriter(true);
        var publication = new RocketMqPublication
        {
            Topic = topic, Type = type, Tag = tag,
            TopicType = nativeDelay ? TopicType.Delay : TopicType.Normal,
            WireMessageWriter = custom ? writer : null
        };
        await using var nativeProducer = await GatewayFactory.CreateProducer(connection, publication);
        using var registry = new ProducerRegistry(new Dictionary<ProducerKey, IAmAMessageProducer>
        {
            [new ProducerKey(topic, type)] = new RocketMqMessageProducer(connection, nativeProducer, publication),
            [new ProducerKey(topic, new CloudEventsType("inactive.rocket.event"))] = new RocketMqMessageProducer(connection, nativeProducer,
                new RocketMqPublication { Topic = topic, Type = new CloudEventsType("inactive.rocket.event"), Tag = tag, WireMessageWriter = inactive })
        });
        await using var observer = await new SimpleConsumer.Builder()
            .SetClientConfig(connection.ClientConfig).SetConsumerGroup(Guid.NewGuid().ToString("N"))
            .SetAwaitDuration(TimeSpan.FromSeconds(1))
            .SetSubscriptionExpression(new Dictionary<string, FilterExpression> { [topic.Value] = new(tag) }).Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
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
        var producer = Assert.IsType<RocketMqMessageProducer>(registry.LookupBy(topic, type));
        producer.Scheduler = scheduler;
        var source = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT, type: type), new MessageBody("scheduled-π"));
        source.Header.Bag["marker"] = "scheduled-λ";
        source.Header.Bag[HeaderNames.Keys] = "key-" + tag;
        source.Header.Bag[HeaderNames.Tag] = tag;
        source.Header.Baggage.Add("scheduled", "rocket");
        using var parent = new Activity("schedule RocketMQ").SetIdFormat(ActivityIdFormat.W3C).Start();
        parent.TraceStateString = "vendor=scheduled";
        producer.Span = parent;
        var previousBaggage = OpenTelemetry.Baggage.Current;
        OpenTelemetry.Baggage.SetBaggage("scheduled", "rocket");
        var delay = TimeSpan.FromSeconds(3);
        var elapsed = Stopwatch.StartNew();
        try
        {
            // Act
            if (useAsync) await producer.SendWithDelayAsync(source, delay, timeout.Token);
            else producer.SendWithDelay(source, delay);

            // Assert
            Assert.Equal(nativeDelay && custom ? 1 : 0, writer.Calls);
            Assert.False(published.Task.IsCompleted);
            var firstPull = observer.Receive(1, TimeSpan.FromSeconds(30));
            var early = await Task.WhenAny(firstPull, Task.Delay(TimeSpan.FromSeconds(1), timeout.Token));
            MessageView? delivered = null;
            if (ReferenceEquals(early, firstPull)) Assert.Empty(await firstPull);
            else delivered = (await firstPull).SingleOrDefault();
            while (delivered == null)
            {
                timeout.Token.ThrowIfCancellationRequested();
                delivered = (await observer.Receive(1, TimeSpan.FromSeconds(30))).SingleOrDefault();
            }
            Assert.True(elapsed.Elapsed >= delay);
            var dispatchSpan = nativeDelay ? parent : await published.Task.WaitAsync(timeout.Token);
            Assert.Equal(custom ? "Ccheduled-π" : "scheduled-π", Encoding.UTF8.GetString(delivered.Body));
            Assert.Equal(topic.Value, delivered.Topic);
            Assert.Equal(tag, delivered.Tag);
            Assert.Contains("key-" + tag, delivered.Keys);
            Assert.Equal(source.Id.Value, delivered.Properties[HeaderNames.MessageId]);
            Assert.Equal(type.Value, delivered.Properties[HeaderNames.Type]);
            Assert.Equal("scheduled-λ", delivered.Properties["marker"]);
            Assert.Equal(dispatchSpan.Id, delivered.Properties[HeaderNames.TraceParent]);
            Assert.Equal("vendor=scheduled", delivered.Properties[HeaderNames.TraceState]);
            Assert.Contains("scheduled=rocket", delivered.Properties[HeaderNames.Baggage]);
            if (!nativeDelay) Assert.NotEqual(parent.Id, dispatchSpan.Id);
            Assert.Equal(custom ? 1 : 0, writer.Calls);
            Assert.Equal(0, inactive.Calls);
            Assert.Equal("scheduled-π", source.Body.Value);
            Assert.Equal(type, source.Header.Type);
            Assert.Equal(topic, source.Header.Topic);
            await observer.Ack(delivered);
            while (outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()).Any())
                await Task.Delay(10, timeout.Token);
            Assert.Empty(await observer.Receive(1, TimeSpan.FromSeconds(30)));
        }
        finally { OpenTelemetry.Baggage.Current = previousBaggage; }
    }
}
