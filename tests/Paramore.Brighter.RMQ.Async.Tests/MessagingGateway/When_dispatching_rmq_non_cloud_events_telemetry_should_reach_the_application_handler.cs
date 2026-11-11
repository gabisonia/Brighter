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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Transactions;
using System;
using OpenTelemetry.Trace;
using OpenTelemetry;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.Observability;
using Paramore.Brighter.RMQ.Async.Tests.TestDoubles;
using Paramore.Brighter.ServiceActivator;
using Polly.Registry;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
public class RmqNonCloudEventsPumpTelemetryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_dispatching_rmq_non_cloud_events_telemetry_should_reach_the_application_handler(
        bool useAsync, bool telemetryInBody)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var suffix = Guid.NewGuid().ToString("N");
        var exchangeName = $"Rmq-Telemetry-{suffix}";
        var queueName = $"Rmq-Telemetry-{suffix}";
        var connection = new RmqMessagingGatewayConnection
        {
            AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(exchangeName),
            PersistMessages = true
        };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
        await using var nativeChannel = await broker.CreateChannelAsync(cancellationToken: timeout.Token);
        var exchangeCreated = false;
        var queueCreated = false;
        using var traceProvider = Sdk.CreateTracerProviderBuilder().Build();
        using var tracer = new BrighterTracer();
        var producerSpans = new ConcurrentQueue<Activity>();
        var processSpans = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracer.ActivitySource),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.Kind == ActivityKind.Producer
                    && Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "publish"))
                    producerSpans.Enqueue(activity);
                if (Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "process"))
                    processSpans.Enqueue(activity);
            }
        };
        ActivitySource.AddActivityListener(listener);
        var originalBaggage = OpenTelemetry.Baggage.Current;
        IAmAChannel? channel = null;
        Task? pumpTask = null;

        try
        {
            OpenTelemetry.Baggage.Current = default;
            await nativeChannel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct, durable: false,
                autoDelete: false, cancellationToken: timeout.Token);
            exchangeCreated = true;
            await nativeChannel.QueueDeclareAsync(queueName, durable: true, exclusive: false,
                autoDelete: false, cancellationToken: timeout.Token);
            queueCreated = true;
            var topic = new RoutingKey("telemetry-business");
            await nativeChannel.QueueBindAsync(queueName, exchangeName, topic.Value, cancellationToken: timeout.Token);
            var publication = new RmqPublication
            {
                Topic = topic, MakeChannels = OnMissingChannel.Validate,
                WireMessageWriter = new NonCloudEventsRmqWireMessageWriter(telemetryInBody)
            };
            var factory = new RmqMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            using var registry = new ProducerRegistry(producers);
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(
                registry, new ResiliencePipelineRegistry<string>().AddBrighterDefault(),
                new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null),
                new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(),
                tracer, new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
            var confirmationReceived = new TaskCompletionSource<PublishConfirmationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.IsAssignableFrom<ISupportPublishConfirmation>(Assert.Single(producers).Value)
                .OnMessagePublished += result => confirmationReceived.TrySetResult(result);
            var context = new RequestContext();
            Message outgoing;
            string depositParent;
            using (var deposit = new Activity("non-cloud-deposit")
                       .SetIdFormat(ActivityIdFormat.W3C)
                       .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                       .Start())
            {
                depositParent = deposit.Id!;
                outgoing = new Message(new MessageHeader(new Id("non-cloud-application"), publication.Topic,
                    MessageType.MT_EVENT, traceParent: new TraceParent(depositParent),
                    traceState: new TraceState("vendor=deposit")), new MessageBody("application payload"));
                outgoing.Header.Baggage.Add("stage", "deposit");
                context.Span = deposit;
                if (useAsync)
                    await mediator.AddToOutboxAsync(outgoing, context, cancellationToken: timeout.Token);
                else
                    mediator.AddToOutbox(outgoing, context);
            }
            var deposited = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(depositParent, deposited.Header.TraceParent?.Value);
            Assert.Equal("vendor=deposit", deposited.Header.TraceState?.Value);
            Assert.Equal("stage=deposit", deposited.Header.Baggage.ToString());
            OpenTelemetry.Baggage.SetBaggage(new Dictionary<string, string?> { ["stage"] = "dispatch" });

            //Act
            using (var dispatch = new Activity("non-cloud-dispatch")
                       .SetIdFormat(ActivityIdFormat.W3C)
                       .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded))
            {
                dispatch.TraceStateString = "vendor=dispatch";
                dispatch.Start();
                context.Span = dispatch;
                if (useAsync)
                    await mediator.ClearOutboxAsync([outgoing.Id], context, cancellationToken: timeout.Token);
                else
                    mediator.ClearOutbox([outgoing.Id], context);
            }
            var confirmation = await confirmationReceived.Task.WaitAsync(timeout.Token);
            while (outbox.OutstandingMessages(TimeSpan.Zero, context).Any())
                await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
            BasicGetResult? native = null;
            while (native is null)
            {
                native = await nativeChannel.BasicGetAsync(queueName, autoAck: false, cancellationToken: timeout.Token);
                if (native is null)
                    await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
            }

            //Assert
            Assert.True(confirmation.Success);
            Assert.Equal(outgoing.Id, confirmation.MessageId);
            Assert.Equal(publication.Topic, confirmation.Topic);
            var producer = Assert.Single(producerSpans);
            Assert.NotNull(native.BasicProperties.Headers);
            Assert.True(native.BasicProperties.Headers.ContainsKey("legacy-id"));
            Assert.True(native.BasicProperties.Headers.ContainsKey(Constants.PublishSequenceNumberHeader));
            Assert.Equal(outgoing.Id.Value, native.BasicProperties.MessageId);
            Assert.Equal("legacy-telemetry", native.BasicProperties.Type);
            Assert.Equal(telemetryInBody ? "application/json" : "text/plain", native.BasicProperties.ContentType);
            Assert.Equal(DeliveryModes.Persistent, native.BasicProperties.DeliveryMode);
            var nativeAttributes = native.BasicProperties.Headers
                .Where(pair => pair.Key != Constants.PublishSequenceNumberHeader)
                .ToDictionary(pair => pair.Key, pair => Encoding.UTF8.GetString(Assert.IsType<byte[]>(pair.Value)));
            var nativeBody = Encoding.UTF8.GetString(native.Body.Span);
            string nativeParent;
            string nativeState;
            string nativeBaggage;
            if (telemetryInBody)
            {
                using var document = JsonDocument.Parse(nativeBody);
                Assert.Equal("application payload", document.RootElement.GetProperty("payload").GetString());
                nativeParent = document.RootElement.GetProperty("parent").GetString()!;
                nativeState = document.RootElement.GetProperty("state").GetString()!;
                nativeBaggage = document.RootElement.GetProperty("baggage").GetString()!;
                Assert.Single(nativeAttributes);
            }
            else
            {
                Assert.Equal("application payload", nativeBody);
                Assert.Equal(4, nativeAttributes.Count);
                nativeParent = nativeAttributes["telemetry-parent"];
                nativeState = nativeAttributes["telemetry-state"];
                nativeBaggage = nativeAttributes["telemetry-baggage"];
            }
            Assert.Equal("non-cloud-application", nativeAttributes["legacy-id"]);
            Assert.DoesNotContain(nativeAttributes.Keys, key => key.StartsWith("cloudEvents_", StringComparison.OrdinalIgnoreCase));
            Assert.Equal(producer.Id, nativeParent);
            Assert.NotEqual(depositParent, nativeParent);
            Assert.Equal("vendor=dispatch", nativeState);
            Assert.Equal("stage=dispatch", nativeBaggage);
            Assert.Equal("application payload", outgoing.Body.Value);
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));

            await nativeChannel.BasicNackAsync(native.DeliveryTag, multiple: false, requeue: true, cancellationToken: timeout.Token);
            var subscription = new RmqSubscription(new SubscriptionName(queueName), new ChannelName(queueName),
                publication.Topic, requestType: useAsync ? typeof(RmqPumpTelemetryAsyncCommand) : typeof(RmqPumpTelemetrySyncCommand),
                isDurable: true, messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = new NonCloudEventsRmqWireMessageReader(telemetryInBody)
            };
            var observed = new TaskCompletionSource<(string Id, string Payload, Activity? Span, string? Baggage)>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var invocations = 0;
            void Observe(Id id, string payload, IRequestContext? requestContext)
            {
                Interlocked.Increment(ref invocations);
                observed.TrySetResult((id.Value, payload, requestContext?.Span, OpenTelemetry.Baggage.GetBaggage("stage")));
            }
            var subscribers = new SubscriberRegistry();
            var requestContexts = new InMemoryRequestContextFactory();
            var channelFactory = new ChannelFactory(new RmqMessageConsumerFactory(connection));
            IAmAMessagePump pump;
            if (useAsync)
            {
                var handler = new RmqPumpTelemetryAsyncHandler(Observe);
                subscribers.RegisterAsync<RmqPumpTelemetryAsyncCommand, RmqPumpTelemetryAsyncHandler>();
                var processor = new CommandProcessor(subscribers, new QuickHandlerFactoryAsync(() => handler),
                    requestContexts, new PolicyRegistry(), new ResiliencePipelineRegistry<string>(),
                    new InMemorySchedulerFactory(), tracer: tracer);
                var mappers = new MessageMapperRegistry(null,
                    new SimpleMessageMapperFactoryAsync(_ => new RmqPumpTelemetryAsyncMapper()));
                mappers.RegisterAsync<RmqPumpTelemetryAsyncCommand, RmqPumpTelemetryAsyncMapper>();
                var asyncChannel = await channelFactory.CreateAsyncChannelAsync(subscription, timeout.Token);
                channel = asyncChannel;
                pump = new Paramore.Brighter.ServiceActivator.Proactor(processor, _ => typeof(RmqPumpTelemetryAsyncCommand), mappers,
                    new EmptyMessageTransformerFactoryAsync(), requestContexts, asyncChannel, tracer)
                {
                    TimeOut = TimeSpan.FromMilliseconds(200), RequeueCount = 0
                };
            }
            else
            {
                var handler = new RmqPumpTelemetrySyncHandler(Observe);
                subscribers.Register<RmqPumpTelemetrySyncCommand, RmqPumpTelemetrySyncHandler>();
                var processor = new CommandProcessor(subscribers, new QuickHandlerFactory(() => handler),
                    requestContexts, new PolicyRegistry(), new ResiliencePipelineRegistry<string>(),
                    new InMemorySchedulerFactory(), tracer: tracer);
                var mappers = new MessageMapperRegistry(
                    new SimpleMessageMapperFactory(_ => new RmqPumpTelemetrySyncMapper()), null);
                mappers.Register<RmqPumpTelemetrySyncCommand, RmqPumpTelemetrySyncMapper>();
                var syncChannel = channelFactory.CreateSyncChannel(subscription);
                channel = syncChannel;
                pump = new Paramore.Brighter.ServiceActivator.Reactor(processor, _ => typeof(RmqPumpTelemetrySyncCommand), mappers,
                    new EmptyMessageTransformerFactory(), requestContexts, syncChannel, tracer)
                {
                    TimeOut = TimeSpan.FromMilliseconds(200), RequeueCount = 0
                };
            }

            OpenTelemetry.Baggage.SetBaggage(new Dictionary<string, string?> { ["stage"] = "unrelated" });
            using var unrelated = new Activity("unrelated-pump-ambient")
                .SetIdFormat(ActivityIdFormat.W3C)
                .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                .Start();

            //Act
            pumpTask = Task.Factory.StartNew(pump.Run, CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
            var handled = await observed.Task.WaitAsync(timeout.Token);
            channel.Enqueue(MessageFactory.CreateQuitMessage(publication.Topic));
            await pumpTask.WaitAsync(timeout.Token);

            //Assert
            Assert.Equal(1, Volatile.Read(ref invocations));
            Assert.Equal(outgoing.Id.Value, handled.Id);
            Assert.Equal("application payload", handled.Payload);
            var process = Assert.Single(processSpans);
            Assert.Equal(producer.TraceId, process.TraceId);
            Assert.Equal(producer.SpanId, process.ParentSpanId);
            Assert.NotEqual(unrelated.TraceId, process.TraceId);
            Assert.Equal("vendor=dispatch", process.TraceStateString);
            Assert.NotNull(handled.Span);
            Assert.Equal(producer.TraceId, handled.Span.TraceId);
            Assert.Equal("vendor=dispatch", handled.Span.TraceStateString);
            Assert.Equal("dispatch", handled.Baggage);
            if (channel is IAmAChannelAsync asyncInput)
                await asyncInput.DisposeAsync();
            else
                channel.Dispose();
            channel = null;
            var remaining = await nativeChannel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token);
            Assert.Null(remaining);
        }
        finally
        {
            OpenTelemetry.Baggage.Current = originalBaggage;
            try
            {
                if (channel != null)
                {
                    try
                    {
                        if (pumpTask != null)
                        {
                            if (!pumpTask.IsCompleted)
                                channel.Enqueue(MessageFactory.CreateQuitMessage(new RoutingKey(queueName)));
                            await pumpTask.WaitAsync(TimeSpan.FromSeconds(10));
                        }
                    }
                    finally
                    {
                        if (channel is IAmAChannelAsync asyncInput)
                            await asyncInput.DisposeAsync();
                        else
                            channel.Dispose();
                    }
                }
            }
            finally
            {
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                try
                {
                    if (queueCreated)
                        await nativeChannel.QueueDeleteAsync(queueName, ifUnused: false, ifEmpty: false, cancellationToken: cleanup.Token);
                }
                finally
                {
                    if (exchangeCreated)
                        await nativeChannel.ExchangeDeleteAsync(exchangeName, cancellationToken: cleanup.Token);
                }
            }
        }
    }
}
