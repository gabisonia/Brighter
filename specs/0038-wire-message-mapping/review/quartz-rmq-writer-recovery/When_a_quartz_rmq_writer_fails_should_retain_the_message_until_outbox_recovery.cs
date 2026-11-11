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
using System.Collections.Concurrent;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessageScheduler.Quartz;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.Observability;
using Paramore.Brighter.RMQ.Async.Tests.TestDoubles;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using Quartz;
using Quartz.Impl.Matchers;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
public class QuartzRmqWriterRecoveryTests
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
    public async Task When_a_quartz_rmq_writer_fails_should_retain_the_message_until_outbox_recovery(
        bool useRequest, bool useAsync, bool returnNull)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"Quartz-Recovery-{Guid.NewGuid():N}";
        var topic = new RoutingKey("quartz-wire-business");
        var connection = new RmqMessagingGatewayConnection
        {
            AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name), PersistMessages = true
        };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
        await using var nativeChannel = await broker.CreateChannelAsync(cancellationToken: timeout.Token);
        using var traceProvider = Sdk.CreateTracerProviderBuilder().Build();
        using var tracer = new BrighterTracer();
        var spans = new ConcurrentQueue<Activity>();
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracer.ActivitySource),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.Kind == ActivityKind.Producer
                    && Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "publish"))
                    spans.Enqueue(activity);
            }
        };
        ActivitySource.AddActivityListener(activityListener);
        var exchangeCreated = false;
        var queueCreated = false;
        try
        {
            await nativeChannel.ExchangeDeclareAsync(name, ExchangeType.Direct, durable: false,
                autoDelete: false, cancellationToken: timeout.Token);
            exchangeCreated = true;
            await nativeChannel.QueueDeclareAsync(name, durable: true, exclusive: false,
                autoDelete: false, cancellationToken: timeout.Token);
            queueCreated = true;
            await nativeChannel.QueueBindAsync(name, name, topic.Value, cancellationToken: timeout.Token);
            var writer = new QuartzRecoverableRmqWireWriter(returnNull);
            var publication = new RmqPublication
            {
                Topic = topic, RequestType = typeof(QuartzWireCommand), MakeChannels = OnMissingChannel.Validate,
                WireMessageWriter = writer
            };
            var producerFactory = new RmqMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await producerFactory.CreateAsync() : producerFactory.Create();
            using var registry = new ProducerRegistry(producers);
            var producer = Assert.Single(producers).Value;
            var confirmed = new TaskCompletionSource<PublishConfirmationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            var confirmations = new ConcurrentQueue<PublishConfirmationResult>();
            Assert.IsAssignableFrom<ISupportPublishConfirmation>(producer).OnMessagePublished += result =>
            {
                confirmations.Enqueue(result);
                if (result.Success)
                    confirmed.TrySetResult(result);
            };
            using var mappers = new MessageMapperRegistry(
                new SimpleMessageMapperFactory(_ => new QuartzWireCommandMapper()),
                new SimpleMessageMapperFactoryAsync(_ => new QuartzWireCommandMapper()));
            mappers.Register<QuartzWireCommand, QuartzWireCommandMapper>();
            mappers.RegisterAsync<QuartzWireCommand, QuartzWireCommandMapper>();
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(registry,
                new ResiliencePipelineRegistry<string>().AddBrighterDefault(), mappers,
                new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer,
                new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
            var quartz = await SchedulerBuilder.Create(new NameValueCollection
                { ["quartz.scheduler.instanceName"] = name }).Build().GetScheduler(timeout.Token);
            try
            {
                var schedulerFactory = new QuartzSchedulerFactory(quartz) { Group = name };
                CommandProcessor? processor = null;
                var subscribers = new SubscriberRegistry();
                subscribers.RegisterAsync<FireSchedulerMessage, FireSchedulerMessageHandler>();
                subscribers.RegisterAsync<FireSchedulerRequest, FireSchedulerRequestHandler>();
                var handlers = new SimpleHandlerFactoryAsync(type => type == typeof(FireSchedulerMessageHandler)
                    ? new FireSchedulerMessageHandler(processor!)
                    : new FireSchedulerRequestHandler(processor!, tracer));
                processor = new CommandProcessor(subscribers, handlers, new InMemoryRequestContextFactory(),
                    new PolicyRegistry(), new ResiliencePipelineRegistry<string>(), mediator, schedulerFactory, tracer: tracer);
                quartz.JobFactory = new QuartzWireJobFactory(processor);
                var completion = new QuartzWireCompletionListener();
                quartz.ListenerManager.AddJobListener(completion, GroupMatcher<JobKey>.AnyGroup());
                producer.Scheduler = schedulerFactory.Create(processor);
                var request = new QuartzWireCommand { Payload = "scheduled native payload" };
                var scheduleParent = $"00-{ActivityTraceId.CreateRandom()}-{ActivitySpanId.CreateRandom()}-01";
                var source = new Message(new MessageHeader(request.Id, topic, MessageType.MT_COMMAND,
                    traceParent: new TraceParent(scheduleParent)), new MessageBody(request.Payload)) { Persist = true };
                source.Header.Bag["tenant"] = "scheduled-tenant";
                var sourceBefore = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
                var requestBefore = JsonSerializer.Serialize(request, JsonSerialisationOptions.Options);
                var delay = TimeSpan.FromSeconds(1);
                var context = new RequestContext();

                //Act
                if (useRequest)
                {
                    if (useAsync)
                        await schedulerFactory.CreateAsync(processor).ScheduleAsync(request, RequestSchedulerType.Post, delay, timeout.Token);
                    else
                        schedulerFactory.CreateSync(processor).Schedule(request, RequestSchedulerType.Post, delay);
                }
                else if (useAsync)
                {
                    await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendWithDelayAsync(source, delay, timeout.Token);
                }
                else
                {
                    Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).SendWithDelay(source, delay);
                }

                //Assert
                Assert.Empty(spans);
                Assert.Equal(0, writer.FailedAttempts);
                Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
                Assert.Null(await nativeChannel.BasicGetAsync(name, autoAck: true, cancellationToken: timeout.Token));
                Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
                Assert.Equal(requestBefore, JsonSerializer.Serialize(request, JsonSerialisationOptions.Options));

                //Act
                await quartz.Start(timeout.Token);
                var jobError = await completion.Completion.WaitAsync(timeout.Token);
                await quartz.Shutdown(waitForJobsToComplete: true, cancellationToken: timeout.Token);

                //Assert
                Assert.Null(jobError);
                Assert.True(writer.FailedAttempts > 0);
                Assert.Null(await nativeChannel.BasicGetAsync(name, autoAck: true, cancellationToken: timeout.Token));
                Assert.DoesNotContain(confirmations, result => result.Success);
                var pending = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
                Assert.Equal(request.Id, pending.Id);
                Assert.Equal(topic, pending.Header.Topic);
                Assert.Equal("scheduled native payload", pending.Body.Value);
                Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
                Assert.Equal(requestBefore, JsonSerializer.Serialize(request, JsonSerialisationOptions.Options));
                var failedSpanIds = spans.Select(span => span.Id).ToArray();
                Assert.NotEmpty(failedSpanIds);
                spans.Clear();
                using var recovery = new Activity("quartz-outbox-recovery")
                    .SetIdFormat(ActivityIdFormat.W3C)
                    .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                    .Start();
                context.Span = recovery;

                //Act
                writer.FailOnWrite = false;
                if (useAsync)
                    await mediator.ClearOutboxAsync([request.Id], context, cancellationToken: timeout.Token);
                else
                    mediator.ClearOutbox([request.Id], context);
                var confirmation = await confirmed.Task.WaitAsync(timeout.Token);

                //Assert
                Assert.True(confirmation.Success);
                Assert.Equal(request.Id, confirmation.MessageId);
                Assert.Equal(topic, confirmation.Topic);
                while (outbox.OutstandingMessages(TimeSpan.Zero, context).Any())
                    await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
                var native = await nativeChannel.BasicGetAsync(name, autoAck: true, cancellationToken: timeout.Token);
                Assert.NotNull(native);
                Assert.NotNull(native.BasicProperties.Headers);
                Assert.True(native.BasicProperties.Headers.ContainsKey("legacy-id"));
                Assert.True(native.BasicProperties.Headers.ContainsKey(Constants.PublishSequenceNumberHeader));
                Assert.Equal(request.Id.Value, native.BasicProperties.MessageId);
                Assert.Equal("legacy-telemetry", native.BasicProperties.Type);
                Assert.Equal("text/plain", native.BasicProperties.ContentType);
                Assert.Equal(DeliveryModes.Persistent, native.BasicProperties.DeliveryMode);
                var attributes = native.BasicProperties.Headers.Where(p => p.Key != Constants.PublishSequenceNumberHeader)
                    .ToDictionary(p => p.Key, p => Encoding.UTF8.GetString(Assert.IsType<byte[]>(p.Value)));
                Assert.Equal(request.Id.Value, attributes["legacy-id"]);
                var payload = Encoding.UTF8.GetString(native.Body.Span);
                var parent = attributes["telemetry-parent"];
                Assert.Equal(4, attributes.Count);
                Assert.Equal("scheduled native payload", payload);
                var span = Assert.Single(spans);
                Assert.Equal(span.Id, parent);
                Assert.Equal(recovery.SpanId, span.ParentSpanId);
                Assert.DoesNotContain(parent, failedSpanIds);
                Assert.NotEqual(scheduleParent, parent);
                Assert.Single(confirmations.Where(result => result.Success));
                Assert.DoesNotContain(attributes.Keys, key => key.StartsWith("cloudEvents_", StringComparison.OrdinalIgnoreCase));
                Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
                Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
                Assert.Equal(requestBefore, JsonSerializer.Serialize(request, JsonSerialisationOptions.Options));
                Assert.Null(await nativeChannel.BasicGetAsync(name, autoAck: true, cancellationToken: timeout.Token));
            }
            finally
            {
                if (!quartz.IsShutdown)
                    await quartz.Shutdown(waitForJobsToComplete: true);
            }
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try
            {
                if (queueCreated)
                    await nativeChannel.QueueDeleteAsync(name, ifUnused: false, ifEmpty: false, cancellationToken: cleanup.Token);
            }
            finally
            {
                if (exchangeCreated)
                    await nativeChannel.ExchangeDeleteAsync(name, cancellationToken: cleanup.Token);
            }
        }
    }
}
