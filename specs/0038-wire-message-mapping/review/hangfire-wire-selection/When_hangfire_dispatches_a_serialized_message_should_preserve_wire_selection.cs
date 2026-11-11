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
using System.Collections.Concurrent;
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
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.Observability;
using Hangfire;
using Hangfire.InMemory;
using Paramore.Brighter.Hangfire.Tests.TestDoubles;
using Paramore.Brighter.MessageScheduler.Hangfire;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.Hangfire.Tests;

/// <summary>
/// Verifies serialized publication selection through actual Hangfire job dispatch.
/// </summary>
[Trait("Category", "RMQ")]
[Collection("Scheduler")]
public class HangfireSelectedWireDispatchTests
{
    /// <summary>
    /// The job retains the selected publication, payload and event type through serialization.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_hangfire_dispatches_a_serialized_message_should_preserve_wire_selection(bool useAsync)
    {
        // Arrange
        var name = "hangfire-selection-" + Guid.NewGuid().ToString("N");
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var topic = new RoutingKey("logical-business");
        var alias = new RoutingKey("local-retry-publication");
        var type = new CloudEventsType("scheduled.retry.event");
        var connection = new RmqMessagingGatewayConnection
        {
            AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name), PersistMessages = true
        };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
        await using var native = await broker.CreateChannelAsync(cancellationToken: timeout.Token);
        await native.ExchangeDeclareAsync(name, ExchangeType.Direct, durable: false,
            autoDelete: false, cancellationToken: timeout.Token);
        try
        {
            await native.QueueDeclareAsync(name, durable: true, exclusive: false,
                autoDelete: false, cancellationToken: timeout.Token);
            await native.QueueBindAsync(name, name, topic.Value, cancellationToken: timeout.Token);
            var publication = new RmqPublication
            {
                Topic = alias, Type = type,
                MakeChannels = OnMissingChannel.Validate,
                WireMessageWriter = new ScheduledMessageWireWriter()
            };
            var factory = new RmqMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            using var registry = new ProducerRegistry(producers);
            var confirmation = new TaskCompletionSource<PublishConfirmationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.IsAssignableFrom<ISupportPublishConfirmation>(Assert.Single(producers).Value)
                .OnMessagePublished += result => confirmation.TrySetResult(result);
            using var telemetry = Sdk.CreateTracerProviderBuilder().Build();
            using var tracer = new BrighterTracer();
            var spans = new ConcurrentQueue<Activity>();
            var publishStopped = new TaskCompletionSource<Activity>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var listener = new ActivityListener
            {
                ShouldListenTo = source => ReferenceEquals(source, tracer.ActivitySource),
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    if (activity.Kind == ActivityKind.Producer
                        && Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "publish"))
                    {
                        spans.Enqueue(activity);
                        publishStopped.TrySetResult(activity);
                    }
                }
            };
            ActivitySource.AddActivityListener(listener);
            using var mappers = new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null);
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(registry,
                new ResiliencePipelineRegistry<string>().AddBrighterDefault(), mappers,
                new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer,
                new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
            using var storage = new InMemoryStorage(new InMemoryStorageOptions { IdType = InMemoryStorageIdType.Guid });
            GlobalConfiguration.Configuration.UseStorage(storage).UseRecommendedSerializerSettings();
            var schedulerFactory = new HangfireMessageSchedulerFactory { Client = new BackgroundJobClient(storage) };
            CommandProcessor? processor = null;
            var subscribers = new SubscriberRegistry();
            subscribers.RegisterAsync<FireSchedulerMessage, FireSchedulerMessageHandler>();
            processor = new CommandProcessor(subscribers,
                new SimpleHandlerFactoryAsync(_ => new FireSchedulerMessageHandler(processor!)),
                new InMemoryRequestContextFactory(), new PolicyRegistry(), new ResiliencePipelineRegistry<string>(),
                mediator, schedulerFactory, tracer: tracer);
            var scheduler = schedulerFactory.Create(processor);
            var source = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT,
                type: type), new MessageBody("serialized-payload-π"));
            source.Header.Bag[Message.ProducerTopicHeaderName] = alias.Value;
            source.Header.Bag["tenant"] = "scheduled-tenant";
            source.Header.TraceParent = new TraceParent("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01");
            var scheduledParent = source.Header.TraceParent.Value;
            var before = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
            var delay = TimeSpan.FromSeconds(2);

            // Act
            var jobId = useAsync
                ? await Assert.IsAssignableFrom<IAmAMessageSchedulerAsync>(scheduler).ScheduleAsync(source, delay, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageSchedulerSync>(scheduler).Schedule(source, delay);

            // Assert
            Assert.False(string.IsNullOrEmpty(jobId));
            Assert.Equal(before, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.Empty(spans);
            Assert.False(confirmation.Task.IsCompleted);
            Assert.Null(await native.BasicGetAsync(name, true, timeout.Token));
            source.Body = new MessageBody("changed-after-scheduling");
            source.Header.Bag[Message.ProducerTopicHeaderName] = "changed-publication";
            source.Header.Bag["tenant"] = "changed-tenant";

            // Act
            BrighterActivator.Processor = processor;
            using var server = new BackgroundJobServer(new BackgroundJobServerOptions
            {
                WorkerCount = 1, SchedulePollingInterval = TimeSpan.FromMilliseconds(50),
                Activator = new BrighterActivator()
            }, storage);
            using var jobs = storage.GetConnection();
            var dispatchError = await Record.ExceptionAsync(async () => await confirmation.Task.WaitAsync(timeout.Token));
            Assert.Null(dispatchError is null ? null : new Exception(
                JsonSerializer.Serialize(jobs.GetStateData(jobId)), dispatchError));
            var confirmed = await confirmation.Task;
            await publishStopped.Task.WaitAsync(timeout.Token);
            while (jobs.GetStateData(jobId)?.Name != "Succeeded"
                || outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()).Any())
                await Task.Delay(10, timeout.Token);
            var delivered = await native.BasicGetAsync(name, true, timeout.Token);

            // Assert
            Assert.True(confirmed.Success);
            Assert.Equal(source.Id, confirmed.MessageId);
            Assert.Equal(topic, confirmed.Topic);
            Assert.NotNull(delivered);
            Assert.Equal(name, delivered.Exchange);
            Assert.Equal(topic.Value, delivered.RoutingKey);
            Assert.Equal(source.Id.Value, delivered.BasicProperties.MessageId);
            Assert.Equal(DeliveryModes.Persistent, delivered.BasicProperties.DeliveryMode);
            Assert.Equal("serialized-payload-π", Encoding.UTF8.GetString(delivered.Body.Span));
            Assert.NotNull(delivered.BasicProperties.Headers);
            var traceKey = "telemetry-parent";
            var parent = Encoding.UTF8.GetString(Assert.IsType<byte[]>(delivered.BasicProperties.Headers[traceKey]));
            Assert.Equal(Assert.Single(spans).Id, parent);
            Assert.NotEqual(scheduledParent, parent);
            Assert.Equal(type.Value, delivered.BasicProperties.Type);
            Assert.Equal("scheduled-tenant", Encoding.UTF8.GetString(Assert.IsType<byte[]>(delivered.BasicProperties.Headers["tenant"])));
            Assert.False(delivered.BasicProperties.Headers.ContainsKey(Message.ProducerTopicHeaderName));
            Assert.Null(await native.BasicGetAsync(name, true, timeout.Token));
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()));
            Assert.Equal("changed-after-scheduling", source.Body.Value);
            Assert.Equal("changed-publication", source.Header.Bag[Message.ProducerTopicHeaderName]);
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await native.QueueDeleteAsync(name, false, false, cancellationToken: cleanup.Token);
            await native.ExchangeDeleteAsync(name, cancellationToken: cleanup.Token);
        }
    }
}
