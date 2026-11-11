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
using Paramore.Brighter.RMQ.Async.Tests.TestDoubles;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

/// <summary>
/// Verifies explicit delayed-retry publication selection and queue isolation.
/// </summary>
[Trait("Category", "RMQ")]
public class RmqScheduledRetrySelectionTests
{
    /// <summary>
    /// A scheduled retry uses its captured publication without changing its source message.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_rmq_subscription_selects_scheduled_retry_mapping_should_target_only_its_queue(
        bool useAsync, bool custom)
    {
        // Arrange
        var name = "scheduled-selection-" + Guid.NewGuid().ToString("N");
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var queue = new ChannelName(name);
        var sibling = name + "-sibling";
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
            await native.QueueDeclareAsync(sibling, durable: true, exclusive: false,
                autoDelete: false, cancellationToken: timeout.Token);
            await native.QueueBindAsync(name, name, topic.Value, cancellationToken: timeout.Token);
            await native.QueueBindAsync(sibling, name, topic.Value, cancellationToken: timeout.Token);
            var publication = new RmqPublication
            {
                Topic = alias, Type = type, DestinationQueue = queue,
                MakeChannels = OnMissingChannel.Validate,
                WireMessageWriter = custom ? new NonCloudEventsRmqWireMessageWriter(false) : null
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
            var schedulerFactory = new InMemorySchedulerFactory();
            CommandProcessor? processor = null;
            var subscribers = new SubscriberRegistry();
            subscribers.RegisterAsync<FireSchedulerMessage, FireSchedulerMessageHandler>();
            processor = new CommandProcessor(subscribers,
                new SimpleHandlerFactoryAsync(_ => new FireSchedulerMessageHandler(processor!)),
                new InMemoryRequestContextFactory(), new PolicyRegistry(), new ResiliencePipelineRegistry<string>(),
                mediator, schedulerFactory, tracer: tracer);
            await using var scheduler = Assert.IsType<InMemoryScheduler>(schedulerFactory.Create(processor));
            var subscription = new RmqSubscription(new SubscriptionName(name), queue, topic,
                requestType: typeof(Message), makeChannels: OnMissingChannel.Validate,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
            {
                ScheduledRequeuePublicationTopic = alias,
                RequeueWireMessageWriter = new RecoverableRmqWireMessageWriter(false)
            };
            var consumerFactory = new RmqMessageConsumerFactory(connection, scheduler);
            await using var consumer = useAsync
                ? (RmqMessageConsumer)consumerFactory.CreateAsync(subscription)
                : (RmqMessageConsumer)consumerFactory.Create(subscription);
            subscription.ScheduledRequeuePublicationTopic = topic;
            var original = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT,
                type: type, handledCount: 7), new MessageBody("original-payload"));
            original.Header.Bag["tenant"] = "scheduled-tenant";
            var initial = new DefaultRmqWireMessageWriter().WriteMessage(original);
            await native.BasicPublishAsync(string.Empty, name, false, initial.Properties, initial.Body, timeout.Token);
            var source = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(2), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(2)));
            Assert.Equal(original.Id, source.Id);
            source.Body = new MessageBody("revised-payload-π");
            var before = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
            var delay = TimeSpan.FromSeconds(3);
            var elapsed = Stopwatch.StartNew();

            // Act
            var accepted = useAsync
                ? await consumer.RequeueAsync(source, delay, timeout.Token)
                : consumer.Requeue(source, delay);

            // Assert
            Assert.True(accepted);
            Assert.Equal(before, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.Empty(spans);
            Assert.False(confirmation.Task.IsCompleted);
            await consumer.DisposeAsync();
            Assert.Null(await native.BasicGetAsync(name, true, timeout.Token));
            Assert.Null(await native.BasicGetAsync(sibling, true, timeout.Token));
            Assert.True(elapsed.Elapsed < delay);

            // Act
            var dispatchError = await Record.ExceptionAsync(async () => await confirmation.Task.WaitAsync(timeout.Token));
            Assert.Null(dispatchError);
            var confirmed = await confirmation.Task;
            while (outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()).Any())
                await Task.Delay(10, timeout.Token);
            await publishStopped.Task.WaitAsync(timeout.Token);
            var delivered = await native.BasicGetAsync(name, true, timeout.Token);

            // Assert
            Assert.True(confirmed.Success);
            Assert.Equal(original.Id, confirmed.MessageId);
            Assert.Equal(topic, confirmed.Topic);
            Assert.True(elapsed.Elapsed >= delay);
            Assert.NotNull(delivered);
            Assert.Equal(string.Empty, delivered.Exchange);
            Assert.Equal(name, delivered.RoutingKey);
            Assert.Equal(original.Id.Value, delivered.BasicProperties.MessageId);
            Assert.Equal(DeliveryModes.Persistent, delivered.BasicProperties.DeliveryMode);
            Assert.Equal("revised-payload-π", Encoding.UTF8.GetString(delivered.Body.Span));
            Assert.NotNull(delivered.BasicProperties.Headers);
            var traceKey = custom ? "telemetry-parent" : HeaderNames.CLOUD_EVENTS_TRACE_PARENT;
            var parent = Encoding.UTF8.GetString(Assert.IsType<byte[]>(delivered.BasicProperties.Headers[traceKey]));
            Assert.Equal(Assert.Single(spans).Id, parent);
            Assert.Equal(custom ? "legacy-telemetry" : initial.Properties.Type, delivered.BasicProperties.Type);
            Assert.False(delivered.BasicProperties.Headers.ContainsKey(Message.ProducerTopicHeaderName));
            Assert.Null(await native.BasicGetAsync(name, true, timeout.Token));
            Assert.Null(await native.BasicGetAsync(sibling, true, timeout.Token));
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()));
            Assert.Equal(before, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await native.QueueDeleteAsync(name, false, false, cancellationToken: cleanup.Token);
            await native.QueueDeleteAsync(sibling, false, false, cancellationToken: cleanup.Token);
            await native.ExchangeDeleteAsync(name, cancellationToken: cleanup.Token);
        }
    }
}
