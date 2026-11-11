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
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.Observability;
using Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

/// <summary>
/// Verifies explicit delayed-retry publication selection and queue isolation.
/// </summary>
[Trait("Category", "RMQ")]
public class RmqSyncScheduledRetrySelectionTests
{
    /// <summary>
    /// A scheduled retry uses its captured publication without changing its source message.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_a_rmq_sync_subscription_selects_scheduled_retry_mapping_should_target_only_its_queue(
        bool custom)
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
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var native = broker.CreateModel();
        native.ExchangeDeclare(name, ExchangeType.Direct, durable: false,
            autoDelete: false);
        try
        {
            native.QueueDeclare(name, durable: true, exclusive: false,
                autoDelete: false);
            native.QueueDeclare(sibling, durable: true, exclusive: false,
                autoDelete: false);
            native.QueueBind(name, name, topic.Value);
            native.QueueBind(sibling, name, topic.Value);
            var publication = new RmqPublication
            {
                Topic = alias, Type = type, DestinationQueue = queue,
                MakeChannels = OnMissingChannel.Validate,
                WireMessageWriter = custom ? new SyncScheduledRetryWireWriter() : null
            };
            var factory = new RmqMessageProducerFactory(connection, [publication]);
            var producers = factory.Create();
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
                messagePumpType: MessagePumpType.Reactor)
            {
                ScheduledRequeuePublicationTopic = alias
            };
            var consumerFactory = new RmqMessageConsumerFactory(connection, scheduler);
            using var consumer = (RmqMessageConsumer)consumerFactory.Create(subscription);
            subscription.ScheduledRequeuePublicationTopic = topic;
            var original = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT,
                type: type, handledCount: 7), new MessageBody("original-payload"));
            original.Header.Bag["tenant"] = "scheduled-tenant";
            var initial = new DefaultRmqWireMessageWriter().WriteMessage(original);
            var initialProperties = native.CreateBasicProperties();
            initial.ConfigureProperties?.Invoke(initialProperties);
            native.BasicPublish(string.Empty, name, false, initialProperties, initial.Body);
            var source = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(2)));
            Assert.Equal(original.Id, source.Id);
            source.Body = new MessageBody("revised-payload-π");
            var before = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
            var delay = TimeSpan.FromSeconds(3);
            var elapsed = Stopwatch.StartNew();

            // Act
            var accepted = consumer.Requeue(source, delay);

            // Assert
            Assert.True(accepted);
            Assert.Equal(before, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.Empty(spans);
            Assert.False(confirmation.Task.IsCompleted);
            consumer.Dispose();
            Assert.Null(native.BasicGet(name, true));
            Assert.Null(native.BasicGet(sibling, true));
            Assert.True(elapsed.Elapsed < delay);

            // Act
            var dispatchError = await Record.ExceptionAsync(async () => await confirmation.Task.WaitAsync(timeout.Token));
            Assert.Null(dispatchError);
            var confirmed = await confirmation.Task;
            while (outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()).Any())
                await Task.Delay(10, timeout.Token);
            var delivered = native.BasicGet(name, true);

            // Assert
            Assert.True(confirmed.Success);
            Assert.Equal(original.Id, confirmed.MessageId);
            Assert.Equal(topic, confirmed.Topic);
            Assert.True(elapsed.Elapsed >= delay);
            Assert.NotNull(delivered);
            Assert.Equal(string.Empty, delivered.Exchange);
            Assert.Equal(name, delivered.RoutingKey);
            Assert.Equal(original.Id.Value, delivered.BasicProperties.MessageId);
            Assert.Equal((byte)2, delivered.BasicProperties.DeliveryMode);
            Assert.Equal("revised-payload-π", Encoding.UTF8.GetString(delivered.Body.Span));
            Assert.NotNull(delivered.BasicProperties.Headers);
            var traceKey = custom ? "telemetry-parent" : HeaderNames.CLOUD_EVENTS_TRACE_PARENT;
            var parent = Encoding.UTF8.GetString(Assert.IsType<byte[]>(delivered.BasicProperties.Headers[traceKey]));
            Assert.Equal(Assert.Single(spans).Id, parent);
            Assert.Equal(custom ? "legacy-telemetry" : initialProperties.Type, delivered.BasicProperties.Type);
            Assert.False(delivered.BasicProperties.Headers.ContainsKey(Message.ProducerTopicHeaderName));
            Assert.Null(native.BasicGet(name, true));
            Assert.Null(native.BasicGet(sibling, true));
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()));
            Assert.Equal(before, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
        }
        finally
        {
            native.QueueDelete(name, false, false);
            native.QueueDelete(sibling, false, false);
            native.ExchangeDelete(name);
        }
    }
}
