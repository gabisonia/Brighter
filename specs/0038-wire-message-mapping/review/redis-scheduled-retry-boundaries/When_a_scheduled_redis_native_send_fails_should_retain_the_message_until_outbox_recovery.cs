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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.Redis;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Redis.Tests.TestDoubles;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using ServiceStack.Redis;
using Xunit;

namespace Paramore.Brighter.Redis.Tests.MessagingGateway;

[Trait("Category", "Redis")]
[Collection("RedisMessagingGateway")]
public class RedisScheduledRetryNativeFailureTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_a_scheduled_redis_native_send_fails_should_retain_the_message_until_outbox_recovery(
        bool useAsync)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var topic = new RoutingKey("scheduled-retry-" + token);
        var applicationTopic = new RoutingKey("scheduled-retry-application-" + token);
        var retryPublicationTopic = new RoutingKey("scheduled-retry-publication-" + token);
        var eventType = new CloudEventsType("scheduled.retry.event");
        var queue = new ChannelName("scheduled-retry-source-" + token);
        var sibling = "scheduled-retry-sibling-" + token;
        var configuration = new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port,
            MessageTimeToLive = TimeSpan.FromHours(1)
        };
        var selected = new RedisPublicationWireWriter("composed");
        var depositWriter = new RedisPublicationWireWriter("composed");
        var immediateWriter = new RedisRecoverableRequeueWireWriter("custom");
        var ordinaryPublication = new RedisMessagePublication
        {
            Topic = applicationTopic, Type = eventType, WireMessageWriter = depositWriter
        };
        var publication = new RedisMessagePublication
        {
            Topic = retryPublicationTopic, Type = eventType,
            DestinationTopic = topic, ChannelName = queue,
            WireMessageWriter = selected
        };
        var producerFactory = new RedisMessageProducerFactory(configuration, [ordinaryPublication, publication]);
        var producers = useAsync ? await producerFactory.CreateAsync() : producerFactory.Create();
        using var registry = new ProducerRegistry(producers);
        using var tracer = new BrighterTracer();
        var dispatchFinished = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var listener = new ActivityListener
        {
            ShouldListenTo = activitySource => ReferenceEquals(activitySource, tracer.ActivitySource),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.Kind == ActivityKind.Producer
                    && Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "publish"))
                    dispatchFinished.TrySetResult(true);
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
        var handlers = new SimpleHandlerFactoryAsync(type => type == typeof(FireSchedulerMessageHandler)
            ? new FireSchedulerMessageHandler(processor!)
            : throw new InvalidOperationException("Unexpected handler: " + type));
        processor = new CommandProcessor(subscribers, handlers, new InMemoryRequestContextFactory(),
            new PolicyRegistry(), new ResiliencePipelineRegistry<string>(), mediator, schedulerFactory, tracer: tracer);
        await using var scheduler = Assert.IsType<InMemoryScheduler>(schedulerFactory.Create(processor));
        var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
            new SubscriptionName("scheduled-retry-subscription-" + token), queue, topic,
            requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
        {
            RequeueWireMessageWriter = immediateWriter,
            ScheduledRequeuePublicationTopic = retryPublicationTopic
        };
        var consumerFactory = new RedisMessageConsumerFactory(configuration, scheduler);
        await using var consumer = useAsync
            ? (RedisMessageConsumer)consumerFactory.CreateAsync(subscription)
            : (RedisMessageConsumer)consumerFactory.Create(subscription);
        subscription.ScheduledRequeuePublicationTopic = applicationTopic;
        using IRedisClient observer = new RedisClient(host, port);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var original = new Message(new MessageHeader(Id.Random(), applicationTopic, MessageType.MT_EVENT, type: eventType, handledCount: 7),
            new MessageBody("scheduled-original-π-" + token));
        original.Header.Bag["source-marker"] = "marker-" + token;
        var next = new Message(new MessageHeader(Id.Random(), applicationTopic, MessageType.MT_EVENT, type: eventType, handledCount: 7),
            new MessageBody("next-λ-" + token));
        using var firstEncoder = new RedisMessagePublisher();
        using var nextEncoder = new RedisMessagePublisher();
        var originalWire = firstEncoder.Create(original);
        var nextWire = nextEncoder.Create(next);
        var revisedBody = "scheduled-revised-π-" + token;
        var context = new RequestContext();
        var delay = TimeSpan.FromSeconds(5);
        try
        {
            observer.SetValue(topic + ".1", originalWire, TimeSpan.FromMinutes(10));
            observer.SetValue(topic + ".2", nextWire, TimeSpan.FromMinutes(10));
            observer.SetValue(topic + ".nextid", "41");
            observer.AddItemToList(queue.Value, "1");
            observer.AddItemToList(queue.Value, "2");
            observer.AddItemToSet(topic + ".queues", queue.Value);
            observer.AddItemToSet(topic + ".queues", sibling);
            observer.AddItemToSet(applicationTopic + ".queues", queue.Value);
            observer.AddItemToSet(applicationTopic + ".queues", sibling);
            var source = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(original.Id, source.Id);
            source.Body = new MessageBody(revisedBody);
            var sourceBefore = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
            var elapsed = Stopwatch.StartNew();

            //Act
            var requeued = useAsync
                ? await consumer.RequeueAsync(source, delay, timeout.Token)
                : consumer.Requeue(source, delay);

            //Assert
            Assert.True(requeued);
            Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.Equal(0, selected.Calls);
            Assert.Equal(0, depositWriter.Calls);
            Assert.Equal(0, immediateWriter.Calls);
            Assert.Equal("41", observer.GetValue(topic + ".nextid"));
            Assert.Null(observer.GetValue(topic + ".42"));
            Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(queue.Value));
            Assert.Empty(observer.GetAllItemsFromList(sibling));
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Message[]? received = null;
            var recoveryError = await Record.ExceptionAsync(async () =>
            {
                received = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : consumer.Receive(TimeSpan.FromSeconds(1));
            });
            Assert.Null(recoveryError);
            var subsequent = Assert.Single(received!);
            Assert.Equal(next.Id, subsequent.Id);
            if (useAsync)
                await consumer.AcknowledgeAsync(subsequent, timeout.Token);
            else
                consumer.Acknowledge(subsequent);
            Assert.Empty(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.True(elapsed.Elapsed < delay, "The before-delay observation must finish before the due time.");

            observer.SetValue(topic + ".nextid", "not-an-integer");

            //Act
            await dispatchFinished.Task.WaitAsync(timeout.Token);
            await scheduler.DisposeAsync().AsTask().WaitAsync(timeout.Token);

            //Assert
            Assert.True(elapsed.Elapsed >= delay);
            Assert.True(selected.Calls > 0);
            var failedAttempts = selected.Calls;
            var pending = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(original.Id, pending.Id);
            Assert.Equal(applicationTopic, pending.Header.Topic);
            Assert.Equal(eventType, pending.Header.Type);
            Assert.Equal(7, pending.Header.HandledCount);
            Assert.Equal(revisedBody, pending.Body.Value);
            Assert.Equal("marker-" + token, pending.Header.Bag["source-marker"].ToString());
            Assert.Equal(retryPublicationTopic.Value,
                Assert.IsType<string>(pending.Header.Bag[Message.ProducerTopicHeaderName]));
            Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.Equal("not-an-integer", observer.GetValue(topic + ".nextid"));
            Assert.Null(observer.GetValue(topic + ".42"));
            Assert.Empty(observer.GetAllItemsFromList(queue.Value));
            Assert.Empty(observer.GetAllItemsFromList(sibling));
            Assert.Equal(0, depositWriter.Calls);
            Assert.Equal(0, immediateWriter.Calls);
            Assert.Equal(originalWire, observer.GetValue(topic + ".1"));
            Assert.Equal(nextWire, observer.GetValue(topic + ".2"));

            //Act
            observer.SetValue(topic + ".nextid", "41");
            if (useAsync)
                await mediator.ClearOutboxAsync([original.Id], context, cancellationToken: timeout.Token);
            else
                mediator.ClearOutbox([original.Id], context);

            //Assert
            Assert.Equal(failedAttempts + 1, selected.Calls);
            Assert.Equal("42", observer.GetValue(topic + ".nextid"));
            Assert.Equal(new[] { "42" }, observer.GetAllItemsFromList(queue.Value));
            Assert.Empty(observer.GetAllItemsFromList(sibling));
            var wire = observer.GetValue(topic + ".42");
            Assert.NotNull(wire);
            Assert.StartsWith("C:<HEADER ", wire);
            var decoded = new DefaultRedisWireMessageReader().ReadMessage(wire[2..]);
            Assert.Equal(original.Id, decoded.Id);
            Assert.Equal(applicationTopic, decoded.Header.Topic);
            Assert.Equal(eventType, decoded.Header.Type);
            Assert.Equal(7, decoded.Header.HandledCount);
            Assert.False(decoded.Header.Bag.ContainsKey(Message.ProducerTopicHeaderName));
            Assert.Equal(revisedBody, decoded.Body.Value);
            Assert.Equal("marker-" + token, decoded.Header.Bag["source-marker"].ToString());
            Assert.InRange(observer.GetTimeToLive(topic + ".42")?.TotalSeconds ?? -1, 3000, 3600);
            Assert.Equal(0, depositWriter.Calls);
            Assert.Equal(0, immediateWriter.Calls);
            Assert.Equal(original.Id, source.Id);
            Assert.Equal(applicationTopic, source.Header.Topic);
            Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.Null(observer.GetValue(applicationTopic + ".nextid"));
            Assert.Null(observer.GetValue(applicationTopic + ".1"));
            Assert.Null(observer.GetValue(retryPublicationTopic + ".nextid"));
            Assert.Null(observer.GetValue(retryPublicationTopic + ".1"));
            Assert.Equal(revisedBody, source.Body.Value);
            Assert.Equal("marker-" + token, source.Header.Bag["source-marker"].ToString());
            Assert.Equal(originalWire, observer.GetValue(topic + ".1"));
            Assert.Equal(nextWire, observer.GetValue(topic + ".2"));
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
        }
        finally
        {
            await scheduler.DisposeAsync();
            foreach (var route in new[] { topic, applicationTopic, retryPublicationTopic })
            {
                foreach (var suffix in new[] { ".1", ".2", ".42", ".nextid", ".queues" })
                    observer.Remove(route + suffix);
            }
            observer.Remove(queue.Value);
            observer.Remove(sibling);
        }
    }
}
