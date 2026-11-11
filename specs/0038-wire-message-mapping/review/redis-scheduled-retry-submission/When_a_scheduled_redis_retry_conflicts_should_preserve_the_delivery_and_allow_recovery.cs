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
using System.Linq;
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
/// <summary>Verifies native delivery recovery after a real scheduler submission conflict.</summary>
public class RedisScheduledRetrySubmissionTests
{
    /// <summary>Preserves the existing job and original delivery, then completes a selected retry.</summary>
    /// <param name="useAsync">Whether to exercise asynchronous factory and retry operations.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_a_scheduled_redis_retry_conflicts_should_preserve_the_delivery_and_allow_recovery(
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
        using var mappers = new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null);
        var outbox = new InMemoryOutbox(TimeProvider.System);
        using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(registry,
            new ResiliencePipelineRegistry<string>().AddBrighterDefault(), mappers,
            new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer,
            new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
        var jobId = "submission-job-" + token;
        var schedulerFactory = new InMemorySchedulerFactory
        {
            GetOrCreateMessageSchedulerId = _ => jobId, OnConflict = OnSchedulerConflict.Throw
        };
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
        var parked = new Message(new MessageHeader(Id.Random(), applicationTopic, MessageType.MT_EVENT, type: eventType),
            new MessageBody("parked-job-λ-" + token));
        parked.Header.Bag[Message.ProducerTopicHeaderName] = retryPublicationTopic.Value;
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
            var source = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(original.Id, source.Id);
            source.Body = new MessageBody(revisedBody);
            var sourceBefore = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
            var parkedId = useAsync
                ? await scheduler.ScheduleAsync(parked, TimeSpan.FromHours(1), timeout.Token)
                : scheduler.Schedule(parked, TimeSpan.FromHours(1));
            Assert.Equal(jobId, parkedId);

            //Act
            var submissionError = await Record.ExceptionAsync(async () =>
            {
                if (useAsync)
                    await consumer.RequeueAsync(source, delay, timeout.Token);
                else
                    consumer.Requeue(source, delay);
            });

            //Assert
            Assert.Contains(jobId, Assert.IsType<InvalidOperationException>(submissionError).Message);
            Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.Equal(originalWire, observer.GetValue(topic + ".1"));
            Assert.Equal(nextWire, observer.GetValue(topic + ".2"));
            Assert.Equal("41", observer.GetValue(topic + ".nextid"));
            Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(queue.Value));
            Assert.Empty(observer.GetAllItemsFromList(sibling));
            Assert.Null(observer.GetValue(topic + ".42"));
            Assert.Equal(0, selected.Calls);
            Assert.Equal(0, depositWriter.Calls);
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            var blockedReceive = await Record.ExceptionAsync(async () =>
            {
                if (useAsync)
                    await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token);
                else
                    consumer.Receive(TimeSpan.FromSeconds(1));
            });
            Assert.Contains(source.Id.Value, Assert.IsType<ChannelFailureException>(blockedReceive).Message);
            Assert.Equal(new[] { "2" }, observer.GetAllItemsFromList(queue.Value));

            //Act
            var existingJobRetained = useAsync
                ? await scheduler.ReSchedulerAsync(jobId, TimeSpan.FromSeconds(1), timeout.Token)
                : scheduler.ReScheduler(jobId, TimeSpan.FromSeconds(1));

            //Assert
            Assert.True(existingJobRetained);
            await WaitForNativePublicationAsync(observer, topic, "42", outbox, context, timeout.Token);
            Assert.Equal("42", observer.GetValue(topic + ".nextid"));
            Assert.Equal(new[] { "2", "42" }, observer.GetAllItemsFromList(queue.Value));
            var parkedWire = observer.GetValue(topic + ".42");
            Assert.StartsWith("C:", parkedWire);
            var deliveredParked = new DefaultRedisWireMessageReader().ReadMessage(parkedWire.Substring(2));
            Assert.Equal(parked.Id, deliveredParked.Id);
            Assert.Equal(parked.Body.Value, deliveredParked.Body.Value);
            Assert.Equal(1, selected.Calls);
            Assert.Equal(0, depositWriter.Calls);
            Assert.Equal(originalWire, observer.GetValue(topic + ".1"));
            Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            if (useAsync)
                await scheduler.CancelAsync(jobId, timeout.Token);
            else
                scheduler.Cancel(jobId);

            //Act
            var recoveredImmediately = useAsync
                ? await consumer.RequeueAsync(source, TimeSpan.Zero, timeout.Token)
                : consumer.Requeue(source, TimeSpan.Zero);

            //Assert
            Assert.True(recoveredImmediately);
            Assert.Equal(new[] { "2", "42", "1" }, observer.GetAllItemsFromList(queue.Value));
            Assert.Equal("42", observer.GetValue(topic + ".nextid"));
            var recoveredWire = observer.GetValue(topic + ".1");
            var recovered = new DefaultRedisWireMessageReader().ReadMessage(recoveredWire);
            Assert.Equal(original.Id, recovered.Id);
            Assert.Equal(revisedBody, recovered.Body.Value);
            Assert.Equal(applicationTopic, recovered.Header.Topic);
            Assert.Equal(7, recovered.Header.HandledCount);
            Assert.Equal("marker-" + token, recovered.Header.Bag["source-marker"].ToString());
            Assert.False(recovered.Header.Bag.ContainsKey(Message.ProducerTopicHeaderName));
            Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.Equal(1, selected.Calls);
            var subsequent = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(next.Id, subsequent.Id);
            if (useAsync)
                await consumer.AcknowledgeAsync(subsequent, timeout.Token);
            else
                consumer.Acknowledge(subsequent);
            Assert.Equal("42", observer.BlockingRemoveStartFromList(queue.Value, TimeSpan.FromSeconds(1)));
            Assert.Equal(new[] { "1" }, observer.GetAllItemsFromList(queue.Value));
            var retrySource = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                : consumer.Receive(TimeSpan.FromSeconds(1)));
            Assert.Equal(original.Id, retrySource.Id);
            Assert.Equal(revisedBody, retrySource.Body.Value);
            var retryBefore = JsonSerializer.Serialize(retrySource, JsonSerialisationOptions.Options);

            //Act
            var acceptedRetry = useAsync
                ? await consumer.RequeueAsync(retrySource, delay, timeout.Token)
                : consumer.Requeue(retrySource, delay);

            //Assert
            Assert.True(acceptedRetry);
            Assert.Equal(1, selected.Calls);
            Assert.Equal("42", observer.GetValue(topic + ".nextid"));
            Assert.Null(observer.GetValue(topic + ".43"));
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Empty(observer.GetAllItemsFromList(queue.Value));
            Assert.Equal(retryBefore, JsonSerializer.Serialize(retrySource, JsonSerialisationOptions.Options));
            await WaitForNativePublicationAsync(observer, topic, "43", outbox, context, timeout.Token);
            await scheduler.DisposeAsync().AsTask().WaitAsync(timeout.Token);
            Assert.Equal("43", observer.GetValue(topic + ".nextid"));
            Assert.Equal(new[] { "43" }, observer.GetAllItemsFromList(queue.Value));
            Assert.Empty(observer.GetAllItemsFromList(sibling));
            var retryWire = observer.GetValue(topic + ".43");
            Assert.StartsWith("C:", retryWire);
            var deliveredRetry = new DefaultRedisWireMessageReader().ReadMessage(retryWire.Substring(2));
            Assert.Equal(original.Id, deliveredRetry.Id);
            Assert.Equal(revisedBody, deliveredRetry.Body.Value);
            Assert.Equal(applicationTopic, deliveredRetry.Header.Topic);
            Assert.Equal(eventType, deliveredRetry.Header.Type);
            Assert.Equal(7, deliveredRetry.Header.HandledCount);
            Assert.Equal("marker-" + token, deliveredRetry.Header.Bag["source-marker"].ToString());
            Assert.False(deliveredRetry.Header.Bag.ContainsKey(Message.ProducerTopicHeaderName));
            Assert.InRange(observer.GetTimeToLive(topic + ".43")?.TotalSeconds ?? -1, 3000, 3600);
            Assert.Equal(2, selected.Calls);
            Assert.Equal(0, depositWriter.Calls);
            Assert.Equal(recoveredWire, observer.GetValue(topic + ".1"));
            Assert.Equal(nextWire, observer.GetValue(topic + ".2"));
            Assert.Equal(parkedWire, observer.GetValue(topic + ".42"));
            Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.Equal(retryBefore, JsonSerializer.Serialize(retrySource, JsonSerialisationOptions.Options));
            Assert.Null(observer.GetValue(applicationTopic + ".nextid"));
            Assert.Null(observer.GetValue(retryPublicationTopic + ".nextid"));
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
        }
        finally
        {
            await scheduler.DisposeAsync();
            foreach (var route in new[] { topic, applicationTopic, retryPublicationTopic })
            {
                foreach (var suffix in new[] { ".1", ".2", ".42", ".43", ".999", ".nextid", ".queues" })
                    observer.Remove(route + suffix);
            }
            observer.Remove(queue.Value);
            observer.Remove(sibling);
        }
    }

    private static async Task WaitForNativePublicationAsync(IRedisClient observer, RoutingKey topic,
        string nativeId, InMemoryOutbox outbox, RequestContext context, CancellationToken cancellationToken)
    {
        while (observer.GetValue(topic + "." + nativeId) is null
               || outbox.OutstandingMessages(TimeSpan.Zero, context).Any())
            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
    }
}
