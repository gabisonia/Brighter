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

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.Logging;
using Paramore.Brighter.MessagingGateway.Redis;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using ServiceStack.Redis;

namespace Paramore.Brighter.Redis.SchedulerFailureProbe;

/// <summary>
/// Runs one native scheduled-retry failure scenario in an independent process.
/// </summary>
public static class Program
{
    /// <summary>
    /// Exercises callback containment, retained outbox data and explicit recovery.
    /// </summary>
    /// <param name="args">Async selection, wrong-type selection and native key namespace.</param>
    /// <returns>Zero when all behavior checks succeed; one for a scenario failure.</returns>
    public static async Task<int> Main(string[] args)
    {
        try
        {
            Require(args.Length == 3, "Expected async, wrong-type and namespace arguments.");
            await RunAsync(bool.Parse(args[0]), bool.Parse(args[1]), args[2]);
            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine(error);
            return 1;
        }
    }

    private static async Task RunAsync(bool useAsync, bool wrongType, string token)
    {
        //Arrange
        using var logs = new SchedulerFailureLogs();
        using var logging = LoggerFactory.Create(builder => builder.AddProvider(logs));
        ApplicationLogging.LoggerFactory = logging;
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var topic = new RoutingKey("registration-source-" + token);
        var application = new RoutingKey("registration-application-" + token);
        var alias = new RoutingKey("registration-retry-" + token);
        var wrongAlias = new RoutingKey("registration-wrong-" + token);
        var healthTopic = new RoutingKey("registration-health-" + token);
        var queue = new ChannelName("registration-queue-" + token);
        var sibling = "registration-sibling-" + token;
        var healthQueue = new ChannelName("registration-health-queue-" + token);
        var eventType = new CloudEventsType("scheduled.registration.event");
        var jobId = "registration-job-" + token;
        var writer = new RecoveryWireWriter();
        var configuration = new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port, MessageTimeToLive = TimeSpan.FromHours(1)
        };
        var ordinary = new RedisMessagePublication
        {
            Topic = application, Type = eventType, DestinationTopic = healthTopic, ChannelName = healthQueue
        };
        var invalid = new RedisMessagePublication
        {
            Topic = wrongType ? alias : wrongAlias,
            Type = wrongType ? new CloudEventsType("different.event") : eventType,
            DestinationTopic = topic, ChannelName = queue, WireMessageWriter = writer
        };
        var factory = new RedisMessageProducerFactory(configuration, [ordinary, invalid]);
        using var registry = new ProducerRegistry(useAsync ? await factory.CreateAsync() : factory.Create());
        using var tracer = new BrighterTracer();
        using var mappers = new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null);
        var outbox = new InMemoryOutbox(TimeProvider.System);
        using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(registry,
            new ResiliencePipelineRegistry<string>().AddBrighterDefault(), mappers,
            new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer,
            new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
        var time = new ObservedSystemTimeProvider();
        var schedulerFactory = new InMemorySchedulerFactory
        {
            TimeProvider = time, GetOrCreateMessageSchedulerId = _ => jobId, OnConflict = OnSchedulerConflict.Throw
        };
        CommandProcessor? processor = null;
        var subscribers = new SubscriberRegistry();
        subscribers.RegisterAsync<FireSchedulerMessage, FireSchedulerMessageHandler>();
        var handlers = new SimpleHandlerFactoryAsync(type => type == typeof(FireSchedulerMessageHandler)
            ? new FireSchedulerMessageHandler(processor!)
            : throw new InvalidOperationException("Unexpected handler: " + type));
        processor = new CommandProcessor(subscribers, handlers, new InMemoryRequestContextFactory(),
            new PolicyRegistry(), new ResiliencePipelineRegistry<string>(), mediator, schedulerFactory, tracer: tracer);
        await using var scheduler = (InMemoryScheduler)schedulerFactory.Create(processor);
        var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(new SubscriptionName("registration-" + token), queue, topic,
            requestType: typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
        {
            ScheduledRequeuePublicationTopic = alias
        };
        var consumers = new RedisMessageConsumerFactory(configuration, scheduler);
        await using var consumer = useAsync
            ? (RedisMessageConsumer)consumers.CreateAsync(subscription)
            : (RedisMessageConsumer)consumers.Create(subscription);
        using IRedisClient observer = new RedisClient(host, port);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var context = new RequestContext();
        var original = new Message(new MessageHeader(Id.Random(), application, MessageType.MT_EVENT,
            type: eventType, handledCount: 7), new MessageBody("original-π-" + token));
        original.Header.Bag["marker"] = "marker-" + token;
        using var encoder = new RedisMessagePublisher();
        var originalWire = encoder.Create(original);
        observer.SetValue(topic + ".1", originalWire, TimeSpan.FromMinutes(10));
        observer.SetValue(topic + ".nextid", "41");
        observer.AddItemToList(queue.Value, "1");
        observer.AddItemToSet(topic + ".queues", queue.Value);
        observer.AddItemToSet(topic + ".queues", sibling);
        var received = useAsync
            ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
            : consumer.Receive(TimeSpan.FromSeconds(1));
        Require(received.Length == 1 && received[0].Id == original.Id, "Native source must be received.");
        var source = received[0];
        var revisedBody = "revised-λ-" + token;
        source.Body = new MessageBody(revisedBody);
        var sourceBefore = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);

        //Act
        var accepted = useAsync
            ? await consumer.RequeueAsync(source, TimeSpan.FromSeconds(5), timeout.Token)
            : consumer.Requeue(source, TimeSpan.FromSeconds(5));

        //Assert
        Require(accepted, "Scheduling acceptance must remain distinct from callback dispatch success.");
        Require(!outbox.OutstandingMessages(TimeSpan.Zero, context).Any(), "No deposit before due time.");
        Require(writer.Calls == 0 && observer.GetValue(topic + ".nextid") == "41", "No send before due time.");
        Require(sourceBefore == JsonSerializer.Serialize(source, JsonSerialisationOptions.Options), "Caller changed at scheduling.");
        Console.WriteLine("PROBE_ACCEPTED:" + token);
        await time.WaitForNextCallbackAsync(timeout.Token);

        //Assert
        var failures = logs.Entries.Where(entry => entry.Level >= LogLevel.Error).ToArray();
        Require(failures.Length == 1 && failures[0].Error is ConfigurationException
            && failures[0].Text.Contains(jobId, StringComparison.Ordinal),
            "Failed callback must log its original configuration exception and scheduler ID once.");
        var pending = outbox.OutstandingMessages(TimeSpan.Zero, context).ToArray();
        Require(pending.Length == 1, "Failed callback must retain exactly one outstanding message.");
        Require(pending[0].Id == original.Id && pending[0].Body.Value == revisedBody
            && pending[0].Header.Topic.Equals(application) && pending[0].Header.Type.Equals(eventType)
            && pending[0].Header.HandledCount == 7
            && pending[0].Header.Bag["marker"].ToString() == "marker-" + token
            && pending[0].Header.Bag[Message.ProducerTopicHeaderName] is string selected && selected == alias.Value,
            "Failed callback must preserve message data and the selected publication.");
        Require(writer.Calls == 0 && observer.GetValue(topic + ".nextid") == "41"
            && observer.GetValue(topic + ".42") is null && observer.GetValue(healthTopic + ".nextid") is null
            && observer.GetAllItemsFromList(queue.Value).Count == 0
            && observer.GetAllItemsFromList(sibling).Count == 0,
            "Invalid registration must not publish or fall back to the ordinary publication.");
        Require(sourceBefore == JsonSerializer.Serialize(source, JsonSerialisationOptions.Options), "Caller changed after failed dispatch.");
        Exception? directError = null;
        try
        {
            if (useAsync)
                await mediator.ClearOutboxAsync([original.Id], context, cancellationToken: timeout.Token);
            else
                mediator.ClearOutbox([original.Id], context);
        }
        catch (Exception error) { directError = error; }
        Require(directError is ConfigurationException, "Explicit dispatch must still expose invalid registration to its caller.");

        //Act
        var health = new Message(new MessageHeader(Id.Random(), application, MessageType.MT_EVENT, type: eventType),
            new MessageBody("healthy-" + token));
        Exception? reuseError = null;
        try
        {
            if (useAsync)
                await scheduler.ScheduleAsync(health, TimeSpan.FromSeconds(1), timeout.Token);
            else
                scheduler.Schedule(health, TimeSpan.FromSeconds(1));
        }
        catch (Exception error) { reuseError = error; }

        //Assert
        Require(reuseError is null, "A completed failed job must release its scheduler ID.");
        await time.WaitForNextCallbackAsync(timeout.Token);
        Require(observer.GetValue(healthTopic + ".nextid") == "1"
            && observer.GetAllItemsFromList(healthQueue.Value).SequenceEqual(new[] { "1" }),
            "The same scheduler must dispatch later valid work.");
        var healthy = new DefaultRedisWireMessageReader().ReadMessage(observer.GetValue(healthTopic + ".1"));
        Require(healthy.Id == health.Id && healthy.Body.Value == health.Body.Value, "Healthy callback payload changed.");
        Require(outbox.OutstandingMessages(TimeSpan.Zero, context).Single().Id == original.Id,
            "Healthy work must settle independently of the retained failure.");

        //Arrange
        var repaired = new RedisMessagePublication
        {
            Topic = alias, Type = eventType, DestinationTopic = topic, ChannelName = queue, WireMessageWriter = writer
        };
        var repairedFactory = new RedisMessageProducerFactory(configuration, [repaired]);
        using var repairedRegistry = new ProducerRegistry(useAsync ? await repairedFactory.CreateAsync() : repairedFactory.Create());
        using var repairedMediator = new OutboxProducerMediator<Message, CommittableTransaction>(repairedRegistry,
            new ResiliencePipelineRegistry<string>().AddBrighterDefault(), mappers,
            new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer,
            new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);

        //Act
        if (useAsync)
            await repairedMediator.ClearOutboxAsync([original.Id], context, cancellationToken: timeout.Token);
        else
            repairedMediator.ClearOutbox([original.Id], context);

        //Assert
        Require(writer.Calls == 1 && observer.GetValue(topic + ".nextid") == "42"
            && observer.GetAllItemsFromList(queue.Value).SequenceEqual(new[] { "42" })
            && observer.GetAllItemsFromList(sibling).Count == 0, "Recovery must deliver once to the selected queue.");
        var wire = observer.GetValue(topic + ".42");
        Require(wire is not null && wire.StartsWith("C:<HEADER ", StringComparison.Ordinal), "Recovery must use the selected writer.");
        var decoded = new DefaultRedisWireMessageReader().ReadMessage(wire![2..]);
        Require(decoded.Id == original.Id && decoded.Body.Value == revisedBody && decoded.Header.Topic.Equals(application)
            && decoded.Header.Type.Equals(eventType) && decoded.Header.HandledCount == 7
            && decoded.Header.Bag["marker"].ToString() == "marker-" + token
            && !decoded.Header.Bag.ContainsKey(Message.ProducerTopicHeaderName), "Recovered wire data changed.");
        Require(observer.GetTimeToLive(topic + ".42")?.TotalSeconds is >= 3000 and <= 3600, "Recovery TTL is invalid.");
        Require(!outbox.OutstandingMessages(TimeSpan.Zero, context).Any(), "Recovered outbox must be settled.");
        Require(sourceBefore == JsonSerializer.Serialize(source, JsonSerialisationOptions.Options)
            && observer.GetValue(topic + ".1") == originalWire, "Recovery changed caller or original native payload.");
        foreach (var route in new[] { application, alias, wrongAlias })
            Require(observer.GetValue(route + ".nextid") is null, "Lookup topics must not become native destinations.");
        Require(logs.Entries.Count(entry => entry.Level >= LogLevel.Error) == 1, "Healthy work must not add scheduler failures.");
        Console.WriteLine("PROBE_RECOVERED:" + token);
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
            throw new InvalidOperationException(message);
    }
}
