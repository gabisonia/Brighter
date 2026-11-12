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
using System.Collections.Generic;
using System.Diagnostics;
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
public class RedisScheduledRetryIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_redis_subscriptions_select_scheduled_retries_should_isolate_destinations_and_repeated_payloads(
        bool useAsync, bool composedFirst)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        var topic = new RoutingKey("scheduled-isolation-native-" + token);
        var applicationTopic = new RoutingKey("scheduled-isolation-application-" + token);
        var eventType = new CloudEventsType("scheduled.retry.isolation");
        var order = composedFirst ? new[] { "composed", "default" } : new[] { "default", "composed" };
        var queues = order.Select(mode => new ChannelName("scheduled-isolation-queue-" + mode + "-" + token)).ToArray();
        var aliases = order.Select(mode => new RoutingKey("scheduled-isolation-publication-" + mode + "-" + token)).ToArray();
        var sibling = "scheduled-isolation-sibling-" + token;
        var configuration = new RedisMessagingGatewayConfiguration
        {
            RedisConnectionString = host + ":" + port,
            MessageTimeToLive = TimeSpan.FromHours(1)
        };
        var composed = new RedisPublicationWireWriter("composed");
        var ordinaryWriter = new RedisPublicationWireWriter("custom");
        var immediateWriter = new RedisRecoverableRequeueWireWriter("custom");
        var publications = order.Select((mode, index) => new RedisMessagePublication
        {
            Topic = aliases[index], Type = eventType,
            DestinationTopic = topic, ChannelName = queues[index],
            WireMessageWriter = mode == "composed" ? composed : null
        }).ToList();
        publications.Add(new RedisMessagePublication
        {
            Topic = applicationTopic, Type = eventType,
            DestinationTopic = topic, ChannelName = new ChannelName(sibling),
            WireMessageWriter = ordinaryWriter
        });
        var producerFactory = new RedisMessageProducerFactory(configuration, publications);
        var producers = useAsync ? await producerFactory.CreateAsync() : producerFactory.Create();
        using var registry = new ProducerRegistry(producers);
        using var tracer = new BrighterTracer();
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
        var consumerFactory = new RedisMessageConsumerFactory(configuration, scheduler);
        var subscriptions = new List<Paramore.Brighter.MessagingGateway.Redis.RedisSubscription>();
        var consumers = new List<RedisMessageConsumer>();
        var originals = new Dictionary<int, (Id Id, string Wire)>();
        var scheduledSources = new List<(Message Message, string Before)>();
        using IRedisClient observer = new RedisClient(host, port);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var context = new RequestContext();
        var delay = TimeSpan.FromSeconds(5);
        try
        {
            for (var endpoint = 0; endpoint < order.Length; endpoint++)
            {
                var subscription = new Paramore.Brighter.MessagingGateway.Redis.RedisSubscription(
                    new SubscriptionName("scheduled-isolation-subscription-" + order[endpoint] + "-" + token),
                    queues[endpoint], topic, requestType: typeof(Message),
                    messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
                {
                    RequeueWireMessageWriter = immediateWriter,
                    ScheduledRequeuePublicationTopic = aliases[endpoint]
                };
                subscriptions.Add(subscription);
                consumers.Add(useAsync
                    ? (RedisMessageConsumer)consumerFactory.CreateAsync(subscription)
                    : (RedisMessageConsumer)consumerFactory.Create(subscription));
                observer.AddItemToSet(topic + ".queues", queues[endpoint].Value);
                for (var round = 0; round < 2; round++)
                {
                    var nativeId = round * 2 + endpoint + 1;
                    var initial = new Message(new MessageHeader(Id.Random(), applicationTopic, MessageType.MT_EVENT,
                        type: eventType, handledCount: 7), new MessageBody("original-" + nativeId + "-" + token));
                    initial.Header.Bag["source-marker"] = "marker-" + nativeId + "-" + token;
                    using var encoder = new RedisMessagePublisher();
                    var wire = encoder.Create(initial);
                    originals.Add(nativeId, (initial.Id, wire));
                    observer.SetValue(topic + "." + nativeId, wire, TimeSpan.FromMinutes(10));
                    observer.AddItemToList(queues[endpoint].Value, nativeId.ToString());
                }
            }
            subscriptions[0].ScheduledRequeuePublicationTopic = aliases[1];
            subscriptions[1].ScheduledRequeuePublicationTopic = aliases[0];
            observer.AddItemToSet(topic + ".queues", sibling);
            observer.SetValue(topic + ".nextid", "41");

            for (var round = 0; round < 2; round++)
            {
                for (var endpoint = 0; endpoint < order.Length; endpoint++)
                {
                    var nativeId = round * 2 + endpoint + 1;
                    var nextNativeId = (41 + nativeId).ToString();
                    var previousCounter = (40 + nativeId).ToString();
                    var consumer = consumers[endpoint];
                    var source = Assert.Single(useAsync
                        ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                        : consumer.Receive(TimeSpan.FromSeconds(1)));
                    Assert.Equal(originals[nativeId].Id, source.Id);
                    var revisedBody = "retry-π-" + order[endpoint] + "-" + round + "-" + token;
                    source.Body = new MessageBody(revisedBody);
                    var sourceBefore = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
                    scheduledSources.Add((source, sourceBefore));
                    var queueBefore = observer.GetAllItemsFromList(queues[endpoint].Value).ToArray();
                    var otherQueueBefore = observer.GetAllItemsFromList(queues[1 - endpoint].Value).ToArray();
                    var composedCallsBefore = composed.Calls;
                    var elapsed = Stopwatch.StartNew();

                    //Act
                    var requeued = useAsync
                        ? await consumer.RequeueAsync(source, delay, timeout.Token)
                        : consumer.Requeue(source, delay);

                    //Assert
                    Assert.True(requeued);
                    Assert.Equal(sourceBefore, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
                    Assert.Equal(composedCallsBefore, composed.Calls);
                    Assert.Equal(0, ordinaryWriter.Calls);
                    Assert.Equal(0, immediateWriter.Calls);
                    Assert.Equal(previousCounter, observer.GetValue(topic + ".nextid"));
                    Assert.Null(observer.GetValue(topic + "." + nextNativeId));
                    Assert.Equal(queueBefore, observer.GetAllItemsFromList(queues[endpoint].Value));
                    Assert.Equal(otherQueueBefore, observer.GetAllItemsFromList(queues[1 - endpoint].Value));
                    Assert.Empty(observer.GetAllItemsFromList(sibling));
                    Assert.True(elapsed.Elapsed < delay, "The before-delay observation must finish before the due time.");

                    //Act
                    while (observer.GetValue(topic + ".nextid") == previousCounter
                           || outbox.OutstandingMessages(TimeSpan.Zero, context).Any())
                        await Task.Delay(TimeSpan.FromMilliseconds(100), timeout.Token);

                    //Assert
                    Assert.True(elapsed.Elapsed >= delay);
                    Assert.Equal(nextNativeId, observer.GetValue(topic + ".nextid"));
                    Assert.Equal(queueBefore.Append(nextNativeId), observer.GetAllItemsFromList(queues[endpoint].Value));
                    Assert.Equal(otherQueueBefore, observer.GetAllItemsFromList(queues[1 - endpoint].Value));
                    Assert.Empty(observer.GetAllItemsFromList(sibling));
                    var wire = observer.GetValue(topic + "." + nextNativeId);
                    Assert.False(string.IsNullOrEmpty(wire));
                    Assert.StartsWith(order[endpoint] == "composed" ? "C:<HEADER " : "<HEADER ", wire);
                    var decoded = new DefaultRedisWireMessageReader().ReadMessage(order[endpoint] == "composed" ? wire[2..] : wire);
                    Assert.Equal(revisedBody, decoded.Body.Value);
                    Assert.Equal(source.Id, decoded.Id);
                    Assert.Equal(applicationTopic, decoded.Header.Topic);
                    Assert.Equal(eventType, decoded.Header.Type);
                    Assert.Equal(7, decoded.Header.HandledCount);
                    Assert.Equal("marker-" + nativeId + "-" + token, decoded.Header.Bag["source-marker"].ToString());
                    Assert.False(decoded.Header.Bag.ContainsKey(Message.ProducerTopicHeaderName));
                    Assert.Equal(composedCallsBefore + (order[endpoint] == "composed" ? 1 : 0), composed.Calls);
                    Assert.Equal(0, ordinaryWriter.Calls);
                    Assert.Equal(0, immediateWriter.Calls);
                    Assert.InRange(observer.GetTimeToLive(topic + "." + nextNativeId)?.TotalSeconds ?? -1, 3000, 3600);
                    Assert.Equal(nextNativeId, observer.RemoveEndFromList(queues[endpoint].Value));
                    Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
                    foreach (var snapshot in scheduledSources)
                        Assert.Equal(snapshot.Before, JsonSerializer.Serialize(snapshot.Message, JsonSerialisationOptions.Options));
                }
            }
            Assert.Equal(2, composed.Calls);
            foreach (var consumer in consumers)
                Assert.Empty(useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : consumer.Receive(TimeSpan.FromSeconds(1)));
            foreach (var original in originals)
                Assert.Equal(original.Value.Wire, observer.GetValue(topic + "." + original.Key));
            foreach (var alias in aliases.Append(applicationTopic))
                Assert.Null(observer.GetValue(alias + ".nextid"));
        }
        finally
        {
            await scheduler.DisposeAsync();
            foreach (var consumer in consumers)
                await consumer.DisposeAsync();
            foreach (var route in aliases.Append(applicationTopic).Append(topic))
            {
                foreach (var suffix in new[] { ".1", ".2", ".3", ".4", ".42", ".43", ".44", ".45", ".nextid", ".queues" })
                    observer.Remove(route + suffix);
            }
            foreach (var queue in queues)
                observer.Remove(queue.Value);
            observer.Remove(sibling);
        }
    }
}
