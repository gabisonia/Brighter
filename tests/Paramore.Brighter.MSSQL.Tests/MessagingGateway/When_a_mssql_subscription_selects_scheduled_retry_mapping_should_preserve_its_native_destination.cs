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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Data.SqlClient;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.MsSql;
using Paramore.Brighter.MSSQL.Tests.TestDoubles;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.MSSQL.Tests.MessagingGateway;

/// <summary>Uses real scheduler callbacks and native SQL to verify delayed retry selection.</summary>
[Trait("Category", "MSSQL")]
public class MsSqlScheduledRetrySelectionTests
{
    /// <summary>Retains native destination and selects the captured publication at actual dispatch.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_mssql_subscription_selects_scheduled_retry_mapping_should_preserve_its_native_destination(
        bool useAsync, bool custom)
    {
        //Arrange
        var helper = new MsSqlTestHelper(tableName: "scheduled_" + Guid.NewGuid().ToString("N"));
        var configuration = helper.QueueConfiguration;
        var topic = new RoutingKey("source-" + Guid.NewGuid().ToString("N"));
        var alias = new RoutingKey("scheduled-alias");
        var type = new CloudEventsType("scheduled.sql.event");
        var writer = new MsSqlSelectedWireWriter(true);
        var inactiveWriter = new MsSqlSelectedWireWriter(true);
        var factory = new MsSqlMessageProducerFactory(configuration,
        [
            new MsSqlWirePublication { Topic = alias, Type = type, WireMessageWriter = custom ? writer : null },
            new MsSqlWirePublication { Topic = topic, Type = type, WireMessageWriter = custom ? null : inactiveWriter }
        ]);
        using var registry = new ProducerRegistry(useAsync ? await factory.CreateAsync() : factory.Create());
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var observer = new SqlConnection(configuration.ConnectionString);
        await observer.OpenAsync(timeout.Token);
        try
        {
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
            var subscription = new MsSqlSubscription(new SubscriptionName("scheduled"), new ChannelName(topic.Value), topic,
                typeof(Message)) { ScheduledRequeuePublicationTopic = alias };
            var consumers = new MsSqlMessageConsumerFactory(configuration, scheduler);
            await using var consumer = useAsync ? (MsSqlMessageConsumer)consumers.CreateAsync(subscription) : (MsSqlMessageConsumer)consumers.Create(subscription);
            subscription.ScheduledRequeuePublicationTopic = topic;
            using var initialProducer = new MsSqlMessageProducer(configuration, new MsSqlWirePublication { Topic = topic });
            var original = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT, type: type), new MessageBody("scheduled-π"));
            original.Header.Bag["tenant"] = "scheduled-λ";
            original.Header.Bag[Message.OriginalMessageIdHeaderName] = original.Id.Value;
            if (useAsync) await initialProducer.SendAsync(original, timeout.Token);
            else initialProducer.Send(original);
            var source = Assert.Single(useAsync ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(5), timeout.Token) : consumer.Receive());
            Assert.Equal(original.Id, source.Id);
            var before = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
            var delay = TimeSpan.FromSeconds(2);
            var elapsed = Stopwatch.StartNew();

            //Act
            Assert.True(useAsync ? await consumer.RequeueAsync(source, delay, timeout.Token) : consumer.Requeue(source, delay));

            //Assert
            Assert.Equal(before, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
            Assert.False(published.Task.IsCompleted);
            using var query = observer.CreateCommand();
            query.CommandText = $"SELECT COUNT_BIG(*) FROM [{configuration.QueueStoreTable}]";
            Assert.Equal(0L, await query.ExecuteScalarAsync(timeout.Token));
            Assert.True(elapsed.Elapsed < delay);
            var span = await published.Task.WaitAsync(timeout.Token);
            Assert.True(elapsed.Elapsed >= delay);
            while (outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()).Any())
                await Task.Delay(10, timeout.Token);
            query.CommandText = $"SELECT Topic, Payload FROM [{configuration.QueueStoreTable}]";
            await using var rows = await query.ExecuteReaderAsync(timeout.Token);
            Assert.True(await rows.ReadAsync(timeout.Token));
            Assert.Equal(topic.Value, rows.GetString(0));
            var payload = rows.GetString(1);
            Assert.Equal(custom, payload.StartsWith("C:", StringComparison.Ordinal));
            var decoded = new DefaultMsSqlWireMessageReader().ReadMessage(custom ? payload[2..] : payload);
            Assert.Equal(original.Id, decoded.Id);
            Assert.Equal(topic, decoded.Header.Topic);
            Assert.Equal(type, decoded.Header.Type);
            Assert.Equal("scheduled-π", decoded.Body.Value);
            Assert.Equal("scheduled-λ", decoded.Header.Bag["tenant"].ToString());
            Assert.Equal(span.Id, decoded.Header.TraceParent.Value);
            Assert.Equal(0, inactiveWriter.Calls);
            Assert.Equal(custom ? 1 : 0, writer.Calls);
            Assert.False(await rows.ReadAsync(timeout.Token));
            Assert.Equal(before, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
        }
        finally
        {
            using var cleanup = observer.CreateCommand();
            cleanup.CommandText = $"DROP TABLE IF EXISTS [{configuration.QueueStoreTable}]";
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
