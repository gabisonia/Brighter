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

using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Transactions;
using System.Collections.Specialized;
using Npgsql;
using Quartz;
using Quartz.Impl;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessageScheduler.Quartz;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using RabbitMQ.Client;
using SchedulerPersistenceProbe;

IScheduler? quartz = null;
try
{
var phase = args[0];
var name = args[1];
var useAsync = bool.Parse(args[2]);
if (!Regex.IsMatch(name, "^[a-z0-9_]+$")) throw new ArgumentException("Invalid probe name");
var directory = args[3];
Directory.CreateDirectory(directory);
var stateFile = Path.Combine(directory, name + ".state.json");
var topic = new RoutingKey("logical-business");
var alias = new RoutingKey("durable-publication");
var type = new CloudEventsType("durable.scheduled.event");
var uri = new Uri("amqp://guest:guest@rabbit:5672/%2f");
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
await using var native = await broker.CreateChannelAsync(cancellationToken: timeout.Token);
await native.ExchangeDeclareAsync(name, ExchangeType.Direct, durable: false, autoDelete: false, cancellationToken: timeout.Token);
await native.QueueDeclareAsync(name, durable: true, exclusive: false, autoDelete: false, cancellationToken: timeout.Token);
await native.QueueBindAsync(name, name, topic.Value, cancellationToken: timeout.Token);
if (phase == "cleanup")
{
    await native.QueueDeleteAsync(name, false, false, cancellationToken: timeout.Token);
    await native.ExchangeDeleteAsync(name, cancellationToken: timeout.Token);
    return;
}
var connection = new RmqMessagingGatewayConnection
{
    AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name), PersistMessages = true
};
var factory = new RmqMessageProducerFactory(connection,
[
    new RmqPublication { Topic = alias, Type = type, MakeChannels = OnMissingChannel.Validate, WireMessageWriter = new PersistedMessageWireWriter() }
]);
var producers = useAsync ? await factory.CreateAsync() : factory.Create();
using var registry = new ProducerRegistry(producers);
var confirmation = new TaskCompletionSource<PublishConfirmationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
((ISupportPublishConfirmation)producers.Single().Value).OnMessagePublished += result => confirmation.TrySetResult(result);
using var telemetry = Sdk.CreateTracerProviderBuilder().Build();
using var tracer = new BrighterTracer();
var publishStopped = new TaskCompletionSource<Activity>(TaskCreationOptions.RunContinuationsAsynchronously);
using var listener = new ActivityListener
{
    ShouldListenTo = source => ReferenceEquals(source, tracer.ActivitySource),
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
    SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
    ActivityStopped = activity =>
    {
        if (activity.Kind == ActivityKind.Producer && Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "publish"))
            publishStopped.TrySetResult(activity);
    }
};
ActivitySource.AddActivityListener(listener);
using var mappers = new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null);
var outbox = new InMemoryOutbox(TimeProvider.System);
using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(registry,
    new ResiliencePipelineRegistry<string>().AddBrighterDefault(), mappers,
    new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer,
    new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
var database = "Host=postgres;Username=postgres;Password=password;Database=brightertests";
if (phase == "register")
{
    await using var admin = new NpgsqlConnection(database);
    await admin.OpenAsync(timeout.Token);
    await using var create = admin.CreateCommand();
    create.CommandText = $"CREATE SCHEMA \"{name}\"";
    await create.ExecuteNonQueryAsync(timeout.Token);
}
var persistedConnection = database + ";SearchPath=" + name;
await using (var schema = new NpgsqlConnection(persistedConnection))
{
    await schema.OpenAsync(timeout.Token);
    await using var check = schema.CreateCommand();
    check.CommandText = "SELECT current_schema()";
    Require((string?)await check.ExecuteScalarAsync(timeout.Token) == name, "owned-database-schema");
    if (phase == "register")
    {
        await using var initialize = schema.CreateCommand();
        initialize.CommandText = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "tables_postgres.sql"), timeout.Token);
        await initialize.ExecuteNonQueryAsync(timeout.Token);
    }
}
var settings = new NameValueCollection
{
    ["quartz.scheduler.instanceName"] = name,
    ["quartz.scheduler.instanceId"] = "AUTO",
    ["quartz.threadPool.threadCount"] = "1",
    ["quartz.jobStore.type"] = "Quartz.Impl.AdoJobStore.JobStoreTX, Quartz",
    ["quartz.jobStore.driverDelegateType"] = "Quartz.Impl.AdoJobStore.PostgreSQLDelegate, Quartz",
    ["quartz.jobStore.tablePrefix"] = "qrtz_",
    ["quartz.jobStore.dataSource"] = "probe",
    ["quartz.dataSource.probe.connectionString"] = persistedConnection,
    ["quartz.dataSource.probe.provider"] = "Npgsql",
    ["quartz.serializer.type"] = "stj",
    ["quartz.jobStore.useProperties"] = "true",
    ["quartz.jobStore.clustered"] = "false"
};
quartz = await new StdSchedulerFactory(settings).GetScheduler(timeout.Token);
var schedulerFactory = new QuartzSchedulerFactory(quartz) { Group = name };
CommandProcessor? processor = null;
var subscribers = new SubscriberRegistry();
subscribers.RegisterAsync<FireSchedulerMessage, FireSchedulerMessageHandler>();
processor = new CommandProcessor(subscribers,
    new SimpleHandlerFactoryAsync(_ => new FireSchedulerMessageHandler(processor!)),
    new InMemoryRequestContextFactory(), new PolicyRegistry(), new ResiliencePipelineRegistry<string>(),
    mediator, schedulerFactory, tracer: tracer);
var scheduler = schedulerFactory.Create(processor);
quartz.JobFactory = new ProbeJobFactory(processor);
var completed = new ProbeCompletionListener();
quartz.ListenerManager.AddJobListener(completed);

if (phase == "register")
{
    var source = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT, type: type), new MessageBody("persisted-payload-π"));
    source.Header.Bag[Message.ProducerTopicHeaderName] = alias.Value;
    source.Header.Bag["tenant"] = "persisted-tenant";
    source.Header.TraceParent = new TraceParent("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01");
    var before = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
    var id = useAsync
        ? await ((IAmAMessageSchedulerAsync)scheduler).ScheduleAsync(source, TimeSpan.FromSeconds(2), timeout.Token)
        : ((IAmAMessageSchedulerSync)scheduler).Schedule(source, TimeSpan.FromSeconds(2));
    Require(before == JsonSerializer.Serialize(source, JsonSerialisationOptions.Options), "source-preservation");
    Require(await quartz.CheckExists(new JobKey(id, name), timeout.Token), "persisted-scheduled-state");
    Require(!confirmation.Task.IsCompleted, "no-dispatch-before-worker");
    Require(await native.BasicGetAsync(name, true, timeout.Token) == null, "empty-native-before-worker");
    await File.WriteAllTextAsync(stateFile, JsonSerializer.Serialize(new ProbeState(id, source.Id.Value, source.Header.TraceParent.Value, Environment.MachineName)), timeout.Token);
    source.Body = new MessageBody("changed-after-registration");
    source.Header.Bag[Message.ProducerTopicHeaderName] = "changed-publication";
    source.Header.Bag["tenant"] = "changed-tenant";
    Console.WriteLine("REGISTERED " + id);
    return;
}

Require(phase == "dispatch", "known-phase");
var state = JsonSerializer.Deserialize<ProbeState>(await File.ReadAllTextAsync(stateFile, timeout.Token))!;
Require(state.SchedulingHost != Environment.MachineName, "fresh-worker-container");
Require(await quartz.CheckExists(new JobKey(state.JobId, name), timeout.Token), "job-survived-process-exit");
await quartz.Start(timeout.Token);
try
{
    var confirmed = await confirmation.Task.WaitAsync(timeout.Token);
    var span = await publishStopped.Task.WaitAsync(timeout.Token);
    Require(await completed.Completion.WaitAsync(timeout.Token) == null, "successful-job-completion");
    while (await quartz.CheckExists(new JobKey(state.JobId, name), timeout.Token) || outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()).Any())
        await Task.Delay(10, timeout.Token);
    var delivered = await native.BasicGetAsync(name, true, timeout.Token);
    Require(confirmed.Success && confirmed.MessageId.Value == state.MessageId && confirmed.Topic == topic, "native-confirmation");
    Require(delivered != null, "native-delivery");
    Require(delivered!.Exchange == name && delivered.RoutingKey == topic.Value, "native-route");
    Require(delivered.BasicProperties.MessageId == state.MessageId, "native-identity");
    Require(delivered.BasicProperties.Type == type.Value, "native-event-type");
    Require(Encoding.UTF8.GetString(delivered.Body.Span) == "persisted-payload-π", "native-payload");
    var headers = delivered.BasicProperties.Headers!;
    var parent = Encoding.UTF8.GetString((byte[])headers["telemetry-parent"]!);
    Require(parent == span.Id && parent != state.ScheduledParent, "fresh-dispatch-telemetry");
    Require(Encoding.UTF8.GetString((byte[])headers["tenant"]!) == "persisted-tenant", "native-tenant");
    Require(!headers.ContainsKey(Message.ProducerTopicHeaderName), "local-selector-not-on-wire");
    Require(await native.BasicGetAsync(name, true, timeout.Token) == null, "one-native-delivery");
    await File.WriteAllTextAsync(Path.Combine(directory, name + ".result.json"), JsonSerializer.Serialize(new
    {
        useAsync, state.JobId, state.MessageId, schedulingHost = state.SchedulingHost,
        dispatchHost = Environment.MachineName, state = "Completed",
        dispatchParent = parent, provider = "Quartz 3.22.4 PostgreSQL JobStoreTX / System.Text.Json", success = true
    }), timeout.Token);
    Console.WriteLine("DISPATCHED " + state.JobId);
}
catch
{
    await File.WriteAllTextAsync(Path.Combine(directory, name + ".failure.json"), JsonSerializer.Serialize(new { jobExists = await quartz.CheckExists(new JobKey(state.JobId, name)), completed = completed.Completion.IsCompleted }));
    throw;
}
finally
{
    using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(15));
    await native.QueueDeleteAsync(name, false, false, cancellationToken: cleanup.Token);
    await native.ExchangeDeleteAsync(name, cancellationToken: cleanup.Token);
}

static void Require(bool condition, string behavior)
{
    if (!condition) throw new InvalidOperationException("Acceptance assertion failed: " + behavior);
}

}
catch (Exception error)
{
    Console.Error.WriteLine(error);
    Environment.ExitCode = 1;
}

finally
{
    if (quartz != null && !quartz.IsShutdown) await quartz.Shutdown(waitForJobsToComplete: true);
}
