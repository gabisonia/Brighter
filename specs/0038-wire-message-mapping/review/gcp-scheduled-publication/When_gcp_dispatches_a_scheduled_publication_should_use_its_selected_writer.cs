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
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.Gcp.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.GcpPubSub;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

/// <summary>Verifies selected GCP mapping at actual scheduler callback dispatch.</summary>
[Trait("Category", "GcpNativeWireWriter")]
[Trait("Requires", "PubSubEmulator")]
public class GcpScheduledPublicationSelectionTests
{
    /// <summary>Dispatches through typed producer lookup with current tracing and native acknowledgement.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_gcp_dispatches_a_scheduled_publication_should_use_its_selected_writer(bool useAsync, bool custom)
    {
        // Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")));
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "scheduled-wire-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var nativeSubscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var route = new RoutingKey(name);
        var type = new CloudEventsType("scheduled.gcp.event");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        var topics = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var observer = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var previousBaggage = OpenTelemetry.Baggage.Current;
        await topics.CreateTopicAsync(topic, calls);
        try
        {
            await observer.CreateSubscriptionAsync(new Google.Cloud.PubSub.V1.Subscription
            { SubscriptionName = nativeSubscription, TopicAsTopicName = topic, AckDeadlineSeconds = 60, EnableMessageOrdering = true }, calls);
            var writer = new GcpRejectionSelectedWireWriter("selected", 0);
            var inactive = new GcpRejectionSelectedWireWriter("inactive", 0);
            var connection = new GcpMessagingGatewayConnection
            {
                ProjectId = project,
                TopicManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
                PublisherConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly
            };
            var factory = new GcpPubSubMessageProducerFactory(connection,
            [
                new GcpPublication { Topic = route, Type = type, MakeChannels = OnMissingChannel.Assume,
                    EnableMessageOrdering = true, WireMessageWriter = custom ? writer : null },
                new GcpPublication { Topic = route, Type = new CloudEventsType("inactive.gcp.event"),
                    MakeChannels = OnMissingChannel.Assume, WireMessageWriter = inactive }
            ]);
            using var registry = new ProducerRegistry(useAsync ? await factory.CreateAsync() : factory.Create());
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
            var producer = Assert.IsType<GcpMessageProducer>(registry.LookupBy(route, type));
            producer.Scheduler = scheduler;
            var source = new Message(new MessageHeader(Id.Random(), route, MessageType.MT_EVENT, type: type,
                partitionKey: new PartitionKey("scheduled-order")), new MessageBody("scheduled-π"));
            source.Header.Bag["tenant"] = "scheduled-λ";
            using var parent = new Activity("schedule GCP").SetIdFormat(ActivityIdFormat.W3C).Start();
            parent.TraceStateString = "vendor=scheduled";
            OpenTelemetry.Baggage.SetBaggage("scheduled", "gcp");
            var delay = TimeSpan.FromSeconds(2);
            var elapsed = Stopwatch.StartNew();

            // Act
            if (useAsync) await producer.SendWithDelayAsync(source, delay, timeout.Token);
            else producer.SendWithDelay(source, delay);

            // Assert
            Assert.Equal(0, writer.Calls);
            Assert.False(published.Task.IsCompleted);
            ReceivedMessage? delivered = null;
            while (delivered == null)
            {
                var response = await observer.PullAsync(new PullRequest
                { SubscriptionAsSubscriptionName = nativeSubscription, MaxMessages = 1 }, calls);
                delivered = response.ReceivedMessages.SingleOrDefault();
            }
            Assert.True(elapsed.Elapsed >= delay);
            var span = await published.Task.WaitAsync(timeout.Token);
            var native = delivered.Message;
            Assert.Equal(custom ? "selected:scheduled-π" : "scheduled-π", native.Data.ToStringUtf8());
            Assert.Equal("scheduled-order", native.OrderingKey);
            Assert.Equal(custom ? 1 : 0, writer.Calls);
            Assert.Equal(0, inactive.Calls);
            var expected = new DefaultGcpPubSubWireMessageWriter().WriteMessage(source);
            foreach (var key in new[] { HeaderNames.Id, HeaderNames.Type, "tenant" })
                Assert.Equal(expected.Attributes[key], native.Attributes[key]);
            Assert.Equal(span.Id, native.Attributes[HeaderNames.TraceParent]);
            Assert.NotEqual(parent.Id, span.Id);
            Assert.Equal("vendor=scheduled", native.Attributes[HeaderNames.TraceState]);
            Assert.Contains("scheduled=gcp", native.Attributes[HeaderNames.Baggage]);
            Assert.Equal("scheduled-π", source.Body.Value);
            Assert.Equal(route, source.Header.Topic);
            Assert.Equal(type, source.Header.Type);
            Assert.False(source.Header.Bag.ContainsKey(Message.ProducerTopicHeaderName));
            await observer.AcknowledgeAsync(nativeSubscription, [delivered.AckId], calls);
            while (outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()).Any())
                await Task.Delay(10, timeout.Token);
        }
        finally
        {
            OpenTelemetry.Baggage.Current = previousBaggage;
            try { await observer.DeleteSubscriptionAsync(nativeSubscription); }
            finally { await topics.DeleteTopicAsync(topic); }
        }
    }
}
