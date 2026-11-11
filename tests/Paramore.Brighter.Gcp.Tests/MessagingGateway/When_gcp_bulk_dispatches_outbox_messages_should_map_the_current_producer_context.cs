#region Licence

/* The MIT License (MIT)
Copyright © 2026 Irakli Gabisonia

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
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

[Trait("Category", "GcpNativeWireWriter")]
[Trait("Requires", "PubSubEmulator")]
public class GcpBulkDispatchTraceTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_gcp_bulk_dispatches_outbox_messages_should_map_the_current_producer_context(bool custom)
    {
        // Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")));
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "bulk-trace-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var subscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var route = new RoutingKey(name);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        var topics = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var observer = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var previousBaggage = OpenTelemetry.Baggage.Current;
        await topics.CreateTopicAsync(topic, calls);
        try
        {
            await observer.CreateSubscriptionAsync(new Google.Cloud.PubSub.V1.Subscription
            { SubscriptionName = subscription, TopicAsTopicName = topic, AckDeadlineSeconds = 60 }, calls);
            var writer = new GcpRejectionSelectedWireWriter("selected", 0);
            var connection = new GcpMessagingGatewayConnection
            {
                ProjectId = project,
                TopicManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
                PublisherConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly
            };
            var factory = new GcpPubSubMessageProducerFactory(connection,
                [new GcpPublication { Topic = route, MakeChannels = OnMissingChannel.Assume,
                    WireMessageWriter = custom ? writer : null }]);
            using var registry = new ProducerRegistry(await factory.CreateAsync());
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
                    if (activity.Kind == ActivityKind.Producer)
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
            const string depositedTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
            var sources = Enumerable.Range(0, 2).Select(index => new Message(
                new MessageHeader(Id.Random(), route, MessageType.MT_EVENT)
                { TraceParent = new TraceParent(depositedTrace), TraceState = new TraceState("vendor=deposit") },
                new MessageBody("bulk-" + index))).ToArray();
            foreach (var source in sources)
                outbox.Add(source, new RequestContext());
            Assert.Equal(0, writer.Calls);
            using var parent = new Activity("clear GCP bulk").SetIdFormat(ActivityIdFormat.W3C).Start();
            parent.TraceStateString = "vendor=dispatch";
            OpenTelemetry.Baggage.SetBaggage("bulk", "dispatch");

            // Act
            await mediator.ClearOutstandingFromOutboxAsync(2, TimeSpan.Zero, useBulk: true,
                new RequestContext { Span = parent }, cancellationToken: timeout.Token);
            var span = await published.Task.WaitAsync(timeout.Token);
            var received = new List<ReceivedMessage>();
            while (received.Count < sources.Length)
                received.AddRange((await observer.PullAsync(new PullRequest
                { SubscriptionAsSubscriptionName = subscription, MaxMessages = sources.Length - received.Count }, calls)).ReceivedMessages);

            // Assert
            Assert.NotEqual(depositedTrace, span.Id);
            Assert.NotEqual(parent.Id, span.Id);
            Assert.Equal(custom ? sources.Length : 0, writer.Calls);
            foreach (var source in sources)
            {
                var native = Assert.Single(received, delivery => delivery.Message.Attributes[HeaderNames.Id] == source.Id.Value).Message;
                Assert.Equal(span.Id, native.Attributes[HeaderNames.TraceParent]);
                Assert.Equal("vendor=dispatch", native.Attributes[HeaderNames.TraceState]);
                Assert.Contains("bulk=dispatch", native.Attributes[HeaderNames.Baggage]);
                Assert.Equal(custom ? "selected:" + source.Body.Value : source.Body.Value, native.Data.ToStringUtf8());
            }
            await observer.AcknowledgeAsync(subscription, received.Select(delivery => delivery.AckId), calls);
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()));
        }
        finally
        {
            OpenTelemetry.Baggage.Current = previousBaggage;
            try { await observer.DeleteSubscriptionAsync(subscription); }
            finally { await topics.DeleteTopicAsync(topic); }
        }
    }
}
