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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Paramore.Brighter.Observability;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

[Trait("Category", "ASB")]
public class AzureServiceBusBulkDispatchTraceTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_asb_bulk_dispatches_outbox_messages_should_map_the_current_producer_context(bool useQueue, bool custom)
    {
        // Arrange
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var client = new InMemoryBatchServiceBusClient();
        var route = new RoutingKey("bulk-trace-" + Guid.NewGuid().ToString("N"));
        var writer = new AsbExternalWireWriter();
        using var registry = new AzureServiceBusProducerRegistryFactory(client,
            [new AzureServiceBusPublication { Topic = route, UseServiceBusQueue = useQueue,
                MakeChannels = OnMissingChannel.Assume, WireMessageWriter = custom ? writer : null }]).Create();
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
        Assert.Equal(0, writer.WriteCount);
        using var parent = new Activity("clear ASB bulk").SetIdFormat(ActivityIdFormat.W3C).Start();
        parent.TraceStateString = "vendor=dispatch";
        var previousBaggage = OpenTelemetry.Baggage.Current;
        OpenTelemetry.Baggage.SetBaggage("bulk", "dispatch");
        try
        {
            // Act
            await mediator.ClearOutstandingFromOutboxAsync(2, TimeSpan.Zero, useBulk: true,
                new RequestContext { Span = parent }, cancellationToken: timeout.Token);
            var span = await published.Task.WaitAsync(timeout.Token);

            // Assert
            Assert.NotEqual(depositedTrace, span.Id);
            Assert.NotEqual(parent.Id, span.Id);
            Assert.Equal(custom ? sources.Length : 0, writer.WriteCount);
            Assert.Equal(sources.Length, client.Sent.Count);
            var prefix = custom ? "external-" : "cloudevents:";
            foreach (var source in sources)
            {
                var native = Assert.Single(client.Sent, message => message.MessageId == (custom ? "wire-" : "") + source.Id.Value);
                Assert.Equal(span.Id, native.ApplicationProperties[prefix + "traceparent"]);
                Assert.Equal("vendor=dispatch", native.ApplicationProperties[prefix + "tracestate"]);
                Assert.Contains("bulk=dispatch", Assert.IsType<string>(native.ApplicationProperties[prefix + "baggage"]));
                Assert.Equal(custom ? new byte[] { 0, 255, 128 }.Concat(source.Body.Memory.ToArray()).ToArray() : source.Body.Memory.ToArray(), native.Body.ToArray());
            }
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, new RequestContext()));
        }
        finally
        {
            OpenTelemetry.Baggage.Current = previousBaggage;
        }
    }
}
