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
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Amazon.SQS.Model;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.AWS.V4.Tests.Helpers;
using Paramore.Brighter.AWS.V4.Tests.TestDoubles;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;
using Paramore.Brighter.Observability;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.AWS.V4.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsNonCloudEventsTelemetryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_dispatching_non_cloud_events_telemetry_should_restore_the_consumer_context(
        bool useAsync, bool telemetryInBody)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Non-Cloud-Telemetry-{Guid.NewGuid():N}");
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var traceProvider = Sdk.CreateTracerProviderBuilder().Build();
        using var tracer = new BrighterTracer();
        var producerSpans = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracer.ActivitySource),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.Kind == ActivityKind.Producer
                    && Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "publish"))
                    producerSpans.Enqueue(activity);
            }
        };
        ActivitySource.AddActivityListener(listener);
        var originalBaggage = OpenTelemetry.Baggage.Current;
        IAmAMessageConsumerAsync? consumer = null;

        try
        {
            OpenTelemetry.Baggage.Current = default;
            var queue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new Dictionary<string, string> { ["VisibilityTimeout"] = "1" }
            }, timeout.Token);
            var publication = new SqsPublication(new ChannelName(queue.QueueUrl),
                findQueueBy: QueueFindBy.Url, makeChannels: OnMissingChannel.Validate)
            {
                Topic = new RoutingKey(queueName),
                WireMessageWriter = new NonCloudEventsSqsWireMessageWriter(telemetryInBody)
            };
            var factory = new SqsMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            using var registry = new ProducerRegistry(producers);
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(
                registry, new ResiliencePipelineRegistry<string>().AddBrighterDefault(),
                new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null),
                new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(),
                tracer, new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
            var context = new RequestContext();
            Message outgoing;
            string depositParent;
            using (var deposit = new Activity("non-cloud-deposit")
                       .SetIdFormat(ActivityIdFormat.W3C)
                       .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                       .Start())
            {
                depositParent = deposit.Id!;
                outgoing = new Message(new MessageHeader(new Id("non-cloud-application"), publication.Topic,
                    MessageType.MT_EVENT, traceParent: new TraceParent(depositParent),
                    traceState: new TraceState("vendor=deposit")), new MessageBody("application payload"));
                outgoing.Header.Baggage.Add("stage", "deposit");
                context.Span = deposit;
                if (useAsync)
                    await mediator.AddToOutboxAsync(outgoing, context, cancellationToken: timeout.Token);
                else
                    mediator.AddToOutbox(outgoing, context);
            }
            var deposited = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(depositParent, deposited.Header.TraceParent?.Value);
            Assert.Equal("vendor=deposit", deposited.Header.TraceState?.Value);
            Assert.Equal("stage=deposit", deposited.Header.Baggage.ToString());
            OpenTelemetry.Baggage.SetBaggage(new Dictionary<string, string?> { ["stage"] = "dispatch" });

            //Act
            using (var dispatch = new Activity("non-cloud-dispatch")
                       .SetIdFormat(ActivityIdFormat.W3C)
                       .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded))
            {
                dispatch.TraceStateString = "vendor=dispatch";
                dispatch.Start();
                context.Span = dispatch;
                if (useAsync)
                    await mediator.ClearOutboxAsync([outgoing.Id], context, cancellationToken: timeout.Token);
                else
                    mediator.ClearOutbox([outgoing.Id], context);
            }
            var nativeResponse = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl, WaitTimeSeconds = 10, MaxNumberOfMessages = 1,
                MessageAttributeNames = ["All"]
            }, timeout.Token);

            //Assert
            var native = Assert.Single(nativeResponse.Messages ?? []);
            var producer = Assert.Single(producerSpans);
            string nativeParent;
            string nativeState;
            string nativeBaggage;
            if (telemetryInBody)
            {
                using var document = JsonDocument.Parse(native.Body);
                Assert.Equal("application payload", document.RootElement.GetProperty("payload").GetString());
                nativeParent = document.RootElement.GetProperty("parent").GetString()!;
                nativeState = document.RootElement.GetProperty("state").GetString()!;
                nativeBaggage = document.RootElement.GetProperty("baggage").GetString()!;
                Assert.Single(native.MessageAttributes);
            }
            else
            {
                Assert.Equal("application payload", native.Body);
                Assert.Equal(4, native.MessageAttributes.Count);
                nativeParent = native.MessageAttributes["telemetry-parent"].StringValue;
                nativeState = native.MessageAttributes["telemetry-state"].StringValue;
                nativeBaggage = native.MessageAttributes["telemetry-baggage"].StringValue;
            }
            Assert.Equal("non-cloud-application", native.MessageAttributes["legacy-id"].StringValue);
            Assert.False(native.MessageAttributes.ContainsKey("cloudeventheaders"));
            Assert.Equal(producer.Id, nativeParent);
            Assert.NotEqual(depositParent, nativeParent);
            Assert.Equal("vendor=dispatch", nativeState);
            Assert.Equal("stage=dispatch", nativeBaggage);
            Assert.Equal("application payload", outgoing.Body.Value);
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));

            await client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
            {
                QueueUrl = queue.QueueUrl, ReceiptHandle = native.ReceiptHandle, VisibilityTimeout = 0
            }, timeout.Token);
            var subscription = new SqsSubscription(new SubscriptionName(queueName), new ChannelName(queue.QueueUrl),
                ChannelType.PointToPoint, publication.Topic, requestType: typeof(Command),
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                findQueueBy: QueueFindBy.Url, makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = new NonCloudEventsSqsWireMessageReader(publication.Topic, telemetryInBody)
            };
            var consumerFactory = new SqsMessageConsumerFactory(connection);
            consumer = useAsync ? consumerFactory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(consumerFactory.Create(subscription));
            var received = useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
            var incoming = Assert.Single(received);
            Assert.Equal(outgoing.Id, incoming.Id);
            Assert.Equal("application payload", incoming.Body.Value);
            Assert.Equal(nativeParent, incoming.Header.TraceParent?.Value);
            Assert.Equal(nativeState, incoming.Header.TraceState?.Value);
            Assert.Equal(nativeBaggage, incoming.Header.Baggage.ToString());
            Assert.False(string.IsNullOrEmpty(Assert.IsType<string>(incoming.Header.Bag["ReceiptHandle"])));

            OpenTelemetry.Baggage.SetBaggage(new Dictionary<string, string?> { ["stage"] = "unrelated" });
            using (var unrelated = new Activity("unrelated-consumer-ambient")
                       .SetIdFormat(ActivityIdFormat.W3C)
                       .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                       .Start())
            {
                tracer.PropagateConsumerContext(incoming);
                using var process = tracer.CreateSpan(MessagePumpSpanOperation.Process, incoming, MessagingSystem.AWSSQS);
                Assert.NotNull(process);
                Assert.Equal(producer.TraceId, process.TraceId);
                Assert.Equal(producer.SpanId, process.ParentSpanId);
                Assert.NotEqual(unrelated.TraceId, process.TraceId);
                Assert.Equal("vendor=dispatch", process.TraceStateString);
                Assert.Equal("dispatch", OpenTelemetry.Baggage.GetBaggage("stage"));
            }
            if (useAsync)
                await consumer.AcknowledgeAsync(incoming, timeout.Token);
            else
                Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Acknowledge(incoming);
            await Task.Delay(TimeSpan.FromSeconds(2), timeout.Token);
            var remaining = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl, WaitTimeSeconds = 1, MaxNumberOfMessages = 1
            }, timeout.Token);
            Assert.Empty(remaining.Messages ?? []);
        }
        finally
        {
            OpenTelemetry.Baggage.Current = originalBaggage;
            try
            {
                if (consumer != null)
                    await consumer.DisposeAsync();
            }
            finally
            {
                await reaper.ReapAsync();
            }
        }
    }
}
