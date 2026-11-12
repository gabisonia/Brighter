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
public class SqsWireWriterDispatchTraceTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_clearing_the_outbox_should_map_the_dispatch_trace_context(bool useAsync)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Wire-Writer-Trace-{Guid.NewGuid():N}");
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
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

        try
        {
            var queue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName
            }, timeout.Token);
            var publication = new SqsPublication(
                new ChannelName(queue.QueueUrl),
                findQueueBy: QueueFindBy.Url,
                makeChannels: OnMissingChannel.Validate)
            {
                Topic = new RoutingKey(queueName),
                WireMessageWriter = new TenantHeaderSqsWireMessageWriter(
                    new DefaultSqsWireMessageWriter(queue.QueueUrl))
            };
            var factory = new SqsMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            using var registry = new ProducerRegistry(producers);
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(
                registry,
                new ResiliencePipelineRegistry<string>().AddBrighterDefault(),
                new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null),
                new EmptyMessageTransformerFactory(),
                new EmptyMessageTransformerFactoryAsync(),
                tracer,
                new FindPublicationByPublicationTopicOrRequestType(),
                outbox: outbox);
            var context = new RequestContext();
            Message message;
            string depositTraceParent;
            using (var deposit = new Activity("wire-message-deposit")
                       .SetIdFormat(ActivityIdFormat.W3C)
                       .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                       .Start())
            {
                depositTraceParent = deposit.Id!;
                message = new Message(
                    new MessageHeader(new Id("trace-application"), publication.Topic, MessageType.MT_EVENT,
                        traceParent: new TraceParent(depositTraceParent)),
                    new MessageBody("trace payload"));
                message.Header.Bag["tenant_id"] = "trace-tenant";
                context.Span = deposit;
                if (useAsync)
                    await mediator.AddToOutboxAsync(message, context, cancellationToken: timeout.Token);
                else
                    mediator.AddToOutbox(message, context);
            }
            var pending = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(depositTraceParent, pending.Header.TraceParent?.Value);
            using var dispatch = new Activity("wire-message-dispatch")
                .SetIdFormat(ActivityIdFormat.W3C)
                .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                .Start();
            context.Span = dispatch;

            //Act
            if (useAsync)
                await mediator.ClearOutboxAsync([message.Id], context, cancellationToken: timeout.Token);
            else
                mediator.ClearOutbox([message.Id], context);
            var response = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                WaitTimeSeconds = 10,
                MaxNumberOfMessages = 1,
                MessageAttributeNames = ["All"]
            }, timeout.Token);

            //Assert
            var native = Assert.Single(response.Messages ?? []);
            var producerSpan = Assert.Single(producerSpans);
            using var cloudEvents = JsonDocument.Parse(native.MessageAttributes["cloudeventheaders"].StringValue);
            var wireTraceParent = cloudEvents.RootElement.GetProperty("traceparent").GetString();
            Assert.Equal(producerSpan.Id, wireTraceParent);
            Assert.Equal(dispatch.TraceId, producerSpan.TraceId);
            Assert.NotEqual(depositTraceParent, wireTraceParent);
            Assert.Equal(producerSpan.Id, message.Header.TraceParent?.Value);
            Assert.Equal("trace-application", native.MessageAttributes["id"].StringValue);
            Assert.Equal("trace payload", native.Body);
            Assert.Equal("trace-tenant", native.MessageAttributes["tenant-id"].StringValue);
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
        }
        finally
        {
            await reaper.ReapAsync();
        }
    }
}
