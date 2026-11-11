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
using Paramore.Brighter.ServiceActivator;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.AWS.V4.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SnsNonCloudEventsPumpTelemetryTests
{
    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task When_dispatching_sns_non_cloud_events_telemetry_should_reach_the_application_handler(
        bool useAsync, bool telemetryInBody, bool rawDelivery)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Sns-Non-Cloud-Pump-{Guid.NewGuid():N}");
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var sns = new AWSClientFactory(connection).CreateSnsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var traceProvider = Sdk.CreateTracerProviderBuilder().Build();
        using var tracer = new BrighterTracer();
        var producerSpans = new ConcurrentQueue<Activity>();
        var processSpans = new ConcurrentQueue<Activity>();
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
                if (Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "process"))
                    processSpans.Enqueue(activity);
            }
        };
        ActivitySource.AddActivityListener(listener);
        var originalBaggage = OpenTelemetry.Baggage.Current;
        IAmAChannel? channel = null;
        Task? pumpTask = null;

        try
        {
            OpenTelemetry.Baggage.Current = default;
            var queue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new Dictionary<string, string> { ["VisibilityTimeout"] = "1" }
            }, timeout.Token);
            var topic = await sns.CreateTopicAsync(reaper.TrackTopic(queueName), timeout.Token);
            var queueAttributes = await client.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = queue.QueueUrl, AttributeNames = ["QueueArn"]
            }, timeout.Token);
            var queueArn = queueAttributes.Attributes["QueueArn"];
            await client.SetQueueAttributesAsync(new SetQueueAttributesRequest
            {
                QueueUrl = queue.QueueUrl,
                Attributes = new()
                {
                    ["Policy"] = JsonSerializer.Serialize(new
                    {
                        Version = "2012-10-17",
                        Statement = new[] { new
                        {
                            Effect = "Allow", Principal = new { Service = "sns.amazonaws.com" },
                            Action = "sqs:SendMessage", Resource = queueArn,
                            Condition = new { ArnEquals = new Dictionary<string, string> { ["aws:SourceArn"] = topic.TopicArn } }
                        } }
                    })
                }
            }, timeout.Token);
            await sns.SubscribeAsync(new Amazon.SimpleNotificationService.Model.SubscribeRequest
            {
                TopicArn = topic.TopicArn, Protocol = "sqs", Endpoint = queueArn,
                Attributes = new() { ["RawMessageDelivery"] = rawDelivery ? "true" : "false" }
            }, timeout.Token);
            var publication = new SnsPublication(TopicFindBy.Arn, topic.TopicArn)
            {
                Topic = new RoutingKey(queueName),
                WireMessageWriter = new NonCloudEventsSnsWireMessageWriter(telemetryInBody)
            };
            var factory = new SnsMessageProducerFactory(connection, [publication]);
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
            var nativeBody = native.Body;
            var nativeAttributes = new Dictionary<string, string>();
            if (rawDelivery)
            {
                foreach (var attribute in native.MessageAttributes)
                {
                    Assert.Equal("String", attribute.Value.DataType);
                    nativeAttributes.Add(attribute.Key, attribute.Value.StringValue);
                }
            }
            else
            {
                using var envelope = JsonDocument.Parse(native.Body);
                Assert.Equal("Notification", envelope.RootElement.GetProperty("Type").GetString());
                Assert.Equal(topic.TopicArn, envelope.RootElement.GetProperty("TopicArn").GetString());
                nativeBody = envelope.RootElement.GetProperty("Message").GetString()!;
                foreach (var attribute in envelope.RootElement.GetProperty("MessageAttributes").EnumerateObject())
                {
                    Assert.Equal("String", attribute.Value.GetProperty("Type").GetString());
                    nativeAttributes.Add(attribute.Name, attribute.Value.GetProperty("Value").GetString()!);
                }
            }
            Assert.True(nativeAttributes.ContainsKey("legacy-id"));
            string nativeParent;
            string nativeState;
            string nativeBaggage;
            if (telemetryInBody)
            {
                using var document = JsonDocument.Parse(nativeBody);
                Assert.Equal("application payload", document.RootElement.GetProperty("payload").GetString());
                nativeParent = document.RootElement.GetProperty("parent").GetString()!;
                nativeState = document.RootElement.GetProperty("state").GetString()!;
                nativeBaggage = document.RootElement.GetProperty("baggage").GetString()!;
                Assert.Single(nativeAttributes);
            }
            else
            {
                Assert.Equal("application payload", nativeBody);
                Assert.Equal(4, nativeAttributes.Count);
                nativeParent = nativeAttributes["telemetry-parent"];
                nativeState = nativeAttributes["telemetry-state"];
                nativeBaggage = nativeAttributes["telemetry-baggage"];
            }
            Assert.Equal("non-cloud-application", nativeAttributes["legacy-id"]);
            Assert.False(nativeAttributes.ContainsKey("cloudeventheaders"));
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
            var subscription = new SqsSubscription(new SubscriptionName(queueName), new ChannelName(queueName),
                ChannelType.PointToPoint, publication.Topic, requestType: useAsync ? typeof(PumpTelemetryAsyncCommand) : typeof(PumpTelemetrySyncCommand),
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                findQueueBy: QueueFindBy.Name, makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = new NonCloudEventsSnsDeliveryWireMessageReader(publication.Topic, telemetryInBody, rawDelivery)
            };
            var observed = new TaskCompletionSource<(string Id, string Payload, Activity? Span, string? Baggage)>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var invocations = 0;
            void Observe(Id id, string payload, IRequestContext? requestContext)
            {
                Interlocked.Increment(ref invocations);
                observed.TrySetResult((id.Value, payload, requestContext?.Span, OpenTelemetry.Baggage.GetBaggage("stage")));
            }
            var subscribers = new SubscriberRegistry();
            var requestContexts = new InMemoryRequestContextFactory();
            var channelFactory = new ChannelFactory(connection);
            IAmAMessagePump pump;
            if (useAsync)
            {
                var handler = new PumpTelemetryAsyncHandler(Observe);
                subscribers.RegisterAsync<PumpTelemetryAsyncCommand, PumpTelemetryAsyncHandler>();
                var processor = new CommandProcessor(subscribers, new QuickHandlerFactoryAsync(() => handler),
                    requestContexts, new PolicyRegistry(), new ResiliencePipelineRegistry<string>(),
                    new InMemorySchedulerFactory(), tracer: tracer);
                var mappers = new MessageMapperRegistry(null,
                    new SimpleMessageMapperFactoryAsync(_ => new PumpTelemetryAsyncMapper()));
                mappers.RegisterAsync<PumpTelemetryAsyncCommand, PumpTelemetryAsyncMapper>();
                var asyncChannel = await channelFactory.CreateAsyncChannelAsync(subscription, timeout.Token);
                channel = asyncChannel;
                pump = new Proactor(processor, _ => typeof(PumpTelemetryAsyncCommand), mappers,
                    new EmptyMessageTransformerFactoryAsync(), requestContexts, asyncChannel, tracer)
                {
                    TimeOut = TimeSpan.FromMilliseconds(200), RequeueCount = 0
                };
            }
            else
            {
                var handler = new PumpTelemetrySyncHandler(Observe);
                subscribers.Register<PumpTelemetrySyncCommand, PumpTelemetrySyncHandler>();
                var processor = new CommandProcessor(subscribers, new QuickHandlerFactory(() => handler),
                    requestContexts, new PolicyRegistry(), new ResiliencePipelineRegistry<string>(),
                    new InMemorySchedulerFactory(), tracer: tracer);
                var mappers = new MessageMapperRegistry(
                    new SimpleMessageMapperFactory(_ => new PumpTelemetrySyncMapper()), null);
                mappers.Register<PumpTelemetrySyncCommand, PumpTelemetrySyncMapper>();
                var syncChannel = channelFactory.CreateSyncChannel(subscription);
                channel = syncChannel;
                pump = new Reactor(processor, _ => typeof(PumpTelemetrySyncCommand), mappers,
                    new EmptyMessageTransformerFactory(), requestContexts, syncChannel, tracer)
                {
                    TimeOut = TimeSpan.FromMilliseconds(200), RequeueCount = 0
                };
            }

            OpenTelemetry.Baggage.SetBaggage(new Dictionary<string, string?> { ["stage"] = "unrelated" });
            using var unrelated = new Activity("unrelated-pump-ambient")
                .SetIdFormat(ActivityIdFormat.W3C)
                .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                .Start();

            //Act
            pumpTask = Task.Factory.StartNew(pump.Run, CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
            var handled = await observed.Task.WaitAsync(timeout.Token);
            channel.Enqueue(MessageFactory.CreateQuitMessage(publication.Topic));
            await pumpTask.WaitAsync(timeout.Token);

            //Assert
            Assert.Equal(1, Volatile.Read(ref invocations));
            Assert.Equal(outgoing.Id.Value, handled.Id);
            Assert.Equal("application payload", handled.Payload);
            var process = Assert.Single(processSpans);
            Assert.Equal(producer.TraceId, process.TraceId);
            Assert.Equal(producer.SpanId, process.ParentSpanId);
            Assert.NotEqual(unrelated.TraceId, process.TraceId);
            Assert.Equal("vendor=dispatch", process.TraceStateString);
            Assert.NotNull(handled.Span);
            Assert.Equal(producer.TraceId, handled.Span.TraceId);
            Assert.Equal("vendor=dispatch", handled.Span.TraceStateString);
            Assert.Equal("dispatch", handled.Baggage);
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
                if (channel != null)
                {
                    try
                    {
                        if (pumpTask != null)
                        {
                            if (!pumpTask.IsCompleted)
                                channel.Enqueue(MessageFactory.CreateQuitMessage(new RoutingKey(queueName)));
                            await pumpTask.WaitAsync(TimeSpan.FromSeconds(10));
                        }
                    }
                    finally
                    {
                        channel.Dispose();
                    }
                }
            }
            finally
            {
                await reaper.ReapAsync();
            }
        }
    }
}
