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
using Microsoft.Extensions.Time.Testing;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.AWS.Tests.Helpers;
using Paramore.Brighter.AWS.Tests.TestDoubles;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.AWS.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SnsScheduledWireWriterDispatchTraceTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_a_scheduled_sns_message_is_dispatched_should_map_the_current_producer_trace_context(bool useAsync)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Wire-Sns-Scheduled-Trace-{Guid.NewGuid():N}");
        var topicName = reaper.TrackTopic($"Wire-Sns-Scheduled-Trace-{Guid.NewGuid():N}");
        var clients = new AWSClientFactory(connection);
        using var client = clients.CreateSqsClient();
        using var sns = clients.CreateSnsClient();
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
            var topic = await sns.CreateTopicAsync(topicName, timeout.Token);
            var queueAttributes = await client.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = queue.QueueUrl,
                AttributeNames = ["QueueArn"]
            }, timeout.Token);
            var queueArn = queueAttributes.Attributes["QueueArn"];
            var policy = JsonSerializer.Serialize(new
            {
                Version = "2012-10-17",
                Statement = new[]
                {
                    new
                    {
                        Effect = "Allow",
                        Principal = new { Service = "sns.amazonaws.com" },
                        Action = "sqs:SendMessage",
                        Resource = queueArn,
                        Condition = new
                        {
                            ArnEquals = new Dictionary<string, string> { ["aws:SourceArn"] = topic.TopicArn }
                        }
                    }
                }
            });
            await client.SetQueueAttributesAsync(new SetQueueAttributesRequest
            {
                QueueUrl = queue.QueueUrl,
                Attributes = new() { ["Policy"] = policy }
            }, timeout.Token);
            await sns.SubscribeAsync(new Amazon.SimpleNotificationService.Model.SubscribeRequest
            {
                TopicArn = topic.TopicArn,
                Protocol = "sqs",
                Endpoint = queueArn,
                Attributes = new() { ["RawMessageDelivery"] = "false" }
            }, timeout.Token);
            var publication = new SnsPublication(TopicFindBy.Arn, topic.TopicArn)
            {
                Topic = new RoutingKey(topicName),
                WireMessageWriter = new TenantHeaderSnsWireMessageWriter(
                    new DefaultSnsWireMessageWriter(topic.TopicArn))
            };
            var factory = new SnsMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            using var registry = new ProducerRegistry(producers);
            var clock = new FakeTimeProvider();
            var schedulerFactory = new InMemorySchedulerFactory { TimeProvider = clock };
            var outbox = new InMemoryOutbox(clock);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(
                registry,
                new ResiliencePipelineRegistry<string>().AddBrighterDefault(),
                new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null),
                new EmptyMessageTransformerFactory(),
                new EmptyMessageTransformerFactoryAsync(),
                tracer,
                new FindPublicationByPublicationTopicOrRequestType(),
                outbox: outbox);
            CommandProcessor? processor = null;
            var subscribers = new SubscriberRegistry();
            subscribers.RegisterAsync<FireSchedulerMessage, FireSchedulerMessageHandler>();
            var handlers = new SimpleHandlerFactoryAsync(type =>
                type == typeof(FireSchedulerMessageHandler)
                    ? new FireSchedulerMessageHandler(processor!)
                    : throw new InvalidOperationException($"Unexpected handler: {type}"));
            var policies = new PolicyRegistry
            {
                [CommandProcessor.RETRYPOLICYASYNC] = Policy.Handle<Exception>().RetryAsync(),
                [CommandProcessor.CIRCUITBREAKERASYNC] =
                    Policy.Handle<Exception>().CircuitBreakerAsync(1, TimeSpan.FromMilliseconds(1))
            };
            processor = new CommandProcessor(subscribers, handlers, new InMemoryRequestContextFactory(),
                policies, new ResiliencePipelineRegistry<string>(), mediator, schedulerFactory, tracer: tracer);
            using var scheduler = Assert.IsType<InMemoryScheduler>(schedulerFactory.Create(processor));
            var producer = Assert.Single(producers).Value;
            producer.Scheduler = scheduler;
            var context = new RequestContext();
            var delay = TimeSpan.FromMinutes(1);
            Message message;
            string scheduledTraceParent;
            using (var deposit = new Activity("wire-message-scheduling")
                       .SetIdFormat(ActivityIdFormat.W3C)
                       .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                       .Start())
            {
                scheduledTraceParent = deposit.Id!;
                message = new Message(
                    new MessageHeader(new Id("trace-application"), publication.Topic, MessageType.MT_EVENT,
                        traceParent: new TraceParent(scheduledTraceParent), subject: "Scheduled subject"),
                    new MessageBody("trace payload"));
                message.Header.Bag["tenant_id"] = "trace-tenant";
                if (useAsync)
                    await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer)
                        .SendWithDelayAsync(message, delay, timeout.Token);
                else
                    Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).SendWithDelay(message, delay);
            }
            Assert.Equal(scheduledTraceParent, message.Header.TraceParent?.Value);
            Assert.Empty(producerSpans);
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            var beforeDispatch = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                WaitTimeSeconds = 0,
                MaxNumberOfMessages = 1
            }, timeout.Token);
            Assert.Empty(beforeDispatch.Messages ?? []);
            using var dispatch = new Activity("wire-message-dispatch")
                .SetIdFormat(ActivityIdFormat.W3C)
                .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                .Start();

            //Act
            clock.Advance(delay);
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
            using var envelope = JsonDocument.Parse(native.Body);
            var root = envelope.RootElement;
            var attributes = root.GetProperty("MessageAttributes");
            using var cloudEvents = JsonDocument.Parse(
                attributes.GetProperty("cloudeventheaders").GetProperty("Value").GetString()!);
            var wireTraceParent = cloudEvents.RootElement.GetProperty("traceparent").GetString();
            Assert.Equal(producerSpan.Id, wireTraceParent);
            Assert.NotEqual(scheduledTraceParent, wireTraceParent);
            Assert.Equal(producerSpan.Id, message.Header.TraceParent?.Value);
            Assert.Equal(topic.TopicArn, root.GetProperty("TopicArn").GetString());
            Assert.Equal(topic.TopicArn, attributes.GetProperty("topic").GetProperty("Value").GetString());
            Assert.Equal("trace-application", attributes.GetProperty("id").GetProperty("Value").GetString());
            Assert.Equal("trace payload", root.GetProperty("Message").GetString());
            Assert.Equal("Scheduled subject", root.GetProperty("Subject").GetString());
            Assert.Equal("trace-tenant", attributes.GetProperty("tenant-id").GetProperty("Value").GetString());
            Assert.Equal("trace-application", message.Id.Value);
            Assert.Equal("trace payload", message.Body.Value);
            Assert.Equal("Scheduled subject", message.Header.Subject);
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            var sourceAttribute = Assert.Single(message.Header.Bag);
            Assert.Equal("tenant_id", sourceAttribute.Key);
            Assert.Equal("trace-tenant", sourceAttribute.Value);
        }
        finally
        {
            await reaper.ReapAsync();
        }
    }
}
