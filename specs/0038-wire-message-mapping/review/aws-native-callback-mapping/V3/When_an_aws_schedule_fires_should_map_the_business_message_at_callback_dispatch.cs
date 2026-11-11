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
using Amazon.IdentityManagement.Model;
using Amazon.Scheduler.Model;
using Amazon.SQS.Model;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.AWS.Tests.Helpers;
using Paramore.Brighter.AWS.Tests.TestDoubles;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessageScheduler.AWS;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Polly;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.AWS.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class AwsNativeCallbackWireMappingTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_an_aws_schedule_fires_should_map_the_business_message_at_callback_dispatch(
        bool useAsync, bool useSns)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var clients = new AWSClientFactory(connection);
        var reaper = new AwsTestResourceReaper(connection);
        var suffix = Guid.NewGuid().ToString("N");
        var callbackName = reaper.TrackQueue($"Callback-{suffix}");
        var businessName = reaper.TrackQueue($"Business-{suffix}");
        var groupName = $"Mapping-{suffix}";
        var roleName = $"Mapping-{suffix}";
        var scheduleId = $"Message-{suffix}";
        using var sqs = clients.CreateSqsClient();
        using var sns = clients.CreateSnsClient();
        using var schedules = clients.CreateSchedulerClient();
        using var iam = Paramore.Brighter.MessageScheduler.AWS.AWSClientFactoryExtensions.CreateIdentityClient(clients);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        using var provider = Sdk.CreateTracerProviderBuilder().Build();
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
        var groupCreated = false;
        var roleCreated = false;
        var policyCreated = false;

        try
        {
            OpenTelemetry.Baggage.Current = default;
            var callbackQueue = await sqs.CreateQueueAsync(callbackName, timeout.Token);
            var businessQueue = await sqs.CreateQueueAsync(businessName, timeout.Token);
            var callbackAttributes = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = callbackQueue.QueueUrl, AttributeNames = ["QueueArn"]
            }, timeout.Token);
            var topic = new RoutingKey(businessName);
            IAmAMessageProducerFactory producerFactory;
            if (useSns)
            {
                var nativeTopic = await sns.CreateTopicAsync(reaper.TrackTopic(businessName), timeout.Token);
                var attributes = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
                {
                    QueueUrl = businessQueue.QueueUrl, AttributeNames = ["QueueArn"]
                }, timeout.Token);
                var queueArn = attributes.Attributes["QueueArn"];
                await sqs.SetQueueAttributesAsync(new SetQueueAttributesRequest
                {
                    QueueUrl = businessQueue.QueueUrl,
                    Attributes = new()
                    {
                        ["Policy"] = JsonSerializer.Serialize(new
                        {
                            Version = "2012-10-17",
                            Statement = new[] { new
                            {
                                Effect = "Allow", Principal = new { Service = "sns.amazonaws.com" },
                                Action = "sqs:SendMessage", Resource = queueArn,
                                Condition = new { ArnEquals = new Dictionary<string, string> { ["aws:SourceArn"] = nativeTopic.TopicArn } }
                            } }
                        })
                    }
                }, timeout.Token);
                await sns.SubscribeAsync(new Amazon.SimpleNotificationService.Model.SubscribeRequest
                {
                    TopicArn = nativeTopic.TopicArn, Protocol = "sqs", Endpoint = queueArn,
                    Attributes = new() { ["RawMessageDelivery"] = "true" }
                }, timeout.Token);
                producerFactory = new SnsMessageProducerFactory(connection, [new SnsPublication(TopicFindBy.Arn, nativeTopic.TopicArn)
                {
                    Topic = topic, WireMessageWriter = new NativeCallbackSnsWireMessageWriter()
                }]);
            }
            else
            {
                producerFactory = new SqsMessageProducerFactory(connection, [new SqsPublication(
                    new ChannelName(businessQueue.QueueUrl), findQueueBy: QueueFindBy.Url, makeChannels: OnMissingChannel.Validate)
                {
                    Topic = topic, WireMessageWriter = new NonCloudEventsSqsWireMessageWriter(true)
                }]);
            }

            var producers = useAsync ? await producerFactory.CreateAsync() : producerFactory.Create();
            using var registry = new ProducerRegistry(producers);
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(registry,
                new ResiliencePipelineRegistry<string>().AddBrighterDefault(),
                new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null),
                new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer,
                new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
            CommandProcessor? processor = null;
            var subscribers = new SubscriberRegistry();
            subscribers.RegisterAsync<FireAwsScheduler, AwsSchedulerFiredHandler>();
            subscribers.RegisterAsync<FireSchedulerMessage, FireSchedulerMessageHandler>();
            var handlers = new SimpleHandlerFactoryAsync(type => type == typeof(AwsSchedulerFiredHandler)
                ? new AwsSchedulerFiredHandler(processor!)
                : type == typeof(FireSchedulerMessageHandler)
                    ? new FireSchedulerMessageHandler(processor!)
                    : throw new InvalidOperationException($"Unexpected handler: {type}"));
            var policies = new PolicyRegistry
            {
                [CommandProcessor.RETRYPOLICYASYNC] = Policy.Handle<Exception>().RetryAsync(),
                [CommandProcessor.CIRCUITBREAKERASYNC] = Policy.Handle<Exception>().CircuitBreakerAsync(1, TimeSpan.FromMilliseconds(1))
            };
            processor = new CommandProcessor(subscribers, handlers, new InMemoryRequestContextFactory(),
                policies, new ResiliencePipelineRegistry<string>(), mediator, new InMemorySchedulerFactory(), tracer: tracer);
            await schedules.CreateScheduleGroupAsync(new CreateScheduleGroupRequest { Name = groupName }, timeout.Token);
            groupCreated = true;
            var role = await iam.CreateRoleAsync(new CreateRoleRequest
            {
                RoleName = roleName,
                AssumeRolePolicyDocument = """
                    {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                    "Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}
                    """
            }, timeout.Token);
            roleCreated = true;
            await iam.PutRolePolicyAsync(new PutRolePolicyRequest
            {
                RoleName = roleName, PolicyName = "deliver-callback",
                PolicyDocument = JsonSerializer.Serialize(new
                {
                    Version = "2012-10-17",
                    Statement = new[] { new { Effect = "Allow", Action = "sqs:SendMessage", Resource = callbackAttributes.Attributes["QueueArn"] } }
                })
            }, timeout.Token);
            policyCreated = true;
            var scheduler = new AwsSchedulerFactory(connection, role.Role.Arn)
            {
                MakeRole = OnMissingRole.Assume, UseMessageTopicAsTarget = false,
                SchedulerTopicOrQueue = new RoutingKey(callbackName),
                Group = new SchedulerGroup { Name = groupName, MakeSchedulerGroup = OnMissingSchedulerGroup.Assume },
                GetOrCreateMessageSchedulerId = _ => scheduleId
            }.Create(processor);
            var depositParent = "00-11111111111111111111111111111111-2222222222222222-01";
            var original = new Message(new MessageHeader(new Id("callback-business-message"), topic, MessageType.MT_EVENT,
                correlationId: new Id("callback-correlation"), traceParent: new TraceParent(depositParent),
                traceState: new TraceState("vendor=deposit")), new MessageBody("callback business payload"));
            original.Header.Baggage.Add("stage", "deposit");
            var before = JsonSerializer.Serialize(original, JsonSerialisationOptions.Options);
            var at = DateTimeOffset.UtcNow.AddSeconds(2);

            //Act
            if (useAsync)
                await ((IAmAMessageSchedulerAsync)scheduler).ScheduleAsync(original, at, timeout.Token);
            else
                ((IAmAMessageSchedulerSync)scheduler).Schedule(original, at);
            Amazon.SQS.Model.Message? nativeCallback = null;
            while (nativeCallback is null)
            {
                var received = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = callbackQueue.QueueUrl, WaitTimeSeconds = 20, MaxNumberOfMessages = 1,
                    MessageAttributeNames = ["All"]
                }, timeout.Token);
                if (received.Messages?.Count > 0)
                    nativeCallback = Assert.Single(received.Messages);
            }
            var callbackMessage = new DefaultSqsWireMessageReader().ReadMessage(nativeCallback);
            var callback = new AwsSchedulerFiredMapper().MapToRequest(callbackMessage);

            //Assert
            Assert.Equal(nameof(FireAwsScheduler), callbackMessage.Header.Subject);
            Assert.Equal(scheduleId, callback.Id.Value);
            Assert.Equal(useAsync, callback.Async);
            Assert.NotNull(callback.Message);
            Assert.Equal(original.Id, callback.Message.Id);
            Assert.Equal(original.Header.Topic, callback.Message.Header.Topic);
            Assert.Equal(depositParent, callback.Message.Header.TraceParent?.Value);
            Assert.Equal("vendor=deposit", callback.Message.Header.TraceState?.Value);
            Assert.Equal("stage=deposit", callback.Message.Header.Baggage.ToString());
            Assert.Empty(producerSpans);
            var earlyBusiness = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = businessQueue.QueueUrl, WaitTimeSeconds = 0, MaxNumberOfMessages = 1
            }, timeout.Token);
            Assert.Empty(earlyBusiness.Messages ?? []);
            OpenTelemetry.Baggage.SetBaggage(new Dictionary<string, string?> { ["stage"] = "dispatch" });
            var context = new RequestContext();

            //Act
            using (var dispatch = new Activity("native-callback-dispatch")
                       .SetIdFormat(ActivityIdFormat.W3C)
                       .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded))
            {
                dispatch.TraceStateString = "vendor=dispatch";
                dispatch.Start();
                context.Span = dispatch;
                await processor.SendAsync(callback, context, timeout.Token);
            }
            var business = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = businessQueue.QueueUrl, WaitTimeSeconds = 20, MaxNumberOfMessages = 1,
                MessageAttributeNames = ["All"]
            }, timeout.Token);

            //Assert
            var nativeBusiness = Assert.Single(business.Messages ?? []);
            var produced = Assert.Single(producerSpans);
            Assert.Single(nativeBusiness.MessageAttributes);
            Assert.Equal(original.Id.Value, nativeBusiness.MessageAttributes["legacy-id"].StringValue);
            using var payload = JsonDocument.Parse(nativeBusiness.Body);
            Assert.Equal("callback business payload", payload.RootElement.GetProperty("payload").GetString());
            Assert.Equal(produced.Id, payload.RootElement.GetProperty("parent").GetString());
            Assert.NotEqual(depositParent, payload.RootElement.GetProperty("parent").GetString());
            Assert.Equal("vendor=dispatch", payload.RootElement.GetProperty("state").GetString());
            Assert.Equal("stage=dispatch", payload.RootElement.GetProperty("baggage").GetString());
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(before, JsonSerializer.Serialize(original, JsonSerialisationOptions.Options));
            await sqs.DeleteMessageAsync(callbackQueue.QueueUrl, nativeCallback.ReceiptHandle, timeout.Token);
            await sqs.DeleteMessageAsync(businessQueue.QueueUrl, nativeBusiness.ReceiptHandle, timeout.Token);
        }
        finally
        {
            OpenTelemetry.Baggage.Current = originalBaggage;
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            try
            {
                await schedules.DeleteScheduleAsync(new DeleteScheduleRequest { Name = scheduleId, GroupName = groupName }, cleanup.Token);
            }
            catch (Amazon.Scheduler.Model.ResourceNotFoundException)
            {
            }
            finally
            {
                try
                {
                    if (groupCreated)
                        await schedules.DeleteScheduleGroupAsync(new DeleteScheduleGroupRequest { Name = groupName }, cleanup.Token);
                }
                finally
                {
                    try
                    {
                        if (policyCreated)
                            await iam.DeleteRolePolicyAsync(new DeleteRolePolicyRequest { RoleName = roleName, PolicyName = "deliver-callback" }, cleanup.Token);
                        if (roleCreated)
                            await iam.DeleteRoleAsync(new DeleteRoleRequest { RoleName = roleName }, cleanup.Token);
                    }
                    finally
                    {
                        await reaper.ReapAsync();
                    }
                }
            }
        }
    }
}
