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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Amazon.IdentityManagement.Model;
using Amazon.Scheduler.Model;
using Amazon.SQS.Model;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.AWS.V4.Tests.Helpers;
using Paramore.Brighter.AWS.V4.Tests.TestDoubles;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessageScheduler.AWS.V4;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Paramore.Brighter.ServiceActivator;
using Paramore.Brighter.MessageMappers;
using Polly;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.AWS.V4.Tests.MessagingGateway;

[Trait("Category", "AWS")]
/// <summary>Verifies automatic callback settlement and explicit business outbox recovery.</summary>
public class AwsCallbackPumpRecoveryTests
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
    public async Task When_an_aws_callback_pump_writer_fails_should_settle_callback_and_preserve_outbox_for_recovery(
        bool useAsync, bool useSns, bool returnNull)
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
        using var iam = Paramore.Brighter.MessageScheduler.AWS.V4.AWSClientFactoryExtensions.CreateIdentityClient(clients);
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
        var processed = new TaskCompletionSource<Activity>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var processListener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracer.ActivitySource),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.Kind == ActivityKind.Consumer && Equals(activity.GetTagItem(BrighterSemanticConventions.MessagingOperationType), "process"))
                    processed.TrySetResult(activity);
            }
        };
        ActivitySource.AddActivityListener(processListener);
        IAmAChannelAsync? callbackChannel = null;
        Task? pumpTask = null;
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
            var sqsWriter = new CallbackRecoveryWireWriter<SqsWireMessage>(new NonCloudEventsSqsWireMessageWriter(true), returnNull);
            var snsWriter = new CallbackRecoveryWireWriter<SnsWireMessage>(new NativeCallbackSnsWireMessageWriter(), returnNull);
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
                    Topic = topic, WireMessageWriter = snsWriter
                }]);
            }
            else
            {
                producerFactory = new SqsMessageProducerFactory(connection, [new SqsPublication(
                    new ChannelName(businessQueue.QueueUrl), findQueueBy: QueueFindBy.Url, makeChannels: OnMissingChannel.Validate)
                {
                    Topic = topic, WireMessageWriter = sqsWriter
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
            var callbackTopic = new RoutingKey(callbackName);
            var subscription = new SqsSubscription(new SubscriptionName(callbackName), new ChannelName(callbackName),
                ChannelType.PointToPoint, callbackTopic, requestType: typeof(FireAwsScheduler),
                messagePumpType: MessagePumpType.Proactor, findQueueBy: QueueFindBy.Name, makeChannels: OnMissingChannel.Validate);
            callbackChannel = await new ChannelFactory(connection).CreateAsyncChannelAsync(subscription, timeout.Token);
            using var callbackMappers = new MessageMapperRegistry(null, new SimpleMessageMapperFactoryAsync(_ => new JsonMessageMapper<FireAwsScheduler>()));
            callbackMappers.RegisterAsync<FireAwsScheduler, JsonMessageMapper<FireAwsScheduler>>();
            var pump = new Proactor(processor, _ => typeof(FireAwsScheduler), callbackMappers,
                new EmptyMessageTransformerFactoryAsync(), new InMemoryRequestContextFactory(), callbackChannel, tracer)
            { TimeOut = TimeSpan.FromMilliseconds(200), RequeueCount = 0 };
            await sqs.SetQueueAttributesAsync(new SetQueueAttributesRequest
            { QueueUrl = callbackQueue.QueueUrl, Attributes = new() { ["VisibilityTimeout"] = "2" } }, timeout.Token);

            //Act
            if (useAsync)
                await ((IAmAMessageSchedulerAsync)scheduler).ScheduleAsync(original, at, timeout.Token);
            else
                ((IAmAMessageSchedulerSync)scheduler).Schedule(original, at);
            pumpTask = Task.Factory.StartNew(pump.Run, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            await processed.Task.WaitAsync(timeout.Token);
            callbackChannel.Enqueue(MessageFactory.CreateQuitMessage(callbackTopic));
            await pumpTask.WaitAsync(timeout.Token);
            Assert.True(useSns ? snsWriter.FailedAttempts > 0 : sqsWriter.FailedAttempts > 0);
            var context = new RequestContext();
            var pending = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(original.Id, pending.Id);
            Assert.Equal(original.Header.Topic, pending.Header.Topic);
            Assert.Equal("callback business payload", pending.Body.Value);
            var absent = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            { QueueUrl = businessQueue.QueueUrl, WaitTimeSeconds = 1, MaxNumberOfMessages = 10 }, timeout.Token);
            Assert.Empty(absent.Messages ?? []);
            await Task.Delay(TimeSpan.FromSeconds(3), timeout.Token);
            var callbackAfterSettlement = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
            { QueueUrl = callbackQueue.QueueUrl, AttributeNames = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] }, timeout.Token);
            Assert.Equal("0", callbackAfterSettlement.Attributes["ApproximateNumberOfMessages"]);
            Assert.Equal("0", callbackAfterSettlement.Attributes["ApproximateNumberOfMessagesNotVisible"]);
            Assert.Equal(before, JsonSerializer.Serialize(original, JsonSerialisationOptions.Options));
            var failedParents = producerSpans.Select(span => span.Id).ToArray();
            Assert.NotEmpty(failedParents);
            producerSpans.Clear();
            sqsWriter.FailOnWrite = false;
            snsWriter.FailOnWrite = false;
            OpenTelemetry.Baggage.SetBaggage(new Dictionary<string, string?> { ["stage"] = "recovery" });
            using var recovery = new Activity("callback-outbox-recovery")
                .SetIdFormat(ActivityIdFormat.W3C)
                .SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded)
                .Start();
            recovery.TraceStateString = "vendor=recovery";
            context.Span = recovery;
            if (useAsync) await mediator.ClearOutboxAsync([original.Id], context, cancellationToken: timeout.Token);
            else mediator.ClearOutbox([original.Id], context);
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
            Assert.Equal("vendor=recovery", payload.RootElement.GetProperty("state").GetString());
            Assert.Equal("stage=recovery", payload.RootElement.GetProperty("baggage").GetString());
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(before, JsonSerializer.Serialize(original, JsonSerialisationOptions.Options));
            Assert.DoesNotContain(produced.Id, failedParents);
            var duplicate = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            { QueueUrl = businessQueue.QueueUrl, WaitTimeSeconds = 1, MaxNumberOfMessages = 10 }, timeout.Token);
            Assert.Empty(duplicate.Messages ?? []);
            await sqs.DeleteMessageAsync(businessQueue.QueueUrl, nativeBusiness.ReceiptHandle, timeout.Token);
        }
        finally
        {
            if (callbackChannel != null)
            {
                callbackChannel.Enqueue(MessageFactory.CreateQuitMessage(new RoutingKey(callbackName)));
                if (pumpTask != null) await pumpTask.WaitAsync(TimeSpan.FromSeconds(15));
                callbackChannel.Dispose();
            }
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
