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
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using Paramore.Brighter.Gcp.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.GcpPubSub;
using Xunit;
using Xunit.Abstractions;

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

public class GcpHostedBufferedShutdownObservationTests(ITestOutputHelper output)
{
    public async Task When_stopping_a_gcp_stream_should_release_the_observed_native_delivery(
        bool useAsync, bool buffered, bool nativeRecovery)
    {
        //Arrange
        Assert.True(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "Hosted execution rejects PUBSUB_EMULATOR_HOST.");
        const string project = "brighter-pubsub-validation";
        var name = "hosted-buffered-shutdown-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var subscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var starts = new ConcurrentDictionary<object, long>(ReferenceEqualityComparer.Instance);
        var completed = new ConcurrentQueue<(long Started, string Method, int? Deadline, string[] AckIds)>();
        var trace = new ConcurrentQueue<string>();
        trace.Enqueue($"RESOURCE {topic} {subscription}");
        var observer = new GcpPurgeFaultInterceptor
        {
            BeforeCall = (method, request) =>
            {
                starts[request] = Stopwatch.GetTimestamp();
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} START {method} {request}");
            },
            AfterCall = (method, request) =>
            {
                Assert.True(starts.TryRemove(request, out var started));
                var deadline = request as ModifyAckDeadlineRequest;
                var ack = request as AcknowledgeRequest;
                completed.Enqueue((started, method, deadline?.AckDeadlineSeconds,
                    deadline?.AckIds.ToArray() ?? ack?.AckIds.ToArray() ?? Array.Empty<string>()));
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} SUCCESS {method} {request}");
            }
        };
        var native = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.ProductionOnly }.Build();
        var topics = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.ProductionOnly }.Build();
        var connection = new GcpMessagingGatewayConnection { ProjectId = project };
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(budget.Token);
        var consumers = new List<GcpPubSubStreamMessageConsumer>();
        var cleanupErrors = new List<Exception>();
        var behaviorSucceeded = false;
        var body = new byte[] { 0, 255, 1, 128 };
        var applicationId = new Id("application-" + name);
        var wire = new PubsubMessage { Data = ByteString.CopyFrom(body) };
        wire.Attributes[HeaderNames.Id] = applicationId.Value;
        wire.Attributes[HeaderNames.Topic] = name;
        wire.Attributes[HeaderNames.ContentType] = "application/octet-stream";

        try
        {
            await topics.CreateTopicAsync(topic, calls);
            await native.CreateSubscriptionAsync(subscription, topic, null, 30, calls);
            var (consumer, stream) = CreateConsumer();
            var sent = await topics.PublishAsync(topic, new[] { wire }, calls);
            var nativeId = Assert.Single(sent.MessageIds);
            var first = Assert.Single(await ReceiveAsync(consumer));
            Assert.Equal(applicationId, first.Id);
            Assert.Equal(body, first.Body.Memory.ToArray());
            var pending = Assert.IsType<GcpStreamMessage>(first.Header.Bag["ReceiptHandle"]);
            Assert.Equal(nativeId, pending.Message.MessageId);
            Assert.False(pending.WaitForCompleteAsync().IsCompleted);

            if (buffered)
            {
                var released = useAsync
                    ? await consumer.RequeueAsync(first, cancellationToken: budget.Token)
                    : consumer.Requeue(first);
                Assert.True(released);
                Assert.Equal(SubscriberClient.Reply.Nack,
                    await pending.WaitForCompleteAsync().WaitAsync(TimeSpan.FromSeconds(3)));
                var hasBufferedDelivery = false;
                var bufferError = await Record.ExceptionAsync(async () =>
                    hasBufferedDelivery = await stream.Reader.WaitToReadAsync(budget.Token).AsTask()
                        .WaitAsync(TimeSpan.FromSeconds(20)));
                Assert.Null(bufferError);
                Assert.True(hasBufferedDelivery);
                Assert.True(stream.Reader.TryPeek(out var bufferedDelivery));
                Assert.NotNull(bufferedDelivery);
                Assert.NotSame(pending, bufferedDelivery);
                pending = bufferedDelivery;
                Assert.Equal(nativeId, pending.Message.MessageId);
                Assert.Equal(body, pending.Message.Data.ToByteArray());
                Assert.False(pending.WaitForCompleteAsync().IsCompleted);
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} BUFFERED {nativeId}");
            }

            //Act
            var shutdownStarted = Stopwatch.GetTimestamp();
            trace.Enqueue($"{DateTimeOffset.UtcNow:O} DISPOSE START {nativeId}");
            var disposal = useAsync ? consumer.DisposeAsync().AsTask() : Task.Run(consumer.Dispose);
            var disposalError = await Record.ExceptionAsync(() => disposal.WaitAsync(TimeSpan.FromSeconds(10)));
            trace.Enqueue($"{DateTimeOffset.UtcNow:O} DISPOSE END {disposal.Status} {disposalError}");

            //Assert
            Assert.Null(disposalError);
            Assert.DoesNotContain(completed, call => call.Method == "Acknowledge");
            Assert.Contains(completed, call => call.Started >= shutdownStarted
                && call.Method == "ModifyAckDeadline" && call.Deadline == 0 && call.AckIds.Length > 0);
            var callback = pending.WaitForCompleteAsync();
            Assert.True(callback.IsCompleted);
            if (callback.IsCompletedSuccessfully)
                Assert.Equal(SubscriberClient.Reply.Nack, await callback);
            else
                Assert.True(callback.IsCanceled);

            if (nativeRecovery)
            {
                var recovered = Assert.Single(await PullAsync());
                Assert.Equal(nativeId, recovered.Message.MessageId);
                Assert.Equal(applicationId.Value, recovered.Message.Attributes[HeaderNames.Id]);
                Assert.Equal(body, recovered.Message.Data.ToByteArray());
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} NATIVE REDELIVERY {recovered.Message.MessageId}");
                await native.AcknowledgeAsync(subscription, new[] { recovered.AckId }, calls);
            }
            else
            {
                var (replacement, _) = CreateConsumer();
                var recovered = Assert.Single(await ReceiveAsync(replacement));
                Assert.Equal(applicationId, recovered.Id);
                Assert.Equal(body, recovered.Body.Memory.ToArray());
                var receipt = Assert.IsType<GcpStreamMessage>(recovered.Header.Bag["ReceiptHandle"]);
                Assert.Equal(nativeId, receipt.Message.MessageId);
                Assert.NotSame(pending, receipt);
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} STREAM REDELIVERY {receipt.Message.MessageId}");
                if (useAsync)
                    await replacement.AcknowledgeAsync(recovered, budget.Token);
                else
                    replacement.Acknowledge(recovered);
                var acknowledgementError = await Record.ExceptionAsync(() =>
                    receipt.WaitForCompleteAsync().WaitAsync(TimeSpan.FromSeconds(3)));
                Assert.Null(acknowledgementError);
                Assert.Equal(SubscriberClient.Reply.Ack, await receipt.WaitForCompleteAsync());
            }
            behaviorSucceeded = true;
        }
        finally
        {
            foreach (var consumer in consumers)
            {
                try { await consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3)); }
                catch (Exception exception)
                {
                    cleanupErrors.Add(exception);
                    trace.Enqueue($"Cleanup consumer: {exception}");
                }
            }
            try { await DeleteAsync(() => native.DeleteSubscriptionAsync(subscription)); }
            finally
            {
                try { await DeleteAsync(() => topics.DeleteTopicAsync(topic)); }
                finally
                {
                    foreach (var entry in trace)
                        output.WriteLine(entry);
                    foreach (var unfinished in starts)
                        output.WriteLine($"RPC without observed success: {unfinished.Key}");
                }
            }
            if (behaviorSucceeded)
                Assert.Empty(cleanupErrors);
        }

        (GcpPubSubStreamMessageConsumer Consumer, GcpStreamConsumer Stream) CreateConsumer()
        {
            var client = new SubscriberClientBuilder
            {
                SubscriptionName = subscription,
                EmulatorDetection = EmulatorDetection.ProductionOnly,
                ClientCount = 1,
                Settings = new SubscriberClient.Settings { FlowControlSettings = new FlowControlSettings(1, null) },
                ApiSettings = new SubscriberServiceApiSettings { Interceptor = observer }
            }.Build();
            var stream = new GcpStreamConsumer(client);
            Assert.True(stream.TryStart());
            var consumer = new GcpPubSubStreamMessageConsumer(connection, stream, subscription, TimeProvider.System);
            consumers.Add(consumer);
            return (consumer, stream);
        }

        async Task<Message[]> ReceiveAsync(GcpPubSubStreamMessageConsumer consumer)
        {
            var messages = useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(20), budget.Token)
                : consumer.Receive(TimeSpan.FromSeconds(20));
            return messages.Where(message => !message.IsEmpty).ToArray();
        }

        async Task<ReceivedMessage[]> PullAsync()
        {
            var elapsed = Stopwatch.StartNew();
            while (elapsed.Elapsed < TimeSpan.FromSeconds(20))
            {
                var remaining = TimeSpan.FromSeconds(20) - elapsed.Elapsed;
                if (remaining <= TimeSpan.Zero)
                    break;
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
                attempt.CancelAfter(remaining);
                try
                {
                    var response = await native.PullAsync(new PullRequest
                    {
                        SubscriptionAsSubscriptionName = subscription,
                        MaxMessages = 1
                    }, CallSettings.FromCancellationToken(attempt.Token));
                    if (response.ReceivedMessages.Count > 0)
                        return response.ReceivedMessages.ToArray();
                }
                catch (RpcException exception) when (!budget.IsCancellationRequested
                    && exception.StatusCode is StatusCode.Cancelled or StatusCode.DeadlineExceeded) { }
            }
            return Array.Empty<ReceivedMessage>();
        }
    }

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
