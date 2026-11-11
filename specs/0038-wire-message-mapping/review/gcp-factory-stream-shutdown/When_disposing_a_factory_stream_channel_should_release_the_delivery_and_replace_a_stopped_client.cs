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
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Paramore.Brighter.MessagingGateway.GcpPubSub;
using Xunit;
using Xunit.Abstractions;

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

[Trait("Category", "GcpNativeStreamLifecycle")]
[Trait("Requires", "PubSubEmulator")]
public class GcpFactoryStreamShutdownRecoveryTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(false, 1, false)]
    [InlineData(false, 1, true)]
    [InlineData(false, 2, false)]
    [InlineData(false, 2, true)]
    [InlineData(true, 1, false)]
    [InlineData(true, 1, true)]
    [InlineData(true, 2, false)]
    [InlineData(true, 2, true)]
    public async Task When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(
        bool useAsync, int clientCount, bool nativeRecovery)
    {
        //Arrange
        var host = Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST");
        Assert.False(string.IsNullOrWhiteSpace(host), "An explicit Pub/Sub emulator is required.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "factory-shutdown-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var nativeSubscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        using var adminTransport = GrpcChannel.ForAddress("http://" + host,
            new Grpc.Net.Client.GrpcChannelOptions { Credentials = ChannelCredentials.Insecure });
        var adminInvoker = adminTransport.CreateCallInvoker();
        var native = new SubscriberServiceApiClientBuilder { CallInvoker = adminInvoker }.Build();
        var topics = new PublisherServiceApiClientBuilder { CallInvoker = adminInvoker }.Build();
        var trace = new ConcurrentQueue<string>();
        var streamBuilds = 0;
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.CallInvoker = adminInvoker,
            SubscriptionManagerConfiguration = builder => builder.CallInvoker = adminInvoker,
            StreamConfiguration = builder =>
            {
                builder.EmulatorDetection = EmulatorDetection.EmulatorOnly;
                builder.ClientCount = clientCount;
                var generation = Interlocked.Increment(ref streamBuilds);
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} BUILD {generation} CLIENTS {clientCount}");
            }
        };
        var factory = new GcpPubSubChannelFactory(connection);
        var subscription = new GcpPubSubSubscription(
            new SubscriptionName(name), new ChannelName(name), new RoutingKey(name),
            requestType: typeof(TestDoubles.MyCommand),
            messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            makeChannels: OnMissingChannel.Assume, projectId: project,
            ackDeadlineSeconds: 30, subscriptionMode: SubscriptionMode.Stream);
        var channels = new List<object>();
        var cleanupErrors = new List<Exception>();
        var behaviorSucceeded = false;
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(budget.Token);
        var bytes = new byte[] { 0, 255, 1, 128 };
        var applicationId = new Id("application-" + name);
        var wire = new PubsubMessage { Data = ByteString.CopyFrom(bytes) };
        wire.Attributes[HeaderNames.Id] = applicationId.Value;
        wire.Attributes[HeaderNames.Topic] = name;
        wire.Attributes[HeaderNames.ContentType] = "application/octet-stream";

        try
        {
            await topics.CreateTopicAsync(topic, calls);
            await native.CreateSubscriptionAsync(nativeSubscription, topic, null, 30, calls);
            var channel = await CreateChannelAsync();
            Assert.Equal(1, Volatile.Read(ref streamBuilds));
            var published = await topics.PublishAsync(topic, new[] { wire }, calls);
            var nativeId = Assert.Single(published.MessageIds);
            var first = await ReceiveAsync(channel);
            Assert.Equal(applicationId, first.Id);
            Assert.Equal(bytes, first.Body.Memory.ToArray());
            var pending = Assert.IsType<GcpStreamMessage>(first.Header.Bag["ReceiptHandle"]);
            Assert.Equal(nativeId, pending.Message.MessageId);
            Assert.False(pending.WaitForCompleteAsync().IsCompleted);
            trace.Enqueue($"{DateTimeOffset.UtcNow:O} RECEIVED {nativeId}");

            //Act
            trace.Enqueue($"{DateTimeOffset.UtcNow:O} DISPOSE START {nativeId}");
            var disposal = DisposeAsync(channel);
            var disposalError = await Record.ExceptionAsync(() => disposal.WaitAsync(TimeSpan.FromSeconds(10)));
            trace.Enqueue($"{DateTimeOffset.UtcNow:O} DISPOSE END {disposal.Status} {disposalError}");

            //Assert
            Assert.Null(disposalError);
            var callback = pending.WaitForCompleteAsync();
            trace.Enqueue($"{DateTimeOffset.UtcNow:O} CALLBACK {callback.Status}");
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
                Assert.Equal(bytes, recovered.Message.Data.ToByteArray());
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} NATIVE REDELIVERY {recovered.Message.MessageId}");
                await native.AcknowledgeAsync(nativeSubscription, new[] { recovered.AckId }, calls);
                Assert.Equal(1, Volatile.Read(ref streamBuilds));
            }
            else
            {
                var replacement = await CreateChannelAsync();
                Assert.Equal(2, Volatile.Read(ref streamBuilds));
                var recovered = await ReceiveAsync(replacement);
                Assert.Equal(applicationId, recovered.Id);
                Assert.Equal(bytes, recovered.Body.Memory.ToArray());
                var receipt = Assert.IsType<GcpStreamMessage>(recovered.Header.Bag["ReceiptHandle"]);
                Assert.Equal(nativeId, receipt.Message.MessageId);
                Assert.NotSame(pending, receipt);
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} STREAM REDELIVERY {receipt.Message.MessageId}");
                if (useAsync)
                    await ((IAmAChannelAsync)replacement).AcknowledgeAsync(recovered, budget.Token);
                else
                    ((IAmAChannelSync)replacement).Acknowledge(recovered);
                var acknowledgementError = await Record.ExceptionAsync(() =>
                    receipt.WaitForCompleteAsync().WaitAsync(TimeSpan.FromSeconds(3)));
                Assert.Null(acknowledgementError);
                Assert.Equal(SubscriberClient.Reply.Ack, await receipt.WaitForCompleteAsync());
            }
            behaviorSucceeded = true;
        }
        finally
        {
            foreach (var channel in channels)
            {
                try { await DisposeAsync(channel).WaitAsync(TimeSpan.FromSeconds(3)); }
                catch (Exception exception)
                {
                    cleanupErrors.Add(exception);
                    trace.Enqueue($"Cleanup channel: {exception}");
                }
            }
            try { await DeleteAsync(() => native.DeleteSubscriptionAsync(nativeSubscription)); }
            finally
            {
                try { await DeleteAsync(() => topics.DeleteTopicAsync(topic)); }
                finally
                {
                    foreach (var entry in trace)
                        output.WriteLine(entry);
                }
            }
            if (behaviorSucceeded)
                Assert.Empty(cleanupErrors);
        }

        async Task<object> CreateChannelAsync()
        {
            object channel = useAsync
                ? await factory.CreateAsyncChannelAsync(subscription, budget.Token)
                : factory.CreateSyncChannel(subscription);
            channels.Add(channel);
            return channel;
        }

        async Task<Message> ReceiveAsync(object channel)
        {
            var received = useAsync
                ? await ((IAmAChannelAsync)channel).ReceiveAsync(TimeSpan.FromSeconds(20), budget.Token)
                : ((IAmAChannelSync)channel).Receive(TimeSpan.FromSeconds(20));
            trace.Enqueue($"{DateTimeOffset.UtcNow:O} RECEIVE {received.Id} {received.Header.MessageType}");
            return received;
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
                        SubscriptionAsSubscriptionName = nativeSubscription,
                        MaxMessages = 1
                    }, CallSettings.FromCancellationToken(attempt.Token));
                    trace.Enqueue($"{DateTimeOffset.UtcNow:O} NATIVE PULL COUNT {response.ReceivedMessages.Count}");
                    if (response.ReceivedMessages.Count > 0)
                        return System.Linq.Enumerable.ToArray(response.ReceivedMessages);
                }
                catch (RpcException exception) when (!budget.IsCancellationRequested
                    && exception.StatusCode is StatusCode.Cancelled or StatusCode.DeadlineExceeded)
                {
                    trace.Enqueue($"{DateTimeOffset.UtcNow:O} NATIVE PULL {exception.StatusCode}");
                }
            }
            return Array.Empty<ReceivedMessage>();
        }
    }

    private static Task DisposeAsync(object channel) => channel is IAmAChannelAsync asynchronous
        ? asynchronous.DisposeAsync().AsTask()
        : Task.Run(((IAmAChannelSync)channel).Dispose);

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
