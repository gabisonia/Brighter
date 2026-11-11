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
using Grpc.Core;
using Paramore.Brighter.Gcp.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.GcpPubSub;
using Xunit;
using Xunit.Abstractions;

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

public class GcpHostedOrderedNackRpcRecoveryTests(ITestOutputHelper output)
{
    public async Task When_nacking_an_ordered_gcp_delivery_should_send_a_native_nack_and_recover_both_messages(
        bool useAsync, int clientCount)
    {
        //Arrange
        Assert.True(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "Hosted execution rejects PUBSUB_EMULATOR_HOST.");
        const string project = "brighter-pubsub-validation";
        var name = "hosted-brighter-nack-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var nativeSubscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var topics = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.ProductionOnly }.Build();
        var admin = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.ProductionOnly }.Build();
        var trace = new ConcurrentQueue<string>();
        trace.Enqueue($"RESOURCE {topic} {nativeSubscription}");
        var observer = new GcpStreamRpcObserver(trace.Enqueue);
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.ProductionOnly,
            SubscriptionManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.ProductionOnly,
            PublisherConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.ProductionOnly,
            StreamConfiguration = builder =>
            {
                builder.EmulatorDetection = EmulatorDetection.ProductionOnly;
                if (clientCount > 0)
                    builder.ClientCount = clientCount;
                builder.ApiSettings ??= new SubscriberServiceApiSettings();
                builder.ApiSettings.Interceptor = observer;
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} BUILD CLIENTS {builder.ClientCount?.ToString() ?? "default"} PROCESSORS {Environment.ProcessorCount}");
            }
        };
        var destination = new RoutingKey(name);
        var publication = new GcpPublication
        {
            Topic = destination,
            MakeChannels = OnMissingChannel.Assume,
            EnableMessageOrdering = true
        };
        var subscription = new GcpPubSubSubscription(new SubscriptionName(name), new ChannelName(name), destination,
            requestType: typeof(HostedOrderedNackCommand),
            messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            makeChannels: OnMissingChannel.Assume, projectId: project,
            ackDeadlineSeconds: 30, subscriptionMode: SubscriptionMode.Stream, enableMessageOrdering: true);
        var ids = new[] { Id.Random(), Id.Random() };
        var bodies = new[] { new byte[] { 1, 0, 255, 128 }, new byte[] { 2, 0, 255, 128 } };
        var sources = Enumerable.Range(0, 2).Select(i => new Message(
            new MessageHeader(ids[i], destination, MessageType.MT_EVENT) { PartitionKey = new PartitionKey("one-order") },
            new MessageBody(bodies[i].ToArray()))).ToArray();
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(budget.Token);
        object? channel = null;
        Exception? cleanupError = null;
        var behaviorSucceeded = false;
        try
        {
            await topics.CreateTopicAsync(topic, calls);
            await admin.CreateSubscriptionAsync(new Google.Cloud.PubSub.V1.Subscription
            {
                SubscriptionName = nativeSubscription,
                TopicAsTopicName = topic,
                AckDeadlineSeconds = 30,
                EnableMessageOrdering = true
            }, calls);
            var producerFactory = new GcpPubSubMessageProducerFactory(connection, new[] { publication });
            var producers = useAsync ? await producerFactory.CreateAsync() : producerFactory.Create();
            await using var producer = Assert.IsType<GcpMessageProducer>(Assert.Single(producers).Value);
            var channelFactory = new GcpPubSubChannelFactory(connection);
            channel = useAsync
                ? await channelFactory.CreateAsyncChannelAsync(subscription, budget.Token)
                : channelFactory.CreateSyncChannel(subscription);
            foreach (var source in sources)
            {
                if (useAsync)
                    await producer.SendAsync(source, budget.Token);
                else
                    producer.Send(source);
            }
            var first = await ReceiveAsync(TimeSpan.FromSeconds(15));
            Assert.Contains(first.Id, ids);
            var firstIndex = Array.IndexOf(ids, first.Id);
            Assert.Equal(bodies[firstIndex], first.Body.Memory.ToArray());
            var pending = Assert.IsType<GcpStreamMessage>(first.Header.Bag["ReceiptHandle"]);
            Assert.False(pending.WaitForCompleteAsync().IsCompleted);
            Assert.Equal("one-order", pending.Message.OrderingKey);
            var firstNativeId = pending.Message.MessageId;
            Assert.False(string.IsNullOrWhiteSpace(firstNativeId));
            var initialDeliveries = observer.Deliveries.Where(d => d.Message.MessageId == firstNativeId).ToArray();
            Assert.NotEmpty(initialDeliveries);
            Assert.All(initialDeliveries, delivery =>
            {
                Assert.False(string.IsNullOrWhiteSpace(delivery.AckId));
                Assert.Equal(first.Id.Value, delivery.Message.Attributes[HeaderNames.Id]);
                Assert.Equal(bodies[firstIndex], delivery.Message.Data.ToByteArray());
            });
            var firstAckIds = initialDeliveries.Select(d => d.AckId).ToHashSet();

            //Act
            var nackStarted = Stopwatch.GetTimestamp();
            trace.Enqueue($"{DateTimeOffset.UtcNow:O} NACK APPLICATION {first.Id} NATIVE {firstNativeId}");
            if (useAsync)
                await ((IAmAChannelAsync)channel).NackAsync(first, budget.Token);
            else
                ((IAmAChannelSync)channel).Nack(first);
            var observedIds = new HashSet<string>();
            var elapsed = Stopwatch.StartNew();
            while (elapsed.Elapsed < TimeSpan.FromSeconds(30) && observedIds.Count < 2)
            {
                var received = await ReceiveAsync(TimeSpan.FromMilliseconds(500));
                if (received.Header.MessageType == MessageType.MT_NONE)
                    continue;
                Assert.Contains(received.Id, ids);
                var index = Array.IndexOf(ids, received.Id);
                Assert.Equal(bodies[index], received.Body.Memory.ToArray());
                var receipt = Assert.IsType<GcpStreamMessage>(received.Header.Bag["ReceiptHandle"]);
                Assert.Equal("one-order", receipt.Message.OrderingKey);
                observedIds.Add(received.Id.Value);
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} ACK APPLICATION {received.Id} NATIVE {receipt.Message.MessageId}");
                if (useAsync)
                    await ((IAmAChannelAsync)channel).AcknowledgeAsync(received, budget.Token);
                else
                    ((IAmAChannelSync)channel).Acknowledge(received);
            }

            //Assert
            Assert.Contains(observer.Completed, call => call.Started >= nackStarted
                && call.Method == "ModifyAckDeadline" && call.Request is ModifyAckDeadlineRequest deadline
                && deadline.Subscription == nativeSubscription.ToString() && deadline.AckDeadlineSeconds == 0
                && deadline.AckIds.Any(firstAckIds.Contains));
            Assert.Equal(SubscriberClient.Reply.Nack,
                await pending.WaitForCompleteAsync().WaitAsync(TimeSpan.FromSeconds(3)));
            Assert.Contains(first.Id.Value, observedIds);
            Assert.Contains(ids[1 - firstIndex].Value, observedIds);
            behaviorSucceeded = true;
        }
        finally
        {
            if (channel != null)
            {
                try
                {
                    var disposal = channel is IAmAChannelAsync asynchronous
                        ? asynchronous.DisposeAsync().AsTask()
                        : Task.Run(((IAmAChannelSync)channel).Dispose);
                    await disposal.WaitAsync(TimeSpan.FromSeconds(10));
                }
                catch (Exception exception)
                {
                    cleanupError = exception;
                    trace.Enqueue($"Cleanup channel: {exception}");
                }
            }
            try { await DeleteAsync(() => admin.DeleteSubscriptionAsync(nativeSubscription)); }
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
                Assert.Null(cleanupError);
        }

        async Task<Message> ReceiveAsync(TimeSpan timeout)
        {
            var received = useAsync
                ? await ((IAmAChannelAsync)channel!).ReceiveAsync(timeout, budget.Token)
                : ((IAmAChannelSync)channel!).Receive(timeout);
            trace.Enqueue($"{DateTimeOffset.UtcNow:O} RECEIVE {received.Id} {received.Header.MessageType}");
            return received;
        }
    }

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
