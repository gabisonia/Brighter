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

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Paramore.Brighter.Gcp.Tests.TestDoubles;

if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")))
    throw new InvalidOperationException("An explicit Pub/Sub emulator is required.");

var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
for (var round = 0; round < 4; round++)
foreach (var clientCount in new[] { 0, 1, 2 })
    await ObserveAsync(round, clientCount);

async Task ObserveAsync(int round, int clientCount)
{
    var name = "native-ordered-nack-" + Guid.NewGuid().ToString("N");
    var topic = TopicName.FromProjectTopic(project, name);
    var subscription = SubscriptionName.FromProjectSubscription(project, name);
    var topics = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
    var admin = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
    var trace = new ConcurrentQueue<string>();
    var observer = new GcpStreamRpcObserver(trace.Enqueue);
    var deliveries = Channel.CreateUnbounded<(PubsubMessage Message, TaskCompletionSource<SubscriberClient.Reply> Reply)>();
    using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(90));
    var calls = CallSettings.FromCancellationToken(budget.Token);
    SubscriberClient? subscriber = null;
    PublisherClient? publisher = null;
    Task? listening = null;
    try
    {
        await topics.CreateTopicAsync(topic, calls);
        await admin.CreateSubscriptionAsync(new Subscription
        {
            SubscriptionName = subscription, TopicAsTopicName = topic,
            AckDeadlineSeconds = 30, EnableMessageOrdering = true
        }, calls);
        publisher = new PublisherClientBuilder
        {
            TopicName = topic, EmulatorDetection = EmulatorDetection.EmulatorOnly,
            Settings = new PublisherClient.Settings { EnableMessageOrdering = true }
        }.Build();
        var builder = new SubscriberClientBuilder
        {
            SubscriptionName = subscription, EmulatorDetection = EmulatorDetection.EmulatorOnly,
            ApiSettings = new SubscriberServiceApiSettings { Interceptor = observer },
            Settings = new SubscriberClient.Settings { FlowControlSettings = new FlowControlSettings(1, null) }
        };
        if (clientCount > 0)
            builder.ClientCount = clientCount;
        trace.Enqueue($"ROUND {round} CLIENTS {builder.ClientCount?.ToString() ?? "default"} PROCESSORS {Environment.ProcessorCount}");
        subscriber = builder.Build();
        listening = subscriber.StartAsync(async (message, token) =>
        {
            var reply = new TaskCompletionSource<SubscriberClient.Reply>();
            using var registration = token.Register(() => reply.TrySetCanceled(token));
            try
            {
                await deliveries.Writer.WriteAsync((message, reply), token);
                return await reply.Task;
            }
            catch (OperationCanceledException) { return SubscriberClient.Reply.Nack; }
        });
        var ids = new[] { Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
        for (var i = 0; i < ids.Length; i++)
            await publisher.PublishAsync(new PubsubMessage
            {
                Data = ByteString.CopyFrom(new byte[] { (byte)(i + 1), 0, 255, 128 }),
                OrderingKey = "one-order", Attributes = { ["ce-id"] = ids[i] }
            }).WaitAsync(budget.Token);

        using var firstWait = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
        firstWait.CancelAfter(TimeSpan.FromSeconds(15));
        var first = await deliveries.Reader.ReadAsync(firstWait.Token);
        var firstAckIds = observer.Deliveries.Where(d => d.Message.MessageId == first.Message.MessageId)
            .Select(d => d.AckId).ToHashSet();
        var started = Stopwatch.GetTimestamp();
        trace.Enqueue($"{DateTimeOffset.UtcNow:O} NACK APPLICATION {first.Message.Attributes["ce-id"]} NATIVE {first.Message.MessageId}");
        first.Reply.TrySetResult(SubscriberClient.Reply.Nack);
        var seen = new HashSet<string>();
        var elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < TimeSpan.FromSeconds(30) && seen.Count < 2)
        {
            using var receive = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
            receive.CancelAfter(TimeSpan.FromMilliseconds(500));
            try
            {
                var delivery = await deliveries.Reader.ReadAsync(receive.Token);
                seen.Add(delivery.Message.Attributes["ce-id"]);
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} ACK APPLICATION {delivery.Message.Attributes["ce-id"]} NATIVE {delivery.Message.MessageId}");
                delivery.Reply.TrySetResult(SubscriberClient.Reply.Ack);
            }
            catch (OperationCanceledException) when (!budget.IsCancellationRequested) { }
        }
        var nackCompleted = observer.Completed.Any(c => c.Started >= started && c.Request is ModifyAckDeadlineRequest d
            && d.Subscription == subscription.ToString() && d.AckDeadlineSeconds == 0 && d.AckIds.Any(firstAckIds.Contains));
        var redelivered = observer.Deliveries.Count(d => d.Message.MessageId == first.Message.MessageId);
        trace.Enqueue($"RESULT round={round} clients={clientCount} nativeNack={nackCompleted} firstReturned={seen.Contains(first.Message.Attributes["ce-id"])} bothReturned={ids.All(seen.Contains)} nativeFirstDeliveries={redelivered}");
    }
    catch (Exception exception)
    {
        trace.Enqueue($"PROBE ERROR {exception}");
        throw;
    }
    finally
    {
        try
        {
            if (subscriber != null)
            {
                await subscriber.StopAsync(new SubscriberClient.ShutdownOptions
                    { Mode = SubscriberClient.ShutdownMode.NackImmediately }, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));
                await subscriber.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
                if (listening != null)
                    await listening.WaitAsync(TimeSpan.FromSeconds(3));
            }
        }
        finally
        {
            try
            {
                if (publisher != null)
                    await publisher.ShutdownAsync(TimeSpan.FromSeconds(10));
            }
            finally
            {
                try { await admin.DeleteSubscriptionAsync(subscription).WaitAsync(TimeSpan.FromSeconds(15)); }
                finally
                {
                    try { await topics.DeleteTopicAsync(topic).WaitAsync(TimeSpan.FromSeconds(15)); }
                    finally { foreach (var entry in trace) Console.WriteLine(entry); }
                }
            }
        }
    }
}
