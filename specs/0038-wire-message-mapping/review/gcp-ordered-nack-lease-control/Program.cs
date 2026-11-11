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
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;

var host = Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST");
if (string.IsNullOrWhiteSpace(host))
    throw new InvalidOperationException("An explicit Pub/Sub emulator is required.");
var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
foreach (var addStream in new[] { false, true })
foreach (var extendAfterNack in new[] { false, true })
    await ObserveAsync(addStream, extendAfterNack);

async Task ObserveAsync(bool addStream, bool extendAfterNack)
{
    var name = "ordered-protocol-" + Guid.NewGuid().ToString("N");
    var topic = TopicName.FromProjectTopic(project, name).ToString();
    var subscription = SubscriptionName.FromProjectSubscription(project, name).ToString();
    using var transport = GrpcChannel.ForAddress("http://" + host,
        new Grpc.Net.Client.GrpcChannelOptions { Credentials = ChannelCredentials.Insecure });
    var publisher = new Publisher.PublisherClient(transport.CreateCallInvoker());
    var subscriber = new Subscriber.SubscriberClient(transport.CreateCallInvoker());
    using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(90));
    using var reading = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
    var trace = new ConcurrentQueue<string>();
    var deliveries = Channel.CreateUnbounded<(int Stream, ReceivedMessage Delivery)>();
    var streams = new List<AsyncDuplexStreamingCall<StreamingPullRequest, StreamingPullResponse>>();
    var readers = new List<Task>();
    trace.Enqueue($"CASE addStream={addStream} extendAfterNack={extendAfterNack}");
    try
    {
        await publisher.CreateTopicAsync(new Topic { Name = topic }, cancellationToken: budget.Token).ResponseAsync;
        await subscriber.CreateSubscriptionAsync(new Subscription
        {
            Name = subscription, Topic = topic, AckDeadlineSeconds = 30, EnableMessageOrdering = true
        }, cancellationToken: budget.Token).ResponseAsync;
        await OpenStreamAsync(0);
        var published = await publisher.PublishAsync(new PublishRequest
        {
            Topic = topic,
            Messages =
            {
                new PubsubMessage { OrderingKey = "ordered-race-0", Data = ByteString.CopyFromUtf8("first") },
                new PubsubMessage { OrderingKey = "ordered-race-0", Data = ByteString.CopyFromUtf8("second") }
            }
        }, cancellationToken: budget.Token).ResponseAsync;
        var originals = new Dictionary<string, ReceivedMessage>();
        using (var initial = CancellationTokenSource.CreateLinkedTokenSource(budget.Token))
        {
            initial.CancelAfter(TimeSpan.FromSeconds(15));
            while (originals.Count < 2)
            {
                var delivery = await deliveries.Reader.ReadAsync(initial.Token);
                if (delivery.Stream != 0 || !published.MessageIds.Contains(delivery.Delivery.Message.MessageId))
                    throw new InvalidOperationException("Unexpected initial delivery.");
                originals.Add(delivery.Delivery.Message.MessageId, delivery.Delivery);
            }
        }
        var first = originals[published.MessageIds[0]];
        var second = originals[published.MessageIds[1]];
        if (addStream)
        {
            await OpenStreamAsync(1);
            using var ready = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
            ready.CancelAfter(TimeSpan.FromSeconds(15));
            while (true)
            {
                var marker = await publisher.PublishAsync(new PublishRequest
                {
                    Topic = topic,
                    Messages = { new PubsubMessage { OrderingKey = "probe-marker-3", Data = ByteString.CopyFromUtf8("ready") } }
                }, cancellationToken: ready.Token).ResponseAsync;
                var received = await deliveries.Reader.ReadAsync(ready.Token);
                if (received.Delivery.Message.MessageId != marker.MessageIds[0])
                    throw new InvalidOperationException("Unexpected delivery during second-stream readiness.");
                await AckAsync(received.Delivery.AckId).WaitAsync(ready.Token);
                if (received.Stream == 1) break;
                trace.Enqueue("READINESS: marker acknowledged on original stream; awaiting stream 1 assignment");
            }
            trace.Enqueue("SECOND STREAM READY: observed marker delivery and completed acknowledgement");
        }
        await AckAsync(second.AckId);
        if (extendAfterNack)
        {
            await NackAsync(first.AckId);
            await ExtendAsync(first.AckId, second.AckId);
        }
        else
        {
            await ExtendAsync(first.AckId, second.AckId);
            await NackAsync(first.AckId);
        }
        var returned = await ObserveFirstAsync(first.Message.MessageId, TimeSpan.FromSeconds(30));
        trace.Enqueue($"RESULT addStream={addStream} extendAfterNack={extendAfterNack} firstReturnedWithin30Seconds={returned}");

    }
    catch (Exception exception)
    {
        trace.Enqueue($"PROBE ERROR {exception}");
        throw;
    }
    finally
    {
        reading.Cancel();
        foreach (var stream in streams) stream.Dispose();
        try { await Task.WhenAll(readers).WaitAsync(TimeSpan.FromSeconds(5)); }
        finally
        {
            try { await DeleteAsync(() => subscriber.DeleteSubscriptionAsync(new DeleteSubscriptionRequest { Subscription = subscription }).ResponseAsync); }
            finally
            {
                try { await DeleteAsync(() => publisher.DeleteTopicAsync(new DeleteTopicRequest { Topic = topic }).ResponseAsync); }
                finally { foreach (var entry in trace) Console.WriteLine(entry); }
            }
        }
    }

    async Task OpenStreamAsync(int index)
    {
        var stream = subscriber.StreamingPull(cancellationToken: reading.Token);
        streams.Add(stream);
        readers.Add(ReadAsync(stream, index));
        await stream.RequestStream.WriteAsync(new StreamingPullRequest
        {
            Subscription = subscription, ClientId = name, StreamAckDeadlineSeconds = 60,
            MaxOutstandingMessages = 1000, MaxOutstandingBytes = 1000000
        }).WaitAsync(budget.Token);
        trace.Enqueue($"{DateTimeOffset.UtcNow:O} STREAM OPEN {index}");
    }

    async Task ReadAsync(AsyncDuplexStreamingCall<StreamingPullRequest, StreamingPullResponse> stream, int index)
    {
        try
        {
            while (await stream.ResponseStream.MoveNext(reading.Token))
            foreach (var delivery in stream.ResponseStream.Current.ReceivedMessages)
            {
                trace.Enqueue($"{DateTimeOffset.UtcNow:O} STREAM {index} DELIVERY {delivery}");
                await deliveries.Writer.WriteAsync((index, delivery), reading.Token);
            }
        }
        catch (OperationCanceledException) when (reading.IsCancellationRequested) { }
        catch (RpcException exception) when (reading.IsCancellationRequested && exception.StatusCode == StatusCode.Cancelled) { }
    }

    async Task AckAsync(string ackId)
    {
        trace.Enqueue($"{DateTimeOffset.UtcNow:O} ACK START {ackId}");
        await subscriber.AcknowledgeAsync(new AcknowledgeRequest { Subscription = subscription, AckIds = { ackId } },
            cancellationToken: budget.Token).ResponseAsync;
        trace.Enqueue($"{DateTimeOffset.UtcNow:O} ACK SUCCESS {ackId}");
    }

    async Task ExtendAsync(string firstAckId, string secondAckId)
    {
        trace.Enqueue($"{DateTimeOffset.UtcNow:O} EXTEND START {firstAckId} {secondAckId}");
        await subscriber.ModifyAckDeadlineAsync(new ModifyAckDeadlineRequest
        {
            Subscription = subscription, AckIds = { firstAckId, secondAckId }, AckDeadlineSeconds = 60
        }, cancellationToken: budget.Token).ResponseAsync;
        trace.Enqueue($"{DateTimeOffset.UtcNow:O} EXTEND SUCCESS {firstAckId} {secondAckId}");
    }

    async Task NackAsync(string ackId)
    {
        trace.Enqueue($"{DateTimeOffset.UtcNow:O} NACK START {ackId}");
        await subscriber.ModifyAckDeadlineAsync(new ModifyAckDeadlineRequest
        {
            Subscription = subscription, AckIds = { ackId }, AckDeadlineSeconds = 0
        }, cancellationToken: budget.Token).ResponseAsync;
        trace.Enqueue($"{DateTimeOffset.UtcNow:O} NACK SUCCESS {ackId}");
    }

    async Task<bool> ObserveFirstAsync(string messageId, TimeSpan timeout)
    {
        var elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < timeout)
        {
            using var receive = CancellationTokenSource.CreateLinkedTokenSource(budget.Token);
            receive.CancelAfter(TimeSpan.FromMilliseconds(500));
            try
            {
                var delivery = await deliveries.Reader.ReadAsync(receive.Token);
                await AckAsync(delivery.Delivery.AckId);
                if (delivery.Delivery.Message.MessageId == messageId) return true;
            }
            catch (OperationCanceledException) when (!budget.IsCancellationRequested) { }
        }
        return false;
    }
}

static async Task DeleteAsync(Func<Task> delete)
{
    try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
    catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
}
