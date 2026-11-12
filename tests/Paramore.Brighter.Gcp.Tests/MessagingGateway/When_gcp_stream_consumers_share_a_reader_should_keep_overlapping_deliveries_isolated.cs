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

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

[Trait("Category", "GcpNativeStreamReader")]
[Trait("Requires", "PubSubEmulator")]
public class GcpConcurrentStreamReaderTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_gcp_stream_consumers_share_a_reader_should_keep_overlapping_deliveries_isolated(bool useAsync)
    {
        //Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "This test requires an explicitly configured native Pub/Sub emulator.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var names = Enumerable.Range(0, 2).Select(_ => "concurrent-stream-reader-" + Guid.NewGuid().ToString("N")).ToArray();
        var topics = names.Select(name => TopicName.FromProjectTopic(project, name)).ToArray();
        var subscriptions = names.Select(name =>
            Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name)).ToArray();
        var publisher = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var observer = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        using var reader = new GcpConcurrentStreamWireReader();
        var acknowledgements = Enumerable.Range(0, 2).Select(_ =>
            new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            SubscriptionManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            StreamConfiguration = builder =>
            {
                builder.EmulatorDetection = EmulatorDetection.EmulatorOnly;
                builder.ClientCount = 1;
                builder.ApiSettings ??= new SubscriberServiceApiSettings();
                builder.ApiSettings.Interceptor = new GcpPurgeFaultInterceptor
                {
                    AfterCall = (method, request) =>
                    {
                        if (method != "Acknowledge" || request is not AcknowledgeRequest acknowledgement)
                            return;
                        for (var index = 0; index < subscriptions.Length; index++)
                        {
                            if (acknowledgement.Subscription == subscriptions[index].ToString())
                                acknowledgements[index].TrySetResult(acknowledgement.AckIds.ToArray());
                        }
                    }
                };
            }
        };
        var factory = new GcpPubSubConsumerFactory(connection);
        var consumers = new List<GcpPubSubStreamMessageConsumer>();
        var bodies = new[] { new byte[] { 0, 255, 1, 128 }, new byte[] { 254, 0, 2, 127 } };
        var handledCounts = new[] { 3, 7 };
        var nativeIds = new string[2];
        try
        {
            for (var index = 0; index < names.Length; index++)
            {
                await publisher.CreateTopicAsync(topics[index], calls);
                await observer.CreateSubscriptionAsync(subscriptions[index], topics[index], null, 10, calls);
                var subscription = new GcpPubSubSubscription(new SubscriptionName(names[index]),
                    new ChannelName(names[index]), new RoutingKey(names[index]), requestType: typeof(Command),
                    messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                    makeChannels: OnMissingChannel.Assume, projectId: project, bufferSize: 1,
                    ackDeadlineSeconds: 10, subscriptionMode: SubscriptionMode.Stream)
                {
                    StreamWireMessageReader = reader
                };
                object created = useAsync ? factory.CreateAsync(subscription) : factory.Create(subscription);
                consumers.Add(Assert.IsType<GcpPubSubStreamMessageConsumer>(created));
                var native = new PubsubMessage { Data = ByteString.CopyFrom(bodies[index]) };
                native.Attributes.Add(HeaderNames.Id, "wire-" + index);
                native.Attributes.Add(HeaderNames.HandledCount, handledCounts[index].ToString());
                native.Attributes.Add("external-marker", "native-" + index);
                nativeIds[index] = Assert.Single((await publisher.PublishAsync(topics[index], new[] { native }, calls)).MessageIds);
            }

            //Act
            var receives = Enumerable.Range(0, 2).Select(index => Task.Factory.StartNew(
                () => ReceiveAsync(index), CancellationToken.None, TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap()).ToArray();
            var mapped = await Task.WhenAll(receives);

            //Assert
            Assert.Equal(2, reader.OverlappedCalls);
            Assert.Equal(2, reader.Deliveries.Length);
            var expectedBodies = bodies.Select(body => Convert.ToBase64String(new byte[] { 82, 0 }.Concat(body).ToArray()))
                .OrderBy(value => value, StringComparer.Ordinal).ToArray();
            var actualBodies = mapped.Select(message => Convert.ToBase64String(message.Body.Memory.ToArray()))
                .OrderBy(value => value, StringComparer.Ordinal).ToArray();
            Assert.Equal(expectedBodies, actualBodies);
            Assert.NotSame(mapped[0], mapped[1]);
            Assert.NotSame(mapped[0].Header, mapped[1].Header);
            for (var index = 0; index < mapped.Length; index++)
            {
                var delivery = Assert.Single(reader.Deliveries, item =>
                    item.Message.Attributes.TryGetValue("external-marker", out var marker) && marker == "native-" + index);
                Assert.Equal(nativeIds[index], delivery.Message.MessageId);
                Assert.Equal(new Id("mapped-" + nativeIds[index]), mapped[index].Id);
                Assert.Equal(new RoutingKey("mapped-topic"), mapped[index].Header.Topic);
#pragma warning disable CS0618 // Preserve the legacy message type as a wire compatibility assertion.
                Assert.Equal(MessageType.MT_EVENT, mapped[index].Header.MessageType);
#pragma warning restore CS0618
                Assert.Equal(new byte[] { 82, 0 }.Concat(bodies[index]).ToArray(), mapped[index].Body.Memory.ToArray());
                Assert.Equal("native-" + index, mapped[index].Header.Bag["mapped-marker"]);
                Assert.Same(delivery, mapped[index].Header.Bag["ReceiptHandle"]);
                Assert.False(delivery.WaitForCompleteAsync().IsCompleted);
                Assert.False(acknowledgements[index].Task.IsCompleted);
                Assert.Equal(handledCounts[index], mapped[index].Header.HandledCount);
                Assert.Null(PubsubExtensions.GetDeliveryAttempt(delivery.Message));
                Assert.Equal(bodies[index], delivery.Message.Data.ToByteArray());
                Assert.Equal(3, delivery.Message.Attributes.Count);
                Assert.Equal("wire-" + index, delivery.Message.Attributes[HeaderNames.Id]);
                Assert.Equal(handledCounts[index].ToString(), delivery.Message.Attributes[HeaderNames.HandledCount]);
                Assert.Equal("native-" + index, delivery.Message.Attributes["external-marker"]);
            }
            var receipts = mapped.Select(message => Assert.IsType<GcpStreamMessage>(message.Header.Bag["ReceiptHandle"])).ToArray();
            Assert.NotSame(receipts[0], receipts[1]);
            for (var index = 0; index < mapped.Length; index++)
            {
                if (useAsync)
                    await consumers[index].AcknowledgeAsync(mapped[index], timeout.Token);
                else
                    consumers[index].Acknowledge(mapped[index]);
                var completion = receipts[index].WaitForCompleteAsync();
                var replyError = await Record.ExceptionAsync(async () =>
                    await completion.WaitAsync(TimeSpan.FromSeconds(3)));
                Assert.Null(replyError);
                Assert.Equal(SubscriberClient.Reply.Ack, await completion);
                var nativeAckError = await Record.ExceptionAsync(async () =>
                    await acknowledgements[index].Task.WaitAsync(TimeSpan.FromSeconds(10)));
                Assert.Null(nativeAckError);
                Assert.False(string.IsNullOrWhiteSpace(Assert.Single(await acknowledgements[index].Task)));
                if (index == 0)
                {
                    Assert.False(receipts[1].WaitForCompleteAsync().IsCompleted);
                    Assert.False(acknowledgements[1].Task.IsCompleted);
                }
            }
            Assert.Equal(2, reader.Deliveries.Length);

            async Task<Message> ReceiveAsync(int index)
            {
                var elapsed = Stopwatch.StartNew();
                while (elapsed.Elapsed < TimeSpan.FromSeconds(12))
                {
                    var messages = useAsync
                        ? await consumers[index].ReceiveAsync(TimeSpan.FromSeconds(3), timeout.Token)
                        : consumers[index].Receive(TimeSpan.FromSeconds(3));
                    var available = messages.Where(message => !message.IsEmpty).ToArray();
                    if (available.Length > 0)
                        return Assert.Single(available);
                }
                throw new TimeoutException("Expected an isolated native delivery for each consumer.");
            }

        }
        finally
        {
            var cleanupErrors = new List<Exception>();
            foreach (var consumer in consumers)
            {
                try { await consumer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)); }
                catch (Exception exception) { cleanupErrors.Add(exception); }
            }
            for (var index = 0; index < subscriptions.Length; index++)
            {
                try { await DeleteAsync(() => observer.DeleteSubscriptionAsync(subscriptions[index])); }
                catch (Exception exception) { cleanupErrors.Add(exception); }
                try { await DeleteAsync(() => publisher.DeleteTopicAsync(topics[index])); }
                catch (Exception exception) { cleanupErrors.Add(exception); }
            }
            Assert.Empty(cleanupErrors);
        }
    }

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
