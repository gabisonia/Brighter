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

[Trait("Category", "GcpNativePullReader")]
[Trait("Requires", "PubSubEmulator")]
public class GcpNativePullReaderTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_a_gcp_pull_subscription_selects_a_reader_should_preserve_mapping_and_native_settlement(bool useAsync)
    {
        //Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "This test requires an explicitly configured native Pub/Sub emulator.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "wire-reader-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var nativeSubscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var topics = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var observer = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        var reader = new GcpNativeProbePullReader();
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            SubscriptionManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly
        };
        var subscription = new GcpPubSubSubscription(new SubscriptionName(name), new ChannelName(name),
            new RoutingKey(name), requestType: typeof(Command),
            messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            makeChannels: OnMissingChannel.Assume, projectId: project,
            bufferSize: 1, ackDeadlineSeconds: 10, subscriptionMode: SubscriptionMode.Pull)
        {
            PullWireMessageReader = reader
        };
        var body = new byte[] { 0, 255, 1, 128 };
        var native = new PubsubMessage { Data = ByteString.CopyFrom(body) };
        native.Attributes.Add(HeaderNames.Id, "wire-id");
        native.Attributes.Add(HeaderNames.HandledCount, "7");
        native.Attributes.Add("external-marker", "native-marker");
        try
        {
            await topics.CreateTopicAsync(topic, calls);
            await observer.CreateSubscriptionAsync(nativeSubscription, topic, null, 10, calls);
            var factory = new GcpPubSubConsumerFactory(connection);
            object created = useAsync ? factory.CreateAsync(subscription) : factory.Create(subscription);
            await using var consumer = Assert.IsType<GcpPullMessageConsumer>(created);
            var published = await topics.PublishAsync(topic, new[] { native }, calls);
            var nativeId = Assert.Single(published.MessageIds);

            //Act
            var first = Assert.Single(await ReceiveAsync());

            //Assert
            Assert.Single(reader.Deliveries);
            AssertMapping(first, reader.Deliveries[0], nativeId);
            await observer.ModifyAckDeadlineAsync(nativeSubscription,
                new[] { reader.Deliveries[0].AckId }, 30, calls);
            var released = useAsync
                ? await consumer.RequeueAsync(first, TimeSpan.Zero, timeout.Token)
                : consumer.Requeue(first, TimeSpan.Zero);
            Assert.True(released);
            var observedRelease = Assert.Single(await PullForAsync(TimeSpan.FromSeconds(5), true));
            Assert.Equal(nativeId, observedRelease.Message.MessageId);
            Assert.Equal(body, observedRelease.Message.Data.ToByteArray());
            Assert.Equal("native-marker", observedRelease.Message.Attributes["external-marker"]);
            await observer.ModifyAckDeadlineAsync(nativeSubscription, new[] { observedRelease.AckId }, 0, calls);
            var redelivered = Assert.Single(await ReceiveAsync());
            Assert.Equal(2, reader.Deliveries.Length);
            AssertMapping(redelivered, reader.Deliveries[1], nativeId);
            Assert.NotSame(first, redelivered);
            var ackError = await Record.ExceptionAsync(async () =>
            {
                if (useAsync)
                    await consumer.AcknowledgeAsync(redelivered, timeout.Token);
                else
                    consumer.Acknowledge(redelivered);
            });
            Assert.Null(ackError);
            await Task.Delay(TimeSpan.FromSeconds(12), timeout.Token);
            var marker = await topics.PublishAsync(topic,
                new[] { new PubsubMessage { Data = ByteString.CopyFromUtf8("after-ack-marker") } }, calls);
            var remaining = await PullForAsync(TimeSpan.FromSeconds(6), false);
            Assert.DoesNotContain(remaining, delivery => delivery.Message.MessageId == nativeId);
            Assert.Contains(remaining, delivery => delivery.Message.MessageId == Assert.Single(marker.MessageIds));
            await observer.AcknowledgeAsync(nativeSubscription, remaining.Select(delivery => delivery.AckId), calls);
            Assert.Equal(2, reader.Deliveries.Length);

            async Task<Message[]> ReceiveAsync()
            {
                var elapsed = Stopwatch.StartNew();
                while (elapsed.Elapsed < TimeSpan.FromSeconds(12))
                {
                    var messages = useAsync
                        ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(3), timeout.Token)
                        : consumer.Receive(TimeSpan.FromSeconds(3));
                    var available = messages.Where(message => !message.IsEmpty).ToArray();
                    if (available.Length > 0)
                        return available;
                }
                return Array.Empty<Message>();
            }

            void AssertMapping(Message mapped, ReceivedMessage delivery, string expectedNativeId)
            {
                Assert.Equal(new Id("mapped-" + expectedNativeId), mapped.Id);
                Assert.Equal(new RoutingKey("mapped-topic"), mapped.Header.Topic);
                Assert.Equal(MessageType.MT_EVENT, mapped.Header.MessageType);
                Assert.Equal(new byte[] { 82, 0, 0, 255, 1, 128 }, mapped.Body.Memory.ToArray());
                Assert.Equal("native-marker", mapped.Header.Bag["mapped-marker"]);
                Assert.Equal(delivery.AckId, Assert.IsType<string>(mapped.Header.Bag["ReceiptHandle"]));
                Assert.NotEqual("reader-invalid-receipt", mapped.Header.Bag["ReceiptHandle"]);
                Assert.Equal(0, delivery.DeliveryAttempt);
                Assert.Equal(7, mapped.Header.HandledCount);
                Assert.Equal(expectedNativeId, delivery.Message.MessageId);
                Assert.Equal(body, delivery.Message.Data.ToByteArray());
                Assert.Equal(3, delivery.Message.Attributes.Count);
                Assert.Equal("wire-id", delivery.Message.Attributes[HeaderNames.Id]);
                Assert.Equal("7", delivery.Message.Attributes[HeaderNames.HandledCount]);
                Assert.Equal("native-marker", delivery.Message.Attributes["external-marker"]);
            }
        }
        finally
        {
            try { await DeleteAsync(() => observer.DeleteSubscriptionAsync(nativeSubscription)); }
            finally { await DeleteAsync(() => topics.DeleteTopicAsync(topic)); }
        }

        async Task<ReceivedMessage[]> PullForAsync(TimeSpan window, bool stopOnFirst)
        {
            var received = new List<ReceivedMessage>();
            var elapsed = Stopwatch.StartNew();
            while (elapsed.Elapsed < window)
            {
                try
                {
                    var response = await observer.PullAsync(new PullRequest
                    {
                        SubscriptionAsSubscriptionName = nativeSubscription,
                        MaxMessages = 10
                    }, CallSettings.FromExpiration(Expiration.FromTimeout(TimeSpan.FromSeconds(3)))
                        .WithCancellationToken(timeout.Token));
                    received.AddRange(response.ReceivedMessages);
                    if (stopOnFirst && received.Count > 0)
                        break;
                }
                catch (RpcException exception) when (exception.StatusCode == StatusCode.DeadlineExceeded) { }
            }
            return received.ToArray();
        }
    }

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
