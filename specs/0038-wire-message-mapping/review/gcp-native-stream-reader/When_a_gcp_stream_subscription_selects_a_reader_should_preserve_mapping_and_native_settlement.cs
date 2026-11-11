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
public class GcpNativeStreamReaderTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_gcp_stream_subscription_selects_a_reader_should_preserve_mapping_and_native_settlement(bool useAsync, bool nack)
    {
        //Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "This test requires an explicitly configured native Pub/Sub emulator.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "wire-stream-reader-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var nativeSubscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var topics = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var observer = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        var reader = new GcpNativeProbeStreamReader();
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            SubscriptionManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            StreamConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly
        };
        var subscription = new GcpPubSubSubscription(new SubscriptionName(name), new ChannelName(name),
            new RoutingKey(name), requestType: typeof(Command),
            messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            makeChannels: OnMissingChannel.Assume, projectId: project,
            bufferSize: 1, ackDeadlineSeconds: 10, subscriptionMode: SubscriptionMode.Stream)
        {
            StreamWireMessageReader = reader
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
            await using var consumer = Assert.IsType<GcpPubSubStreamMessageConsumer>(created);
            var published = await topics.PublishAsync(topic, new[] { native }, calls);
            var nativeId = Assert.Single(published.MessageIds);

            //Act
            var first = Assert.Single(await ReceiveAsync());

            //Assert
            Assert.Single(reader.Deliveries);
            AssertMapping(first, reader.Deliveries[0], nativeId);
            Assert.False(reader.Deliveries[0].WaitForCompleteAsync().IsCompleted);
            if (nack)
            {
                if (useAsync)
                    await consumer.NackAsync(first, timeout.Token);
                else
                    consumer.Nack(first);
            }
            else
            {
                var released = useAsync
                    ? await consumer.RequeueAsync(first, TimeSpan.Zero, timeout.Token)
                    : consumer.Requeue(first, TimeSpan.Zero);
                Assert.True(released);
            }
            await AssertReplyAsync(reader.Deliveries[0], SubscriberClient.Reply.Nack);
            var redelivered = Assert.Single(await ReceiveAsync());
            Assert.Equal(2, reader.Deliveries.Length);
            AssertMapping(redelivered, reader.Deliveries[1], nativeId);
            Assert.NotSame(first, redelivered);
            Assert.NotSame(reader.Deliveries[0], reader.Deliveries[1]);
            Assert.False(reader.Deliveries[1].WaitForCompleteAsync().IsCompleted);
            await AcknowledgeAsync(redelivered);
            await AssertReplyAsync(reader.Deliveries[1], SubscriberClient.Reply.Ack);

            var marker = native.Clone();
            marker.Attributes["external-marker"] = "after-ack-marker";
            var markerPublication = await topics.PublishAsync(topic, new[] { marker }, calls);
            var fresh = Assert.Single(await ReceiveAsync());
            Assert.Equal(3, reader.Deliveries.Length);
            Assert.Equal(Assert.Single(markerPublication.MessageIds), reader.Deliveries[2].Message.MessageId);
            Assert.Equal("after-ack-marker", fresh.Header.Bag["mapped-marker"]);
            Assert.Same(reader.Deliveries[2], fresh.Header.Bag["ReceiptHandle"]);
            await AcknowledgeAsync(fresh);
            await AssertReplyAsync(reader.Deliveries[2], SubscriberClient.Reply.Ack);

            async Task AcknowledgeAsync(Message message)
            {
                if (useAsync)
                    await consumer.AcknowledgeAsync(message, timeout.Token);
                else
                    consumer.Acknowledge(message);
            }

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

            void AssertMapping(Message mapped, GcpStreamMessage delivery, string expectedNativeId)
            {
                Assert.Equal(new Id("mapped-" + expectedNativeId), mapped.Id);
                Assert.Equal(new RoutingKey("mapped-topic"), mapped.Header.Topic);
                Assert.Equal(MessageType.MT_EVENT, mapped.Header.MessageType);
                Assert.Equal(new byte[] { 82, 0, 0, 255, 1, 128 }, mapped.Body.Memory.ToArray());
                Assert.Equal("native-marker", mapped.Header.Bag["mapped-marker"]);
                Assert.Same(delivery, mapped.Header.Bag["ReceiptHandle"]);
                Assert.Null(PubsubExtensions.GetDeliveryAttempt(delivery.Message));
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

    }

    private static async Task AssertReplyAsync(GcpStreamMessage delivery, SubscriberClient.Reply expected)
    {
        var completion = delivery.WaitForCompleteAsync();
        var error = await Record.ExceptionAsync(async () =>
            await completion.WaitAsync(TimeSpan.FromSeconds(3)));
        Assert.Null(error);
        Assert.Equal(expected, await completion);
    }

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
