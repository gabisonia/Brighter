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
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Paramore.Brighter.Gcp.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.GcpPubSub;
using Xunit;

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

[Trait("Category", "GcpNativePullReader")]
[Trait("Requires", "PubSubEmulator")]
public class GcpPullReaderFailureRecoveryTests
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
    public async Task When_a_gcp_pull_reader_fails_should_preserve_the_delivery_and_recover(
        bool useAsync, bool returnNull, bool hasTopic)
    {
        //Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "This test requires an explicitly configured native Pub/Sub emulator.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "wire-pull-failure-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var nativeSubscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var settlements = new ConcurrentQueue<(string Method, int? Deadline, string[] AckIds)>();
        var interceptor = new GcpPurgeFaultInterceptor
        {
            AfterCall = (method, request) =>
            {
                if (request is AcknowledgeRequest acknowledged)
                    settlements.Enqueue((method, null, acknowledged.AckIds.ToArray()));
                else if (request is ModifyAckDeadlineRequest released)
                    settlements.Enqueue((method, released.AckDeadlineSeconds, released.AckIds.ToArray()));
            }
        };
        using var transport = GrpcChannel.ForAddress(
            "http://" + Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST"),
            new Grpc.Net.Client.GrpcChannelOptions { Credentials = ChannelCredentials.Insecure });
        var invoker = transport.Intercept(interceptor);
        var topics = new PublisherServiceApiClientBuilder { CallInvoker = invoker }.Build();
        var observer = new SubscriberServiceApiClientBuilder { CallInvoker = invoker }.Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        var reader = new GcpRecoverablePullWireReader(returnNull);
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.CallInvoker = invoker,
            SubscriptionManagerConfiguration = builder => builder.CallInvoker = invoker
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
        if (hasTopic)
            native.Attributes.Add(HeaderNames.Topic, name);
        native.Attributes.Add(HeaderNames.HandledCount, "7");
        native.Attributes.Add(HeaderNames.ContentType, "not-a-mime-type");
        native.Attributes.Add("external-marker", "failed-marker");
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
            Message[] received = Array.Empty<Message>();
            var error = await Record.ExceptionAsync(async () => received = await ReceiveAsync());

            //Assert
            Assert.Null(error);
            var failed = Assert.Single(received);
            var firstDelivery = Assert.Single(reader.Deliveries);
            Assert.Equal(MessageType.MT_UNACCEPTABLE, failed.Header.MessageType);
            Assert.Equal(new Id(nativeId), failed.Id);
            Assert.Equal(new RoutingKey(hasTopic ? name : string.Empty), failed.Header.Topic);
            Assert.Equal(body, failed.Body.Memory.ToArray());
            Assert.Equal(7, failed.Header.HandledCount);
            Assert.Equal(firstDelivery.AckId, Assert.IsType<string>(failed.Header.Bag["ReceiptHandle"]));
            AssertNative(firstDelivery, nativeId, "failed-marker");
            Assert.Empty(settlements);

            reader.Fail = false;
            var released = useAsync
                ? await consumer.RequeueAsync(failed, TimeSpan.Zero, timeout.Token)
                : consumer.Requeue(failed, TimeSpan.Zero);
            Assert.True(released);
            AssertSettlement(0, "ModifyAckDeadline", firstDelivery.AckId, 0);

            var recovered = Assert.Single(await ReceiveAsync());
            Assert.Equal(2, reader.Deliveries.Length);
            var recoveredDelivery = reader.Deliveries[1];
            Assert.NotSame(firstDelivery, recoveredDelivery);
            Assert.NotSame(failed, recovered);
            AssertMapped(recovered, recoveredDelivery, nativeId, "failed-marker");
            Assert.Single(settlements);
            await AcknowledgeAsync(recovered);
            AssertSettlement(1, "Acknowledge", recoveredDelivery.AckId);

            var marker = native.Clone();
            marker.Attributes["external-marker"] = "after-recovery-marker";
            var markerPublication = await topics.PublishAsync(topic, new[] { marker }, calls);
            var fresh = Assert.Single(await ReceiveAsync());
            Assert.Equal(3, reader.Deliveries.Length);
            var freshDelivery = reader.Deliveries[2];
            var markerId = Assert.Single(markerPublication.MessageIds);
            Assert.NotEqual(nativeId, markerId);
            AssertMapped(fresh, freshDelivery, markerId, "after-recovery-marker");
            await AcknowledgeAsync(fresh);
            AssertSettlement(2, "Acknowledge", freshDelivery.AckId);

            void AssertSettlement(int index, string method, string ackId, int? deadline = null)
            {
                var recorded = settlements.ToArray();
                Assert.Equal(index + 1, recorded.Length);
                Assert.Equal(method, recorded[index].Method);
                Assert.Equal(deadline, recorded[index].Deadline);
                Assert.Equal(ackId, Assert.Single(recorded[index].AckIds));
            }

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
                    timeout.Token.ThrowIfCancellationRequested();
                    var messages = useAsync
                        ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(3), timeout.Token)
                        : consumer.Receive(TimeSpan.FromSeconds(3));
                    var available = messages.Where(message => !message.IsEmpty).ToArray();
                    if (available.Length > 0)
                        return available;
                }
                return Array.Empty<Message>();
            }

            void AssertMapped(Message mapped, ReceivedMessage delivery, string expectedId, string expectedMarker)
            {
                Assert.Equal(MessageType.MT_EVENT, mapped.Header.MessageType);
                Assert.Equal(new Id("recovered-" + expectedId), mapped.Id);
                Assert.Equal(new RoutingKey("mapped-topic"), mapped.Header.Topic);
                Assert.Equal(new byte[] { 82, 0, 0, 255, 1, 128 }, mapped.Body.Memory.ToArray());
                Assert.Equal(expectedMarker, mapped.Header.Bag["mapped-marker"]);
                Assert.Equal(7, mapped.Header.HandledCount);
                Assert.Equal(delivery.AckId, Assert.IsType<string>(mapped.Header.Bag["ReceiptHandle"]));
                AssertNative(delivery, expectedId, expectedMarker);
            }

            void AssertNative(ReceivedMessage delivery, string expectedId, string expectedMarker)
            {
                Assert.Equal(expectedId, delivery.Message.MessageId);
                Assert.Equal(body, delivery.Message.Data.ToByteArray());
                Assert.False(string.IsNullOrWhiteSpace(delivery.AckId));
                Assert.Equal(0, delivery.DeliveryAttempt);
                Assert.Equal(hasTopic ? 4 : 3, delivery.Message.Attributes.Count);
                Assert.Equal(hasTopic, delivery.Message.Attributes.ContainsKey(HeaderNames.Topic));
                if (hasTopic)
                    Assert.Equal(name, delivery.Message.Attributes[HeaderNames.Topic]);
                Assert.Equal("7", delivery.Message.Attributes[HeaderNames.HandledCount]);
                Assert.Equal("not-a-mime-type", delivery.Message.Attributes[HeaderNames.ContentType]);
                Assert.Equal(expectedMarker, delivery.Message.Attributes["external-marker"]);
            }
        }
        finally
        {
            try { await DeleteAsync(() => observer.DeleteSubscriptionAsync(nativeSubscription)); }
            finally { await DeleteAsync(() => topics.DeleteTopicAsync(topic)); }
        }
    }

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
