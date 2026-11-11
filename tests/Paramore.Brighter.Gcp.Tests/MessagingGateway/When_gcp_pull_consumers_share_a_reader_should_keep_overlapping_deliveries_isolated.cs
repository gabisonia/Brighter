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
public class GcpConcurrentPullReaderTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_gcp_pull_consumers_share_a_reader_should_keep_overlapping_deliveries_isolated(bool useAsync)
    {
        //Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "This test requires an explicitly configured native Pub/Sub emulator.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var names = Enumerable.Range(0, 2).Select(_ => "concurrent-reader-" + Guid.NewGuid().ToString("N")).ToArray();
        var topics = names.Select(name => TopicName.FromProjectTopic(project, name)).ToArray();
        var subscriptions = names.Select(name =>
            Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name)).ToArray();
        var publisher = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var observer = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        using var reader = new GcpConcurrentPullWireReader();
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            SubscriptionManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly
        };
        var factory = new GcpPubSubConsumerFactory(connection);
        var consumers = new List<GcpPullMessageConsumer>();
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
                    ackDeadlineSeconds: 10, subscriptionMode: SubscriptionMode.Pull)
                {
                    PullWireMessageReader = reader
                };
                object created = useAsync ? factory.CreateAsync(subscription) : factory.Create(subscription);
                consumers.Add(Assert.IsType<GcpPullMessageConsumer>(created));
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
                Assert.Equal(MessageType.MT_EVENT, mapped[index].Header.MessageType);
                Assert.Equal(new byte[] { 82, 0 }.Concat(bodies[index]).ToArray(), mapped[index].Body.Memory.ToArray());
                Assert.Equal("native-" + index, mapped[index].Header.Bag["mapped-marker"]);
                Assert.Equal(delivery.AckId, Assert.IsType<string>(mapped[index].Header.Bag["ReceiptHandle"]));
                Assert.NotEqual("reader-invalid-receipt", mapped[index].Header.Bag["ReceiptHandle"]);
                Assert.Equal(handledCounts[index], mapped[index].Header.HandledCount);
                Assert.Equal(0, delivery.DeliveryAttempt);
                Assert.Equal(bodies[index], delivery.Message.Data.ToByteArray());
                Assert.Equal(3, delivery.Message.Attributes.Count);
                Assert.Equal("wire-" + index, delivery.Message.Attributes[HeaderNames.Id]);
                Assert.Equal(handledCounts[index].ToString(), delivery.Message.Attributes[HeaderNames.HandledCount]);
                Assert.Equal("native-" + index, delivery.Message.Attributes["external-marker"]);
            }
            Assert.NotEqual(mapped[0].Header.Bag["ReceiptHandle"], mapped[1].Header.Bag["ReceiptHandle"]);
            for (var index = 0; index < mapped.Length; index++)
            {
                if (useAsync)
                    await consumers[index].AcknowledgeAsync(mapped[index], timeout.Token);
                else
                    consumers[index].Acknowledge(mapped[index]);
            }
            await Task.Delay(TimeSpan.FromSeconds(12), timeout.Token);
            for (var index = 0; index < subscriptions.Length; index++)
            {
                var markerId = Assert.Single((await publisher.PublishAsync(topics[index],
                    new[] { new PubsubMessage { Data = ByteString.CopyFromUtf8("after-ack-marker") } }, calls)).MessageIds);
                var remaining = await PullForAsync(index);
                Assert.DoesNotContain(remaining, delivery => delivery.Message.MessageId == nativeIds[index]);
                Assert.Contains(remaining, delivery => delivery.Message.MessageId == markerId);
                await observer.AcknowledgeAsync(subscriptions[index], remaining.Select(delivery => delivery.AckId), calls);
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

            async Task<ReceivedMessage[]> PullForAsync(int index)
            {
                var received = new List<ReceivedMessage>();
                var elapsed = Stopwatch.StartNew();
                while (elapsed.Elapsed < TimeSpan.FromSeconds(6))
                {
                    try
                    {
                        var response = await observer.PullAsync(new PullRequest
                        {
                            SubscriptionAsSubscriptionName = subscriptions[index],
                            MaxMessages = 10
                        }, CallSettings.FromExpiration(Expiration.FromTimeout(TimeSpan.FromSeconds(3)))
                            .WithCancellationToken(timeout.Token));
                        received.AddRange(response.ReceivedMessages);
                    }
                    catch (RpcException exception) when (exception.StatusCode == StatusCode.DeadlineExceeded) { }
                }
                return received.ToArray();
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
