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

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

/// <summary>Verifies consumer-selected rejection formats and recovery through native Pub/Sub.</summary>
[Trait("Category", "GcpNativeWireWriter")]
[Trait("Requires", "PubSubEmulator")]
public class GcpRejectionWireSelectionTests
{
    /// <summary>Captures each route's writer, including a shared topic and the invalid-to-DLQ fallback.</summary>
    [Theory]
    [InlineData(false, false, false, 0)]
    [InlineData(false, false, false, 1)]
    [InlineData(false, false, false, 2)]
    [InlineData(false, false, true, 0)]
    [InlineData(false, false, true, 1)]
    [InlineData(false, false, true, 2)]
    [InlineData(false, true, false, 0)]
    [InlineData(false, true, false, 1)]
    [InlineData(false, true, false, 2)]
    [InlineData(false, true, true, 0)]
    [InlineData(false, true, true, 1)]
    [InlineData(false, true, true, 2)]
    [InlineData(true, false, false, 0)]
    [InlineData(true, false, false, 1)]
    [InlineData(true, false, false, 2)]
    [InlineData(true, false, true, 0)]
    [InlineData(true, false, true, 1)]
    [InlineData(true, false, true, 2)]
    [InlineData(true, true, false, 0)]
    [InlineData(true, true, false, 1)]
    [InlineData(true, true, false, 2)]
    [InlineData(true, true, true, 0)]
    [InlineData(true, true, true, 1)]
    [InlineData(true, true, true, 2)]
    public async Task When_gcp_rejects_with_selected_writers_should_preserve_route_selection_and_recover(bool useAsync, bool stream, bool fallback, int behavior)
    {
        // Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")));
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "rejection-selection-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var destination = TopicName.FromProjectTopic(project, name + "-rejected");
        var sourceSubscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var observedSubscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name + "-rejected");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        var topics = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var observer = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var deadWriter = new GcpRejectionSelectedWireWriter("dead", behavior);
        var invalidWriter = new GcpRejectionSelectedWireWriter("invalid", behavior);
        var replacement = new GcpRejectionSelectedWireWriter("replacement", 0);
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = b => b.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            SubscriptionManagerConfiguration = b => b.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            PublisherConfiguration = b => b.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            StreamConfiguration = b => { b.EmulatorDetection = EmulatorDetection.EmulatorOnly; b.ClientCount = 1; }
        };
        var settings = new GcpPubSubSubscription(new SubscriptionName(name), new ChannelName(name), new RoutingKey(name),
            requestType: typeof(Command), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            makeChannels: OnMissingChannel.Assume, projectId: project, bufferSize: 1, ackDeadlineSeconds: 10,
            subscriptionMode: stream ? SubscriptionMode.Stream : SubscriptionMode.Pull,
            deadLetterRoutingKey: new RoutingKey(destination.TopicId),
            invalidMessageRoutingKey: fallback ? null : new RoutingKey(destination.TopicId))
        { DeadLetterWireMessageWriter = deadWriter, InvalidMessageWireMessageWriter = invalidWriter };
        IAmAMessageConsumerAsync? consumer = null;
        try
        {
            await topics.CreateTopicAsync(topic, calls);
            await topics.CreateTopicAsync(destination, calls);
            await observer.CreateSubscriptionAsync(sourceSubscription, topic, null, 10, calls);
            await observer.CreateSubscriptionAsync(observedSubscription, destination, null, 10, calls);
            var factory = new GcpPubSubConsumerFactory(connection);
            consumer = useAsync ? factory.CreateAsync(settings) : (IAmAMessageConsumerAsync)factory.Create(settings);
            settings.DeadLetterWireMessageWriter = replacement;
            settings.InvalidMessageWireMessageWriter = replacement;
            foreach (var invalid in new[] { false, true })
            {
                var writer = invalid && !fallback ? invalidWriter : deadWriter;
                writer.Fail = behavior != 0;
                var beforeCalls = writer.Calls;
                var original = new Message(new MessageHeader(Id.Random(), new RoutingKey(name), MessageType.MT_EVENT), new MessageBody("rejected-π"));
                original.Header.Bag["tenant"] = "tenant-λ";
                await topics.PublishAsync(topic, new[] { new DefaultGcpPubSubWireMessageWriter().WriteMessage(original) }, calls);
                var received = await Receive(original.Id);
                var reason = new MessageRejectionReason(invalid ? RejectionReason.Unacceptable : RejectionReason.DeliveryError, "selected route");

                // Act
                await Reject(received, reason);

                // Assert
                Assert.Equal(beforeCalls + 1, writer.Calls);
                if (behavior != 0)
                {
                    var redelivered = await Receive(original.Id);
                    Assert.Equal(original.Id, redelivered.Id);
                    Assert.Equal("rejected-π", redelivered.Body.Value);
                    Assert.Equal(new RoutingKey(name), redelivered.Header.Topic);
                    writer.Fail = false;
                    await Reject(redelivered, reason);
                    Assert.Equal(beforeCalls + 2, writer.Calls);
                }
                var response = await observer.PullAsync(new PullRequest
                { SubscriptionAsSubscriptionName = observedSubscription, MaxMessages = 10 }, calls);
                var delivery = Assert.Single(response.ReceivedMessages);
                var native = delivery.Message;
                Assert.Equal(writer.Route + ":rejected-π", native.Data.ToStringUtf8());
                Assert.Equal(writer.Route, native.Attributes["selected-route"]);
                Assert.Equal(original.Id.Value, native.Attributes[HeaderNames.Id]);
                Assert.Equal("tenant-λ", native.Attributes["tenant"]);
                Assert.Equal(name, native.Attributes[RejectionMetadataKeyNames.OriginalTopic]);
                Assert.Equal(invalid ? "Unacceptable" : "DeliveryError", native.Attributes[RejectionMetadataKeyNames.RejectionReason]);
                Assert.Equal(destination.TopicId, native.Attributes[HeaderNames.Topic]);
                Assert.False(native.Attributes.ContainsKey("ReceiptHandle"));
                Assert.Equal(0, replacement.Calls);
                Assert.Equal("rejected-π", original.Body.Value);
                Assert.Equal(new RoutingKey(name), original.Header.Topic);
                await observer.AcknowledgeAsync(observedSubscription, new[] { delivery.AckId }, calls);
            }
            Assert.Equal(fallback ? 0 : behavior == 0 ? 1 : 2, invalidWriter.Calls);
        }
        finally
        {
            try { if (consumer != null) await consumer.DisposeAsync(); }
            finally
            {
                await Delete(() => observer.DeleteSubscriptionAsync(sourceSubscription));
                await Delete(() => observer.DeleteSubscriptionAsync(observedSubscription));
                await Delete(() => topics.DeleteTopicAsync(topic));
                await Delete(() => topics.DeleteTopicAsync(destination));
            }
        }

        async Task<Message> Receive(Id id)
        {
            while (true)
            {
                timeout.Token.ThrowIfCancellationRequested();
                var messages = useAsync ? await consumer!.ReceiveAsync(TimeSpan.FromSeconds(1), timeout.Token)
                    : ((IAmAMessageConsumerSync)consumer!).Receive(TimeSpan.FromSeconds(1));
                var message = messages.SingleOrDefault(m => m.Id == id);
                if (message != null) return message;
            }
        }
        async Task Reject(Message message, MessageRejectionReason reason)
        {
            if (useAsync) await consumer!.RejectAsync(message, reason, timeout.Token);
            else Assert.True(((IAmAMessageConsumerSync)consumer!).Reject(message, reason));
        }
    }

    private static async Task Delete(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException e) when (e.StatusCode == StatusCode.NotFound) { }
    }
}
