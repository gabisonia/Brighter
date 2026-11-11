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

[Trait("Category", "GcpNativeWireWriter")]
[Trait("Requires", "PubSubEmulator")]
public class GcpNativeWireWriterTests
{
    [Theory]
    [InlineData("sync", false)]
    [InlineData("sync", true)]
    [InlineData("async", false)]
    [InlineData("async", true)]
    [InlineData("bulk", false)]
    [InlineData("bulk", true)]
    public async Task When_a_gcp_publication_selects_a_wire_writer_should_preserve_native_mapping_and_ordering(
        string mode, bool ordering)
    {
        //Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "This test requires an explicitly configured native Pub/Sub emulator.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "wire-writer-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var subscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var topics = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var observer = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        var writer = new GcpNativeProbeWireWriter();
        var destination = new RoutingKey(name);
        var bodies = new[] { new byte[] { 0, 255, 1, 128 }, new byte[] { 254, 0, 2, 127 } };
        var ids = new[] { Id.Random(), Id.Random() };
        var sources = bodies.Select((body, index) => new Message(
            new MessageHeader(ids[index], destination, MessageType.MT_EVENT)
            { PartitionKey = new PartitionKey("source-order") }, new MessageBody(body))).ToArray();
        for (var index = 0; index < sources.Length; index++)
            sources[index].Header.Bag["source-marker"] = "source-" + index;
        var publication = new GcpPublication
        {
            Topic = destination,
            MakeChannels = OnMissingChannel.Assume,
            EnableMessageOrdering = ordering,
            WireMessageWriter = writer
        };
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            PublisherConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly
        };
        try
        {
            await topics.CreateTopicAsync(topic, calls);
            await observer.CreateSubscriptionAsync(new Google.Cloud.PubSub.V1.Subscription
            {
                SubscriptionName = subscription,
                TopicAsTopicName = topic,
                AckDeadlineSeconds = 60,
                EnableMessageOrdering = ordering
            }, calls);
            var factory = new GcpPubSubMessageProducerFactory(connection, new[] { publication });
            var producers = mode == "sync" ? factory.Create() : await factory.CreateAsync();
            await using var producer = Assert.IsType<GcpMessageProducer>(Assert.Single(producers).Value);

            //Act
            var sendError = await Record.ExceptionAsync(async () =>
            {
                if (mode == "bulk")
                {
                    foreach (var batch in await producer.CreateBatchesAsync(sources, timeout.Token))
                        await producer.SendAsync(batch, timeout.Token);
                }
                else
                {
                    foreach (var source in sources)
                    {
                        if (mode == "sync")
                            producer.Send(source);
                        else
                            await producer.SendAsync(source, timeout.Token);
                    }
                }
            });

            //Assert
            Assert.Null(sendError);
            Assert.Equal(2, writer.Calls);
            var received = new List<ReceivedMessage>();
            while (received.Count < sources.Length)
            {
                var response = await observer.PullAsync(new PullRequest
                {
                    SubscriptionAsSubscriptionName = subscription,
                    MaxMessages = sources.Length - received.Count
                }, calls);
                received.AddRange(response.ReceivedMessages);
            }
            Assert.Equal(2, received.Count);
            for (var index = 0; index < sources.Length; index++)
            {
                var native = Assert.Single(received.Where(item =>
                    item.Message.Attributes.TryGetValue("external-id", out var id) && id == ids[index].ToString())).Message;
                Assert.Equal(new byte[] { 87, 0 }.Concat(bodies[index]).ToArray(), native.Data.ToByteArray());
                Assert.Equal(2, native.Attributes.Count);
                Assert.Equal("source-" + index, native.Attributes["external-marker"]);
                Assert.Equal(ordering ? "source-order" : string.Empty, native.OrderingKey);
                Assert.False(string.IsNullOrWhiteSpace(native.MessageId));
                Assert.Equal(bodies[index], sources[index].Body.Memory.ToArray());
                Assert.Equal(ids[index], sources[index].Header.MessageId);
                Assert.Equal(destination, sources[index].Header.Topic);
                Assert.Equal(new PartitionKey("source-order"), sources[index].Header.PartitionKey);
                Assert.Single(sources[index].Header.Bag);
                Assert.Equal("source-" + index, sources[index].Header.Bag["source-marker"]);
            }
            await observer.AcknowledgeAsync(subscription, received.Select(item => item.AckId), calls);
        }
        finally
        {
            try { await DeleteAsync(() => observer.DeleteSubscriptionAsync(subscription)); }
            finally { await DeleteAsync(() => topics.DeleteTopicAsync(topic)); }
        }
    }

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
