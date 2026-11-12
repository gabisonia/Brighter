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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Paramore.Brighter.Gcp.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.GcpPubSub;
using Xunit;

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

[Trait("Category", "GcpNativeWireWriter")]
[Trait("Requires", "PubSubEmulator")]
public class GcpSendCancellationRecoveryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_gcp_send_is_cancelled_before_publication_should_preserve_the_source_and_recover(
        bool useBulk, bool cancelDuringMapping)
    {
        //Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "This test requires an explicitly configured native Pub/Sub emulator.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "wire-send-cancel-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var subscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        var attempted = new ConcurrentQueue<PublishRequest>();
        var completed = new ConcurrentQueue<PublishRequest>();
        var interceptor = new GcpPurgeFaultInterceptor
        {
            BeforeCall = (_, request) =>
            {
                if (request is PublishRequest publish)
                    attempted.Enqueue(publish.Clone());
            },
            AfterCall = (_, request) =>
            {
                if (request is PublishRequest publish)
                    completed.Enqueue(publish.Clone());
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
        using var cancellation = new CancellationTokenSource();
        var writer = new GcpCancellationWireWriter(cancellation)
        { CancelDuringMapping = cancelDuringMapping };
        var initialMappings = cancelDuringMapping ? 1 : 0;
        var destination = new RoutingKey(name);
        var body = new byte[] { 0, 255, 1, 128 };
        var first = CreateSource("retry-marker");
        var fresh = CreateSource("fresh-marker");
        var firstId = first.Id;
        var freshId = fresh.Id;
        var publication = new GcpPublication
        {
            Topic = destination,
            MakeChannels = OnMissingChannel.Assume,
            EnableMessageOrdering = true,
            WireMessageWriter = writer
        };
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.CallInvoker = invoker,
            PublisherConfiguration = builder =>
            {
                builder.CallInvoker = invoker;
                builder.ClientCount = 1;
            }
        };
        try
        {
            await topics.CreateTopicAsync(topic, calls);
            await observer.CreateSubscriptionAsync(new Google.Cloud.PubSub.V1.Subscription
            {
                SubscriptionName = subscription,
                TopicAsTopicName = topic,
                AckDeadlineSeconds = 60,
                EnableMessageOrdering = true
            }, calls);
            var factory = new GcpPubSubMessageProducerFactory(connection, new[] { publication });
            var producers = await factory.CreateAsync();
            await using var producer = Assert.IsType<GcpMessageProducer>(Assert.Single(producers).Value);

            if (!cancelDuringMapping)
                cancellation.Cancel();

            //Act
            var failure = await Record.ExceptionAsync(() => SendAsync(first, cancellation.Token));

            //Assert
            var cancelled = Assert.IsAssignableFrom<OperationCanceledException>(failure);
            Assert.Equal(cancellation.Token, cancelled.CancellationToken);
            Assert.Empty(attempted);
            Assert.Empty(completed);
            Assert.Equal(initialMappings, writer.Sources.Length);
            if (cancelDuringMapping)
                Assert.Same(first, Assert.Single(writer.Sources));
            AssertSource(first, firstId, "retry-marker");

            writer.CancelDuringMapping = false;
            var recoveryError = await Record.ExceptionAsync(() => SendAsync(first, timeout.Token));
            Assert.Null(recoveryError);
            Assert.Equal(initialMappings + 1, writer.Sources.Length);
            Assert.Same(first, writer.Sources[initialMappings]);
            AssertPublicationCount(1);
            var firstNativeId = await ObserveAsync(first, "retry-marker");
            AssertSource(first, firstId, "retry-marker");

            var freshError = await Record.ExceptionAsync(() => SendAsync(fresh, timeout.Token));
            Assert.Null(freshError);
            Assert.Equal(initialMappings + 2, writer.Sources.Length);
            Assert.Same(fresh, writer.Sources[initialMappings + 1]);
            AssertPublicationCount(2);
            var freshNativeId = await ObserveAsync(fresh, "fresh-marker");
            Assert.NotEqual(firstNativeId, freshNativeId);
            AssertSource(first, firstId, "retry-marker");
            AssertSource(fresh, freshId, "fresh-marker");

            async Task SendAsync(Message source, CancellationToken token)
            {
                if (useBulk)
                {
                    var batches = await producer.CreateBatchesAsync(new[] { source }, timeout.Token);
                    await producer.SendAsync(Assert.Single(batches), token);
                }
                else
                    await producer.SendAsync(source, token);
            }

            void AssertPublicationCount(int expected)
            {
                Assert.Equal(expected, attempted.Count);
                Assert.Equal(expected, completed.Count);
                Assert.All(completed, request =>
                {
                    Assert.Equal(topic.ToString(), request.Topic);
                    Assert.Single(request.Messages);
                });
            }

            async Task<string> ObserveAsync(Message source, string marker)
            {
                PullResponse response;
                do
                {
                    response = await observer.PullAsync(new PullRequest
                    {
                        SubscriptionAsSubscriptionName = subscription,
                        MaxMessages = 1
                    }, calls);
                } while (response.ReceivedMessages.Count == 0);
                var delivery = Assert.Single(response.ReceivedMessages);
                var native = delivery.Message;
                Assert.Equal(new byte[] { 67, 0, 0, 255, 1, 128 }, native.Data.ToByteArray());
                Assert.Equal(source.Id.Value, native.Attributes["external-id"]);
                Assert.Equal(marker, native.Attributes["external-marker"]);
                Assert.Equal(2, native.Attributes.Count);
                Assert.Equal("source-order", native.OrderingKey);
                Assert.False(string.IsNullOrWhiteSpace(native.MessageId));
                Assert.False(string.IsNullOrWhiteSpace(delivery.AckId));
                await observer.AcknowledgeAsync(subscription, new[] { delivery.AckId }, calls);
                return native.MessageId;
            }
        }
        finally
        {
            try { await DeleteAsync(() => observer.DeleteSubscriptionAsync(subscription)); }
            finally { await DeleteAsync(() => topics.DeleteTopicAsync(topic)); }
        }

        Message CreateSource(string marker)
        {
            var header = new MessageHeader(Id.Random(), destination, MessageType.MT_EVENT)
            { PartitionKey = new PartitionKey("source-order") };
            header.Bag["source-marker"] = marker;
            return new Message(header, new MessageBody(body.ToArray()));
        }

        void AssertSource(Message source, Id expectedId, string marker)
        {
            Assert.Equal(expectedId, source.Id);
            Assert.Equal(body, source.Body.Memory.ToArray());
            Assert.Equal(destination, source.Header.Topic);
            Assert.Equal(new PartitionKey("source-order"), source.Header.PartitionKey);
            Assert.Single(source.Header.Bag);
            Assert.Equal(marker, source.Header.Bag["source-marker"]);
        }
    }

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
