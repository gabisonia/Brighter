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
using System.Collections.Generic;
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
public class GcpPartialBulkWriterFailureRecoveryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_a_gcp_bulk_writer_fails_after_progress_should_preserve_the_prefix_and_retry_the_remainder(
        bool returnNull)
    {
        //Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "This test requires an explicitly configured native Pub/Sub emulator.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var name = "wire-bulk-failure-" + Guid.NewGuid().ToString("N");
        var topic = TopicName.FromProjectTopic(project, name);
        var subscription = Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(timeout.Token);
        var writer = new GcpRecoverableWireWriter(returnNull) { Fail = false };
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
                if (request is not PublishRequest publish)
                    return;
                completed.Enqueue(publish.Clone());
                if (completed.Count == 1)
                    writer.Fail = true;
            }
        };
        using var transport = GrpcChannel.ForAddress(
            "http://" + Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST"),
            new Grpc.Net.Client.GrpcChannelOptions { Credentials = ChannelCredentials.Insecure });
        var invoker = transport.Intercept(interceptor);
        var topics = new PublisherServiceApiClientBuilder { CallInvoker = invoker }.Build();
        var observer = new SubscriberServiceApiClientBuilder { CallInvoker = invoker }.Build();
        var destination = new RoutingKey(name);
        var ids = Enumerable.Range(0, 3).Select(_ => Id.Random()).ToArray();
        var bodies = Enumerable.Range(0, 3).Select(i => new byte[] { (byte)i, 255, 0, 128 }).ToArray();
        var sources = Enumerable.Range(0, 3).Select(i =>
        {
            var header = new MessageHeader(ids[i], destination, MessageType.MT_EVENT)
            { PartitionKey = new PartitionKey("source-order") };
            header.Bag["source-marker"] = "source-" + i;
            return new Message(header, new MessageBody(bodies[i].ToArray()));
        }).ToArray();
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
            var batch = Assert.Single(await producer.CreateBatchesAsync(sources, timeout.Token));

            //Act
            var failure = await Record.ExceptionAsync(() => producer.SendAsync(batch, timeout.Token));

            //Assert
            var rejected = Assert.IsType<InvalidOperationException>(failure);
            if (returnNull)
                Assert.Equal("The wire message writer returned null.", rejected.Message);
            else
                Assert.Same(writer.Failure, rejected);
            Assert.Equal(2, writer.Sources.Length);
            Assert.Same(sources[0], writer.Sources[0]);
            Assert.Same(sources[1], writer.Sources[1]);
            AssertPublicationCount(1);
            Assert.Equal(ids[0].Value, Assert.Single(completed.Single().Messages).Attributes["external-id"]);
            AssertSources();
            var nativeIds = new List<string> { await ObserveAsync(0) };

            writer.Fail = false;
            var remainder = Assert.Single(await producer.CreateBatchesAsync(sources.Skip(1), timeout.Token));
            var recoveryError = await Record.ExceptionAsync(() => producer.SendAsync(remainder, timeout.Token));
            Assert.Null(recoveryError);
            Assert.Equal(4, writer.Sources.Length);
            Assert.Same(sources[1], writer.Sources[2]);
            Assert.Same(sources[2], writer.Sources[3]);
            AssertPublicationCount(3);
            nativeIds.Add(await ObserveAsync(1));
            nativeIds.Add(await ObserveAsync(2));
            Assert.Equal(3, nativeIds.Distinct().Count());
            AssertSources();

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

            async Task<string> ObserveAsync(int index)
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
                Assert.Equal(new byte[] { 87, 0 }.Concat(bodies[index]).ToArray(), native.Data.ToByteArray());
                Assert.True(native.Attributes.TryGetValue("external-id", out var id));
                Assert.Equal(ids[index].Value, id);
                Assert.Equal("source-" + index, native.Attributes["external-marker"]);
                Assert.Equal(2, native.Attributes.Count);
                Assert.Equal("source-order", native.OrderingKey);
                Assert.False(string.IsNullOrWhiteSpace(native.MessageId));
                Assert.False(string.IsNullOrWhiteSpace(delivery.AckId));
                await observer.AcknowledgeAsync(subscription, new[] { delivery.AckId }, calls);
                return native.MessageId;
            }

            void AssertSources()
            {
                for (var i = 0; i < sources.Length; i++)
                {
                    Assert.Equal(ids[i], sources[i].Id);
                    Assert.Equal(bodies[i], sources[i].Body.Memory.ToArray());
                    Assert.Equal(destination, sources[i].Header.Topic);
                    Assert.Equal(new PartitionKey("source-order"), sources[i].Header.PartitionKey);
                    Assert.Single(sources[i].Header.Bag);
                    Assert.Equal("source-" + i, sources[i].Header.Bag["source-marker"]);
                }
            }
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
