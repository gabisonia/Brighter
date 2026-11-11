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
using Xunit.Abstractions;

namespace Paramore.Brighter.Gcp.Tests.MessagingGateway;

[Trait("Category", "GcpNativeWireWriter")]
[Trait("Requires", "PubSubEmulator")]
public class GcpPublicationWriterIsolationTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("sync", false)]
    [InlineData("sync", true)]
    [InlineData("async", false)]
    [InlineData("async", true)]
    [InlineData("bulk", false)]
    [InlineData("bulk", true)]
    public async Task When_gcp_publications_select_different_writers_should_keep_native_mapping_isolated(
        string mode, bool reverseCreation)
    {
        //Arrange
        Assert.False(string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")),
            "An explicit native Pub/Sub emulator is required.");
        var project = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "brighter-test";
        var names = Enumerable.Range(0, 3).Select(index => "writer-isolation-" + Guid.NewGuid().ToString("N") + "-" + index).ToArray();
        var topics = names.Select(name => TopicName.FromProjectTopic(project, name)).ToArray();
        var subscriptions = names.Select(name => Google.Cloud.PubSub.V1.SubscriptionName.FromProjectSubscription(project, name)).ToArray();
        var destinations = names.Select(name => new RoutingKey(name)).ToArray();
        var firstWriter = new GcpEndpointIsolationWireWriter(65, "endpoint-a");
        var secondWriter = new GcpEndpointIsolationWireWriter(66, "endpoint-b");
        var publications = destinations.Select((destination, index) => new GcpPublication
        {
            Topic = destination,
            MakeChannels = OnMissingChannel.Assume,
            WireMessageWriter = index == 0 ? firstWriter : index == 2 ? secondWriter : null
        }).ToArray();
        var bodies = Enumerable.Range(0, 3).Select(endpoint => Enumerable.Range(0, 2)
            .Select(index => new byte[] { (byte)(endpoint * 10 + index), 0, 255, 128 }).ToArray()).ToArray();
        var ids = Enumerable.Range(0, 3).Select(_ => new[] { Id.Random(), Id.Random() }).ToArray();
        var sources = Enumerable.Range(0, 3).Select(endpoint => Enumerable.Range(0, 2).Select(index =>
        {
            var message = new Message(new MessageHeader(ids[endpoint][index], destinations[endpoint], MessageType.MT_EVENT),
                new MessageBody(bodies[endpoint][index].ToArray()));
            message.Header.Bag["source-marker"] = endpoint + ":" + index;
            return message;
        }).ToArray()).ToArray();
        var topicAdmin = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var observer = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.Build();
        var connection = new GcpMessagingGatewayConnection
        {
            ProjectId = project,
            TopicManagerConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly,
            PublisherConfiguration = builder => builder.EmulatorDetection = EmulatorDetection.EmulatorOnly
        };
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var calls = CallSettings.FromCancellationToken(budget.Token);
        var producers = new List<GcpMessageProducer>();
        var cleanupErrors = new List<Exception>();
        var behaviorSucceeded = false;
        try
        {
            for (var endpoint = 0; endpoint < 3; endpoint++)
            {
                await topicAdmin.CreateTopicAsync(topics[endpoint], calls);
                await observer.CreateSubscriptionAsync(subscriptions[endpoint], topics[endpoint], null, 60, calls);
            }
            var factoryOrder = reverseCreation ? publications.Reverse().ToArray() : publications;
            var factory = new GcpPubSubMessageProducerFactory(connection, factoryOrder);
            var created = mode == "sync" ? factory.Create() : await factory.CreateAsync();
            foreach (var producer in created.Values)
                producers.Add(Assert.IsType<GcpMessageProducer>(producer));
            Assert.Equal(3, producers.Count);
            var endpoints = destinations.Select(destination => Assert.IsType<GcpMessageProducer>(created[new ProducerKey(destination)])).ToArray();

            //Act
            if (mode == "bulk")
            {
                for (var endpoint = 0; endpoint < 3; endpoint++)
                {
                    foreach (var batch in await endpoints[endpoint].CreateBatchesAsync(sources[endpoint], budget.Token))
                        await endpoints[endpoint].SendAsync(batch, budget.Token);
                }
            }
            else
            {
                for (var index = 0; index < 2; index++)
                for (var endpoint = 0; endpoint < 3; endpoint++)
                {
                    if (mode == "sync")
                        endpoints[endpoint].Send(sources[endpoint][index]);
                    else
                        await endpoints[endpoint].SendAsync(sources[endpoint][index], budget.Token);
                }
            }

            //Assert
            for (var endpoint = 0; endpoint < 3; endpoint++)
            {
                var received = new Dictionary<string, ReceivedMessage>();
                while (received.Count < 2)
                {
                    var response = await observer.PullAsync(new PullRequest
                    {
                        SubscriptionAsSubscriptionName = subscriptions[endpoint], MaxMessages = 2
                    }, calls);
                    foreach (var delivery in response.ReceivedMessages)
                        received.TryAdd(delivery.Message.MessageId, delivery);
                    if (response.ReceivedMessages.Count > 0)
                        await observer.AcknowledgeAsync(subscriptions[endpoint], response.ReceivedMessages.Select(delivery => delivery.AckId), calls);
                }
                Assert.Equal(2, received.Count);
                for (var index = 0; index < 2; index++)
                {
                    var id = ids[endpoint][index].Value;
                    var native = Assert.Single(received.Values, delivery =>
                        delivery.Message.Attributes.TryGetValue("external-id", out var externalId) && externalId == id
                        || delivery.Message.Attributes.TryGetValue(HeaderNames.Id, out var defaultId) && defaultId == id).Message;
                    var expected = endpoint == 1 ? bodies[endpoint][index]
                        : new[] { (byte)(endpoint == 0 ? 65 : 66) }.Concat(bodies[endpoint][index]).ToArray();
                    Assert.Equal(expected, native.Data.ToByteArray());
                    if (endpoint == 1)
                    {
                        Assert.Equal(id, native.Attributes[HeaderNames.Id]);
                        Assert.False(native.Attributes.ContainsKey("external-id"));
                        Assert.False(native.Attributes.ContainsKey("endpoint-marker"));
                    }
                    else
                    {
                        Assert.Equal(3, native.Attributes.Count);
                        Assert.Equal(endpoint == 0 ? "endpoint-a" : "endpoint-b", native.Attributes["endpoint-marker"]);
                        Assert.Equal(endpoint + ":" + index, native.Attributes["source-marker"]);
                    }
                    Assert.False(string.IsNullOrWhiteSpace(native.MessageId));
                    Assert.Equal(string.Empty, native.OrderingKey);
                    Assert.Equal(bodies[endpoint][index], sources[endpoint][index].Body.Memory.ToArray());
                    Assert.Equal(ids[endpoint][index], sources[endpoint][index].Id);
                    Assert.Equal(destinations[endpoint], sources[endpoint][index].Header.Topic);
                    Assert.Single(sources[endpoint][index].Header.Bag);
                    Assert.Equal(endpoint + ":" + index, sources[endpoint][index].Header.Bag["source-marker"]);
                }
            }
            Assert.Equal(2, firstWriter.Calls);
            Assert.Equal(2, secondWriter.Calls);
            Assert.Same(firstWriter, publications[0].WireMessageWriter);
            Assert.Null(publications[1].WireMessageWriter);
            Assert.Same(secondWriter, publications[2].WireMessageWriter);
            behaviorSucceeded = true;
        }
        finally
        {
            foreach (var producer in producers)
            {
                try { await producer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)); }
                catch (Exception exception) { cleanupErrors.Add(exception); }
            }
            for (var endpoint = 0; endpoint < 3; endpoint++)
            {
                try { await DeleteAsync(() => observer.DeleteSubscriptionAsync(subscriptions[endpoint])); }
                catch (Exception exception) { cleanupErrors.Add(exception); }
                try { await DeleteAsync(() => topicAdmin.DeleteTopicAsync(topics[endpoint])); }
                catch (Exception exception) { cleanupErrors.Add(exception); }
            }
            foreach (var exception in cleanupErrors)
                output.WriteLine(exception.ToString());
            if (behaviorSucceeded)
                Assert.Empty(cleanupErrors);
        }
    }

    private static async Task DeleteAsync(Func<Task> delete)
    {
        try { await delete().WaitAsync(TimeSpan.FromSeconds(15)); }
        catch (RpcException exception) when (exception.StatusCode == StatusCode.NotFound) { }
    }
}
