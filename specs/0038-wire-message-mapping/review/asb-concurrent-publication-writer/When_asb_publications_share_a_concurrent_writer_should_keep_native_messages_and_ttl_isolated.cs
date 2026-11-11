#region Licence

/* The MIT License (MIT)
Copyright © 2026 Irakli Gabisonia

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
using System.Net.Mime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Xunit;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

[Trait("Category", "ASB")]
[Collection("ASB wire mapping")]
public class AsbConcurrentPublicationWriterTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task When_asb_publications_share_a_concurrent_writer_should_keep_native_messages_and_ttl_isolated(
        int sendMode)
    {
        //Arrange
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var queue = Environment.GetEnvironmentVariable("BrighterTestsASBWireQueue") ?? "wire-mapping-queue";
        var topic = Environment.GetEnvironmentVariable("BrighterTestsASBWireTopic") ?? "wire-mapping-topic";
        var subscription = Environment.GetEnvironmentVariable("BrighterTestsASBWireSubscription") ?? "wire-observer";
        var provider = ASBCreds.ASBClientProvider;
        await using var client = provider.GetServiceBusClient();
        await using var queueObserver = client.CreateReceiver(queue);
        await using var topicObserver = client.CreateReceiver(topic, subscription);
        using var writer = new AsbConcurrentWireWriter();
        var destinations = new[] { new RoutingKey(queue), new RoutingKey(topic) };
        var ttls = new[] { TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(7) };
        var publications = destinations.Select((destination, i) => new AzureServiceBusPublication
        {
            Topic = destination,
            UseServiceBusQueue = i == 0,
            MakeChannels = OnMissingChannel.Assume,
            TimeToLive = ttls[i],
            WireMessageWriter = writer
        }).ToArray();
        using var registry = new AzureServiceBusProducerRegistryFactory(provider, publications).Create();
        var ids = destinations.Select(_ => Id.Random()).ToArray();
        var bodies = destinations.Select((_, i) => Encoding.UTF8.GetBytes($"source-{i}-café\0")).ToArray();
        var sources = destinations.Select((destination, i) => new Message(
            new MessageHeader(ids[i], destination, MessageType.MT_EVENT, contentType: new ContentType("application/json"))
            {
                Subject = $"subject-{i}",
                CorrelationId = new Id($"correlation-{i}"),
                TraceParent = new TraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
                TraceState = new TraceState($"vendor=endpoint{i}")
            }, new MessageBody(bodies[i]))).ToArray();

        //Act
        var sends = Enumerable.Range(0, 2).Select(i => Task.Factory.StartNew(
            () => Send(i), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap()).ToArray();
        await Task.WhenAll(sends);

        //Assert
        Assert.Equal(2, writer.WriteCount);
        for (var i = 0; i < 2; i++)
        {
            var observer = i == 0 ? queueObserver : topicObserver;
            var native = Assert.IsType<ServiceBusReceivedMessage>(
                await observer.ReceiveMessageAsync(TimeSpan.FromSeconds(20), timeout.Token));
            try
            {
                Assert.Equal("wire-" + ids[i].Value, native.MessageId);
                Assert.Equal(new byte[] { 0, 255, 128 }.Concat(bodies[i]).ToArray(), native.Body.ToArray());
                Assert.Equal(ttls[i], native.TimeToLive);
                Assert.Equal("wire-subject", native.Subject);
                Assert.Equal("wire-correlation", native.CorrelationId);
                Assert.Equal("application/octet-stream", native.ContentType);
                Assert.Equal($"vendor=endpoint{i}", Assert.IsType<string>(native.ApplicationProperties["external-tracestate"]));
                Assert.DoesNotContain(native.ApplicationProperties.Keys,
                    key => key.StartsWith("cloudevents:", StringComparison.OrdinalIgnoreCase));
                Assert.Equal(ids[i], sources[i].Id);
                Assert.Equal(destinations[i], sources[i].Header.Topic);
                Assert.Equal(bodies[i], sources[i].Body.Memory.ToArray());
                Assert.Equal($"subject-{i}", sources[i].Header.Subject);
                Assert.Equal(new Id($"correlation-{i}"), sources[i].Header.CorrelationId);
                Assert.Equal(new ContentType("application/json"), sources[i].Header.ContentType);
                Assert.Equal($"vendor=endpoint{i}", sources[i].Header.TraceState!.Value);
            }
            finally
            {
                await observer.CompleteMessageAsync(native, timeout.Token);
            }
        }

        async Task Send(int endpoint)
        {
            var producer = registry.LookupBy(destinations[endpoint]);
            if (sendMode == 0)
                ((IAmAMessageProducerSync)producer).Send(sources[endpoint]);
            else if (sendMode == 1)
                await ((IAmAMessageProducerAsync)producer).SendAsync(sources[endpoint], timeout.Token);
            else
            {
                var bulk = Assert.IsAssignableFrom<IAmABulkMessageProducerAsync>(producer);
                var batch = Assert.Single(await bulk.CreateBatchesAsync([sources[endpoint]], timeout.Token));
                Assert.Equal(ids[endpoint], Assert.Single(batch.Ids()));
                await bulk.SendAsync(batch, timeout.Token);
            }
        }
    }
}
