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
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Xunit;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

[Trait("Category", "ASB")]
[Collection("ASB wire mapping")]
public class AsbBatchWireSplittingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_asb_batches_split_should_map_each_source_once_and_preserve_native_deliveries(
        bool useQueue)
    {
        //Arrange
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var queue = Environment.GetEnvironmentVariable("BrighterTestsASBWireQueue") ?? "wire-mapping-queue";
        var topic = Environment.GetEnvironmentVariable("BrighterTestsASBWireTopic") ?? "wire-mapping-topic";
        var subscription = Environment.GetEnvironmentVariable("BrighterTestsASBWireSubscription") ?? "wire-observer";
        var destination = new RoutingKey(useQueue ? queue : topic);
        var provider = ASBCreds.ASBClientProvider;
        await using var client = provider.GetServiceBusClient();
        await using var observer = useQueue ? client.CreateReceiver(queue) : client.CreateReceiver(topic, subscription);
        await using var nativeSender = client.CreateSender(destination.Value);
        using var capacityProbe = await nativeSender.CreateMessageBatchAsync(timeout.Token);
        Assert.InRange(capacityProbe.MaxSizeInBytes, 16 * 1024L, 16 * 1024 * 1024L);
        var payloadSize = checked((int)(capacityProbe.MaxSizeInBytes * 3 / 5));
        var externalWriter = new AsbExternalWireWriter();
        var publication = new AzureServiceBusPublication
        {
            Topic = destination,
            UseServiceBusQueue = useQueue,
            MakeChannels = OnMissingChannel.Assume,
            TimeToLive = TimeSpan.FromMinutes(5),
            WireMessageWriter = externalWriter
        };
        using var registry = new AzureServiceBusProducerRegistryFactory(provider, [publication]).Create();
        var producer = Assert.IsAssignableFrom<IAmABulkMessageProducerAsync>(registry.LookupBy(destination));
        var ids = Enumerable.Range(0, 3).Select(_ => Id.Random()).ToArray();
        var bodies = Enumerable.Range(0, 3).Select(i => Enumerable.Repeat((byte)(128 + i), payloadSize).ToArray()).ToArray();
        var sources = ids.Select((id, i) => new Message(new MessageHeader(id, destination, MessageType.MT_EVENT,
            contentType: new ContentType("application/json"))
        {
            Subject = $"source-{i}",
            CorrelationId = new Id($"correlation-{i}"),
            TraceParent = new TraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            TraceState = new TraceState("vendor=ready")
        }, new MessageBody(bodies[i]))).ToArray();

        //Act
        var batches = (await producer.CreateBatchesAsync(sources, timeout.Token)).ToArray();
        foreach (var batch in batches)
            await producer.SendAsync(batch, timeout.Token);

        //Assert
        Assert.Equal(sources.Length, batches.Length);
        Assert.All(batches, batch =>
        {
            var nativeBatch = Assert.IsType<AzureServiceBusMessageBatch>(batch);
            Assert.Single(nativeBatch.Ids());
            Assert.Equal(1, nativeBatch.Content.Count);
            Assert.InRange(nativeBatch.Content.SizeInBytes, 1L, nativeBatch.Content.MaxSizeInBytes);
        });
        Assert.Equal(ids, batches.SelectMany(batch => batch.Ids()).ToArray());
        Assert.All(batches, batch => Assert.Equal(destination, batch.RoutingKey));
        Assert.Equal(sources.Length, externalWriter.WriteCount);
        var observed = new System.Collections.Generic.HashSet<string>();
        for (var count = 0; count < sources.Length; count++)
        {
            var native = Assert.IsType<ServiceBusReceivedMessage>(
                await observer.ReceiveMessageAsync(TimeSpan.FromSeconds(20), timeout.Token));
            try
            {
                var i = Array.FindIndex(ids, id => native.MessageId == ("wire-" + id.Value));
                Assert.InRange(i, 0, sources.Length - 1);
                Assert.True(observed.Add(native.MessageId), "Each source must have a distinct native delivery.");
                Assert.Equal("wire-subject", native.Subject);
                Assert.Equal("wire-correlation", native.CorrelationId);
                Assert.Equal("application/octet-stream", native.ContentType);
                Assert.Equal(new byte[] { 0, 255, 128 }.Concat(bodies[i]).ToArray(), native.Body.ToArray());
                Assert.Equal(TimeSpan.FromMinutes(5), native.TimeToLive);
                Assert.Equal("vendor=ready", Assert.IsType<string>(native.ApplicationProperties["external-tracestate"]));
                Assert.DoesNotContain(native.ApplicationProperties.Keys,
                    key => key.StartsWith("cloudevents:", StringComparison.OrdinalIgnoreCase));
            }
            finally
            {
                await observer.CompleteMessageAsync(native, timeout.Token);
            }
        }
        for (var i = 0; i < sources.Length; i++)
        {
            Assert.Equal(ids[i], sources[i].Id);
            Assert.Equal(destination, sources[i].Header.Topic);
            Assert.Equal(bodies[i], sources[i].Body.Memory.ToArray());
            Assert.Equal($"source-{i}", sources[i].Header.Subject);
            Assert.Equal(new Id($"correlation-{i}"), sources[i].Header.CorrelationId);
            Assert.Equal(new ContentType("application/json"), sources[i].Header.ContentType);
        }
    }
}
