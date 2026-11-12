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
public class AsbIndividualWriterFailureRecoveryTests
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
    public async Task When_an_asb_individual_writer_fails_should_report_failure_and_recover(
        bool useQueue, bool useAsync, bool returnNull)
    {
        //Arrange
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var queue = Environment.GetEnvironmentVariable("BrighterTestsASBWireQueue") ?? "wire-mapping-queue";
        var topic = Environment.GetEnvironmentVariable("BrighterTestsASBWireTopic") ?? "wire-mapping-topic";
        var subscription = Environment.GetEnvironmentVariable("BrighterTestsASBWireSubscription") ?? "wire-observer";
        var destination = new RoutingKey(useQueue ? queue : topic);
        var provider = ASBCreds.ASBClientProvider;
        await using var client = provider.GetServiceBusClient();
        await using var observer = useQueue ? client.CreateReceiver(queue) : client.CreateReceiver(topic, subscription);
        var writer = new AsbRecoverableBatchWireWriter(returnNull);
        var publication = new AzureServiceBusPublication
        {
            Topic = destination,
            UseServiceBusQueue = useQueue,
            MakeChannels = OnMissingChannel.Assume,
            TimeToLive = TimeSpan.FromMinutes(5),
            WireMessageWriter = writer
        };
        using var registry = new AzureServiceBusProducerRegistryFactory(provider, [publication]).Create();
        var producer = registry.LookupBy(destination);
        var asyncProducer = Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer);
        var syncProducer = Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer);
        var ids = Enumerable.Range(0, 2).Select(_ => Id.Random()).ToArray();
        var bodies = Enumerable.Range(0, 2).Select(i => new byte[] { 0, 255, 128, (byte)i }).ToArray();
        var sources = ids.Select((id, i) => new Message(new MessageHeader(id, destination, MessageType.MT_EVENT,
            contentType: new ContentType("application/json"))
        {
            Subject = $"source-{i}",
            CorrelationId = new Id($"correlation-{i}"),
            TraceParent = new TraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            TraceState = new TraceState("vendor=ready")
        }, new MessageBody(bodies[i]))).ToArray();
        sources[0].Header.Bag["batch-writer-failure"] = true;

        //Act
        ChannelFailureException failure;
        if (useAsync)
            failure = await Assert.ThrowsAsync<ChannelFailureException>(() =>
                asyncProducer.SendAsync(sources[0], timeout.Token));
        else
            failure = Assert.Throws<ChannelFailureException>(() => syncProducer.Send(sources[0]));

        //Assert
        Assert.IsType<InvalidOperationException>(failure.InnerException);
        if (!returnNull)
            Assert.Same(writer.Failure, failure.InnerException);
        Assert.Equal(1, writer.WriteCount);
        Assert.Null(await observer.PeekMessageAsync(cancellationToken: timeout.Token));

        //Act
        if (useAsync)
            await asyncProducer.SendAsync(sources[1], timeout.Token);
        else
            syncProducer.Send(sources[1]);
        var native = Assert.IsType<ServiceBusReceivedMessage>(
            await observer.ReceiveMessageAsync(TimeSpan.FromSeconds(20), timeout.Token));
        try
        {
            //Assert
            Assert.Equal(2, writer.WriteCount);
            Assert.Equal("wire-" + ids[1].Value, native.MessageId);
            Assert.Equal("wire-subject", native.Subject);
            Assert.Equal("wire-correlation", native.CorrelationId);
            Assert.Equal("application/octet-stream", native.ContentType);
            Assert.Equal(new byte[] { 0, 255, 128 }.Concat(bodies[1]).ToArray(), native.Body.ToArray());
            Assert.Equal(TimeSpan.FromMinutes(5), native.TimeToLive);
            Assert.Equal("vendor=ready", Assert.IsType<string>(native.ApplicationProperties["external-tracestate"]));
            Assert.DoesNotContain(native.ApplicationProperties.Keys,
                key => key.StartsWith("cloudevents:", StringComparison.OrdinalIgnoreCase));
            for (var i = 0; i < sources.Length; i++)
            {
                Assert.Equal(ids[i], sources[i].Id);
                Assert.Equal(destination, sources[i].Header.Topic);
                Assert.Equal(bodies[i], sources[i].Body.Memory.ToArray());
                Assert.Equal($"source-{i}", sources[i].Header.Subject);
                Assert.Equal(new Id($"correlation-{i}"), sources[i].Header.CorrelationId);
                Assert.Equal(new ContentType("application/json"), sources[i].Header.ContentType);
            }
            Assert.True(Assert.IsType<bool>(sources[0].Header.Bag["batch-writer-failure"]));
        }
        finally
        {
            await observer.CompleteMessageAsync(native, timeout.Token);
        }
    }
}
