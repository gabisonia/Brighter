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
public class AsbScheduledWireWriterTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_an_asb_publication_schedules_with_a_custom_writer_should_preserve_mapping_and_delay(
        bool useQueue, bool useAsync)
    {
        //Arrange
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var queue = Environment.GetEnvironmentVariable("BrighterTestsASBWireQueue") ?? "wire-mapping-queue";
        var topic = Environment.GetEnvironmentVariable("BrighterTestsASBWireTopic") ?? "wire-mapping-topic";
        var subscription = Environment.GetEnvironmentVariable("BrighterTestsASBWireSubscription") ?? "wire-observer";
        var destination = new RoutingKey(useQueue ? queue : topic);
        var provider = ASBCreds.ASBClientProvider;
        await using var client = provider.GetServiceBusClient();
        await using var observer = useQueue ? client.CreateReceiver(queue) : client.CreateReceiver(topic, subscription);
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
        var producer = registry.LookupBy(destination);
        var sourceId = Id.Random();
        var sourceBody = Encoding.UTF8.GetBytes("source-café\0");
        const string traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        const string traceState = "vendor=ready";
        var source = new Message(new MessageHeader(sourceId, destination, MessageType.MT_EVENT,
            contentType: new ContentType("application/json"))
        {
            Subject = "source-subject",
            CorrelationId = new Id("source-correlation"),
            TraceParent = new TraceParent(traceParent),
            TraceState = new TraceState(traceState)
        }, new MessageBody(sourceBody));
        source.Header.Baggage.Add("tenant", "acme");
        source.Header.Bag["source-only"] = "unchanged";

        var delay = TimeSpan.FromSeconds(5);

        //Act
        var startedAt = DateTimeOffset.UtcNow;
        if (useAsync)
            await ((IAmAMessageProducerAsync)producer).SendWithDelayAsync(source, delay, timeout.Token);
        else
            ((IAmAMessageProducerSync)producer).SendWithDelay(source, delay);
        var submittedAt = DateTimeOffset.UtcNow;
        var native = Assert.IsType<ServiceBusReceivedMessage>(
            await observer.ReceiveMessageAsync(TimeSpan.FromSeconds(30), timeout.Token));
        var receivedAt = DateTimeOffset.UtcNow;

        try
        {
            //Assert
            Assert.InRange(native.ScheduledEnqueueTime,
                startedAt.Add(delay).AddSeconds(-1), submittedAt.Add(delay).AddSeconds(1));
            Assert.True(receivedAt >= startedAt.Add(delay).AddSeconds(-1), "Delivery arrived before its requested delay.");
            Assert.Equal("wire-" + sourceId.Value, native.MessageId);
            Assert.Equal("wire-subject", native.Subject);
            Assert.Equal("wire-correlation", native.CorrelationId);
            Assert.Equal("application/octet-stream", native.ContentType);
            Assert.Equal(new byte[] { 0, 255, 128 }.Concat(sourceBody).ToArray(), native.Body.ToArray());
            Assert.Equal(TimeSpan.FromMinutes(5), native.TimeToLive);
            Assert.Equal(1, externalWriter.WriteCount);
            Assert.Equal(traceParent, Property("external-traceparent"));
            Assert.Equal(traceState, Property("external-tracestate"));
            Assert.Equal("tenant=acme", Property("external-baggage"));
            Assert.DoesNotContain(native.ApplicationProperties.Keys,
                key => key.StartsWith("cloudevents:", StringComparison.OrdinalIgnoreCase));
            Assert.False(native.ApplicationProperties.ContainsKey("source-only"));
            Assert.Equal(sourceId, source.Id);
            Assert.Equal(destination, source.Header.Topic);
            Assert.Equal(sourceBody, source.Body.Memory.ToArray());
            Assert.Equal("source-subject", source.Header.Subject);
            Assert.Equal(new Id("source-correlation"), source.Header.CorrelationId);
            Assert.Equal(new ContentType("application/json"), source.Header.ContentType);
            Assert.Equal(traceParent, source.Header.TraceParent!.Value);
            Assert.Equal(traceState, source.Header.TraceState!.Value);
            Assert.Equal("tenant=acme", source.Header.Baggage.ToString());
            Assert.Equal("unchanged", Assert.IsType<string>(Assert.Single(source.Header.Bag).Value));
        }
        finally
        {
            await observer.CompleteMessageAsync(native);
        }

        string Property(string key)
        {
            Assert.True(native.ApplicationProperties.TryGetValue(key, out var value), $"Native property '{key}' is missing.");
            return Assert.IsType<string>(value);
        }
    }
}
