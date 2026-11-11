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
public class AsbNativeWireWriterTests
{
    [Theory]
    [InlineData(false, false, 0)]
    [InlineData(false, false, 1)]
    [InlineData(false, false, 2)]
    [InlineData(false, true, 0)]
    [InlineData(false, true, 1)]
    [InlineData(false, true, 2)]
    [InlineData(true, false, 0)]
    [InlineData(true, false, 1)]
    [InlineData(true, false, 2)]
    [InlineData(true, true, 0)]
    [InlineData(true, true, 1)]
    [InlineData(true, true, 2)]
    public async Task When_an_asb_publication_selects_a_wire_writer_should_preserve_native_mapping_and_delivery_controls(
        bool useQueue, bool useAsync, int writerSelection)
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
            WireMessageWriter = writerSelection switch
            {
                0 => null,
                1 => new DefaultAzureServiceBusWireMessageWriter(),
                _ => externalWriter
            }
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

        //Act
        if (useAsync)
            await ((IAmAMessageProducerAsync)producer).SendAsync(source, timeout.Token);
        else
            ((IAmAMessageProducerSync)producer).Send(source);
        var native = Assert.IsType<ServiceBusReceivedMessage>(
            await observer.ReceiveMessageAsync(TimeSpan.FromSeconds(20), timeout.Token));

        try
        {
            //Assert
            var custom = writerSelection == 2;
            Assert.Equal(custom ? "wire-" + sourceId.Value : sourceId.Value, native.MessageId);
            Assert.Equal(custom ? "wire-subject" : "source-subject", native.Subject);
            Assert.Equal(custom ? "wire-correlation" : "source-correlation", native.CorrelationId);
            Assert.Equal(custom ? "application/octet-stream" : "application/json", native.ContentType);
            Assert.Equal(custom ? new byte[] { 0, 255, 128 }.Concat(sourceBody).ToArray() : sourceBody, native.Body.ToArray());
            Assert.Equal(TimeSpan.FromMinutes(5), native.TimeToLive);
            Assert.Equal(custom ? 1 : 0, externalWriter.WriteCount);
            if (custom)
            {
                Assert.Equal(traceParent, Property("external-traceparent"));
                Assert.Equal(traceState, Property("external-tracestate"));
                Assert.Equal("tenant=acme", Property("external-baggage"));
                Assert.DoesNotContain(native.ApplicationProperties.Keys,
                    key => key.StartsWith("cloudevents:", StringComparison.OrdinalIgnoreCase));
                Assert.False(native.ApplicationProperties.ContainsKey("source-only"));
            }
            else
            {
                Assert.Equal(sourceId.Value, Property("cloudEvents:id"));
                Assert.Equal(traceParent, Property("cloudevents:traceparent"));
                Assert.Equal(traceState, Property("cloudevents:tracestate"));
                Assert.Equal("tenant=acme", Property("cloudevents:baggage"));
                Assert.Equal("unchanged", Property("source-only"));
                Assert.False(native.ApplicationProperties.ContainsKey("external-traceparent"));
            }
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
