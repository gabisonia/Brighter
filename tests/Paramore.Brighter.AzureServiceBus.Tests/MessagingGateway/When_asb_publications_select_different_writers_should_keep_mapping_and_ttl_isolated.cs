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
public class AsbPublicationWriterIsolationTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 0)]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    public async Task When_asb_publications_select_different_writers_should_keep_mapping_and_ttl_isolated(
        bool customQueue, int sendMode)
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
        var writer = new AsbExternalWireWriter();
        var destinations = new[] { new RoutingKey(queue), new RoutingKey(topic) };
        var ttls = new[] { TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(7) };
        var publications = destinations.Select((destination, i) => new AzureServiceBusPublication
        {
            Topic = destination,
            UseServiceBusQueue = i == 0,
            MakeChannels = OnMissingChannel.Assume,
            TimeToLive = ttls[i],
            WireMessageWriter = (i == 0) == customQueue ? writer : null
        }).ToArray();
        using var registry = new AzureServiceBusProducerRegistryFactory(provider, publications).Create();
        var customCount = 0;
        for (var step = 0; step < 4; step++)
        {
            var endpoint = step % 2;
            var destination = destinations[endpoint];
            var custom = (endpoint == 0) == customQueue;
            var observer = endpoint == 0 ? queueObserver : topicObserver;
            var producer = registry.LookupBy(destination);
            var id = Id.Random();
            var bytes = Encoding.UTF8.GetBytes($"source-{step}-café\0");
            var source = new Message(new MessageHeader(id, destination, MessageType.MT_EVENT,
                contentType: new ContentType("application/json"))
            {
                Subject = $"subject-{step}",
                CorrelationId = new Id($"correlation-{step}"),
                TraceParent = new TraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
                TraceState = new TraceState("vendor=ready")
            }, new MessageBody(bytes));
            source.Header.Bag["source-only"] = step;

            //Act
            if (sendMode == 0)
                ((IAmAMessageProducerSync)producer).Send(source);
            else if (sendMode == 1)
                await ((IAmAMessageProducerAsync)producer).SendAsync(source, timeout.Token);
            else
            {
                var bulk = Assert.IsAssignableFrom<IAmABulkMessageProducerAsync>(producer);
                var batch = Assert.Single(await bulk.CreateBatchesAsync([source], timeout.Token));
                Assert.Equal(id, Assert.Single(batch.Ids()));
                await bulk.SendAsync(batch, timeout.Token);
            }
            var native = Assert.IsType<ServiceBusReceivedMessage>(
                await observer.ReceiveMessageAsync(TimeSpan.FromSeconds(20), timeout.Token));
            try
            {
                //Assert
                if (custom)
                    customCount++;
                Assert.Equal(customCount, writer.WriteCount);
                Assert.Equal(custom ? "wire-" + id.Value : id.Value, native.MessageId);
                Assert.Equal(custom ? "wire-subject" : $"subject-{step}", native.Subject);
                Assert.Equal(custom ? "wire-correlation" : $"correlation-{step}", native.CorrelationId);
                Assert.Equal(custom ? "application/octet-stream" : "application/json", native.ContentType);
                Assert.Equal(custom ? new byte[] { 0, 255, 128 }.Concat(bytes).ToArray() : bytes, native.Body.ToArray());
                Assert.Equal(ttls[endpoint], native.TimeToLive);
                if (custom)
                {
                    Assert.Equal("vendor=ready", Assert.IsType<string>(native.ApplicationProperties["external-tracestate"]));
                    Assert.False(native.ApplicationProperties.ContainsKey("source-only"));
                    Assert.DoesNotContain(native.ApplicationProperties.Keys,
                        key => key.StartsWith("cloudevents:", StringComparison.OrdinalIgnoreCase));
                }
                else
                {
                    Assert.Equal(id.Value, Assert.IsType<string>(native.ApplicationProperties["cloudEvents:id"]));
                    Assert.Equal(step, Assert.IsType<int>(native.ApplicationProperties["source-only"]));
                    Assert.False(native.ApplicationProperties.ContainsKey("external-tracestate"));
                }
                Assert.Equal(id, source.Id);
                Assert.Equal(destination, source.Header.Topic);
                Assert.Equal(bytes, source.Body.Memory.ToArray());
                Assert.Equal($"subject-{step}", source.Header.Subject);
                Assert.Equal(new Id($"correlation-{step}"), source.Header.CorrelationId);
                Assert.Equal(new ContentType("application/json"), source.Header.ContentType);
                Assert.Equal(step, Assert.IsType<int>(Assert.Single(source.Header.Bag).Value));
            }
            finally
            {
                await observer.CompleteMessageAsync(native, timeout.Token);
            }
        }
        Assert.Equal(2, writer.WriteCount);
    }
}
