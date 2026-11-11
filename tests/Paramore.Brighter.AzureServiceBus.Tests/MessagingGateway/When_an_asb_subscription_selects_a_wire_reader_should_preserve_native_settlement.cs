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
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Xunit;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

[Trait("Category", "ASB")]
[Collection("ASB wire mapping")]
public class AsbNativeWireReaderTests
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
    public async Task When_an_asb_subscription_selects_a_wire_reader_should_preserve_native_settlement(
        bool useQueue, bool useAsync, int readerSelection)
    {
        //Arrange
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var queue = Environment.GetEnvironmentVariable("BrighterTestsASBWireQueue") ?? "wire-mapping-queue";
        var topic = Environment.GetEnvironmentVariable("BrighterTestsASBWireTopic") ?? "wire-mapping-topic";
        var observerName = Environment.GetEnvironmentVariable("BrighterTestsASBWireSubscription") ?? "wire-observer";
        var destination = new RoutingKey(useQueue ? queue : topic);
        var provider = ASBCreds.ASBClientProvider;
        await using var client = provider.GetServiceBusClient();
        await using var sender = client.CreateSender(destination.Value);
        await using var observer = useQueue ? client.CreateReceiver(queue) : client.CreateReceiver(topic, observerName);
        var sourceId = Id.Random().Value;
        var sourceBody = new byte[] { 0, 255, 128, 65 };
        var native = new ServiceBusMessage(new BinaryData(sourceBody))
        {
            MessageId = sourceId,
            Subject = "native-subject",
            CorrelationId = "native-correlation",
            ContentType = "application/octet-stream"
        };
        native.ApplicationProperties["MessageType"] = "MT_EVENT";
        native.ApplicationProperties["source-only"] = "unchanged";
        var subscription = new AzureServiceBusSubscription<ASBTestCommand>(
            new SubscriptionName("wire-reader"), new ChannelName(useQueue ? queue : observerName), destination,
            bufferSize: 1, messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            makeChannels: OnMissingChannel.Assume,
            subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration { UseServiceBusQueue = useQueue });
        var externalReader = new AsbExternalWireReader(destination);
        subscription.WireMessageReader = readerSelection switch
        {
            0 => null,
            1 => new DefaultAzureServiceBusWireMessageReader(subscription),
            _ => externalReader
        };
        var factory = new AzureServiceBusConsumerFactory(provider);
        await using var asyncConsumer = useAsync ? factory.CreateAsync(subscription) : null;
        using var syncConsumer = useAsync ? null : factory.Create(subscription);

        //Act
        await sender.SendMessageAsync(native, timeout.Token);
        var first = await Receive();
        AssertMapping(first, 1);
        var firstToken = Assert.IsType<string>(first.Header.Bag["LockToken"]);
        var sequence = Assert.IsType<long>(first.Header.Bag["SequenceNumber"]);
        Assert.NotEqual(Guid.Empty.ToString(), firstToken);
        Assert.True(Guid.TryParse(firstToken, out _));
        Assert.True(sequence > 0);
        if (useAsync) await asyncConsumer!.NackAsync(first, timeout.Token);
        else syncConsumer!.Nack(first);
        var second = await Receive();

        //Assert
        AssertMapping(second, 2);
        Assert.NotSame(first, second);
        Assert.Equal(sequence, Assert.IsType<long>(second.Header.Bag["SequenceNumber"]));
        Assert.NotEqual(firstToken, Assert.IsType<string>(second.Header.Bag["LockToken"]));
        if (readerSelection == 2)
        {
            Assert.Equal(2, externalReader.LastDelivery!.DeliveryCount);
            Assert.Equal(sourceId, externalReader.LastDelivery.MessageId);
            Assert.Equal(sourceBody, externalReader.LastDelivery.Body.ToArray());
            Assert.Equal("native-subject", externalReader.LastDelivery.Subject);
            Assert.Equal("native-correlation", externalReader.LastDelivery.CorrelationId);
            Assert.Equal("application/octet-stream", externalReader.LastDelivery.ContentType);
            Assert.Equal("unchanged", externalReader.LastDelivery.ApplicationProperties["source-only"]);
        }
        if (useAsync) await asyncConsumer!.AcknowledgeAsync(second, timeout.Token);
        else syncConsumer!.Acknowledge(second);
        ServiceBusReceivedMessage? remaining = null;
        for (var attempt = 0; attempt < 20; attempt++)
        {
            remaining = await observer.PeekMessageAsync(sequence, timeout.Token);
            if (remaining is null) break;
            await Task.Delay(100, timeout.Token);
        }
        Assert.Null(remaining);

        async Task<Message> Receive() => Assert.Single(useAsync
            ? await asyncConsumer!.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
            : syncConsumer!.Receive(TimeSpan.FromSeconds(10)));

        void AssertMapping(Message message, int expectedCalls)
        {
            var custom = readerSelection == 2;
            Assert.Equal(custom ? "mapped-" + sourceId : sourceId, message.Id.Value);
            Assert.Equal(custom ? "mapped-subject" : "native-subject", message.Header.Subject);
            Assert.Equal(new Id(custom ? "mapped-correlation" : "native-correlation"), message.Header.CorrelationId);
            Assert.Equal(custom ? new byte[] { 42 }.Concat(sourceBody).ToArray() : sourceBody, message.Body.Memory.ToArray());
            Assert.Equal(destination, message.Header.Topic);
            Assert.Equal(MessageType.MT_EVENT, message.Header.MessageType);
            Assert.Equal(custom ? expectedCalls : 0, externalReader.ReadCount);
            Assert.Equal(!custom, message.Header.Bag.ContainsKey("source-only"));
            Assert.Equal(custom, message.Header.Bag.ContainsKey("reader-only"));
            if (custom)
            {
                Assert.Equal(externalReader.LastDelivery!.LockToken, message.Header.Bag["LockToken"]);
                Assert.Equal(externalReader.LastDelivery.SequenceNumber, message.Header.Bag["SequenceNumber"]);
            }
        }
    }
}
