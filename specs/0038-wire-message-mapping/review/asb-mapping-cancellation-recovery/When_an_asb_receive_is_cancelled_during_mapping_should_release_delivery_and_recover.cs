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
public class AsbMappingCancellationRecoveryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_an_asb_receive_is_cancelled_during_mapping_should_release_delivery_and_recover(bool useQueue)
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
            bufferSize: 1, messagePumpType: MessagePumpType.Proactor,
            makeChannels: OnMissingChannel.Assume,
            subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration { UseServiceBusQueue = useQueue });
        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var externalReader = new AsbCancellingWireReader(destination, cancelled);
        subscription.WireMessageReader = externalReader;
        var factory = new AzureServiceBusConsumerFactory(provider);
        await using var consumer = factory.CreateAsync(subscription);

        //Act
        await sender.SendMessageAsync(native, timeout.Token);
        ServiceBusReceivedMessage? visible = null;
        for (var attempt = 0; attempt < 20; attempt++)
        {
            visible = await observer.PeekMessageAsync(0, timeout.Token);
            if (visible is not null) break;
            await Task.Delay(100, timeout.Token);
        }
        var retainedBefore = Assert.IsType<ServiceBusReceivedMessage>(visible);
        Assert.Equal(sourceId, retainedBefore.MessageId);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            consumer.ReceiveAsync(TimeSpan.FromSeconds(10), cancelled.Token));

        //Assert
        Assert.False(timeout.IsCancellationRequested);
        Assert.True(cancelled.IsCancellationRequested);
        Assert.Equal(1, externalReader.ReadCount);
        var firstDelivery = Assert.IsType<ServiceBusReceivedMessage>(externalReader.FirstDelivery);
        Assert.Equal(sourceId, firstDelivery.MessageId);
        Assert.Equal(1, firstDelivery.DeliveryCount);
        var retainedAfter = Assert.IsType<ServiceBusReceivedMessage>(
            await observer.PeekMessageAsync(retainedBefore.SequenceNumber, timeout.Token));
        Assert.Equal(sourceId, retainedAfter.MessageId);
        Assert.Equal(sourceBody, retainedAfter.Body.ToArray());

        //Act
        var recovered = Assert.Single(await consumer.ReceiveAsync(TimeSpan.FromSeconds(20), timeout.Token));

        //Assert
        Assert.Equal(2, externalReader.ReadCount);
        Assert.True(DateTimeOffset.UtcNow < firstDelivery.LockedUntil, "Recovery must occur before the original lock expires.");
        Assert.NotEqual(firstDelivery.LockToken, externalReader.LastDelivery!.LockToken);
        Assert.Equal("mapped-" + sourceId, recovered.Id.Value);
        Assert.Equal(destination, recovered.Header.Topic);
        Assert.Equal("mapped-subject", recovered.Header.Subject);
        Assert.Equal(new Id("mapped-correlation"), recovered.Header.CorrelationId);
        Assert.Equal(new byte[] { 42 }.Concat(sourceBody).ToArray(), recovered.Body.Memory.ToArray());
        Assert.Equal("mapped", Assert.IsType<string>(recovered.Header.Bag["reader-only"]));
        Assert.False(recovered.Header.Bag.ContainsKey("source-only"));
        Assert.Equal(2, externalReader.LastDelivery!.DeliveryCount);
        Assert.Equal(sourceId, externalReader.LastDelivery.MessageId);
        Assert.Equal(sourceBody, externalReader.LastDelivery.Body.ToArray());
        Assert.Equal(externalReader.LastDelivery.LockToken, Assert.IsType<string>(recovered.Header.Bag["LockToken"]));
        var sequence = Assert.IsType<long>(recovered.Header.Bag["SequenceNumber"]);
        Assert.Equal(retainedBefore.SequenceNumber, sequence);
        Assert.Equal(externalReader.LastDelivery.SequenceNumber, sequence);
        Assert.NotEqual(Guid.Empty.ToString(), externalReader.LastDelivery.LockToken);
        await consumer.AcknowledgeAsync(recovered, timeout.Token);
        ServiceBusReceivedMessage? remaining = null;
        for (var attempt = 0; attempt < 20; attempt++)
        {
            remaining = await observer.PeekMessageAsync(sequence, timeout.Token);
            if (remaining is null) break;
            await Task.Delay(100, timeout.Token);
        }
        Assert.Null(remaining);

    }
}
