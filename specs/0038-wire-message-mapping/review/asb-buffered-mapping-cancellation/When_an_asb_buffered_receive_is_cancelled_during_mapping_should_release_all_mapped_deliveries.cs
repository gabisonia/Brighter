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
using System.Collections.Generic;
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
public class AsbBufferedMappingCancellationRecoveryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_an_asb_buffered_receive_is_cancelled_during_mapping_should_release_all_mapped_deliveries(bool useQueue)
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
        var sourceIds = new[] { Id.Random().Value, Id.Random().Value };
        var bodies = new[] { new byte[] { 0, 255, 128, 65 }, new byte[] { 1, 254, 129, 66 } };
        var sources = sourceIds.Select((id, index) => new ServiceBusMessage(new BinaryData(bodies[index]))
        {
            MessageId = id,
            Subject = "native-subject",
            CorrelationId = "native-correlation",
            ContentType = "application/octet-stream"
        }).ToArray();
        var subscription = new AzureServiceBusSubscription<ASBTestCommand>(
            new SubscriptionName("wire-reader"), new ChannelName(useQueue ? queue : observerName), destination,
            bufferSize: 2, messagePumpType: MessagePumpType.Proactor,
            makeChannels: OnMissingChannel.Assume,
            subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration { UseServiceBusQueue = useQueue });
        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var reader = new AsbBufferedCancellingWireReader(destination, cancelled);
        subscription.WireMessageReader = reader;
        await using var consumer = new AzureServiceBusConsumerFactory(provider).CreateAsync(subscription);
        await sender.SendMessagesAsync(sources, timeout.Token);
        IReadOnlyList<ServiceBusReceivedMessage> visible = Array.Empty<ServiceBusReceivedMessage>();
        for (var attempt = 0; attempt < 20; attempt++)
        {
            visible = await observer.PeekMessagesAsync(2, 0, timeout.Token);
            if (visible.Count == 2) break;
            await Task.Delay(100, timeout.Token);
        }
        Assert.Equal(sourceIds.OrderBy(id => id), visible.Select(message => message.MessageId).OrderBy(id => id));

        //Act
        var failure = await Record.ExceptionAsync(() => consumer.ReceiveAsync(TimeSpan.FromSeconds(10), cancelled.Token));

        //Assert
        Assert.Equal(2, reader.Deliveries.Count);
        Assert.True(cancelled.IsCancellationRequested);
        Assert.False(timeout.IsCancellationRequested);
        Assert.IsAssignableFrom<OperationCanceledException>(failure);
        var original = reader.Deliveries.ToDictionary(message => message.MessageId);
        Assert.Equal(sourceIds.OrderBy(id => id), original.Keys.OrderBy(id => id));
        Assert.All(original.Values, message => Assert.Equal(1, message.DeliveryCount));
        var deadline = DateTimeOffset.UtcNow.AddSeconds(20);
        var recoveredIds = new HashSet<string>();

        //Act
        while (recoveredIds.Count < 2 && DateTimeOffset.UtcNow < deadline)
        {
            var recovered = await consumer.ReceiveAsync(TimeSpan.FromSeconds(2), timeout.Token);
            foreach (var message in recovered)
            {
                //Assert
                var id = sourceIds.Single(source => "mapped-" + source == message.Id.Value);
                Assert.True(recoveredIds.Add(id), "Each withheld delivery must recover exactly once.");
                var first = original[id];
                var native = reader.Deliveries.Last(delivery => delivery.MessageId == id);
                Assert.True(DateTimeOffset.UtcNow < first.LockedUntil, "Recovery must precede original lock expiry.");
                Assert.NotEqual(first.LockToken, native.LockToken);
                Assert.Equal(2, native.DeliveryCount);
                Assert.Equal(first.SequenceNumber, native.SequenceNumber);
                Assert.Equal(bodies[Array.IndexOf(sourceIds, id)], native.Body.ToArray());
                Assert.Equal(destination, message.Header.Topic);
                Assert.Equal("mapped-subject", message.Header.Subject);
                Assert.Equal(new Id("mapped-correlation"), message.Header.CorrelationId);
                Assert.Equal(new byte[] { 42 }.Concat(native.Body.ToArray()).ToArray(), message.Body.Memory.ToArray());
                Assert.Equal("mapped", Assert.IsType<string>(message.Header.Bag["reader-only"]));
                Assert.Equal(native.LockToken, Assert.IsType<string>(message.Header.Bag["LockToken"]));
                Assert.Equal(native.SequenceNumber, Assert.IsType<long>(message.Header.Bag["SequenceNumber"]));
                await consumer.AcknowledgeAsync(message, timeout.Token);
            }
        }
        Assert.Equal(sourceIds.OrderBy(id => id), recoveredIds.OrderBy(id => id));
        Assert.Equal(4, reader.Deliveries.Count);
        ServiceBusReceivedMessage? remaining = null;
        for (var attempt = 0; attempt < 20; attempt++)
        {
            remaining = await observer.PeekMessageAsync(visible.Min(message => message.SequenceNumber), timeout.Token);
            if (remaining is null) break;
            await Task.Delay(100, timeout.Token);
        }
        Assert.Null(remaining);
    }
}
