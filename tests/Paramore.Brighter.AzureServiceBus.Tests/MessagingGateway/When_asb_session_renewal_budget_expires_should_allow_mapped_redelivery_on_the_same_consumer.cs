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
public class AsbSessionRenewalBudgetRecoveryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_asb_session_renewal_budget_expires_should_allow_mapped_redelivery_on_the_same_consumer(bool useQueue)
    {
        //Arrange
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var queue = Environment.GetEnvironmentVariable("BrighterTestsASBBudgetRecoveryQueue") ?? "wire-budget-session-queue";
        var topic = Environment.GetEnvironmentVariable("BrighterTestsASBBudgetRecoveryTopic") ?? "wire-budget-session-topic";
        var observerName = Environment.GetEnvironmentVariable("BrighterTestsASBBudgetRecoverySubscription") ?? "budget-session-observer";
        var destination = new RoutingKey(useQueue ? queue : topic);
        var connectionString = Environment.GetEnvironmentVariable("BrighterTestsASBConnectionString")
            ?? throw new InvalidOperationException("The native session fixture requires a connection string.");
        var observingClient = new AsbObservingSessionClient(connectionString);
        var provider = new AsbObservingSessionClientProvider(connectionString, observingClient);
        await using var client = provider.GetServiceBusClient();
        await using var sender = client.CreateSender(destination.Value);
        await using var observer = useQueue ? client.CreateReceiver(queue) : client.CreateReceiver(topic, observerName);
        var sourceId = Id.Random().Value;
        var sessionId = Id.Random().Value;
        var sourceBody = new byte[] { 0, 255, 128, 65 };
        var native = new ServiceBusMessage(new BinaryData(sourceBody))
        {
            MessageId = sourceId,
            SessionId = sessionId,
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
            subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration
            {
                UseServiceBusQueue = useQueue,
                RequireSession = true,
                MaxAutoLockRenewalDuration = TimeSpan.FromSeconds(6)
            });
        var externalReader = new AsbExternalWireReader(destination);
        subscription.WireMessageReader = externalReader;
        var factory = new AzureServiceBusConsumerFactory(provider);
        await using var consumer = factory.CreateAsync(subscription);
        Assert.Equal(0, externalReader.ReadCount);

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
        //Act
        var delivery = Assert.Single(await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token));
        var nativeSession = Assert.IsAssignableFrom<ServiceBusSessionReceiver>(observingClient.LastSessionReceiver);
        var originalExpiry = observingClient.InitialSessionLockedUntil;
        Assert.Equal(sessionId, nativeSession.SessionId);
        Assert.True(originalExpiry > DateTimeOffset.UtcNow, "The accepted native session must initially be locked.");

        //Assert
        Assert.Equal(1, externalReader.ReadCount);
        Assert.Equal("mapped-" + sourceId, delivery.Id.Value);
        Assert.Equal(destination, delivery.Header.Topic);
        Assert.Equal("mapped-subject", delivery.Header.Subject);
        Assert.Equal(new Id("mapped-correlation"), delivery.Header.CorrelationId);
        Assert.Equal(new byte[] { 42 }.Concat(sourceBody).ToArray(), delivery.Body.Memory.ToArray());
        Assert.Equal("mapped", Assert.IsType<string>(delivery.Header.Bag["reader-only"]));
        Assert.False(delivery.Header.Bag.ContainsKey("source-only"));
        Assert.Equal(1, externalReader.LastDelivery!.DeliveryCount);
        Assert.Equal(sourceId, externalReader.LastDelivery.MessageId);
        Assert.Equal(sessionId, externalReader.LastDelivery.SessionId);
        Assert.Equal(sourceBody, externalReader.LastDelivery.Body.ToArray());
        Assert.Equal(externalReader.LastDelivery.LockToken, Assert.IsType<string>(delivery.Header.Bag["LockToken"]));
        var sequence = Assert.IsType<long>(delivery.Header.Bag["SequenceNumber"]);
        Assert.Equal(retainedBefore.SequenceNumber, sequence);
        Assert.Equal(externalReader.LastDelivery.SequenceNumber, sequence);
        Assert.NotEqual(Guid.Empty.ToString(), externalReader.LastDelivery.LockToken);

        //Act
        await Task.Delay(TimeSpan.FromSeconds(13), timeout.Token);

        //Assert
        Assert.False(timeout.IsCancellationRequested);
        Assert.True(nativeSession.SessionLockedUntil > originalExpiry,
            "The native session must renew before the configured budget is exhausted.");
        Assert.True(nativeSession.SessionLockedUntil < DateTimeOffset.UtcNow,
            "Renewal must stop at the configured budget and allow the native lock to expire.");

        //Act
        Message[]? recoveredMessages = null;
        var recoveryFailure = await Record.ExceptionAsync(async () =>
        {
            recoveredMessages = await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token);
        });
        if (recoveryFailure is not null)
        {
            Assert.IsType<ChannelFailureException>(recoveryFailure);
            recoveredMessages = await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token);
        }
        var recovered = Assert.Single(Assert.IsType<Message[]>(recoveredMessages));

        //Assert
        Assert.NotSame(nativeSession, observingClient.LastSessionReceiver);
        Assert.Equal(sessionId, observingClient.LastSessionReceiver!.SessionId);
        Assert.Equal(2, externalReader.ReadCount);
        Assert.Equal("mapped-" + sourceId, recovered.Id.Value);
        Assert.Equal(destination, recovered.Header.Topic);
        Assert.Equal("mapped-subject", recovered.Header.Subject);
        Assert.Equal(new Id("mapped-correlation"), recovered.Header.CorrelationId);
        Assert.Equal(new byte[] { 42 }.Concat(sourceBody).ToArray(), recovered.Body.Memory.ToArray());
        Assert.Equal("mapped", Assert.IsType<string>(recovered.Header.Bag["reader-only"]));
        Assert.False(recovered.Header.Bag.ContainsKey("source-only"));
        Assert.Equal(sourceId, externalReader.LastDelivery!.MessageId);
        Assert.Equal(sessionId, externalReader.LastDelivery.SessionId);
        Assert.Equal(sourceBody, externalReader.LastDelivery.Body.ToArray());
        Assert.Equal(2, externalReader.LastDelivery.DeliveryCount);
        Assert.Equal(externalReader.LastDelivery.LockToken, Assert.IsType<string>(recovered.Header.Bag["LockToken"]));
        Assert.NotEqual(delivery.Header.Bag["LockToken"], recovered.Header.Bag["LockToken"]);
        Assert.Equal(sequence, Assert.IsType<long>(recovered.Header.Bag["SequenceNumber"]));
        Assert.Equal(sequence, externalReader.LastDelivery.SequenceNumber);

        //Act
        var settlementFailure = await Record.ExceptionAsync(() => consumer.AcknowledgeAsync(recovered, timeout.Token));

        //Assert
        Assert.Null(settlementFailure);
        Assert.False(timeout.IsCancellationRequested);
        Assert.Equal(2, externalReader.ReadCount);
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
