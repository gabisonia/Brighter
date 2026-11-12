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
public class AsbReaderFailureRecoveryTests
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
    public async Task When_an_asb_reader_fails_should_preserve_native_rejection_and_recover(
        bool useQueue, bool useAsync, bool returnNull)
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
        var deadLetterOptions = new ServiceBusReceiverOptions
        {
            SubQueue = SubQueue.DeadLetter,
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        };
        await using var deadLetters = useQueue
            ? client.CreateReceiver(queue, deadLetterOptions)
            : client.CreateReceiver(topic, observerName, deadLetterOptions);
        await using var observer = useQueue ? client.CreateReceiver(queue) : client.CreateReceiver(topic, observerName);
        var reader = new AsbRecoverableWireReader(destination, returnNull);
        var subscription = new AzureServiceBusSubscription<ASBTestCommand>(
            new SubscriptionName("wire-reader-recovery"), new ChannelName(useQueue ? queue : observerName), destination,
            bufferSize: 1, messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            makeChannels: OnMissingChannel.Assume,
            subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration { UseServiceBusQueue = useQueue })
        {
            WireMessageReader = reader
        };
        var factory = new AzureServiceBusConsumerFactory(provider);
        await using var asyncConsumer = useAsync ? factory.CreateAsync(subscription) : null;
        using var syncConsumer = useAsync ? null : factory.Create(subscription);
        var failedId = Id.Random().Value;
        var failedBody = new byte[] { 0, 255, 128, 65 };
        var native = new ServiceBusMessage(new BinaryData(failedBody))
        {
            MessageId = failedId,
            Subject = "native-failed-subject",
            CorrelationId = "native-failed-correlation",
            ContentType = "application/octet-stream"
        };
        native.ApplicationProperties["reader-failure"] = "deliberate";
        native.ApplicationProperties["source-only"] = "unchanged";

        //Act
        await sender.SendMessageAsync(native, timeout.Token);
        var failed = await Receive();

        //Assert
#pragma warning disable CS0618 // Verify the transport control signal used for mapping failures.
        Assert.Equal(MessageType.MT_UNACCEPTABLE, failed.Header.MessageType);
#pragma warning restore CS0618
        Assert.Equal(failedId, failed.Id.Value);
        Assert.Equal(destination, failed.Header.Topic);
        Assert.Equal(failedBody, failed.Body.Memory.ToArray());
        Assert.Equal(1, reader.ReadCount);
        Assert.Equal(reader.LastDelivery!.LockToken, Assert.IsType<string>(failed.Header.Bag["LockToken"]));
        Assert.Equal(reader.LastDelivery.SequenceNumber, Assert.IsType<long>(failed.Header.Bag["SequenceNumber"]));
        Assert.Equal(failedBody, reader.LastDelivery.Body.ToArray());
        var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "wire mapping failed");
        Assert.True(useAsync
            ? await asyncConsumer!.RejectAsync(failed, reason, timeout.Token)
            : syncConsumer!.Reject(failed, reason));
        var dead = Assert.IsType<ServiceBusReceivedMessage>(
            await deadLetters.ReceiveMessageAsync(TimeSpan.FromSeconds(10), timeout.Token));
        Assert.Equal(failedId, dead.MessageId);
        Assert.Equal(failedBody, dead.Body.ToArray());
        Assert.Equal("native-failed-subject", dead.Subject);
        Assert.Equal("native-failed-correlation", dead.CorrelationId);
        Assert.Equal("application/octet-stream", dead.ContentType);
        Assert.Equal("deliberate", dead.ApplicationProperties["reader-failure"]);
        Assert.Equal("unchanged", dead.ApplicationProperties["source-only"]);
        Assert.Equal("Unacceptable", dead.DeadLetterReason);
        Assert.Equal("wire mapping failed", dead.DeadLetterErrorDescription);

        var healthyId = Id.Random().Value;
        var healthyBody = new byte[] { 128, 0, 255, 66 };
        await sender.SendMessageAsync(new ServiceBusMessage(new BinaryData(healthyBody)) { MessageId = healthyId }, timeout.Token);
        var healthy = await Receive();
        Assert.Equal("mapped-" + healthyId, healthy.Id.Value);
        Assert.Equal("recovered-subject", healthy.Header.Subject);
        Assert.Equal(new byte[] { 42 }.Concat(healthyBody).ToArray(), healthy.Body.Memory.ToArray());
        Assert.Equal(2, reader.ReadCount);
        var sequence = Assert.IsType<long>(healthy.Header.Bag["SequenceNumber"]);
        Assert.Equal(reader.LastDelivery!.SequenceNumber, sequence);
        Assert.Equal(reader.LastDelivery.LockToken, Assert.IsType<string>(healthy.Header.Bag["LockToken"]));
        if (useAsync) await asyncConsumer!.AcknowledgeAsync(healthy, timeout.Token);
        else syncConsumer!.Acknowledge(healthy);
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
    }
}
