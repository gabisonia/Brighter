#region Licence

/* The MIT License (MIT)
Copyright © 2026 gabisonia

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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;
using Paramore.Brighter.MessageScheduler.Azure;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

/// <summary>Verifies selected retry mapping and settlement using public APIs with in-memory SDK I/O.</summary>
public class AzureServiceBusRetryWireSelectionTests
{
    /// <summary>Preserves gateway-owned retry identity/session and the source on mapping failure.</summary>
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
    public async Task When_asb_retries_with_selected_writers_should_preserve_native_controls_and_recover(bool synchronous, bool global, int behavior)
    {
        // Arrange
        using var telemetry = Sdk.CreateTracerProviderBuilder().Build();
        byte[] bytes = [0, 255, 128, 17];
        var delivery = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: new BinaryData(bytes), messageId: "original", sessionId: "source-session", partitionKey: "source-session",
            lockTokenGuid: Guid.NewGuid(), lockedUntil: DateTimeOffset.UtcNow.AddMinutes(1),
            properties: new Dictionary<string, object> { ["MessageType"] = "MT_COMMAND", ["tenant"] = "source-tenant" });
        await using var client = new InMemoryRetryServiceBusClient(delivery);
        var writer = new AsbRetrySelectedWireWriter(behavior);
        var replacement = new AsbRetrySelectedWireWriter(0);
        var publication = new AzureServiceBusPublication
        { WireMessageWriter = writer, RequeueScheduler = global ? RequeueSchedulerMode.Global : RequeueSchedulerMode.Native };
        var consumerFactory = new AzureServiceBusConsumerFactory(client, publication);
        var schedulerFactory = new AzureServiceBusSchedulerFactory(client, new RoutingKey("callbacks"))
        { RequeueWireMessageWriter = writer };
        var processor = new CommandProcessor(new SubscriberRegistry(),
            new SimpleHandlerFactoryAsync(_ => throw new InvalidOperationException("No handler is needed for native scheduling.")),
            new InMemoryRequestContextFactory(), new DefaultPolicy(), new ResiliencePipelineRegistry<string>(), new InMemorySchedulerFactory());
        if (global) consumerFactory.Scheduler = schedulerFactory.Create(processor);
        var channels = new AzureServiceBusChannelFactory(consumerFactory);
        var subscription = new AzureServiceBusSubscription(new SubscriptionName("retry"), new ChannelName("retry-queue"), new RoutingKey("retry-queue"),
            requestType: typeof(Command), messagePumpType: synchronous ? MessagePumpType.Reactor : MessagePumpType.Proactor,
            makeChannels: OnMissingChannel.Assume, timeOut: TimeSpan.FromSeconds(1),
            subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration { UseServiceBusQueue = true });
        using var syncChannel = synchronous ? channels.CreateSyncChannel(subscription) : null;
        await using var asyncChannel = synchronous ? null : await channels.CreateAsyncChannelAsync(subscription);
        var message = synchronous ? syncChannel!.Receive(TimeSpan.FromSeconds(1)) : await asyncChannel!.ReceiveAsync(TimeSpan.FromSeconds(1));
        message.Header.UpdateHandledCount();
        var beforeBody = message.Body.Memory.ToArray();
        var beforeTrace = message.Header.TraceParent;
        var beforeBag = new Dictionary<string, object>(message.Header.Bag);
        publication.WireMessageWriter = replacement;
        schedulerFactory.RequeueWireMessageWriter = replacement;
        var attempts = client.SenderCreationAttempts;
        client.OnSenderCreation = () => Assert.Equal(0, client.Receiver.CompletionAttempts);
        using var activity = new Activity("retry selection").SetIdFormat(ActivityIdFormat.W3C).Start();
        activity.TraceStateString = "vendor=retry";
        var previousBaggage = OpenTelemetry.Baggage.Current;
        OpenTelemetry.Baggage.SetBaggage("request", "retry");

        try
        {
            // Act
            var error = await Record.ExceptionAsync(Retry);

            // Assert
            Assert.Equal(1, writer.Calls);
            Assert.Equal(0, replacement.Calls);
            if (behavior != 0)
            {
                if (behavior == 1) Assert.IsType<FormatException>(error);
                else Assert.IsType<InvalidOperationException>(error);
                Assert.Empty(client.Scheduled);
                Assert.Equal(attempts, client.SenderCreationAttempts);
                Assert.Equal(0, client.Receiver.CompletionAttempts);
                Assert.Equal(beforeBody, message.Body.Memory.ToArray());
                Assert.Equal(beforeTrace, message.Header.TraceParent);
                Assert.Equal(beforeBag, message.Header.Bag);
                writer.Recover = true;
                await Retry();
                Assert.Equal(2, writer.Calls);
            }
            else Assert.Null(error);
            var scheduled = Assert.Single(client.Scheduled);
            Assert.Equal("retry-queue", scheduled.Queue);
            Assert.Equal(new byte[] { 83, 0 }.Concat(bytes), scheduled.Message.Body.ToArray());
            Assert.StartsWith("brighter-retry-", scheduled.Message.MessageId);
            Assert.Equal("original", scheduled.Message.ApplicationProperties[Message.OriginalMessageIdHeaderName]);
            Assert.Equal("original", scheduled.Message.ApplicationProperties["cloudEvents:id"]);
            Assert.Equal("source-session", scheduled.Message.SessionId);
            Assert.Equal("source-session", scheduled.Message.PartitionKey);
            Assert.Equal(message.Header.HandledCount, scheduled.Message.ApplicationProperties["HandledCount"]);
            Assert.Equal("source-tenant", scheduled.Message.ApplicationProperties["custom-tenant"]);
            Assert.Equal(activity.Id, scheduled.Message.ApplicationProperties["custom-parent"]);
            Assert.Equal("vendor=retry", scheduled.Message.ApplicationProperties["custom-state"]);
            Assert.Contains("request=retry", (string)scheduled.Message.ApplicationProperties["custom-baggage"]);
            Assert.Equal(1, client.Receiver.CompletionAttempts);
            Assert.True(client.Senders.Last().Disposed);
            Assert.Equal(beforeBody, message.Body.Memory.ToArray());
            Assert.Equal(beforeTrace, message.Header.TraceParent);
            Assert.Equal(beforeBag, message.Header.Bag);
            Assert.Equal("original", message.Id.Value);
        }
        finally
        {
            OpenTelemetry.Baggage.Current = previousBaggage;
        }

        async Task Retry()
        {
            if (synchronous) Assert.True(syncChannel!.Requeue(message, TimeSpan.FromSeconds(2)));
            else Assert.True(await asyncChannel!.RequeueAsync(message, TimeSpan.FromSeconds(2)));
        }
    }
}
