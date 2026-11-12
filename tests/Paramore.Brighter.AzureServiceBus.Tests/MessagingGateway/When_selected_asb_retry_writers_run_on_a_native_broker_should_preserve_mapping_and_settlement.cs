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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;
using Paramore.Brighter.MessageScheduler.Azure;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

/// <summary>Verifies selected retry writers and settlement against a native Service Bus broker.</summary>
[Trait("Category", "AzureServiceBus")]
[Collection("AzureServiceBus")]
public class AsbHostedRetryWriterAcceptanceTests
{
    /// <summary>Preserves mapping, delayed delivery and settlement through writer failure and recovery.</summary>
    /// <param name="synchronous">Whether to exercise the synchronous channel.</param>
    /// <param name="global">Whether to use the global scheduler rather than native scheduling.</param>
    /// <param name="session">Whether the queue requires sessions.</param>
    /// <param name="behavior">Writer behavior: zero succeeds, one throws, and two returns null.</param>
    /// <returns>The asynchronous acceptance check.</returns>
    [Theory]
    [InlineData(false, false, false, 0)]
    [InlineData(false, false, false, 1)]
    [InlineData(false, false, false, 2)]
    [InlineData(false, false, true, 0)]
    [InlineData(false, false, true, 1)]
    [InlineData(false, false, true, 2)]
    [InlineData(false, true, false, 0)]
    [InlineData(false, true, false, 1)]
    [InlineData(false, true, false, 2)]
    [InlineData(false, true, true, 0)]
    [InlineData(false, true, true, 1)]
    [InlineData(false, true, true, 2)]
    [InlineData(true, false, false, 0)]
    [InlineData(true, false, false, 1)]
    [InlineData(true, false, false, 2)]
    [InlineData(true, false, true, 0)]
    [InlineData(true, false, true, 1)]
    [InlineData(true, false, true, 2)]
    [InlineData(true, true, false, 0)]
    [InlineData(true, true, false, 1)]
    [InlineData(true, true, false, 2)]
    [InlineData(true, true, true, 0)]
    [InlineData(true, true, true, 1)]
    [InlineData(true, true, true, 2)]
    public async Task When_selected_asb_retry_writers_run_on_a_native_broker_should_preserve_mapping_and_settlement(
        bool synchronous, bool global, bool session, int behavior)
    {
        //Arrange
        using var telemetry = Sdk.CreateTracerProviderBuilder().Build();
        var provider = ASBCreds.ASBClientProvider;
        await using var client = provider.GetServiceBusClient();
        var administration = provider.GetServiceBusAdministrationClient();
        var queue = "selected-retry-" + Guid.NewGuid().ToString("N");
        var callbacks = "callbacks-" + Guid.NewGuid().ToString("N");
        await administration.CreateQueueAsync(new CreateQueueOptions(queue)
        {
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(5),
            RequiresSession = session
        });
        try
        {
            await administration.CreateQueueAsync(callbacks);
            byte[] bytes = [0, 255, 128, 17];
            var original = new ServiceBusMessage(new BinaryData(bytes))
            {
                MessageId = Guid.NewGuid().ToString(),
                SessionId = session ? "source-session" : null
            };
            original.ApplicationProperties["MessageType"] = "MT_COMMAND";
            original.ApplicationProperties["tenant"] = "source-tenant";
            await using var sender = client.CreateSender(queue);
            await sender.SendMessageAsync(original);
            var writer = new AsbRetrySelectedWireWriter(behavior);
            var publication = new AzureServiceBusPublication
            {
                WireMessageWriter = writer,
                RequeueScheduler = global ? RequeueSchedulerMode.Global : RequeueSchedulerMode.Native
            };
            var consumerFactory = new AzureServiceBusConsumerFactory(provider, publication);
            var scheduler = new AzureServiceBusSchedulerFactory(provider, new RoutingKey(callbacks))
            { RequeueWireMessageWriter = writer };
            var processor = new CommandProcessor(new SubscriberRegistry(),
                new SimpleHandlerFactoryAsync(_ => throw new InvalidOperationException("No request handler is needed.")),
                new InMemoryRequestContextFactory(), new DefaultPolicy(), new ResiliencePipelineRegistry<string>(), new InMemorySchedulerFactory());
            if (global) consumerFactory.Scheduler = scheduler.Create(processor);
            var factory = new AzureServiceBusChannelFactory(consumerFactory);
            var subscription = new AzureServiceBusSubscription<ASBTestCommand>(
                channelName: new ChannelName(queue), routingKey: new RoutingKey(queue),
                messagePumpType: synchronous ? MessagePumpType.Reactor : MessagePumpType.Proactor,
                makeChannels: OnMissingChannel.Assume,
                subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration { UseServiceBusQueue = true, RequireSession = session });
            using var sync = synchronous ? factory.CreateSyncChannel(subscription) : null;
            await using var asyncChannel = synchronous ? null : await factory.CreateAsyncChannelAsync(subscription);
            await using var observer = client.CreateReceiver(queue);
            var message = await Receive();
            Assert.Equal(original.MessageId, message.Id.Value);
            message.Header.UpdateHandledCount();
            var before = message.Body.Memory.ToArray();
            using var activity = new Activity("hosted selected retry").SetIdFormat(ActivityIdFormat.W3C).Start();
            activity.TraceStateString = "vendor=hosted";
            var delay = TimeSpan.FromSeconds(2);
            var elapsed = Stopwatch.StartNew();

            //Act
            var error = await Record.ExceptionAsync(Retry);

            //Assert
            Assert.Equal(1, writer.Calls);
            if (behavior != 0)
            {
                if (behavior == 1) Assert.IsType<FormatException>(error);
                else Assert.IsType<InvalidOperationException>(error);
                var retained = await observer.PeekMessagesAsync(10);
                Assert.Equal(original.MessageId, Assert.Single(retained).MessageId);
                Assert.Equal(before, message.Body.Memory.ToArray());
                writer.Recover = true;
                elapsed.Restart();
                await Retry();
                Assert.Equal(2, writer.Calls);
            }
            else Assert.Null(error);
            var redelivery = await Receive();
            Assert.True(elapsed.Elapsed >= delay - TimeSpan.FromMilliseconds(500), "The retry arrived before its delay.");
            Assert.StartsWith("brighter-retry-", redelivery.Id.Value);
            Assert.NotEqual(original.MessageId, redelivery.Id.Value);
            var wire = Assert.Single(await observer.PeekMessagesAsync(10));
            Assert.Equal(redelivery.Id.Value, wire.MessageId);
            Assert.Equal(new byte[] { 83, 0 }.Concat(bytes).ToArray(), wire.Body.ToArray());
            Assert.Equal(original.MessageId, wire.ApplicationProperties[Message.OriginalMessageIdHeaderName]);
            Assert.Equal(original.MessageId, wire.ApplicationProperties["cloudEvents:id"]);
            Assert.Equal(session ? "source-session" : null, wire.SessionId);
            Assert.Equal(1, wire.ApplicationProperties["HandledCount"]);
            Assert.Equal("source-tenant", wire.ApplicationProperties["custom-tenant"]);
            Assert.Equal(activity.Id, wire.ApplicationProperties["custom-parent"]);
            Assert.Equal("vendor=hosted", wire.ApplicationProperties["custom-state"]);
            Assert.Equal(before, message.Body.Memory.ToArray());
            Assert.Equal(original.MessageId, message.Id.Value);
            if (synchronous) sync!.Acknowledge(redelivery);
            else await asyncChannel!.AcknowledgeAsync(redelivery);
            ServiceBusReceivedMessage? remaining = null;
            for (var attempt = 0; attempt < 20; attempt++)
            {
                remaining = await observer.PeekMessageAsync();
                if (remaining is null) break;
                await Task.Delay(100);
            }
            Assert.Null(remaining);

            Task<Message> Receive() => synchronous ? Task.FromResult(sync!.Receive(TimeSpan.FromSeconds(15))) : asyncChannel!.ReceiveAsync(TimeSpan.FromSeconds(15));
            async Task Retry()
            {
                if (synchronous) Assert.True(sync!.Requeue(message, delay));
                else Assert.True(await asyncChannel!.RequeueAsync(message, delay));
            }
        }
        finally
        {
            await administration.DeleteQueueAsync(queue);
            if ((await administration.QueueExistsAsync(callbacks)).Value) await administration.DeleteQueueAsync(callbacks);
        }
    }
}
