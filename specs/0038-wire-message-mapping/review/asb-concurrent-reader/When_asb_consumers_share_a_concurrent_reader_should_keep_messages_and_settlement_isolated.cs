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
public class AsbConcurrentReaderTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_asb_consumers_share_a_concurrent_reader_should_keep_messages_and_settlement_isolated(
        bool useAsync)
    {
        //Arrange
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var queue = Environment.GetEnvironmentVariable("BrighterTestsASBWireQueue") ?? "wire-mapping-queue";
        var topic = Environment.GetEnvironmentVariable("BrighterTestsASBWireTopic") ?? "wire-mapping-topic";
        var observerName = Environment.GetEnvironmentVariable("BrighterTestsASBWireSubscription") ?? "wire-observer";
        var provider = ASBCreds.ASBClientProvider;
        await using var client = provider.GetServiceBusClient();
        await using var queueSender = client.CreateSender(queue);
        await using var topicSender = client.CreateSender(topic);
        await using var queueObserver = client.CreateReceiver(queue);
        await using var topicObserver = client.CreateReceiver(topic, observerName);
        using var reader = new AsbConcurrentWireReader();
        var destinations = new[] { new RoutingKey(queue), new RoutingKey(topic) };
        var ids = destinations.Select(_ => Id.Random().Value).ToArray();
        var bodies = new[] { new byte[] { 0, 255, 128, 1 }, new byte[] { 0, 255, 128, 2 } };
        var subscriptions = destinations.Select((destination, i) => new AzureServiceBusSubscription<ASBTestCommand>(
            new SubscriptionName($"concurrent-reader-{i}"), new ChannelName(i == 0 ? queue : observerName), destination,
            bufferSize: 1, messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            makeChannels: OnMissingChannel.Assume,
            subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration { UseServiceBusQueue = i == 0 })
        {
            WireMessageReader = reader
        }).ToArray();
        var factory = new AzureServiceBusConsumerFactory(provider);
        await using var asyncQueue = useAsync ? factory.CreateAsync(subscriptions[0]) : null;
        await using var asyncTopic = useAsync ? factory.CreateAsync(subscriptions[1]) : null;
        using var syncQueue = useAsync ? null : factory.Create(subscriptions[0]);
        using var syncTopic = useAsync ? null : factory.Create(subscriptions[1]);
        for (var i = 0; i < 2; i++)
        {
            var native = new ServiceBusMessage(new BinaryData(bodies[i]))
            {
                MessageId = ids[i], Subject = $"subject-{i}", CorrelationId = $"correlation-{i}"
            };
            native.ApplicationProperties["destination"] = destinations[i].Value;
            native.ApplicationProperties["MessageType"] = "MT_EVENT";
            native.ApplicationProperties["source-only"] = i;
            await (i == 0 ? queueSender : topicSender).SendMessageAsync(native, timeout.Token);
        }

        //Act
        var receives = Enumerable.Range(0, 2).Select(i => Task.Factory.StartNew(
            () => Receive(i), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap()).ToArray();
        var messages = await Task.WhenAll(receives);

        //Assert
        Assert.Equal(2, reader.ReadCount);
        Assert.NotSame(messages[0], messages[1]);
        Assert.NotSame(messages[0].Header.Bag, messages[1].Header.Bag);
        Assert.NotEqual(messages[0].Header.Bag["LockToken"], messages[1].Header.Bag["LockToken"]);
        for (var i = 0; i < 2; i++)
        {
            var message = messages[i];
            var delivery = reader.Delivery(ids[i]);
            Assert.Equal("mapped-" + ids[i], message.Id.Value);
            Assert.Equal(destinations[i], message.Header.Topic);
            Assert.Equal(new byte[] { 42 }.Concat(bodies[i]).ToArray(), message.Body.Memory.ToArray());
            Assert.Equal($"mapped-subject-{i}", message.Header.Subject);
            Assert.Equal(new Id($"mapped-correlation-{i}"), message.Header.CorrelationId);
            Assert.Equal(ids[i], Assert.IsType<string>(message.Header.Bag["reader-only"]));
            Assert.False(message.Header.Bag.ContainsKey("source-only"));
            Assert.Equal(delivery.LockToken, Assert.IsType<string>(message.Header.Bag["LockToken"]));
            Assert.Equal(delivery.SequenceNumber, Assert.IsType<long>(message.Header.Bag["SequenceNumber"]));
            Assert.NotEqual(Guid.Empty.ToString(), delivery.LockToken);
            Assert.True(delivery.SequenceNumber > 0);
            Assert.Equal(bodies[i], delivery.Body.ToArray());
            var observer = i == 0 ? queueObserver : topicObserver;
            var retained = Assert.IsType<ServiceBusReceivedMessage>(
                await observer.PeekMessageAsync(delivery.SequenceNumber, timeout.Token));
            Assert.Equal(ids[i], retained.MessageId);
        }

        //Act
        for (var i = 0; i < 2; i++)
        {
            if (useAsync)
                await (i == 0 ? asyncQueue! : asyncTopic!).AcknowledgeAsync(messages[i], timeout.Token);
            else
                (i == 0 ? syncQueue! : syncTopic!).Acknowledge(messages[i]);
        }

        //Assert
        for (var i = 0; i < 2; i++)
        {
            var observer = i == 0 ? queueObserver : topicObserver;
            var sequence = reader.Delivery(ids[i]).SequenceNumber;
            ServiceBusReceivedMessage? remaining = null;
            for (var attempt = 0; attempt < 20; attempt++)
            {
                remaining = await observer.PeekMessageAsync(sequence, timeout.Token);
                if (remaining is null) break;
                await Task.Delay(100, timeout.Token);
            }
            Assert.Null(remaining);
        }

        async Task<Message> Receive(int endpoint) => Assert.Single(useAsync
            ? await (endpoint == 0 ? asyncQueue! : asyncTopic!).ReceiveAsync(TimeSpan.FromSeconds(30), timeout.Token)
            : (endpoint == 0 ? syncQueue! : syncTopic!).Receive(TimeSpan.FromSeconds(30)));
    }
}
