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

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AzureServiceBus;
using Xunit;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

public class AsbCancellationCleanupFailureTests
{
    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 2)]
    public async Task When_abandonment_fails_during_cancellation_should_attempt_the_entire_batch(bool useQueue, int failures)
    {
        //Arrange
        var receiver = new InMemoryFailingAbandonServiceBusReceiver(failures);
        await using var client = new InMemoryLockingServiceBusClient(receiver);
        using var cancelled = new CancellationTokenSource();
        var destination = new RoutingKey("cleanup-failure");
        var reader = new AsbCancellingWireReader(destination, cancelled);
        var subscription = new AzureServiceBusSubscription<ASBTestCommand>(
            new SubscriptionName("cleanup-failure"), new ChannelName("cleanup-failure"), destination,
            bufferSize: 3, makeChannels: OnMissingChannel.Assume,
            subscriptionConfiguration: new AzureServiceBusSubscriptionConfiguration { UseServiceBusQueue = useQueue });
        subscription.WireMessageReader = reader;
        await using var consumer = new AzureServiceBusConsumerFactory(client).CreateAsync(subscription);

        //Act
        var failure = await Record.ExceptionAsync(() => consumer.ReceiveAsync(TimeSpan.FromSeconds(1), cancelled.Token));

        //Assert
        Assert.Equal(3, receiver.Attempts.Distinct().Count());
        Assert.Equal(3 - failures, receiver.AbandonedCount);
        Assert.Same(receiver.FirstFailure, failure);
        Assert.Equal(1, reader.ReadCount);
        Assert.True(receiver.IsClosed);
    }
}
