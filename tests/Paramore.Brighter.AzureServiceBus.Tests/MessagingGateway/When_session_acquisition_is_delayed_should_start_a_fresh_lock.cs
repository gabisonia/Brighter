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
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;

public class AsbInMemorySessionAcquisitionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_session_acquisition_is_delayed_should_start_a_fresh_lock(bool useQueue)
    {
        //Arrange
        var duration = TimeSpan.FromSeconds(1);
        var receiver = new InMemoryLockingServiceBusReceiver(1, duration);
        var session = new InMemoryLockingServiceBusSessionReceiver(receiver, duration);
        await using var client = new InMemoryLockingServiceBusClient(session);
        await Task.Delay(duration + TimeSpan.FromMilliseconds(100));

        //Act
        var acquiredAt = DateTimeOffset.UtcNow;
        var acquired = useQueue
            ? await client.AcceptNextSessionAsync("queue")
            : await client.AcceptNextSessionAsync("topic", "subscription");

        //Assert
        Assert.True(acquired.SessionLockedUntil >= acquiredAt + duration,
            "A session lock starts at acquisition, not fixture construction.");
        var message = Assert.Single(await acquired.ReceiveMessagesAsync(1));
        await acquired.CompleteMessageAsync(message);
        Assert.Equal(1, receiver.CompletedCount);

        //Act
        await Task.Delay(duration + TimeSpan.FromMilliseconds(100));
        var failure = await Assert.ThrowsAsync<ServiceBusException>(() => acquired.RenewSessionLockAsync());

        //Assert
        Assert.Equal(ServiceBusFailureReason.SessionLockLost, failure.Reason);
    }
}
