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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;

internal sealed class InMemoryFailingAbandonServiceBusReceiver(int failures, bool failClose = false)
    : InMemoryLockingServiceBusReceiver(3, TimeSpan.FromSeconds(30))
{
    public List<string> Attempts { get; } = [];
    public ServiceBusException FirstFailure { get; } = new("Injected lost lock", ServiceBusFailureReason.MessageLockLost);

    public override Task AbandonMessageAsync(ServiceBusReceivedMessage message,
        IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default)
    {
        Attempts.Add(message.LockToken);
        if (Attempts.Count == 1 && failures > 0) throw FirstFailure;
        if (Attempts.Count <= failures)
            throw new ServiceBusException("Another lost lock", ServiceBusFailureReason.MessageLockLost);
        return base.AbandonMessageAsync(message, propertiesToModify, cancellationToken);
    }
    public override async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        var wasClosed = IsClosed;
        await base.CloseAsync(cancellationToken);
        if (failClose && !wasClosed) throw new InvalidOperationException("Secondary close failure");
    }

}
