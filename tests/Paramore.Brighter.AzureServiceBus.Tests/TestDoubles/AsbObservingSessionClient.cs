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
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;

internal sealed class AsbObservingSessionClient(string connectionString)
    : ServiceBusClient(connectionString, new ServiceBusClientOptions
    {
        RetryOptions = new ServiceBusRetryOptions
        {
            TryTimeout = TimeSpan.FromSeconds(10),
            MaxRetries = 0
        }
    })
{
    public ServiceBusSessionReceiver? LastSessionReceiver { get; private set; }
    public DateTimeOffset InitialSessionLockedUntil { get; private set; }

    public override async Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string queueName,
        ServiceBusSessionReceiverOptions? options = null, CancellationToken cancellationToken = default)
    {
        var receiver = await base.AcceptNextSessionAsync(queueName, options, cancellationToken);
        InitialSessionLockedUntil = receiver.SessionLockedUntil;
        LastSessionReceiver = receiver;
        return receiver;
    }

    public override async Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string topicName,
        string subscriptionName, ServiceBusSessionReceiverOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var receiver = await base.AcceptNextSessionAsync(topicName, subscriptionName, options, cancellationToken);
        InitialSessionLockedUntil = receiver.SessionLockedUntil;
        LastSessionReceiver = receiver;
        return receiver;
    }
}
