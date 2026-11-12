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
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus.Administration;

namespace Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;

internal sealed class AsbSessionCleanupFixture(string connectionString, bool useQueue) : IAsyncDisposable
{
    private readonly ServiceBusAdministrationClient _administration = new(connectionString);
    private bool _created;
    public string EntityName { get; } = "wire-cleanup-" + Guid.NewGuid().ToString("N");
    public string SubscriptionName => "observer";

    public async Task CreateAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        if (useQueue)
        {
            await _administration.CreateQueueAsync(new CreateQueueOptions(EntityName)
            {
                RequiresSession = true,
                LockDuration = TimeSpan.FromMinutes(1),
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5)
            }, timeout.Token);
            _created = true;
        }
        else
        {
            await _administration.CreateTopicAsync(new CreateTopicOptions(EntityName)
            {
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5)
            }, timeout.Token);
            _created = true;
            await _administration.CreateSubscriptionAsync(new CreateSubscriptionOptions(EntityName, SubscriptionName)
            {
                RequiresSession = true,
                LockDuration = TimeSpan.FromMinutes(1)
            }, timeout.Token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_created) return;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        if (useQueue) await _administration.DeleteQueueAsync(EntityName, timeout.Token);
        else await _administration.DeleteTopicAsync(EntityName, timeout.Token);
        _created = false;
    }
}
