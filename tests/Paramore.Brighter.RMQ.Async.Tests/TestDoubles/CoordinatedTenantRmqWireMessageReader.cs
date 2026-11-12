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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using RabbitMQ.Client.Events;

namespace Paramore.Brighter.RMQ.Async.Tests.TestDoubles;

internal sealed class CoordinatedTenantRmqWireMessageReader : IAmAWireMessageReader<BasicDeliverEventArgs>, IDisposable
{
    private readonly DefaultRmqWireMessageReader _defaultReader = new();
    private readonly Barrier _readers = new(2);
    private readonly ConcurrentQueue<BasicDeliverEventArgs> _deliveries = new();

    public IReadOnlyCollection<BasicDeliverEventArgs> Deliveries => _deliveries.ToArray();

    public Message ReadMessage(BasicDeliverEventArgs nativeMessage)
    {
        _deliveries.Enqueue(nativeMessage);
        if (!_readers.SignalAndWait(TimeSpan.FromSeconds(10)))
            throw new TimeoutException("Both consumers must enter the shared reader.");

        var message = _defaultReader.ReadMessage(nativeMessage);
        if (message.Header.Bag.TryGetValue("tenant-id", out var tenantId))
        {
            message.Header.Bag["tenant_id"] = tenantId;
            message.Header.Bag.Remove("tenant-id");
        }
        return message;
    }

    public void Dispose() => _readers.Dispose();
}
