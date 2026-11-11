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
using System.Threading;
using Paramore.Brighter.MessagingGateway.RMQ.Async;

namespace Paramore.Brighter.RMQ.Async.Tests.TestDoubles;

internal sealed class CoordinatedTenantRmqWireMessageWriter : IAmAWireMessageWriter<RmqWireMessage>, IDisposable
{
    private readonly DefaultRmqWireMessageWriter _defaultWriter = new();
    private readonly object _mappingLock = new();
    private readonly Barrier _writers = new(2);

    public RmqWireMessage WriteMessage(Message message)
    {
        RmqWireMessage wireMessage;
        lock (_mappingLock)
        {
            wireMessage = _defaultWriter.WriteMessage(message);
            var headers = wireMessage.Properties.Headers
                ?? throw new InvalidOperationException("Default mapping must provide headers.");
            headers["tenant-id"] = headers["tenant_id"];
            headers.Remove("tenant_id");
        }
        if (!_writers.SignalAndWait(TimeSpan.FromSeconds(10)))
            throw new TimeoutException("Both retries must finish mapping before either publishes.");
        return wireMessage;
    }

    public void Dispose() => _writers.Dispose();
}
