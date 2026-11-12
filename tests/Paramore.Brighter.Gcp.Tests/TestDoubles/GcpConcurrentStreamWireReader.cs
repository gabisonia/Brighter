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
using Paramore.Brighter.MessagingGateway.GcpPubSub;

namespace Paramore.Brighter.Gcp.Tests.TestDoubles;

internal sealed class GcpConcurrentStreamWireReader : IAmAWireMessageReader<GcpStreamMessage>, IDisposable
{
    private readonly GcpNativeProbeStreamReader _reader = new();
    private readonly Barrier _mapped = new(2);
    private int _overlappedCalls;
    public int OverlappedCalls => Volatile.Read(ref _overlappedCalls);
    public GcpStreamMessage[] Deliveries => _reader.Deliveries;

    public Message ReadMessage(GcpStreamMessage delivery)
    {
        var mapped = _reader.ReadMessage(delivery);
        if (!_mapped.SignalAndWait(TimeSpan.FromSeconds(20)))
            throw new TimeoutException("Both native reader mappings must overlap.");
        Interlocked.Increment(ref _overlappedCalls);
        return mapped;
    }

    public void Dispose() => _mapped.Dispose();
}
