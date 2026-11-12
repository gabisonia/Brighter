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
using System.Collections.Concurrent;
using System.Threading;
using Paramore.Brighter.MessagingGateway.Redis;

namespace Paramore.Brighter.Redis.Tests.TestDoubles;

internal sealed class RedisRecoverableWireReader(bool returnNull) : IAmAWireMessageReader<string>
{
    private readonly DefaultRedisWireMessageReader _default = new();
    private readonly ConcurrentQueue<string> _inputs = new();
    private int _calls;
    public string[] Inputs => _inputs.ToArray();

    public Message ReadMessage(string wire)
    {
        _inputs.Enqueue(wire);
        if (Interlocked.Increment(ref _calls) == 1)
        {
            if (returnNull)
                return null!;
            throw new InvalidOperationException("Deliberate Redis reader failure.");
        }
        return _default.ReadMessage(wire);
    }
}
