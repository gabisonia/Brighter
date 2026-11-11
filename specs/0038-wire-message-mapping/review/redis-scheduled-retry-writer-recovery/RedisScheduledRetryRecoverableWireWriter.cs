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
using Paramore.Brighter.MessagingGateway.Redis;

namespace Paramore.Brighter.Redis.Tests.TestDoubles;

internal sealed class RedisScheduledRetryRecoverableWireWriter(bool returnNull) : IAmAWireMessageWriter<string>
{
    private readonly DefaultRedisWireMessageWriter _default = new();
    private int _failing = 1;
    private int _failedAttempts;
    private int _successfulWrites;

    public int FailedAttempts => Volatile.Read(ref _failedAttempts);
    public int SuccessfulWrites => Volatile.Read(ref _successfulWrites);
    public int Calls => FailedAttempts + SuccessfulWrites;

    public void Recover() => Volatile.Write(ref _failing, 0);

    public string WriteMessage(Message message)
    {
        if (Volatile.Read(ref _failing) != 0)
        {
            Interlocked.Increment(ref _failedAttempts);
            if (returnNull)
                return null!;
            throw new InvalidOperationException("Deliberate scheduled Redis writer failure.");
        }

        var wire = "C:" + _default.WriteMessage(message);
        Interlocked.Increment(ref _successfulWrites);
        return wire;
    }
}
