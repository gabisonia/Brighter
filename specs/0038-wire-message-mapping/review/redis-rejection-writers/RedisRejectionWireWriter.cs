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

internal sealed class RedisRejectionWireWriter(string prefix, string mode) : IAmAWireMessageWriter<string>
{
    private readonly DefaultRedisWireMessageWriter _default = new();
    private int _calls;
    public int Calls => Volatile.Read(ref _calls);
    public InvalidOperationException Failure { get; } = new("Deliberate Redis rejection writer failure.");
    public RoutingKey? LastTopic { get; private set; }
    public string? LastOriginalTopic { get; private set; }
    public string? LastReason { get; private set; }
    public string? LastDescription { get; private set; }
    public string? LastTimestamp { get; private set; }

    public string WriteMessage(Message message)
    {
        var call = Interlocked.Increment(ref _calls);
        LastTopic = message.Header.Topic;
        LastOriginalTopic = message.Header.Bag.TryGetValue("originalTopic", out var original) ? original.ToString() : null;
        LastReason = message.Header.Bag.TryGetValue("rejectionReason", out var reason) ? reason.ToString() : null;
        LastDescription = message.Header.Bag.TryGetValue("rejectionMessage", out var description) ? description.ToString() : null;
        LastTimestamp = message.Header.Bag.TryGetValue("rejectionTimestamp", out var timestamp) ? timestamp.ToString() : null;
        if (call == 1 && mode == "throw")
            throw Failure;
        if (call == 1 && mode == "null")
            return null!;
        return prefix + (mode == "composed" ? _default.WriteMessage(message) : message.Body.Value);
    }
}
