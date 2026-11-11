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
using System.Linq;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;

namespace Paramore.Brighter.Gcp.Tests.TestDoubles;

internal sealed class GcpRecoverableWireWriter(bool returnNull) : IAmAWireMessageWriter<PubsubMessage>
{
    private readonly ConcurrentQueue<Message> _sources = new();
    public Message[] Sources => _sources.ToArray();
    public bool Fail { get; set; } = true;
    public InvalidOperationException Failure { get; } = new("The external publication mapping is unavailable.");

    public PubsubMessage WriteMessage(Message source)
    {
        _sources.Enqueue(source);
        if (Fail)
        {
            if (returnNull)
                return null!;
            throw Failure;
        }

        return new PubsubMessage
        {
            Data = ByteString.CopyFrom(new byte[] { 87, 0 }.Concat(source.Body.Memory.ToArray()).ToArray()),
            OrderingKey = "writer-supplied-order",
            Attributes =
            {
                { "external-id", source.Id.Value },
                { "external-marker", (string)source.Header.Bag["source-marker"] }
            }
        };
    }
}
