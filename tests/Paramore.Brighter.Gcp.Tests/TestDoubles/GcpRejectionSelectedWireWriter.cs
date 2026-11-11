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
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Paramore.Brighter.MessagingGateway.GcpPubSub;
namespace Paramore.Brighter.Gcp.Tests.TestDoubles;

/// <summary>Composes native rejection metadata and permits controlled mapping recovery.</summary>
internal sealed class GcpRejectionSelectedWireWriter(string route, int behavior) : IAmAWireMessageWriter<PubsubMessage>
{
    public string Route => route;
    public int Calls { get; private set; }
    public bool Fail { get; set; }
    public PubsubMessage WriteMessage(Message message)
    {
        Calls++;
        if (Fail && behavior == 1) throw new FormatException("Selected rejection writer failed.");
        if (Fail && behavior == 2) return null!;
        var wire = new DefaultGcpPubSubWireMessageWriter().WriteMessage(message);
        wire.Data = ByteString.CopyFromUtf8(route + ":" + message.Body.Value);
        wire.Attributes["selected-route"] = route;
        return wire;
    }
}
