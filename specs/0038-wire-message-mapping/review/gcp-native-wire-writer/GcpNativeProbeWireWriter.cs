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

using System.Linq;
using System.Threading;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;

namespace Paramore.Brighter.Gcp.Tests.TestDoubles;

internal sealed class GcpNativeProbeWireWriter : IAmAWireMessageWriter<PubsubMessage>
{
    private int _calls;
    public int Calls => Volatile.Read(ref _calls);

    public PubsubMessage WriteMessage(Message message)
    {
        Interlocked.Increment(ref _calls);
        var native = new PubsubMessage
        {
            Data = ByteString.CopyFrom(new byte[] { 87, 0 }.Concat(message.Body.Memory.ToArray()).ToArray()),
            OrderingKey = "writer-must-not-control-ordering"
        };
        native.Attributes.Add("external-id", message.Header.MessageId.ToString());
        native.Attributes.Add("external-marker", (string)message.Header.Bag["source-marker"]);
        return native;
    }
}
