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
using System.Linq;
using Azure.Messaging.ServiceBus;

namespace Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;

internal sealed class AsbRetrySelectedWireWriter(int behavior) : IAmAWireMessageWriter<ServiceBusMessage>
{
    public int Calls { get; private set; }
    public bool Recover { get; set; }
    public ServiceBusMessage WriteMessage(Message message)
    {
        Calls++;
        if (!Recover && behavior == 1) throw new FormatException("Selected retry writer failed.");
        if (!Recover && behavior == 2) return null!;
        var wire = new ServiceBusMessage(new BinaryData(new byte[] { 83, 0 }.Concat(message.Body.Memory.ToArray()).ToArray()))
        { MessageId = "writer-id", PartitionKey = "writer-session", SessionId = "writer-session" };
        wire.ApplicationProperties[Message.OriginalMessageIdHeaderName] = "writer-original";
        wire.ApplicationProperties["custom-tenant"] = message.Header.Bag["tenant"];
        wire.ApplicationProperties["custom-parent"] = message.Header.TraceParent.Value;
        wire.ApplicationProperties["custom-state"] = message.Header.TraceState.Value;
        wire.ApplicationProperties["custom-baggage"] = message.Header.Baggage.ToString();
        return wire;
    }
}
