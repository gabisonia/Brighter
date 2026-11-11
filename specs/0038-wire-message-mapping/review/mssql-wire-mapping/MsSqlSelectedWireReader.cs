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
using System.Text.Json;
using System.Threading;
using Paramore.Brighter.MessagingGateway.MsSql;

namespace Paramore.Brighter.MSSQL.Tests.TestDoubles;

/// <summary>Reads complete or composed JSON and attempts to supply a false receipt handle.</summary>
public sealed class MsSqlSelectedWireReader(bool compose) : IAmAWireMessageReader<string>
{
    private int _calls;
    /// <summary>Gets the number of conversions.</summary>
    public int Calls => Volatile.Read(ref _calls);
    /// <inheritdoc />
    public Message ReadMessage(string message)
    {
        Interlocked.Increment(ref _calls);
        using var content = JsonDocument.Parse(message.Substring(2));
        var json = content.RootElement;
        Message result;
        if (compose)
            result = new DefaultMsSqlWireMessageReader().ReadMessage(json.GetRawText());
        else
        {
            var header = new MessageHeader(new Id(json.GetProperty("id").GetString()!),
                new RoutingKey(json.GetProperty("topic").GetString()!), MessageType.MT_EVENT,
                handledCount: json.GetProperty("handled").GetInt32(),
                traceParent: new TraceParent(json.GetProperty("trace").GetString()!),
                traceState: new TraceState(json.GetProperty("state").GetString()!));
            header.Baggage.LoadBaggage(json.GetProperty("baggage").GetString()!);
            header.Bag["marker"] = json.GetProperty("marker").GetString()!;
            result = new Message(header, new MessageBody(json.GetProperty("body").GetString()!));
        }
        result.Header.Bag["ReceiptHandle"] = -777L;
        return result;
    }
}
