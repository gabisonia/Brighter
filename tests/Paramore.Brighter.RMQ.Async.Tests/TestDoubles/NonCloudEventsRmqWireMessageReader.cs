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

using System.Net.Mime;
using System.Text;
using System.Text.Json;
using Paramore.Brighter.Observability;
using RabbitMQ.Client.Events;

namespace Paramore.Brighter.RMQ.Async.Tests.TestDoubles;

internal sealed class NonCloudEventsRmqWireMessageReader(bool telemetryInBody) : IAmAWireMessageReader<BasicDeliverEventArgs>
{
    public Message ReadMessage(BasicDeliverEventArgs native)
    {
        var attributes = native.BasicProperties.Headers!;
        string body;
        string parent;
        string state;
        string baggage;
        if (telemetryInBody)
        {
            using var document = JsonDocument.Parse(native.Body);
            var root = document.RootElement;
            body = root.GetProperty("payload").GetString()!;
            parent = root.GetProperty("parent").GetString()!;
            state = root.GetProperty("state").GetString()!;
            baggage = root.GetProperty("baggage").GetString()!;
        }
        else
        {
            body = Encoding.UTF8.GetString(native.Body.Span);
            parent = Encoding.UTF8.GetString((byte[])attributes["telemetry-parent"]!);
            state = Encoding.UTF8.GetString((byte[])attributes["telemetry-state"]!);
            baggage = Encoding.UTF8.GetString((byte[])attributes["telemetry-baggage"]!);
        }
        var header = new MessageHeader(new Id(Encoding.UTF8.GetString((byte[])attributes["legacy-id"]!)),
            new RoutingKey(native.RoutingKey), MessageType.MT_EVENT, contentType: new ContentType("text/plain"),
            traceParent: new TraceParent(parent), traceState: new TraceState(state));
        header.Baggage.LoadBaggage(baggage);
        return new Message(header, new MessageBody(body));
    }
}
