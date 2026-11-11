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

using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using RabbitMQ.Client;

namespace Paramore.Brighter.RMQ.Async.Tests.TestDoubles;

internal sealed class NonCloudEventsRmqWireMessageWriter(bool telemetryInBody) : IAmAWireMessageWriter<RmqWireMessage>
{
    public RmqWireMessage WriteMessage(Message message)
    {
        var attributes = new Dictionary<string, object?> { ["legacy-id"] = Encoding.UTF8.GetBytes(message.Id.Value) };
        string body;
        if (telemetryInBody)
        {
            body = JsonSerializer.Serialize(new
            {
                payload = message.Body.Value, parent = message.Header.TraceParent?.Value,
                state = message.Header.TraceState?.Value, baggage = message.Header.Baggage.ToString()
            });
        }
        else
        {
            body = message.Body.Value;
            attributes["telemetry-parent"] = Encoding.UTF8.GetBytes(message.Header.TraceParent?.Value ?? string.Empty);
            attributes["telemetry-state"] = Encoding.UTF8.GetBytes(message.Header.TraceState?.Value ?? string.Empty);
            attributes["telemetry-baggage"] = Encoding.UTF8.GetBytes(message.Header.Baggage.ToString());
        }
        return new RmqWireMessage
        {
            Body = Encoding.UTF8.GetBytes(body),
            Properties = new BasicProperties
            {
                MessageId = message.Id.Value, Type = "legacy-telemetry",
                ContentType = telemetryInBody ? "application/json" : "text/plain", Headers = attributes
            }
        };
    }
}
