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
using System.Text;
using System.Text.Json;
using Paramore.Brighter.MessagingGateway.RocketMQ;
using Xunit;

namespace Paramore.Brighter.RocketMQ.Tests.TestDoubles;

/// <summary>Writes a complete custom payload or composes the default native mapping.</summary>
public sealed class RocketSelectedWireWriter(bool compose) : IAmAWireMessageWriter<RocketMqWireMessage>
{
    /// <summary>Gets the number of conversions.</summary>
    public int Calls { get; private set; }

    /// <inheritdoc />
    public RocketMqWireMessage WriteMessage(Message message)
    {
        Calls++;
        Assert.NotNull(message.Header.TraceParent);
        Assert.NotNull(message.Header.TraceState);
        if (compose)
        {
            var result = new DefaultRocketMqWireMessageWriter().WriteMessage(message);
            result.Body[0] = (byte)'C';
            result.Properties["custom-format"] = "composed";
            return result;
        }
        var payload = JsonSerializer.Serialize(new
        {
            id = message.Id.Value, topic = message.Header.Topic.Value, body = message.Body.Value,
            trace = message.Header.TraceParent.Value, state = message.Header.TraceState.Value,
            baggage = message.Header.Baggage.ToString(), marker = message.Header.Bag["marker"].ToString()
        });
        var wire = new RocketMqWireMessage(Encoding.UTF8.GetBytes("W:" + payload));
        wire.Properties["custom-format"] = "complete";
        return wire;
    }
}
