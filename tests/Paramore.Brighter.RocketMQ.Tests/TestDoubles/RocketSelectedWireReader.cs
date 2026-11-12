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
using Org.Apache.Rocketmq;
using Paramore.Brighter.MessagingGateway.RocketMQ;

namespace Paramore.Brighter.RocketMQ.Tests.TestDoubles;

/// <summary>Reads selected payloads and attempts to forge native delivery controls.</summary>
public sealed class RocketSelectedWireReader(bool compose) : IAmAWireMessageReader<MessageView>
{
    /// <summary>Gets the number of conversions.</summary>
    public int Calls { get; private set; }
    /// <summary>Gets the actual SDK delivery passed to the reader.</summary>
    public MessageView? LastDelivery { get; private set; }

    /// <inheritdoc />
    public Message ReadMessage(MessageView message)
    {
        Calls++;
        LastDelivery = message;
        var body = Encoding.UTF8.GetString(message.Body);
        Message result;
        if (compose)
        {
            result = new DefaultRocketMqWireMessageReader().ReadMessage(message);
            result.Body = new MessageBody("m" + body[1..]);
        }
        else
        {
            using var content = JsonDocument.Parse(body[2..]);
            var json = content.RootElement;
            var header = new MessageHeader(new Id(json.GetProperty("id").GetString()!),
                new RoutingKey(json.GetProperty("topic").GetString()!), MessageType.MT_EVENT,
                traceParent: new TraceParent(json.GetProperty("trace").GetString()!),
                traceState: new TraceState(json.GetProperty("state").GetString()!));
            header.Baggage.LoadBaggage(json.GetProperty("baggage").GetString()!);
            header.Bag["marker"] = json.GetProperty("marker").GetString()!;
            result = new Message(header, new MessageBody(json.GetProperty("body").GetString()!));
        }
        result.Header.Bag["ReceiptHandle"] = "forged-receipt";
        result.Header.HandledCount = 777;
        return result;
    }
}
