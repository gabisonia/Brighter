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
using RabbitMQ.Client.Events;

namespace Paramore.Brighter.RMQ.Async.Tests.TestDoubles;

internal sealed class LegacyTenantRmqWireMessageReader : IAmAWireMessageReader<BasicDeliverEventArgs>
{
    public Message ReadMessage(BasicDeliverEventArgs nativeMessage)
    {
        var headers = nativeMessage.BasicProperties.Headers!;
        var message = new Message(
            new MessageHeader(
                new Id(Encoding.UTF8.GetString((byte[])headers["legacy-id"]!)),
                new RoutingKey(nativeMessage.RoutingKey),
                MessageType.MT_EVENT,
                contentType: new ContentType(nativeMessage.BasicProperties.ContentType!)),
            new MessageBody(Encoding.UTF8.GetString(nativeMessage.Body.Span)))
        {
            DeliveryTag = ulong.MaxValue,
            Redelivered = true,
            Persist = false
        };
        message.Header.Bag["tenant_id"] = Encoding.UTF8.GetString((byte[])headers["tenant-id"]!);
        return message;
    }
}
