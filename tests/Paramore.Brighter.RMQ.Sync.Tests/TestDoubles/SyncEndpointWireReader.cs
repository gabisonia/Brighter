#region Licence

/* The MIT License (MIT)
Copyright © 2026 Irakli Gabisonia

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
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using RabbitMQ.Client.Events;

namespace Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;

internal sealed class SyncEndpointWireReader(bool composeDefault) : IAmAWireMessageReader<BasicDeliverEventArgs>
{
    public Message ReadMessage(BasicDeliverEventArgs native)
    {
        var message = composeDefault
            ? new DefaultRmqWireMessageReader().ReadMessage(native)
            : new Message(
                new MessageHeader(new Id(Encoding.UTF8.GetString((byte[])native.BasicProperties.Headers["legacy-id"])),
                    new RoutingKey("mapped-topic"), MessageType.MT_EVENT),
                new MessageBody(Encoding.UTF8.GetString(native.Body.Span)));
        message.Header.Bag["tenant_id"] = Encoding.UTF8.GetString((byte[])native.BasicProperties.Headers["tenant-id"]);
        message.Header.Bag.Remove("tenant-id");
        message.Header.HandledCount = 99;
        message.DeliveryTag = ulong.MaxValue;
        message.Redelivered = true;
        message.Persist = false;
        return message;
    }
}
