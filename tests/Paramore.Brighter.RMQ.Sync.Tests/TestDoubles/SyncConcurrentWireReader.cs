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

using System;
using System.Threading;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using RabbitMQ.Client.Events;

namespace Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;

internal sealed class SyncConcurrentWireReader(bool compose, Barrier reads) : IAmAWireMessageReader<BasicDeliverEventArgs>
{
    private int _readCount;
    public int ReadCount => Volatile.Read(ref _readCount);

    public Message ReadMessage(BasicDeliverEventArgs native)
    {
        Interlocked.Increment(ref _readCount);
        var message = compose ? new DefaultRmqWireMessageReader().ReadMessage(native)
            : new Message(new MessageHeader(new Id($"mapped-{native.BasicProperties.MessageId}"),
                new RoutingKey("mapped-topic"), MessageType.MT_EVENT),
                new MessageBody($"parallel:{Convert.ToBase64String(native.Body.ToArray())}"));
        message.Header.Bag["reader-member"] = native.BasicProperties.Headers["member"];
        message.Header.HandledCount = 99;
        message.DeliveryTag = ulong.MaxValue;
        message.Redelivered = true;
        message.Persist = native.BasicProperties.DeliveryMode != 2;
        if (!reads.SignalAndWait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Reader calls did not overlap.");
        return message;
    }
}
