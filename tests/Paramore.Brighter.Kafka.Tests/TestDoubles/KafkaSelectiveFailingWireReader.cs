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
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Paramore.Brighter.MessagingGateway.Kafka;

namespace Paramore.Brighter.Kafka.Tests.TestDoubles;

internal sealed class KafkaSelectiveFailingWireReader(bool returnNull) : IAmAWireMessageReader<ConsumeResult<string, byte[]>>
{
    private int _readCount;
    public int ReadCount => Volatile.Read(ref _readCount);

    public Message ReadMessage(ConsumeResult<string, byte[]> native)
    {
        Interlocked.Increment(ref _readCount);
        var id = Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID));
        if (id == "bad-id")
        {
            if (returnNull) return null!;
            throw new InvalidOperationException("Cannot map the external delivery.");
        }

        return new Message(new MessageHeader(new Id("mapped-good-id"), new RoutingKey("mapped-topic"), MessageType.MT_EVENT),
            new MessageBody("mapped:" + Convert.ToBase64String(native.Message.Value)));
    }
}
