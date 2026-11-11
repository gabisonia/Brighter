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

internal sealed class KafkaRecoverableWireWriter(bool returnNull) : IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>
{
    private int _writeCount;
    public int WriteCount => Volatile.Read(ref _writeCount);
    public bool FailBadMessage { get; set; } = true;

    public Confluent.Kafka.Message<string, byte[]> WriteMessage(Message message)
    {
        Interlocked.Increment(ref _writeCount);
        if (FailBadMessage && message.Id == new Id("bad-id"))
        {
            if (returnNull) return null!;
            throw new InvalidOperationException("Cannot map the selected external message.");
        }

        return new Confluent.Kafka.Message<string, byte[]>
        {
            Key = "mapped-" + message.Id.Value,
            Value = Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(message.Body.Memory.ToArray())),
            Headers = new Headers
            {
                { HeaderNames.MESSAGE_ID, Encoding.UTF8.GetBytes("wire-" + message.Id.Value) }
            }
        };
    }
}
