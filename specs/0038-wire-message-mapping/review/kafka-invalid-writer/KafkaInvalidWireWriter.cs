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

internal sealed class KafkaInvalidWireWriter(bool compose) : IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>
{
    private int _writeCount;
    public int WriteCount => Volatile.Read(ref _writeCount);

    public Confluent.Kafka.Message<string, byte[]> WriteMessage(Message message)
    {
        Interlocked.Increment(ref _writeCount);
        var wire = compose ? new DefaultKafkaWireMessageWriter().WriteMessage(message)
            : new Confluent.Kafka.Message<string, byte[]>
            {
                Headers = new Headers(),
                Value = Encoding.UTF8.GetBytes("rejected:" + Convert.ToBase64String(message.Body.Memory.ToArray()))
            };
        wire.Key = "rejection-key";
        wire.Headers.Remove(HeaderNames.MESSAGE_ID);
        wire.Headers.Add(HeaderNames.MESSAGE_ID, Encoding.UTF8.GetBytes("wire-rejected-id"));
        wire.Headers.Add("external-source-id", Encoding.UTF8.GetBytes(message.Id.Value));
        wire.Headers.Add("external-original-topic", Encoding.UTF8.GetBytes(
            message.Header.Bag.TryGetValue(HeaderNames.ORIGINAL_TOPIC, out var topic) ? topic.ToString()! : string.Empty));
        wire.Headers.Add("external-reason", Encoding.UTF8.GetBytes(
            message.Header.Bag.TryGetValue(HeaderNames.REJECTION_REASON, out var reason) ? reason.ToString()! : string.Empty));
        wire.Headers.Add("external-tenant", Encoding.UTF8.GetBytes(
            message.Header.Bag.TryGetValue("tenant", out var tenant) ? tenant.ToString()! : string.Empty));
        return wire;
    }
}
