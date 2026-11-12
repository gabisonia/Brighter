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
using Paramore.Brighter.Observability;

namespace Paramore.Brighter.Kafka.Tests.TestDoubles;

internal sealed class KafkaExplicitUnacceptableWireReader(bool compose) : IAmAWireMessageReader<ConsumeResult<string, byte[]>>
{
    private readonly DefaultKafkaWireMessageReader _defaultReader = new();
    private int _readCount;
    public int ReadCount => Volatile.Read(ref _readCount);

    public Message ReadMessage(ConsumeResult<string, byte[]> native)
    {
        Interlocked.Increment(ref _readCount);
        var id = Encoding.UTF8.GetString(native.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID));
        var mapped = compose ? _defaultReader.ReadMessage(native)
            : new Message(new MessageHeader(new Id("mapped-" + id), new RoutingKey(native.Topic), MessageType.MT_EVENT),
                new MessageBody(native.Message.Value));
#pragma warning disable CS0618
        mapped.Header.MessageType = id == "bad-id" ? MessageType.MT_UNACCEPTABLE : MessageType.MT_EVENT;
#pragma warning restore CS0618
        mapped.Body = new MessageBody("selected:" + Convert.ToBase64String(native.Message.Value));
        mapped.Header.Topic = new RoutingKey("selected-topic");
        mapped.Header.PartitionKey = new Paramore.Brighter.PartitionKey("selected-key");
        mapped.Header.HandledCount = 7;
        mapped.Header.TraceParent = new TraceParent("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
        mapped.Header.Bag["tenant"] = "south";
        mapped.Header.Bag[HeaderNames.PARTITION_OFFSET] = new TopicPartitionOffset("wrong-topic", new Partition(7), new Offset(999));
        return mapped;
    }
}
