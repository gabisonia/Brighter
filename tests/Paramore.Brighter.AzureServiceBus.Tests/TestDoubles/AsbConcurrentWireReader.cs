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
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Azure.Messaging.ServiceBus;

namespace Paramore.Brighter.AzureServiceBus.Tests.TestDoubles;

internal sealed class AsbConcurrentWireReader : IAmAWireMessageReader<ServiceBusReceivedMessage>, IDisposable
{
    private readonly Barrier _mapped = new(2);
    private readonly ConcurrentDictionary<string, ServiceBusReceivedMessage> _deliveries = new();
    private int _readCount;
    public int ReadCount => Volatile.Read(ref _readCount);
    public ServiceBusReceivedMessage Delivery(string id) => _deliveries[id];

    public Message ReadMessage(ServiceBusReceivedMessage delivery)
    {
        Interlocked.Increment(ref _readCount);
        _deliveries[delivery.MessageId] = delivery;
        var destination = new RoutingKey((string)delivery.ApplicationProperties["destination"]);
        var header = new MessageHeader(new Id("mapped-" + delivery.MessageId), destination,
            MessageType.MT_EVENT, correlationId: new Id("mapped-" + delivery.CorrelationId),
            subject: "mapped-" + delivery.Subject);
        header.Bag["LockToken"] = Guid.Empty.ToString();
        header.Bag["SequenceNumber"] = -1L;
        header.Bag["reader-only"] = delivery.MessageId;
        var message = new Message(header, new MessageBody(new byte[] { 42 }.Concat(delivery.Body.ToArray()).ToArray()));
        if (!_mapped.SignalAndWait(TimeSpan.FromSeconds(20)))
            throw new TimeoutException("Both consumer readers must overlap.");
        return message;
    }

    public void Dispose() => _mapped.Dispose();
}
