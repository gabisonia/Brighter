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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;

namespace Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;

internal sealed class SyncConcurrentWireWriter(bool compose, bool synchronizeMapping, Barrier writes, Barrier callbacks) : IAmAWireMessageWriter<RmqWireMessage>
{
    private int _writeCount;
    private int _callbackCount;
    public int WriteCount => Volatile.Read(ref _writeCount);
    public int CallbackCount => Volatile.Read(ref _callbackCount);

    public RmqWireMessage WriteMessage(Message message)
    {
        Interlocked.Increment(ref _writeCount);
        var wire = compose ? new DefaultRmqWireMessageWriter().WriteMessage(message)
            : new RmqWireMessage { Body = Encoding.UTF8.GetBytes($"parallel:{Convert.ToBase64String(message.Body.Memory.ToArray())}") };
        var member = message.Header.Bag["member"];
        var id = message.Id.Value;
        var topic = message.Header.Topic.Value;
        var count = message.Header.HandledCount;
        if (synchronizeMapping && !writes.SignalAndWait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Writer calls did not overlap.");
        var configure = wire.ConfigureProperties;
        wire.ConfigureProperties = properties =>
        {
            Interlocked.Increment(ref _callbackCount);
            configure?.Invoke(properties);
            if (!compose)
            {
                properties.ContentType = "application/vnd.parallel";
                properties.Type = "application/vnd.parallel";
            }
            properties.MessageId = id;
            properties.DeliveryMode = 1;
            var headers = properties.Headers ??= new Dictionary<string, object>();
            headers["writer-member"] = member;
            headers["writer-id"] = id;
            headers["writer-topic"] = topic;
            headers[HeaderNames.HANDLED_COUNT] = count;
            headers[HeaderNames.DELIVERY_TAG] = "writer-tag";
            headers[HeaderNames.DELAY_MILLISECONDS] = 60_000;
            if (synchronizeMapping && !callbacks.SignalAndWait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Property callbacks did not overlap.");
        };
        return wire;
    }
}
