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
using Paramore.Brighter.MessagingGateway.RMQ.Sync;

namespace Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;

internal sealed class SyncRetryProfileWireWriter(bool compose) : IAmAWireMessageWriter<RmqWireMessage>
{
    public int FailureMode { get; set; }

    public RmqWireMessage WriteMessage(Message message)
    {
        if (FailureMode == 1) throw new FormatException("Retry mapping unavailable.");
        if (FailureMode == 2) return null!;
        var wire = compose ? new DefaultRmqWireMessageWriter().WriteMessage(message)
            : new RmqWireMessage { Body = Encoding.UTF8.GetBytes($"retry:{Convert.ToBase64String(message.Body.Memory.ToArray())}") };
        var configure = wire.ConfigureProperties;
        wire.ConfigureProperties = properties =>
        {
            configure?.Invoke(properties);
            if (FailureMode == 3) throw new InvalidOperationException("Retry properties unavailable.");
            if (!compose)
            {
                properties.ContentType = "application/vnd.retry-order";
                properties.Type = "application/vnd.retry-order";
            }
            properties.MessageId = "writer-owned-id";
            properties.DeliveryMode = 1;
            var headers = properties.Headers ??= new Dictionary<string, object>();
            headers["retry-profile"] = "selected-retry";
            headers["retry-tenant"] = message.Header.Bag["tenant"];
            headers[HeaderNames.HANDLED_COUNT] = -17;
            headers[HeaderNames.DELIVERY_TAG] = "writer-owned-tag";
            headers[HeaderNames.DELAY_MILLISECONDS] = 60_000;
            headers[HeaderNames.ORIGINAL_MESSAGE_ID] = "writer-owned-original";
            headers[HeaderNames.ORIGINAL_MESSAGE_ID.ToUpperInvariant()] = "writer-owned-variant";
        };
        return wire;
    }
}
