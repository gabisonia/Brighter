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

using System.Collections.Generic;
using System.Text;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;

namespace Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;

internal sealed class SyncEndpointWireWriter(bool composeDefault) : IAmAWireMessageWriter<RmqWireMessage>
{
    public RmqWireMessage WriteMessage(Message message)
    {
        var wire = composeDefault
            ? new DefaultRmqWireMessageWriter().WriteMessage(message)
            : new RmqWireMessage { Body = Encoding.UTF8.GetBytes($"legacy:{message.Body.Value}") };
        var configureDefaults = wire.ConfigureProperties;
        var messageId = message.Id.Value;
        var tenant = message.Header.Bag["tenant_id"];
        var traceParent = message.Header.TraceParent!.Value;
        wire.ConfigureProperties = properties =>
        {
            configureDefaults?.Invoke(properties);
            if (!composeDefault)
            {
                properties.MessageId = $"legacy-{messageId}";
                properties.Type = "legacy-order";
                properties.ContentType = "application/vnd.legacy.order";
                properties.Headers = new Dictionary<string, object>();
            }
            properties.Headers["tenant-id"] = tenant;
            properties.Headers["telemetry-parent"] = traceParent;
            properties.Headers[HeaderNames.DELIVERY_TAG] = "999";
            properties.Headers[HeaderNames.DELAY_MILLISECONDS] = 60000;
            properties.DeliveryMode = 1;
        };
        return wire;
    }
}
