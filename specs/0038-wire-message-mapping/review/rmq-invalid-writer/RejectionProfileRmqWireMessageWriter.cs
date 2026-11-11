#region Licence

/* The MIT License (MIT)
Copyright © 2026 gabisonia

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
using System.Text.Json;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using RabbitMQ.Client;

namespace Paramore.Brighter.RMQ.Async.Tests.TestDoubles;

internal sealed class RejectionProfileRmqWireMessageWriter(bool compose)
    : IAmAWireMessageWriter<RmqWireMessage>
{
    public int FailureMode { get; set; }

    public RmqWireMessage WriteMessage(Message message)
    {
        if (FailureMode == 1)
            throw new FormatException("Invalid-message mapping is temporarily unavailable.");
        if (FailureMode == 2)
            return null!;

        var proof = JsonSerializer.Serialize(new
        {
            id = message.Id.Value,
            payload = Convert.ToBase64String(message.Body.Memory.ToArray()),
            handledCount = message.Header.HandledCount,
            originalTopic = message.Header.Bag[HeaderNames.ORIGINAL_TOPIC],
            originalType = message.Header.Bag[HeaderNames.ORIGINAL_TYPE],
            reason = message.Header.Bag[HeaderNames.REJECTION_REASON],
            description = message.Header.Bag[HeaderNames.REJECTION_MESSAGE],
            timestamp = message.Header.Bag[HeaderNames.REJECTION_TIMESTAMP]
        });
        var wire = compose
            ? new DefaultRmqWireMessageWriter().WriteMessage(message)
            : new RmqWireMessage
            {
                Body = Encoding.UTF8.GetBytes(proof),
                Properties = new BasicProperties
                {
                    MessageId = message.Id.Value,
                    ContentType = "application/vnd.rejected-order+json"
                }
            };
        wire.Properties.DeliveryMode = DeliveryModes.Transient;
        var headers = wire.Properties.Headers ??= new Dictionary<string, object?>();
        headers["rejection-profile"] = "invalid-profile";
        headers[HeaderNames.DELAY_MILLISECONDS] = 60_000;
        headers[HeaderNames.DELIVERY_TAG] = "writer-owned-tag";
        if (compose)
            headers["rejection-proof"] = proof;
        return wire;
    }
}
