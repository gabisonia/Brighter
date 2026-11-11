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

using System;
using System.Collections.Generic;
using System.Linq;
using Paramore.Brighter.Extensions;

namespace Paramore.Brighter.MessagingGateway.RocketMQ;

/// <summary>Writes the default RocketMQ body and application property representation.</summary>
/// <remarks>Each call returns a fresh payload suitable for composition by a custom writer.</remarks>
public sealed class DefaultRocketMqWireMessageWriter : IAmAWireMessageWriter<RocketMqWireMessage>
{
    /// <inheritdoc />
    public RocketMqWireMessage WriteMessage(Message message)
    {
        var wire = new RocketMqWireMessage(message.Body.Memory.ToArray());
        var headerOwnedKeys = AddHeaderProperties(wire, message.Id, message.Header);
        foreach (var (key, val) in message.Header.Bag
                     .Where(x => x.Key != HeaderNames.Keys
                                 && x.Key != HeaderNames.Tag
                                 && !MessageHeader.IsLocalHeader(x.Key)
                                 && !headerOwnedKeys.Contains(x.Key)))
        {
            wire.Properties[key] = val.ToString()!;
        }
        return wire;
    }

    private static HashSet<string> AddHeaderProperties(
        RocketMqWireMessage wire, Id messageId, MessageHeader header)
    {
        var writtenKeys = new HashSet<string>(StringComparer.Ordinal);

        void Add(string key, string? value)
        {
            wire.Properties[key] = value!;
            writtenKeys.Add(key);
        }

        Add(HeaderNames.MessageId, messageId);
        Add(HeaderNames.Topic, header.Topic.Value);
        Add(HeaderNames.HandledCount, header.HandledCount.ToString());
#pragma warning disable CS0618 // Preserve the legacy message type for transport compatibility.
        Add(HeaderNames.MessageType, header.MessageType.ToString());
#pragma warning restore CS0618
        Add(HeaderNames.TimeStamp, header.TimeStamp.ToRfc3339());
        Add(HeaderNames.Source, header.Source.ToString());
        Add(HeaderNames.SpecVersion, header.SpecVersion);

        var baggage = header.Baggage.ToString();
        if (!string.IsNullOrEmpty(baggage))
        {
            Add(HeaderNames.Baggage, baggage);
        }

        if (header.Type != CloudEventsType.Empty)
        {
            Add(HeaderNames.Type, header.Type);
        }

        if (!string.IsNullOrEmpty(header.Subject))
        {
            Add(HeaderNames.Subject, header.Subject);
        }

        if (header.DataSchema != null)
        {
            Add(HeaderNames.DataSchema, header.DataSchema.ToString());
        }

        Add(HeaderNames.ContentType, header.ContentType.ToString());
        Add(HeaderNames.DataContentType, header.ContentType.ToString());

        if (!string.IsNullOrEmpty(header.CorrelationId))
        {
            Add(HeaderNames.CorrelationId, header.CorrelationId);
        }

        if (!RoutingKey.IsNullOrEmpty(header.ReplyTo))
        {
            Add(HeaderNames.ReplyTo, header.ReplyTo);
        }

        if (!string.IsNullOrEmpty(header.DataRef))
        {
            Add(HeaderNames.DataRef, header.DataRef);
        }

        if (!TraceParent.IsNullOrEmpty(header.TraceParent))
        {
            Add(HeaderNames.TraceParent, header.TraceParent.Value);
        }

        if (!TraceState.IsNullOrEmpty(header.TraceState))
        {
            Add(HeaderNames.TraceState, header.TraceState.Value);
        }

        return writtenKeys;
    }
}
