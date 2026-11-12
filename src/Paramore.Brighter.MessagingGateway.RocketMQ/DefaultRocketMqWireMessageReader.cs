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
using System.Globalization;
using System.Net.Mime;
using Org.Apache.Rocketmq;
using Paramore.Brighter.Observability;

namespace Paramore.Brighter.MessagingGateway.RocketMQ;

/// <summary>Reads the default RocketMQ body and application property representation.</summary>
/// <remarks>The consumer attaches the native receipt and resolves delivery count after mapping.</remarks>
public sealed class DefaultRocketMqWireMessageReader : IAmAWireMessageReader<MessageView>
{
    /// <inheritdoc />
    public Message ReadMessage(MessageView message)
    {
        var topic = ReadTopic(message);
        var messageId = ReadMessageId(message);
        var timeStamp = ReadTimeStamp(message);
        var messageType = ReadMessageType(message);
        var correlationId = ReadCorrelationId(message);
        var partitionKey = ReadPartitionKey(message);
        var replyTo = ReadReplyTo(message);
        var contentType = ReadContentType(message) ?? new ContentType("plain/text");
        var handledCount = ReadHandledCount(message);
        var delay = ReadDelay(message);
        var source = ReadSource(message);
        var specVersion = ReadSpecVersion(message);
        var type = ReadType(message);
        var dateSchema = ReadDataSchema(message);
        var subject = ReadSubject(message);
        var dataRef = ReadDataRef(message);
        var traceParent = ReadTraceParent(message);
        var traceState = ReadTraceState(message);
        var baggage = ReadBaggage(message);

        var header = new MessageHeader(
            messageId: messageId,
            topic: topic,
            messageType,
            source: source,
            type: type,
            timeStamp: timeStamp,
            contentType: contentType,
            correlationId: correlationId,
            replyTo: replyTo,
            partitionKey: partitionKey,
            handledCount: handledCount,
            dataSchema: dateSchema,
            subject: subject,
            delayed: delay,
            traceParent: traceParent,
            traceState: traceState
        )
        {
            DataRef = dataRef,
            SpecVersion = specVersion,
            Baggage = baggage
        };

        foreach (var property in message.Properties)
        {
            header.Bag[property.Key] = property.Value;
        }

        var body = new MessageBody(message.Body, header.ContentType);

        return new Message(header, body);

        static RoutingKey ReadTopic(MessageView message) => new(message.Topic);
        static Id ReadMessageId(MessageView message)
        {
            return message.Properties.TryGetValue(HeaderNames.MessageId, out var messageId)
                ? Id.Create(messageId)
                : Id.Create(message.MessageId);
        }

        static DateTimeOffset ReadTimeStamp(MessageView message)
        {
            if (message.Properties.TryGetValue(HeaderNames.TimeStamp, out var timestamp)
                && DateTimeOffset.TryParse(timestamp, DateTimeFormatInfo.InvariantInfo, DateTimeStyles.AdjustToUniversal, out var datetime))
            {
                return datetime;
            }

            if (message.DeliveryTimestamp != null)
            {
                return message.DeliveryTimestamp.Value;
            }

            return DateTimeOffset.UtcNow;
        }

        static MessageType ReadMessageType(MessageView message)
        {
            if (message.Properties.TryGetValue(HeaderNames.MessageType, out var type) && Enum.TryParse<MessageType>(type, true, out var messageType))
            {
                return messageType;
            }

            return MessageType.MT_EVENT;
        }

        static Id? ReadCorrelationId(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.CorrelationId);
            if (string.IsNullOrEmpty(val))
            {
                return null;
            }

            return Id.Create(val);
        }

        static PartitionKey? ReadPartitionKey(MessageView message)
        {
            if (string.IsNullOrEmpty(message.MessageGroup))
            {
                return null;
            }

            return new PartitionKey(message.MessageGroup);
        }

        static RoutingKey? ReadReplyTo(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.ReplyTo);
            if (string.IsNullOrEmpty(val))
            {
                return null;
            }

            return new RoutingKey(val);
        }

        static ContentType? ReadContentType(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.DataContentType);
            if (!string.IsNullOrEmpty(val))
            {
                return new ContentType(val);
            }

            val = message.Properties.GetValueOrDefault(HeaderNames.ContentType);
            if (!string.IsNullOrEmpty(val))
            {
                return new ContentType(val);
            }

            return null;
        }

        static int ReadHandledCount(MessageView message)
        {
            if (message.Properties.TryGetValue(HeaderNames.HandledCount, out var handledCount)
                && int.TryParse(handledCount, out var count))
            {
                return count;
            }

            return 0;
        }

        static TimeSpan ReadDelay(MessageView message)
        {
            if (message.Properties.TryGetValue(HeaderNames.HandledCount, out var delayString)
                && TimeSpan.TryParse(delayString, out var delay))
            {
                return delay;
            }

            return TimeSpan.Zero;
        }

        static Uri ReadSource(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.Source);
            if (!string.IsNullOrEmpty(val) && Uri.TryCreate(val, UriKind.RelativeOrAbsolute, out var source))
            {
                return source;
            }

            return new Uri(MessageHeader.DefaultSource);
        }

        static string ReadSpecVersion(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.SpecVersion);
            if (!string.IsNullOrEmpty(val))
            {
                return val;
            }

            return MessageHeader.DefaultSpecVersion;
        }

        static CloudEventsType ReadType(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.Type);
            return string.IsNullOrEmpty(val) ? CloudEventsType.Empty : new CloudEventsType(val);
        }

        static Uri? ReadDataSchema(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.DataSchema);
            if (!string.IsNullOrEmpty(val) && Uri.TryCreate(val, UriKind.RelativeOrAbsolute, out var source))
            {
                return source;
            }

            return null;
        }

        static string? ReadSubject(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.Subject);
            if (!string.IsNullOrEmpty(val))
            {
                return val;
            }

            return null;
        }

        static string? ReadDataRef(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.DataRef);
            if (!string.IsNullOrEmpty(val))
            {
                return val;
            }

            return null;
        }

        static TraceParent? ReadTraceParent(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.TraceParent);
            if (!string.IsNullOrEmpty(val))
            {
                return new TraceParent(val);
            }

            return null;
        }

        static TraceState? ReadTraceState(MessageView message)
        {
            var val = message.Properties.GetValueOrDefault(HeaderNames.TraceState);
            if (!string.IsNullOrEmpty(val))
            {
                return new TraceState(val);
            }

            return null;
        }

        static Baggage ReadBaggage(MessageView message)
        {
            var baggage = new Baggage();
            var val = message.Properties.GetValueOrDefault(HeaderNames.Baggage);
            if (!string.IsNullOrEmpty(val))
            {
                baggage.LoadBaggage(val);
            }

            return baggage;
        }
    }

}
