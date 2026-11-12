#region Licence

/* The MIT License (MIT)
Copyright © 2014 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>
Copyright © 2026 gabisonia

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using Paramore.Brighter.Extensions;
using RabbitMQ.Client;

namespace Paramore.Brighter.MessagingGateway.RMQ.Async;

/// <summary>
/// Maps Brighter messages to the default RabbitMQ wire representation.
/// </summary>
/// <remarks>
/// Compose this writer to customize selected properties or headers. Each call creates fresh
/// properties and headers without changing the source message. The body retains its read-only
/// memory representation. The gateway applies destination and delivery controls separately.
/// </remarks>
public sealed class DefaultRmqWireMessageWriter : IAmAWireMessageWriter<RmqWireMessage>
{
    private static readonly HashSet<string> _headersToReset =
    [
        HeaderNames.DELAY_MILLISECONDS,
        HeaderNames.MESSAGE_TYPE,
        HeaderNames.TOPIC,
        HeaderNames.HANDLED_COUNT,
        HeaderNames.DELIVERY_TAG,
        HeaderNames.CORRELATION_ID
    ];

    /// <summary>
    /// Maps a message using the default RabbitMQ body, properties and headers.
    /// </summary>
    /// <param name="message">The Brighter message to map.</param>
    /// <returns>Native properties and headers with the message body.</returns>
    /// <exception cref="ConfigurationException">A header value has an unsupported AMQP type.</exception>
    public RmqWireMessage WriteMessage(Message message)
    {
        var headers = AddCloudEventsHeaders(message);
        AddUserDefinedHeaders(message, headers);
        return new RmqWireMessage
        {
            Properties = CreateBasicProperties(
                message.Id.Value,
                message.Header.TimeStamp,
                message.Body.ContentType is not null ? message.Body.ContentType.ToString() : MediaTypeNames.Text.Plain,
                message.Header.ContentType is not null ? message.Header.ContentType.ToString() : MediaTypeNames.Text.Plain,
                message.Header.ReplyTo?.Value ?? string.Empty,
                message.Persist,
                headers),
            Body = message.Body.Memory
        };
    }

    private static Dictionary<string, object?> AddCloudEventsHeaders(Message message)
    {
        var headers = new Dictionary<string, object?>
        {
            [HeaderNames.CLOUD_EVENTS_ID] = message.Header.MessageId.Value,
            [HeaderNames.CLOUD_EVENTS_SPEC_VERSION] = message.Header.SpecVersion,
            [HeaderNames.CLOUD_EVENTS_TYPE] = message.Header.Type.Value,
            [HeaderNames.CLOUD_EVENTS_SOURCE] = message.Header.Source.ToString(),
            [HeaderNames.CLOUD_EVENTS_TIME] = message.Header.TimeStamp.ToRfc3339(),

#pragma warning disable CS0618 // Preserve the legacy message type for transport compatibility.
            [HeaderNames.MESSAGE_TYPE] = message.Header.MessageType.ToString(),
#pragma warning restore CS0618
            [HeaderNames.TOPIC] = message.Header.Topic.Value,
            [HeaderNames.HANDLED_COUNT] = message.Header.HandledCount,
        };

        if (!string.IsNullOrEmpty(message.Header.Subject))
        {
            headers.Add(HeaderNames.CLOUD_EVENTS_SUBJECT, message.Header.Subject);
        }

        if (message.Header.DataSchema != null)
        {
            headers.Add(HeaderNames.CLOUD_EVENTS_DATA_SCHEMA, message.Header.DataSchema.ToString());
        }

        if (!Id.IsNullOrEmpty(message.Header.CorrelationId))
            headers.Add(HeaderNames.CORRELATION_ID, message.Header.CorrelationId.Value);

        if (!string.IsNullOrEmpty(message.Header.TraceParent?.Value))
            headers.Add(HeaderNames.CLOUD_EVENTS_TRACE_PARENT, message.Header.TraceParent?.Value!);

        if (!string.IsNullOrEmpty(message.Header.TraceState?.Value))
        {
            headers.Add(HeaderNames.CLOUD_EVENTS_TRACE_STATE, message.Header.TraceState?.Value!);
#pragma warning disable CS0618 // Type or member is obsolete
            headers.Add(HeaderNames.CLOUD_EVENTS_TRACE_STATE_DEPRECATED, message.Header.TraceState?.Value!);
#pragma warning restore CS0618 // Type or member is obsolete
        }

        if (message.Header.Baggage.Any())
            headers.Add(HeaderNames.W3C_BAGGAGE, message.Header.Baggage.ToString());
        return headers;
    }

    private static void AddUserDefinedHeaders(Message message, Dictionary<string, object?> headers)
    {
        message.Header.Bag.Each(header =>
        {
            if (!_headersToReset.Contains(header.Key) && !MessageHeader.IsLocalHeader(header.Key))
            {
                headers[header.Key] = header.Value;
            }
        });
    }

    private static BasicProperties CreateBasicProperties(
        string id,
        DateTimeOffset timeStamp,
        string type,
        string contentType,
        string replyTo,
        bool persistMessage,
        IDictionary<string, object?>? headers = null)
    {
        var basicProperties = new BasicProperties
        {
            DeliveryMode =
                persistMessage
                    ? DeliveryModes.Persistent
                    : DeliveryModes.Transient,
            ContentType = contentType,
            Type = type,
            MessageId = id,
            Timestamp = new AmqpTimestamp(UnixTimestamp.GetUnixTimestampSeconds(timeStamp.DateTime))
        };

        if (!string.IsNullOrEmpty(replyTo))
        {
            basicProperties.ReplyTo = replyTo;
        }

        if (headers is not null)
        {
            foreach (var header in headers)
            {
                if (header.Value is not null && !IsAnAmqpType(header.Value))
                {
                    throw new ConfigurationException(
                        $"The value {header.Value} is type {header.Value.GetType()} for header {header.Key} value only supports the AMQP 0-8/0-9 standard entry types S, I, D, T and F, as well as the QPid-0-8 specific b, d, f, l, s, t, x and V types and the AMQP 0-9-1 A type.");
                }
            }

            basicProperties.Headers = headers;
        }

        return basicProperties;
    }

    private static bool IsAnAmqpType(object value)
    {
        return value switch
        {
            null or string _ or byte[] _ or int _ or uint _ or decimal _ or AmqpTimestamp _ or IDictionary _ or IList _
                or byte _ or sbyte _ or double _ or float _ or long _ or short _ or bool _ => true,
            _ => false
        };
    }
}
