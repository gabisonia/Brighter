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
using Amazon.SQS.Model;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.JsonConverters;

namespace Paramore.Brighter.MessagingGateway.AWSSQS.V4;

/// <summary>
/// Maps a Brighter message to the default SQS body and attributes.
/// </summary>
/// <param name="queueUrl">The queue URL written to the default topic attribute.</param>
/// <remarks>
/// Compose this writer with a custom writer to retain the default CloudEvents and Brighter
/// attributes while changing selected fields. The sender owns the actual destination and delivery controls.
/// Each call creates independent attributes and does not modify the source message's bag.
/// </remarks>
public sealed class DefaultSqsWireMessageWriter(string queueUrl) : IAmAWireMessageWriter<SqsWireMessage>
{
    /// <inheritdoc />
    public SqsWireMessage WriteMessage(Message message)
    {
        return new SqsWireMessage
        {
            Body = message.Body.Value,
            MessageAttributes = CreateMessageAttributes(message)
        };
    }

    private Dictionary<string, MessageAttributeValue> CreateMessageAttributes(Message message)
    {
        string cloudEventHeadersJson = CreateCloudEventHeadersJson(message);

        var contentType = message.Header.ContentType ?? new ContentType(MediaTypeNames.Text.Plain);
        var messageAttributes = new Dictionary<string, MessageAttributeValue>
        {
            [HeaderNames.Id] = new() { StringValue = message.Header.MessageId, DataType = "String" },
            [HeaderNames.CloudEventHeaders] = new() { StringValue = cloudEventHeadersJson, DataType = "String" },
            [HeaderNames.Topic] = new() { StringValue = queueUrl, DataType = "String" },
#pragma warning disable CS0618 // Preserve the legacy message type for transport compatibility.
            [HeaderNames.MessageType] = new() { StringValue = message.Header.MessageType.ToString(), DataType = "String" },
#pragma warning restore CS0618
            [HeaderNames.ContentType] = new() { StringValue = contentType.ToString(), DataType = "String" },
            [HeaderNames.Timestamp] = new() { StringValue = Convert.ToString(message.Header.TimeStamp.ToRfc3339()), DataType = "String" }
        };

        if (!RoutingKey.IsNullOrEmpty(message.Header.ReplyTo))
        {
            messageAttributes.Add(HeaderNames.ReplyTo, new MessageAttributeValue { StringValue = message.Header.ReplyTo, DataType = "String" });
        }

        if (!string.IsNullOrEmpty(message.Header.Subject))
        {
            messageAttributes.Add(HeaderNames.Subject, new MessageAttributeValue { StringValue = message.Header.Subject, DataType = "String" });
        }

        if (!Id.IsNullOrEmpty(message.Header.CorrelationId))
        {
            messageAttributes.Add(HeaderNames.CorrelationId, new MessageAttributeValue { StringValue = message.Header.CorrelationId, DataType = "String" });
        }

        messageAttributes[HeaderNames.Bag] = new() { StringValue = CreateMessageBagJson(message), DataType = "String" };
        return messageAttributes;
    }

    private static string CreateMessageBagJson(Message message)
    {
        var bag = message.Header.BagWithoutLocalHeaders();
        if (!MessageHeader.IsLocalHeader(HeaderNames.HandledCount))
        {
            bag[HeaderNames.HandledCount] = message.Header.HandledCount.ToString(CultureInfo.InvariantCulture);
        }

        return System.Text.Json.JsonSerializer.Serialize(bag, JsonSerialisationOptions.Options);
    }

    private static string CreateCloudEventHeadersJson(Message message)
    {
        var contentType = message.Header.ContentType ?? new ContentType(MediaTypeNames.Text.Plain);
        var cloudEventHeaders = new Dictionary<string, string>
        {
            [HeaderNames.Id] = Convert.ToString(message.Header.MessageId),
            [HeaderNames.DataContentType] = contentType.ToString(),
            [HeaderNames.DataSchema] = message.Header.DataSchema?.ToString() ?? string.Empty,
            [HeaderNames.SpecVersion] = message.Header.SpecVersion,
            [HeaderNames.Type] = message.Header.Type.Value,
            [HeaderNames.Source] = message.Header.Source.ToString(),
            [HeaderNames.Time] = message.Header.TimeStamp.ToRfc3339(),
            [HeaderNames.Baggage] = message.Header.Baggage.ToString()
        };

        if (!string.IsNullOrEmpty(message.Header.Subject))
        {
            cloudEventHeaders[HeaderNames.Subject] = message.Header.Subject!;
        }

        if (message.Header.DataSchema != null)
        {
            cloudEventHeaders[HeaderNames.DataSchema] = message.Header.DataSchema.ToString();
        }

        if (message.Header.DataRef != null)
        {
            cloudEventHeaders[HeaderNames.DataRef] = message.Header.DataRef;
        }
        
        if (message.Header.TraceParent != null)
        {
            cloudEventHeaders[HeaderNames.TraceParent] = message.Header.TraceParent.Value;
        }

        if (message.Header.TraceState != null)
        {
            cloudEventHeaders[HeaderNames.TraceState] = message.Header.TraceState.Value;
        }

        var cloudEventHeadersJson = System.Text.Json.JsonSerializer.Serialize(cloudEventHeaders, JsonSerialisationOptions.Options);
        return cloudEventHeadersJson;
    }
}
