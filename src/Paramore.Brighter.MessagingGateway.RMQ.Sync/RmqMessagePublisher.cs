#region Licence
/* The MIT License (MIT)
Copyright © 2014 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>
Copyright © 2026 Irakli Gabisonia

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
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Paramore.Brighter.Logging;
using RabbitMQ.Client;

namespace Paramore.Brighter.MessagingGateway.RMQ.Sync;

internal sealed partial class RmqMessagePublisher
{
    private static readonly ILogger s_logger = ApplicationLogging.CreateLogger<RmqMessagePublisher>();
    private static readonly DefaultRmqWireMessageWriter s_defaultWriter = new();
    private readonly IModel _channel;
    private readonly RmqMessagingGatewayConnection _connection;
    private readonly ChannelName? _destinationQueue;
    private readonly IAmAWireMessageWriter<RmqWireMessage> _wireMessageWriter;

    public RmqMessagePublisher(IModel channel, RmqMessagingGatewayConnection connection,
        IAmAWireMessageWriter<RmqWireMessage>? wireMessageWriter = null, ChannelName? destinationQueue = null)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _destinationQueue = destinationQueue;
        _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        _wireMessageWriter = wireMessageWriter ?? s_defaultWriter;
    }

    public void PublishMessage(Message message, TimeSpan? delay = null, bool mandatory = false)
    {
        if (_connection.Exchange is null)
            throw new InvalidOperationException("RmqMessagePublisher.PublishMessage: Connections Exchange is null");

        var wireMessage = _wireMessageWriter.WriteMessage(message);
        var properties = _channel.CreateBasicProperties();
        wireMessage.ConfigureProperties?.Invoke(properties);
        properties.DeliveryMode = (byte)(message.Persist ? 2 : 1);
        var headers = properties.Headers ??= new Dictionary<string, object>();
        headers.Remove(HeaderNames.DELIVERY_TAG);
        headers.Remove(HeaderNames.DELAY_MILLISECONDS);
        var deliveryTag = message.Header.Bag.ContainsKey(HeaderNames.DELIVERY_TAG) ? message.DeliveryTag.ToString() : null;
        AddDeliveryHeaders(delay, headers, deliveryTag);

        _channel.BasicPublish(PublicationExchange(delay),
            _destinationQueue?.Value ?? message.Header.Topic.Value, mandatory,
            properties, wireMessage.Body);
    }

    public void RequeueMessage(Message message, ChannelName queueName, TimeSpan timeOut)
    {
        var messageId = Uuid.NewAsString();
        const string deliveryTag = "1";
        Log.RequeueMessageInformation(s_logger, message.Id.Value, deliveryTag, messageId, 1);

        var wireMessage = _wireMessageWriter.WriteMessage(message);
        var properties = _channel.CreateBasicProperties();
        wireMessage.ConfigureProperties?.Invoke(properties);
        properties.MessageId = messageId;
        properties.DeliveryMode = (byte)(message.Persist ? 2 : 1);
        var headers = properties.Headers ??= new Dictionary<string, object>();
        headers[HeaderNames.HANDLED_COUNT] = message.Header.HandledCount;
        headers.Remove(HeaderNames.DELAY_MILLISECONDS);
        AddDeliveryHeaders(timeOut, headers, deliveryTag);
        AddOriginalMessageIdOnRepublish(message, headers);

        _channel.BasicPublish(
            timeOut > TimeSpan.Zero ? RmqDelayedRequeue.ExchangeName(_connection) : string.Empty,
            queueName.Value, false, properties, wireMessage.Body);
    }

    private string PublicationExchange(TimeSpan? delay)
    {
        if (_destinationQueue == null)
            return _connection.Exchange!.Name;

        return delay > TimeSpan.Zero && _connection.Exchange!.SupportDelay
            ? RmqDelayedRequeue.ExchangeName(_connection)
            : string.Empty;
    }

    private static void AddOriginalMessageIdOnRepublish(Message message, IDictionary<string, object> headers)
    {
        var originalMessageId = message.Header.Bag.FirstOrDefault(h =>
            h.Key.Equals(HeaderNames.ORIGINAL_MESSAGE_ID, StringComparison.OrdinalIgnoreCase));
        foreach (var headerName in headers.Keys.Where(key =>
                     key.Equals(HeaderNames.ORIGINAL_MESSAGE_ID, StringComparison.OrdinalIgnoreCase)).ToArray())
        {
            headers.Remove(headerName);
        }

        if (originalMessageId.Key is null)
        {
            headers[HeaderNames.ORIGINAL_MESSAGE_ID] = message.Id.Value;
        }
        else
        {
            headers[originalMessageId.Key] = originalMessageId.Value;
        }
    }

    private static void AddDeliveryHeaders(TimeSpan? delay, IDictionary<string, object> headers, string? deliveryTag)
    {
        if (!string.IsNullOrEmpty(deliveryTag))
        {
            headers[HeaderNames.DELIVERY_TAG] = deliveryTag!;
        }

        if (delay > TimeSpan.Zero)
        {
            headers[HeaderNames.DELAY_MILLISECONDS] = delay.Value.TotalMilliseconds;
        }
    }

    private static partial class Log
    {
        [LoggerMessage(LogLevel.Information, "RmqMessagePublisher: Regenerating message {OldMessageId} with DeliveryTag of {OldDeliveryTag} to {MessageId} with DeliveryTag of {DeliveryTag}")]
        public static partial void RequeueMessageInformation(ILogger logger, string oldMessageId, string oldDeliveryTag, string messageId, int deliveryTag);
    }
}
