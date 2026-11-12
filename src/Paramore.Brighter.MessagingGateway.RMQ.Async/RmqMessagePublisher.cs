#region Licence

/* The MIT License (MIT)
Copyright © 2014 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Paramore.Brighter.Logging;
using RabbitMQ.Client;

namespace Paramore.Brighter.MessagingGateway.RMQ.Async;

/// <summary>
/// Class RmqMessagePublisher.
/// </summary>
internal sealed partial class RmqMessagePublisher
{
    private static readonly ILogger s_logger = ApplicationLogging.CreateLogger<RmqMessagePublisher>();

    private static readonly DefaultRmqWireMessageWriter s_defaultWireMessageWriter = new();

    private readonly IAmAWireMessageWriter<RmqWireMessage> _wireMessageWriter;
    private readonly IChannel _channel;
    private readonly ChannelName? _destinationQueue;
    private readonly RmqMessagingGatewayConnection _connection;

    /// <summary>
    /// Initializes a new instance of the <see cref="RmqMessagePublisher"/> class.
    /// </summary>
    /// <param name="channel">The channel.</param>
    /// <param name="connection">The exchange we want to talk to.</param>
    /// <exception cref="System.ArgumentNullException">
    /// channel
    /// or
    /// exchangeName
    /// </exception>
    public RmqMessagePublisher(IChannel channel, RmqMessagingGatewayConnection connection)
        : this(channel, connection, null)
    {
    }

    public RmqMessagePublisher(IChannel channel, RmqMessagingGatewayConnection connection,
        IAmAWireMessageWriter<RmqWireMessage>? wireMessageWriter, ChannelName? destinationQueue = null)
    {
        if (channel is null)
        {
            throw new ArgumentNullException(nameof(channel));
        }

        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        _connection = connection;
        _destinationQueue = destinationQueue;

        _channel = channel;
        _wireMessageWriter = wireMessageWriter ?? s_defaultWireMessageWriter;
    }

    /// <summary>
    /// Publishes the message.
    /// </summary>
    /// <param name="message">The message.</param>
    /// <param name="delay">The delay in ms. 0 is no delay. Defaults to 0</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> that cancels the Publish operation</param>
    /// <param name="mandatory">Return the message if no queue matches its routing key.</param>
    public async Task PublishMessageAsync(Message message, TimeSpan? delay = null, CancellationToken cancellationToken = default, bool mandatory = false)
    {
        if (_connection.Exchange is null)
            throw new InvalidOperationException("RMQMessagingGateway: No Exchange specified");

        var deliveryTag = message.Header.Bag.ContainsKey(HeaderNames.DELIVERY_TAG) ? message.DeliveryTag.ToString() : null;
        var wireMessage = _wireMessageWriter.WriteMessage(message);
        wireMessage.Properties.DeliveryMode = message.Persist ? DeliveryModes.Persistent : DeliveryModes.Transient;
        wireMessage.Properties.Headers ??= new Dictionary<string, object?>();
        AddDeliveryHeaders(delay, deliveryTag, wireMessage.Properties.Headers);

        await _channel.BasicPublishAsync(
            PublicationExchange(delay),
            _destinationQueue?.Value ?? message.Header.Topic.Value,
            mandatory,
            wireMessage.Properties,
            wireMessage.Body,
            cancellationToken);
    }

    /// <summary>
    /// Requeues the message.
    /// </summary>
    /// <param name="message">The message.</param>
    /// <param name="queueName">The queue name.</param>
    /// <param name="timeOut">Delay. Set to TimeSpan.Zero for not delay</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> that cancels the requeue</param>
    public async Task RequeueMessageAsync(Message message, ChannelName queueName, TimeSpan timeOut, CancellationToken cancellationToken = default)
    {
        var messageId = Uuid.NewAsString();
        const string deliveryTag = "1";

        Log.RegeneratingMessage(s_logger, message.Id.Value, deliveryTag, messageId, 1);

        var wireMessage = _wireMessageWriter.WriteMessage(message);
        wireMessage.Properties.MessageId = messageId;
        wireMessage.Properties.DeliveryMode = message.Persist ? DeliveryModes.Persistent : DeliveryModes.Transient;
        var headers = wireMessage.Properties.Headers ??= new Dictionary<string, object?>();
        headers[HeaderNames.HANDLED_COUNT] = message.Header.HandledCount;
        AddDeliveryHeaders(timeOut, deliveryTag, headers);
        AddOriginalMessageIdOnRepublish(message, headers);

        // Immediate retries use the default exchange; delayed retries use queue-specific bindings.
        await _channel.BasicPublishAsync(
            timeOut > TimeSpan.Zero ? RmqDelayedRequeue.ExchangeName(_connection) : string.Empty,
            queueName.Value,
            false,
            wireMessage.Properties,
            wireMessage.Body, cancellationToken);
    }

    private string PublicationExchange(TimeSpan? delay)
    {
        if (_destinationQueue == null)
            return _connection.Exchange!.Name;

        return delay > TimeSpan.Zero && _connection.Exchange!.SupportDelay
            ? RmqDelayedRequeue.ExchangeName(_connection)
            : string.Empty;
    }

    private static void AddDeliveryHeaders(TimeSpan? delay, string? deliveryTag, IDictionary<string, object?> headers)
    {
        headers.Remove(HeaderNames.DELIVERY_TAG);
        if (!string.IsNullOrEmpty(deliveryTag))
        {
            headers[HeaderNames.DELIVERY_TAG] = deliveryTag!;
        }

        headers.Remove(HeaderNames.DELAY_MILLISECONDS);
        if (delay > TimeSpan.Zero)
        {
            headers[HeaderNames.DELAY_MILLISECONDS] = delay.Value.TotalMilliseconds;
        }
    }

    private static void AddOriginalMessageIdOnRepublish(Message message, IDictionary<string, object?> headers)
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

    private static partial class Log
    {
        [LoggerMessage(LogLevel.Information, "RmqMessagePublisher: Regenerating message {OldMessageId} with DeliveryTag of {OldDeliveryTag} to {MessageId} with DeliveryTag of {DeliveryTag}")]
        public static partial void RegeneratingMessage(ILogger logger, string oldMessageId, string oldDeliveryTag, string messageId, int deliveryTag);
    }
}
