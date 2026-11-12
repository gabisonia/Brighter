using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Org.Apache.Rocketmq;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.Logging;
using Paramore.Brighter.Tasks;
using Paramore.Brighter.Observability;

namespace Paramore.Brighter.MessagingGateway.RocketMQ;

/// <summary>
/// RocketMQ message consumer implementation for Brighter.
/// Integrates RocketMQ's consumer group pattern and message filtering capabilities.
/// </summary>
/// <param name="consumer">The underlying RocketMQ simple consumer.</param>
/// <param name="bufferSize">The number of messages to retrieve per receive call.</param>
/// <param name="invisibilityTimeout">How long messages remain invisible after being received.</param>
/// <param name="connection">The gateway connection configuration, used for lazy DLQ producer creation.</param>
/// <param name="deadLetterRoutingKey">The routing key for the dead letter queue topic.</param>
/// <param name="invalidMessageRoutingKey">The routing key for the invalid message topic.</param>
public partial class RocketMessageConsumer(SimpleConsumer consumer,
    int bufferSize,
    TimeSpan invisibilityTimeout,
    RocketMessagingGatewayConnection? connection = null,
    RoutingKey? deadLetterRoutingKey = null,
    RoutingKey? invalidMessageRoutingKey = null)
    : IAmAMessageConsumerAsync, IAmAMessageConsumerSync, IHaveAMessagingSystem
{
    /// <inheritdoc />
    public MessagingSystem MessagingSystem => MessagingSystem.RocketMQ;

    private static readonly ILogger s_logger = ApplicationLogging.CreateLogger<RocketMessageConsumer>();
    private static readonly DefaultRocketMqWireMessageReader s_defaultWireMessageReader = new();
    private readonly IAmAWireMessageReader<MessageView>? _wireMessageReader;
    private readonly IAmAWireMessageWriter<RocketMqWireMessage>? _deadLetterWireMessageWriter;
    private readonly IAmAWireMessageWriter<RocketMqWireMessage>? _invalidMessageWireMessageWriter;

    internal RocketMessageConsumer(SimpleConsumer consumer, int bufferSize, TimeSpan invisibilityTimeout,
        RocketMessagingGatewayConnection? connection, RoutingKey? deadLetterRoutingKey,
        RoutingKey? invalidMessageRoutingKey, IAmAWireMessageReader<MessageView>? wireMessageReader,
        IAmAWireMessageWriter<RocketMqWireMessage>? deadLetterWireMessageWriter = null,
        IAmAWireMessageWriter<RocketMqWireMessage>? invalidMessageWireMessageWriter = null)
        : this(consumer, bufferSize, invisibilityTimeout, connection, deadLetterRoutingKey, invalidMessageRoutingKey)
    {
        _wireMessageReader = wireMessageReader;
        _deadLetterWireMessageWriter = deadLetterWireMessageWriter;
        _invalidMessageWireMessageWriter = invalidMessageWireMessageWriter;
    }

    // The broker rejects a longer invisible duration (response code 40011)
    private static readonly TimeSpan s_maxInvisibleDuration = TimeSpan.FromHours(12);

    private readonly RocketMessagingGatewayConnection? _connection = connection;
    private readonly RoutingKey? _deadLetterRoutingKey = deadLetterRoutingKey;
    private readonly RoutingKey? _invalidMessageRoutingKey = invalidMessageRoutingKey;
    // Thread-safe: message pumps are single-threaded per consumer, so null-coalescing
    // assignment in GetProducerForRouteAsync() cannot race.
    private RocketMqMessageProducer? _deadLetterProducer;
    private RocketMqMessageProducer? _invalidMessageProducer;

    /// <inheritdoc />
    public void Acknowledge(Message message) 
        => BrighterAsyncContext.Run(() => AcknowledgeAsync(message));
    
    /// <inheritdoc />
    public async Task AcknowledgeAsync(Message message, CancellationToken cancellationToken = default)
    {
        if (!message.Header.Bag.TryGetValue("ReceiptHandle", out var handler) || handler is not MessageView view)
        {
           return;
        }
        
        await consumer.Ack(view);
    }
    
    /// <inheritdoc />
    public void Purge() 
        => BrighterAsyncContext.Run(() => PurgeAsync());

    /// <inheritdoc />
    public async Task PurgeAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var messages = await consumer.Receive(bufferSize, invisibilityTimeout);
            if (messages == null || messages.Count == 0)
            {
                break;
            }
            
            await messages.EachAsync(async message => await consumer.Ack(message));
        }
    }
    
    /// <inheritdoc />
    public Message[] Receive(TimeSpan? timeOut = null)
        => BrighterAsyncContext.Run(() => ReceiveAsync(timeOut));

    /// <inheritdoc />
    public async Task<Message[]> ReceiveAsync(TimeSpan? timeOut = null, CancellationToken cancellationToken = default(CancellationToken))
    {
        var messageView = await consumer.Receive(bufferSize, invisibilityTimeout);
        if (messageView == null || messageView.Count == 0)
        {
            return [new Message()];
        }
        
        var messages = new Message[messageView.Count];
        for (int i = 0; i < messageView.Count; i++)
        {
            messages[i] = CreateMessage(messageView[i]);
        }
        
        return messages;
    }
    
    /// <inheritdoc />
    public void Nack(Message message)
        => BrighterAsyncContext.Run(() => NackAsync(message));

    /// <inheritdoc />
    /// <remarks>
    /// Sets the message's invisible duration to zero, so the broker makes it available on the next receive
    /// instead of waiting for the receive-time invisibility timeout to lapse.
    /// </remarks>
    public async Task NackAsync(Message message, CancellationToken cancellationToken = default)
    {
        if (!message.Header.Bag.TryGetValue("ReceiptHandle", out var handler) || handler is not MessageView view)
        {
            return;
        }

        await ChangeInvisibleDurationSafeAsync(view, message.Id, TimeSpan.Zero);
    }

    /// <inheritdoc />
    public bool Reject(Message message, MessageRejectionReason? reason)
        => BrighterAsyncContext.Run(() => RejectAsync(message, reason));


    /// <inheritdoc />
    public async Task<bool> RejectAsync(Message message, MessageRejectionReason? reason = null, CancellationToken cancellationToken = default)
    {
        if (!message.Header.Bag.TryGetValue("ReceiptHandle", out var handler) || handler is not MessageView view)
        {
            return false;
        }

        Log.RejectingMessage(s_logger, message.Id.Value);

        if (_deadLetterRoutingKey == null && _invalidMessageRoutingKey == null)
        {
            if (reason != null)
                Log.NoChannelsConfiguredForRejection(s_logger, message.Id.Value, reason.RejectionReason.ToString());

            await consumer.Ack(view);
            return true;
        }

        var rejectionReason = reason?.RejectionReason ?? RejectionReason.None;

        try
        {
            RefreshMetadata(message, reason);

            var (routingKey, shouldRoute, isFallingBackToDlq) = DetermineRejectionRoute(rejectionReason);

            RocketMqMessageProducer? producer = null;
            if (shouldRoute)
            {
                message.Header.Topic = routingKey!;
                if (isFallingBackToDlq)
                    Log.FallingBackToDlq(s_logger, message.Id.Value);

                producer = await GetProducerForRouteAsync(routingKey!);
            }

            if (producer != null)
            {
                await producer.SendAsync(message, cancellationToken);
                Log.MessageSentToRejectionChannel(s_logger, message.Id.Value, rejectionReason.ToString());
            }
            else
            {
                Log.NoChannelsConfiguredForRejection(s_logger, message.Id.Value, rejectionReason.ToString());
            }
        }
        catch (Exception ex)
        {
            Log.ErrorSendingToRejectionChannel(s_logger, ex, message.Id.Value, rejectionReason.ToString());
            return true;
        }
        finally
        {
            await AckSourceMessageSafeAsync(view);
        }

        return true;
    }
    
    /// <inheritdoc />
    public bool Requeue(Message message, TimeSpan? delay = null)
        => BrighterAsyncContext.Run(() => RequeueAsync(message, delay));

    /// <inheritdoc />
    /// <remarks>
    /// Sets the message's invisible duration to <paramref name="delay"/>, so the broker redelivers it once
    /// the delay has passed, whether that is shorter or longer than the receive-time invisibility timeout.
    /// </remarks>
    public async Task<bool> RequeueAsync(Message message, TimeSpan? delay = null, CancellationToken cancellationToken = default)
    {
        if (!message.Header.Bag.TryGetValue("ReceiptHandle", out var handler) || handler is not MessageView view)
        {
            return false;
        }

        await ChangeInvisibleDurationSafeAsync(view, message.Id, InvisibleDurationFor(message.Id, delay ?? TimeSpan.Zero));
        return true;
    }

    /// <summary>
    /// Brings a requeue delay within the range the broker accepts for an invisible duration: a negative
    /// delay becomes zero, and one above the broker's maximum is held at that maximum, the closest it allows.
    /// </summary>
    private static TimeSpan InvisibleDurationFor(Id messageId, TimeSpan delay)
    {
        if (delay < TimeSpan.Zero) return TimeSpan.Zero;
        if (delay <= s_maxInvisibleDuration) return delay;

        Log.RequeueDelayAboveMaximum(s_logger, messageId.Value, delay, s_maxInvisibleDuration);
        return s_maxInvisibleDuration;
    }

    /// <summary>
    /// Changes how long the message stays invisible, logging rather than throwing on failure. The
    /// message still holds its receive-time invisibility timeout, so on failure it reappears when that
    /// lapses: throwing would stop the message pump, and reporting failure would make the pump
    /// acknowledge, and so lose, the message.
    /// </summary>
    private async Task ChangeInvisibleDurationSafeAsync(MessageView view, Id messageId, TimeSpan invisibleDuration)
    {
        try
        {
            await consumer.ChangeInvisibleDuration(view, invisibleDuration);
        }
        catch (Exception ex)
        {
            Log.ErrorChangingInvisibleDuration(s_logger, ex, messageId.Value, invisibleDuration);
        }
    }

    /// <summary>
    /// Acknowledges the source message, returning <c>false</c> on failure so that an ACK
    /// exception in a <c>finally</c> block cannot mask the DLQ send result.
    /// On failure the message will reappear after the invisibility timeout as a safety net.
    /// </summary>
    private async Task<bool> AckSourceMessageSafeAsync(MessageView view)
    {
        try
        {
            await consumer.Ack(view);
            return true;
        }
        catch (Exception ackEx)
        {
            Log.ErrorAckingSourceMessage(s_logger, ackEx);
            return false;
        }
    }

    private async Task<RocketMqMessageProducer?> GetProducerForRouteAsync(RoutingKey routingKey)
    {
        if (routingKey == _invalidMessageRoutingKey)
            return _invalidMessageProducer ??= await CreateProducerAsync(_invalidMessageRoutingKey, _invalidMessageWireMessageWriter);
        if (routingKey == _deadLetterRoutingKey)
            return _deadLetterProducer ??= await CreateProducerAsync(_deadLetterRoutingKey, _deadLetterWireMessageWriter);
        return null;
    }

    private async Task<RocketMqMessageProducer?> CreateProducerAsync(RoutingKey? routingKey,
        IAmAWireMessageWriter<RocketMqWireMessage>? writer)
    {
        if (routingKey == null || _connection == null)
            return null;

        try
        {
            var rocketProducer = await new Producer.Builder()
                .SetClientConfig(_connection.ClientConfig)
                .SetMaxAttempts(_connection.MaxAttempts)
                .SetTopics(routingKey.Value)
                .Build();

            return new RocketMqMessageProducer(
                _connection,
                rocketProducer,
                new RocketMqPublication { Topic = routingKey, WireMessageWriter = writer });
        }
        catch (Exception ex)
        {
            Log.ErrorCreatingProducer(s_logger, ex, routingKey.Value);
            return null;
        }
    }

    private static void RefreshMetadata(Message message, MessageRejectionReason? reason)
    {
        message.Header.Bag[RejectionMetadataKeyNames.OriginalTopic] = message.Header.Topic.Value;
        message.Header.Bag[RejectionMetadataKeyNames.RejectionTimestamp] = DateTimeOffset.UtcNow.ToString("o");
#pragma warning disable CS0618 // Preserve the legacy message type for transport compatibility.
        message.Header.Bag[RejectionMetadataKeyNames.OriginalMessageType] = message.Header.MessageType.ToString();
#pragma warning restore CS0618

        if (reason == null)
        {
            message.Header.Bag[RejectionMetadataKeyNames.RejectionReason] = RejectionReason.None.ToString();
            return;
        }

        message.Header.Bag[RejectionMetadataKeyNames.RejectionReason] = reason.RejectionReason.ToString();
        if (!string.IsNullOrEmpty(reason.Description))
            message.Header.Bag[RejectionMetadataKeyNames.RejectionMessage] = reason.Description ?? string.Empty;
    }

    private (RoutingKey? routingKey, bool foundProducer, bool isFallingBackToDlq) DetermineRejectionRoute(
        RejectionReason rejectionReason)
    {
        switch (rejectionReason)
        {
            case RejectionReason.Unacceptable:
                if (_invalidMessageRoutingKey != null)
                    return (_invalidMessageRoutingKey, true, false);
                if (_deadLetterRoutingKey != null)
                    return (_deadLetterRoutingKey, true, true);
                return (null, false, false);

            case RejectionReason.DeliveryError:
            case RejectionReason.None:
            default:
                if (_deadLetterRoutingKey != null)
                    return (_deadLetterRoutingKey, true, false);
                return (null, false, false);
        }
    }

    private Message CreateMessage(MessageView message)
    {
        var mapped = ReadWireMessage(message);
        mapped.Header.Bag["ReceiptHandle"] = message;
        mapped.Header.HandledCount = DeliveryCount.Resolve(mapped.Header.HandledCount, message.DeliveryAttempt, mapped.Header.Bag);
        return mapped;
    }

    private Message ReadWireMessage(MessageView message)
    {
        if (_wireMessageReader == null)
            return s_defaultWireMessageReader.ReadMessage(message);

        try
        {
            return _wireMessageReader.ReadMessage(message) ?? CreateMappingFailure(message);
        }
        catch (Exception)
        {
            return CreateMappingFailure(message);
        }
    }

    private static Message CreateMappingFailure(MessageView message) =>
        new(MessageHeader.FailureMessageHeader(new RoutingKey(message.Topic), new Id(message.MessageId)),
            new MessageBody(message.Body));

    /// <inheritdoc />
    public void Dispose() => consumer.Dispose();

    /// <inheritdoc />
    public async ValueTask DisposeAsync() => await consumer.DisposeAsync();

    private static partial class Log
    {
        [LoggerMessage(LogLevel.Information, "RocketMessageConsumer: Rejecting message {MessageId}")]
        public static partial void RejectingMessage(ILogger logger, string messageId);

        [LoggerMessage(LogLevel.Warning, "RocketMessageConsumer: No DLQ or invalid message channels configured for message {MessageId}, rejection reason: {RejectionReason}")]
        public static partial void NoChannelsConfiguredForRejection(ILogger logger, string messageId, string rejectionReason);

        [LoggerMessage(LogLevel.Information, "RocketMessageConsumer: Message {MessageId} sent to rejection channel, reason: {RejectionReason}")]
        public static partial void MessageSentToRejectionChannel(ILogger logger, string messageId, string rejectionReason);

        [LoggerMessage(LogLevel.Warning, "RocketMessageConsumer: Falling back to DLQ for message {MessageId}")]
        public static partial void FallingBackToDlq(ILogger logger, string messageId);

        [LoggerMessage(LogLevel.Warning, "RocketMessageConsumer: Could not change the invisible duration of message {MessageId} to {InvisibleDuration}; it will reappear when its invisibility timeout lapses")]
        public static partial void ErrorChangingInvisibleDuration(ILogger logger, Exception ex, string messageId, TimeSpan invisibleDuration);

        [LoggerMessage(LogLevel.Warning, "RocketMessageConsumer: Requeue delay {RequestedDelay} for message {MessageId} is above the broker's maximum invisible duration; holding it for {MaximumDelay}")]
        public static partial void RequeueDelayAboveMaximum(ILogger logger, string messageId, TimeSpan requestedDelay, TimeSpan maximumDelay);

        [LoggerMessage(LogLevel.Error, "RocketMessageConsumer: Error sending message {MessageId} to rejection channel, reason: {RejectionReason}")]
        public static partial void ErrorSendingToRejectionChannel(ILogger logger, Exception ex, string messageId, string rejectionReason);

        [LoggerMessage(LogLevel.Error, "RocketMessageConsumer: Error acknowledging source message after rejection")]
        public static partial void ErrorAckingSourceMessage(ILogger logger, Exception ex);

        [LoggerMessage(LogLevel.Error, "RocketMessageConsumer: Error creating producer for routing key {RoutingKey}")]
        public static partial void ErrorCreatingProducer(ILogger logger, Exception ex, string routingKey);
    }
}
