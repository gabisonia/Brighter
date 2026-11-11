using System;

namespace Paramore.Brighter.MessagingGateway.Postgres;

/// <summary>
/// Represents a subscription to a PostgreSQL message queue within the Brighter framework.
/// This class extends the base <see cref="Subscription"/> class with PostgreSQL-specific
/// configuration options for consuming messages.
/// </summary>
public class PostgresSubscription : Subscription, IUseBrighterDeadLetterSupport, IUseBrighterInvalidMessageSupport
{
    /// <summary>Gets or sets the reader for this subscription's JSON content.</summary>
    /// <remarks>
    /// Null preserves the default parser, including streaming for large messages.
    /// A custom reader receives materialized JSON text and is captured when the consumer
    /// is created. The gateway replaces receipt metadata with the actual row identity.
    /// Reader exceptions or null results produce rejectable messages retaining that identity.
    /// </remarks>
    public IAmAWireMessageReader<string>? WireMessageReader { get; set; }

    /// <summary>Gets or sets the writer used when requeue updates a row's JSON content.</summary>
    /// <remarks>
    /// Null selects the default JSON mapping. The consumer captures this selection on
    /// creation. The reader must understand the selected writer's output. The gateway
    /// retains row identity and controls visibility independently of the mapping.
    /// </remarks>
    public IAmAWireMessageWriter<string>? RequeueWireMessageWriter { get; set; }

    /// <summary>Gets or sets the writer for consumer-created dead-letter forwarding.</summary>
    /// <remarks>Null preserves default JSON. Selection is captured at consumer creation.
    /// Rejection retains its existing settlement behavior, including deletion after forwarding failure.</remarks>
    public IAmAWireMessageWriter<string>? DeadLetterWireMessageWriter { get; set; }

    /// <summary>Gets or sets the writer for consumer-created invalid-message forwarding.</summary>
    /// <remarks>Null preserves default JSON. Selection is captured at consumer creation.
    /// Rejection retains its existing settlement behavior, including deletion after forwarding failure.</remarks>
    public IAmAWireMessageWriter<string>? InvalidMessageWireMessageWriter { get; set; }

    /// <inheritdoc />
    public override Type ChannelFactoryType => typeof(PostgresChannelFactory);

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresSubscription"/> class.
    /// </summary>
    /// <param name="subscriptionName">The name. Defaults to the data type's full name.</param>
    /// <param name="channelName">The channel name. Defaults to the data type's full name.</param>
    /// <param name="routingKey">The routing key. Defaults to the data type's full name.</param>
    /// <param name="dataType">Type of the data.</param>
    /// <param name="getRequestType">The <see cref="Func{Message, Type}"/> that determines how we map a message to a type. Defaults to returning the <paramref name="dataType"/> if null</param>
    /// <param name="bufferSize">The number of messages to buffer at any one time, also the number of messages to retrieve at once. Min of 1 Max of 10</param>
    /// <param name="noOfPerformers">The no of threads reading this channel.</param>
    /// <param name="timeOut">The timeout for the subscription to consider the queue empty and pause</param>
    /// <param name="requeueCount">The number of times you want to requeue a message before dropping it.</param>
    /// <param name="requeueDelay">The delay the delivery of a requeue message for.</param>
    /// <param name="unacceptableMessageLimit">The number of unacceptable messages to handle, before stopping reading from the channel.</param>
    /// <param name="messagePumpType">Is this channel read asynchronously</param>
    /// <param name="channelFactory">The channel factory to create channels for Consumer.</param>
    /// <param name="makeChannels">Should we make channels if they don't exist, defaults to creating</param>
    /// <param name="emptyChannelDelay">How long to pause when a channel is empty in milliseconds</param>
    /// <param name="channelFailureDelay">How long to pause when there is a channel failure in milliseconds</param>
    /// <param name="schemaName">The schema name where the queue store table resides in the PostgreSQL database.</param>
    /// <param name="queueStoreTable">The name of the queue store table in the PostgreSQL database.</param>
    /// <param name="visibleTimeout">The duration for which a retrieved message is hidden from other consumers.</param>
    /// <param name="tableWithLargeMessage">A flag indicating whether the queue table is configured to handle large messages stored as streams.</param>
    /// <param name="binaryMessagePayload">A flag indicating whether the message payload is stored as binary JSON (JSONB) in the database.</param>
    /// <param name="deadLetterRoutingKey">The routing key for the dead letter queue. When set, rejected messages with <see cref="MessageRejectionReason.DeliveryError"/> are forwarded to this topic.</param>
    /// <param name="invalidMessageRoutingKey">The routing key for the invalid message channel. When set, rejected messages with <see cref="MessageRejectionReason.Unacceptable"/> are forwarded to this topic.</param>
    public PostgresSubscription(SubscriptionName subscriptionName,
        ChannelName channelName,
        RoutingKey routingKey,
        Type? dataType = null,
        Func<Message, Type>? getRequestType = null,
        int bufferSize = 1,
        int noOfPerformers = 1,
        TimeSpan? timeOut = null,
        int requeueCount = -1,
        TimeSpan? requeueDelay = null,
        int unacceptableMessageLimit = 0,
        TimeSpan? unacceptableMessageLimitWindow = null,
        MessagePumpType messagePumpType = MessagePumpType.Unknown,
        IAmAChannelFactory? channelFactory = null,
        OnMissingChannel makeChannels = OnMissingChannel.Create,
        TimeSpan? emptyChannelDelay = null,
        TimeSpan? channelFailureDelay = null,
        string? schemaName = null,
        string? queueStoreTable = null,
        TimeSpan? visibleTimeout = null,
        bool tableWithLargeMessage = false,
        bool? binaryMessagePayload = null,
        RoutingKey? deadLetterRoutingKey = null,
        RoutingKey? invalidMessageRoutingKey = null)
        : base(subscriptionName, channelName, routingKey, dataType, getRequestType, bufferSize, noOfPerformers, timeOut, requeueCount, requeueDelay, unacceptableMessageLimit, messagePumpType, channelFactory, makeChannels, emptyChannelDelay, channelFailureDelay, unacceptableMessageLimitWindow)
    {
        SchemaName = schemaName;
        QueueStoreTable = queueStoreTable;
        VisibleTimeout = visibleTimeout ?? TimeSpan.FromSeconds(30);
        TableWithLargeMessage = tableWithLargeMessage;
        BinaryMessagePayload = binaryMessagePayload;
        DeadLetterRoutingKey = deadLetterRoutingKey;
        InvalidMessageRoutingKey = invalidMessageRoutingKey;
    }
    
    /// <summary>
    /// Gets the schema name where the queue store table resides in the PostgreSQL database.
    /// If not explicitly set, the default schema name configured in the
    /// <see cref="PostgresMessagingGatewayConnection"/> will be used.
    /// </summary>
    public string? SchemaName { get; }
    
    /// <summary>
    /// Gets the name of the queue store table in the PostgreSQL database.
    /// If not explicitly set, the default queue store table name configured in the
    /// <see cref="PostgresMessagingGatewayConnection"/> will be used.
    /// </summary>
    public string? QueueStoreTable { get; }
    
    /// <summary>
    /// Gets the duration for which a retrieved message is hidden from other consumers.
    /// Defaults to 30 seconds if not explicitly set.
    /// </summary>
    public TimeSpan VisibleTimeout { get; }
    
    /// <summary>
    /// Gets a flag indicating whether the queue table is configured to handle large messages stored as streams.
    /// </summary>
    public bool TableWithLargeMessage { get; }
    
    /// <summary>
    /// Gets a flag indicating whether the message payload is stored as binary JSON (JSONB) in the database.
    /// If not explicitly set, the default setting configured in the
    /// <see cref="PostgresMessagingGatewayConnection"/> will be used.
    /// </summary>
    public bool? BinaryMessagePayload { get; }

    /// <inheritdoc />
    public RoutingKey? DeadLetterRoutingKey { get; set; }

    /// <inheritdoc />
    public RoutingKey? InvalidMessageRoutingKey { get; set; }
}

/// <summary>
/// Represents a typed subscription to a PostgreSQL message queue within the Brighter framework.
/// This generic class extends <see cref="PostgresSubscription"/> and simplifies the creation
/// of subscriptions for specific request types.
/// </summary>
/// <typeparam name="T">The type of the request message for this subscription.</typeparam>
public class PostgresSubscription<T> : PostgresSubscription
    where T : class, IRequest
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresSubscription"/> class.
    /// </summary>
    /// <param name="subscriptionName">The name. Defaults to the data type's full name.</param>
    /// <param name="channelName">The channel name. Defaults to the data type's full name.</param>
    /// <param name="routingKey">The routing key. Defaults to the data type's full name.</param>
    /// <param name="getRequestType">The <see cref="Func{Message, Type}"/> that determines how we map a message to a type. Defaults to returning the <paramref name="dataType"/> if null</param>
    /// <param name="bufferSize">The number of messages to buffer at any one time, also the number of messages to retrieve at once. Min of 1 Max of 10</param>
    /// <param name="noOfPerformers">The no of threads reading this channel.</param>
    /// <param name="timeOut">The timeout for the subscription to consider the queue empty and pause</param>
    /// <param name="requeueCount">The number of times you want to requeue a message before dropping it.</param>
    /// <param name="requeueDelay">The delay the delivery of a requeue message for.</param>
    /// <param name="unacceptableMessageLimit">The number of unacceptable messages to handle, before stopping reading from the channel.</param>
    /// <param name="messagePumpType">Is this channel read asynchronously</param>
    /// <param name="channelFactory">The channel factory to create channels for Consumer.</param>
    /// <param name="makeChannels">Should we make channels if they don't exist, defaults to creating</param>
    /// <param name="emptyChannelDelay">How long to pause when a channel is empty in milliseconds</param>
    /// <param name="channelFailureDelay">How long to pause when there is a channel failure in milliseconds</param>
    /// <param name="schemaName">The schema name where the queue store table resides in the PostgreSQL database.</param>
    /// <param name="queueStoreTable">The name of the queue store table in the PostgreSQL database.</param>
    /// <param name="visibleTimeout">The duration for which a retrieved message is hidden from other consumers.</param>
    /// <param name="tableWithLargeMessage">A flag indicating whether the queue table is configured to handle large messages stored as streams.</param>
    /// <param name="binaryMessagePayload">A flag indicating whether the message payload is stored as binary JSON (JSONB) in the database.</param>
    /// <param name="deadLetterRoutingKey">The routing key for the dead letter queue. When set, rejected messages with <see cref="MessageRejectionReason.DeliveryError"/> are forwarded to this topic.</param>
    /// <param name="invalidMessageRoutingKey">The routing key for the invalid message channel. When set, rejected messages with <see cref="MessageRejectionReason.Unacceptable"/> are forwarded to this topic.</param>
    public PostgresSubscription(
        SubscriptionName? subscriptionName = null,
        ChannelName? channelName = null,
        RoutingKey? routingKey = null,
        Func<Message, Type>? getRequestType = null,
        int bufferSize = 1,
        int noOfPerformers = 1,
        TimeSpan? timeOut = null,
        int requeueCount = -1,
        TimeSpan? requeueDelay = null,
        int unacceptableMessageLimit = 0,
        TimeSpan? unacceptableMessageLimitWindow = null,
        MessagePumpType messagePumpType = MessagePumpType.Unknown,
        IAmAChannelFactory? channelFactory = null,
        OnMissingChannel makeChannels = OnMissingChannel.Create,
        TimeSpan? emptyChannelDelay = null,
        TimeSpan? channelFailureDelay = null,
        string? schemaName = null,
        string? queueStoreTable = null,
        TimeSpan? visibleTimeout = null,
        bool tableWithLargeMessage = false,
        bool? binaryMessagePayload = null,
        RoutingKey? deadLetterRoutingKey = null,
        RoutingKey? invalidMessageRoutingKey = null)
        : base(
            subscriptionName ?? new SubscriptionName(typeof(T).FullName!),
            channelName ?? new ChannelName(typeof(T).FullName!),
            routingKey ?? new RoutingKey(typeof(T).FullName!),
            typeof(T),
            getRequestType,
            bufferSize,
            noOfPerformers,
            timeOut,
            requeueCount,
            requeueDelay,
            unacceptableMessageLimit,
            unacceptableMessageLimitWindow,
            messagePumpType,
            channelFactory,
            makeChannels,
            emptyChannelDelay,
            channelFailureDelay,
            schemaName,
            queueStoreTable,
            visibleTimeout,
            tableWithLargeMessage,
            binaryMessagePayload,
            deadLetterRoutingKey,
            invalidMessageRoutingKey)
    {
    }
}
