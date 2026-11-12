using System;

namespace Paramore.Brighter.MessagingGateway.Redis
{
    /// <summary>
    /// Configures a Redis publication and its stored message writer.
    /// </summary>
    public class RedisMessagePublication : Publication
    {
        /// <summary>
        /// Gets or sets the writer used to map messages for this publication.
        /// </summary>
        /// <value>The writer, or null to use <see cref="DefaultRedisWireMessageWriter"/>.</value>
        /// <remarks>
        /// The writer supplies the stored string. The gateway owns source-topic routing,
        /// storage keys, message counters, expiry and recipient queue delivery.
        /// Writers must leave the source message unchanged and support concurrent calls when shared.
        /// A null result throws <see cref="InvalidOperationException"/> before counter, storage or queue changes.
        /// Exceptions thrown by the writer propagate without being replaced.
        /// </remarks>
        public IAmAWireMessageWriter<string>? WireMessageWriter { get; set; }

        /// <summary>
        /// Gets or sets the native Redis topic used for payload storage and counters.
        /// </summary>
        /// <value>The native topic, or null to use the message's logical topic.</value>
        /// <remarks>The override does not change the message topic supplied to the writer.</remarks>
        public RoutingKey? DestinationTopic { get; set; }

        /// <summary>
        /// Gets or sets a single destination queue for this publication.
        /// </summary>
        /// <value>The queue, or null to use the native topic's recipient set.</value>
        /// <remarks>A configured queue receives the new native ID without sibling fan-out.</remarks>
        public ChannelName? ChannelName { get; set; }
    }

    /// <summary>
    /// Represents a publication for Redis messages, associating a specific message type with the publication.
    /// This allows for strongly-typed publication to a Redis channel or stream.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the request (message) that this publication handles.
    /// This type must be a class and implement the <see cref="IRequest"/> interface.
    /// </typeparam>
    public class RedisMessagePublication<T> : RedisMessagePublication
        where T: class, IRequest
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RedisMessagePublication{T}"/> class.
        /// </summary>
        public RedisMessagePublication()
        {
            RequestType = typeof(T);
        }
    }
}
