using System;

namespace Paramore.Brighter.MessagingGateway.RMQ.Async
{
    public class RmqPublication : Publication
    {
        /// <summary>
        /// Gets or sets an explicit native queue destination.
        /// </summary>
        /// <remarks>
        /// Use a queue-specific retry publication to avoid routing scheduled retries to sibling queues.
        /// Null preserves exchange routing by the message's logical topic. This does not change
        /// the publication selector or the logical message topic. The destination queue must exist.
        /// Zero-delay sends use the default exchange. Native delayed sends use the queue binding
        /// on the connection's queue-specific delayed exchange. Create mode provisions that binding;
        /// Validate and Assume require it to exist. The destination queue itself must already exist.
        /// </remarks>
        public ChannelName? DestinationQueue { get; set; }

        /// <summary>
        /// Gets or sets the writer used to map outgoing messages to RabbitMQ body and properties.
        /// </summary>
        /// <remarks>
        /// Null preserves the existing mapping. A configured writer replaces application metadata and
        /// body mapping; the gateway retains delivery controls and publisher-confirmation identity.
        /// A writer-supplied delay header is discarded before the gateway applies its delivery delay.
        /// A writer-supplied delivery tag is discarded; only a tag from the source message is emitted.
        /// Writers may be called concurrently and must return independent output without mutating the source.
        /// </remarks>
        public IAmAWireMessageWriter<RmqWireMessage>? WireMessageWriter { get; set; }

        /// <summary>
        /// How long should we wait on shutdown for the broker to finish confirming delivery of messages.
        /// If we shut down without confirmation then messages will not be marked as sent in the Outbox
        /// and any sweeper will resend.
        /// When non-zero, this value also bounds the wait for in-flight sends to complete on shutdown.
        /// Use 0 to skip waiting for publisher confirmations; in-flight sends are still awaited with an
        /// internal 5-second fallback to avoid aborting partially-sent messages (which would otherwise
        /// be marked Dispatched in the Outbox and re-sent as duplicates), so disposal can block for up
        /// to that fallback when sends are in flight.
        /// </summary>
        public int WaitForConfirmsTimeOutInMilliseconds { get; set; } = 500;
    }
    
    /// <summary>
    /// Represents a publication for RabbitMQ (RMQ), associating a specific message type with the publication.
    /// This allows for strongly-typed publication to a RabbitMQ exchange or queue.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the request (message) that this publication handles.
    /// This type must be a class and implement the <see cref="IRequest"/> interface.
    /// </typeparam>
    public class RmqPublication<T> : RmqPublication 
       where T: class, IRequest
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RmqPublication{T}"/> class.
        /// </summary>
        public RmqPublication()
        {
            RequestType = typeof(T);
        }
    }
}
