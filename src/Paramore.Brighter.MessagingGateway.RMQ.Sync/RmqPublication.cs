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

namespace Paramore.Brighter.MessagingGateway.RMQ.Sync
{
    /// <summary>
    /// Configures publication to RabbitMQ.
    /// </summary>
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
        /// Gets or sets the wire writer. Null uses Brighter's default RabbitMQ format.
        /// </summary>
        /// <remarks>
        /// The gateway controls routing, persistence and delivery bookkeeping. Return fresh output
        /// for each call; compose <see cref="DefaultRmqWireMessageWriter"/> to customize the default format.
        /// </remarks>
        public IAmAWireMessageWriter<RmqWireMessage>? WireMessageWriter { get; set; }

        /// <summary>
        /// How long should we wait on shutdown for the broker to finish confirming delivery of messages
        /// If we shut down without confirmation then messages will not be marked as sent in the Outbox
        /// Any sweeper will then resend.
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
