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

using RabbitMQ.Client.Events;

namespace Paramore.Brighter.MessagingGateway.RMQ.Async;

/// <summary>
/// Maps RabbitMQ deliveries using Brighter's default message format.
/// </summary>
/// <remarks>
/// Compose this reader to customize selected message fields. Each call creates a fresh message
/// without changing the native delivery. This stateless reader can be reused concurrently.
/// The gateway owns acknowledgement, rejection and other delivery operations.
/// </remarks>
public sealed class DefaultRmqWireMessageReader : IAmAWireMessageReader<BasicDeliverEventArgs>
{
    /// <summary>
    /// Reads the default RabbitMQ body, properties and headers.
    /// </summary>
    /// <param name="message">The native RabbitMQ delivery to map.</param>
    /// <returns>
    /// A new Brighter message, or an unacceptable message retaining the payload and delivery
    /// metadata when default parsing fails.
    /// </returns>
    public Message ReadMessage(BasicDeliverEventArgs message) => RmqMessageCreator.CreateMessage(message);
}
