#region Licence

/* The MIT License (MIT)
Copyright © 2026 Irakli Gabisonia

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

using Confluent.Kafka;

namespace Paramore.Brighter.MessagingGateway.Kafka;

/// <summary>
/// Maps Kafka deliveries using Brighter's default message format.
/// </summary>
/// <remarks>
/// Compose this reader to customize application fields while preserving the default mapping.
/// Each call creates a new message using the existing Kafka creator, including its header precedence,
/// native partition offset and malformed-message behavior. Native input is not modified.
/// Body buffers retain their read-only ownership convention. The gateway owns acknowledgement and
/// other delivery operations.
/// </remarks>
public sealed class DefaultKafkaWireMessageReader : IAmAWireMessageReader<ConsumeResult<string, byte[]>>
{
    private readonly KafkaMessageCreator _creator = new();

    /// <summary>
    /// Reads the default Kafka body, headers and native partition offset.
    /// </summary>
    /// <param name="message">The native Kafka delivery.</param>
    /// <returns>A new Brighter message, or an unacceptable message when default parsing fails.</returns>
    public Message ReadMessage(ConsumeResult<string, byte[]> message) => _creator.CreateMessage(message);
}
