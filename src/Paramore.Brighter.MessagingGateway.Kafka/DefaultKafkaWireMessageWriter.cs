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
/// Maps Brighter messages to the default Kafka key, body and headers.
/// </summary>
/// <remarks>
/// Compose this writer to customize the native representation. Each call creates a new Kafka message;
/// the default header builder also creates a new header collection. A supplied header builder must
/// support the caller's concurrency and return independent headers without modifying the source.
/// The body is copied so composition may edit it without changing the source or another mapping.
/// Header byte arrays from a supplied header builder retain that builder's ownership convention.
/// The gateway owns topic selection and delivery operations.
/// </remarks>
public sealed class DefaultKafkaWireMessageWriter : IAmAWireMessageWriter<Message<string, byte[]>>
{
    private readonly IKafkaMessageHeaderBuilder _headerBuilder;

    /// <summary>
    /// Initializes a writer with an optional existing Kafka header strategy.
    /// </summary>
    /// <param name="headerBuilder">The header strategy, or null to use the default Kafka headers.</param>
    public DefaultKafkaWireMessageWriter(IKafkaMessageHeaderBuilder? headerBuilder = null)
    {
        _headerBuilder = headerBuilder ?? KafkaDefaultMessageHeaderBuilder.Instance;
    }

    /// <summary>
    /// Maps the message's partition key, body and application metadata to a Kafka message.
    /// </summary>
    /// <param name="message">The Brighter message, including trace context available at dispatch.</param>
    /// <returns>A new Kafka message using the selected header strategy.</returns>
    public Message<string, byte[]> WriteMessage(Message message)
    {
        return new Message<string, byte[]>
        {
            Headers = _headerBuilder.Build(message),
            Key = message.Header.PartitionKey.Value,
            Value = message.Body.Memory.ToArray()
        };
    }
}
