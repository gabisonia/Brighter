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

using Amazon.SQS;

namespace Paramore.Brighter.MessagingGateway.AWSSQS.V4;

/// <summary>
/// Maps native SQS deliveries using the default Brighter message format.
/// </summary>
/// <param name="rawMessageDelivery">
/// True to read body and attributes directly from SQS; false to read an SNS envelope in the body.
/// Defaults to true.
/// </param>
/// <remarks>
/// Compose this reader to customize selected fields after default parsing. Each call uses an
/// independent parser and returns a new message. Parsing failures retain the native body and
/// delivery metadata, using the native message ID when no application ID could be parsed.
/// The consumer remains responsible for acknowledgement and rejection operations.
/// </remarks>
public sealed class DefaultSqsWireMessageReader(bool rawMessageDelivery = true)
    : IAmAWireMessageReader<Amazon.SQS.Model.Message>
{
    /// <summary>
    /// Reads the native delivery using the selected default format.
    /// </summary>
    /// <param name="message">The native SQS delivery, including body, attributes and receipt metadata.</param>
    /// <returns>The parsed message, or an unacceptable message when the default parser cannot read it.</returns>
    public Message ReadMessage(Amazon.SQS.Model.Message message)
    {
        var parsed = SqsMessageCreatorFactory.Create(rawMessageDelivery).CreateMessage(message);
#pragma warning disable CS0618 // Message types still identify pump control signals.
        if (parsed.Header.MessageType != MessageType.MT_UNACCEPTABLE
            || parsed.Header.Bag.ContainsKey("ReceiptHandle"))
            return parsed;
#pragma warning restore CS0618

        var failure = Message.FailureMessage(parsed.Header.Topic,
            Id.IsNullOrEmpty(parsed.Id) ? new Id(message.MessageId) : parsed.Id);
        failure.Body = new MessageBody(message.Body);
        failure.Header.Bag = parsed.Header.Bag;
        failure.Header.Bag["ReceiptHandle"] = message.ReceiptHandle;
        int? brokerCount = null;
        if (message.Attributes != null
            && message.Attributes.TryGetValue(MessageSystemAttributeName.ApproximateReceiveCount, out var rawCount)
            && int.TryParse(rawCount, out var count))
        {
            brokerCount = count;
        }

        failure.Header.HandledCount = DeliveryCount.Resolve(parsed.Header.HandledCount, brokerCount, failure.Header.Bag);
        return failure;
    }
}
