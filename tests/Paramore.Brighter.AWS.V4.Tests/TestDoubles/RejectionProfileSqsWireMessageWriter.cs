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

#nullable enable

using System;
using System.Text.Json;
using Amazon.SQS.Model;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;

namespace Paramore.Brighter.AWS.V4.Tests.TestDoubles;

internal sealed class RejectionProfileSqsWireMessageWriter(string profile, string queueUrl, bool compose)
    : IAmAWireMessageWriter<SqsWireMessage>
{
    public int FailureMode { get; set; }

    public SqsWireMessage WriteMessage(Message message)
    {
        if (FailureMode == 1)
            throw new FormatException("Rejection mapping is temporarily unavailable.");
        if (FailureMode == 2)
            return null!;

        var proof = JsonSerializer.Serialize(new
        {
            id = message.Id.Value,
            payload = message.Body.Value,
            handledCount = message.Header.HandledCount,
            originalTopic = message.Header.Bag[RejectionMetadataKeyNames.OriginalTopic],
            originalType = message.Header.Bag[RejectionMetadataKeyNames.OriginalMessageType],
            reason = message.Header.Bag[RejectionMetadataKeyNames.RejectionReason],
            description = message.Header.Bag[RejectionMetadataKeyNames.RejectionMessage],
            timestamp = message.Header.Bag[RejectionMetadataKeyNames.RejectionTimestamp]
        });
        var wire = compose
            ? new DefaultSqsWireMessageWriter(queueUrl).WriteMessage(message)
            : new SqsWireMessage { Body = proof };
        wire.MessageAttributes["rejection-profile"] = new MessageAttributeValue
        {
            DataType = "String", StringValue = profile
        };
        if (compose)
            wire.MessageAttributes["rejection-proof"] = new MessageAttributeValue
            {
                DataType = "String", StringValue = proof
            };
        return wire;
    }
}
