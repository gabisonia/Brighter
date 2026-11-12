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

using System.Collections.Generic;
using System.Text.Json;
using Amazon.SQS.Model;

namespace Paramore.Brighter.AWS.Tests.TestDoubles;

internal sealed class NonCloudEventsSnsDeliveryWireMessageReader(RoutingKey topic, bool telemetryInBody, bool rawDelivery)
    : IAmAWireMessageReader<Amazon.SQS.Model.Message>
{
    public Message ReadMessage(Amazon.SQS.Model.Message native)
    {
        var reader = new NonCloudEventsSqsWireMessageReader(topic, telemetryInBody);
        if (rawDelivery)
            return reader.ReadMessage(native);

        using var envelope = JsonDocument.Parse(native.Body);
        var attributes = new Dictionary<string, MessageAttributeValue>();
        foreach (var attribute in envelope.RootElement.GetProperty("MessageAttributes").EnumerateObject())
        {
            attributes.Add(attribute.Name, new MessageAttributeValue
            {
                DataType = attribute.Value.GetProperty("Type").GetString(),
                StringValue = attribute.Value.GetProperty("Value").GetString()
            });
        }
        return reader.ReadMessage(new Amazon.SQS.Model.Message
        {
            Body = envelope.RootElement.GetProperty("Message").GetString(),
            MessageAttributes = attributes
        });
    }
}
