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

using System;
using System.Linq;
using MQTTnet;

namespace Paramore.Brighter.MessagingGateway.MQTT;

/// <summary>
/// Maps the default MQTT JSON wire representation to a Brighter message.
/// </summary>
/// <remarks>
/// Uses the same JSON decoding rules as <see cref="MqttMessageCreator"/>.
/// Each successful call returns a fresh message without modifying the native payload.
/// </remarks>
public sealed class DefaultMqttWireMessageReader : IAmAWireMessageReader<MqttApplicationMessage>
{
    /// <summary>
    /// Deserializes the native payload as a complete Brighter message.
    /// </summary>
    /// <param name="message">The MQTT application message to read.</param>
    /// <returns>The decoded Brighter message.</returns>
    /// <exception cref="FormatException">The payload cannot be decoded as a Brighter message.</exception>
    public Message ReadMessage(MqttApplicationMessage message)
        => MqttMessageCreator.CreateMessage(message.PayloadSegment.ToArray(), message.Topic)
           ?? throw new FormatException("The MQTT payload cannot be decoded as a Brighter message.");
}
