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

using MQTTnet;

namespace Paramore.Brighter.MessagingGateway.MQTT;

/// <summary>
/// Configures an MQTT publication and its native message writer.
/// </summary>
public class MqttPublication : Publication
{
    /// <summary>
    /// Gets or sets the writer used to map messages for this publication.
    /// </summary>
    /// <value>The writer, or null to use <see cref="DefaultMqttWireMessageWriter"/>.</value>
    /// <remarks>
    /// The writer controls the payload. The gateway replaces the native topic with the
    /// configured prefix and source message topic, and sets QoS to AtLeastOnce.
    /// Writers must return a fresh native message without modifying the source message
    /// and must support concurrent calls when shared between producers.
    /// A null result causes the send to throw <see cref="System.InvalidOperationException"/> before native publication.
    /// Exceptions thrown by the writer propagate to the sender without being replaced.
    /// </remarks>
    public IAmAWireMessageWriter<MqttApplicationMessage>? WireMessageWriter { get; set; }
}
