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
using System.Linq;
using System.Threading;
using MQTTnet;
using MQTTnet.Protocol;
using Paramore.Brighter.MessagingGateway.MQTT;

namespace Paramore.Brighter.MQTT.Tests.TestDoubles;

internal sealed class MqttProbeWireReader(string mode, Id id, RoutingKey mappedTopic)
    : IAmAWireMessageReader<MqttApplicationMessage>
{
    private readonly DefaultMqttWireMessageReader _default = new();
    private int _calls;
    public int Calls => Volatile.Read(ref _calls);
    public byte[] Before { get; private set; } = Array.Empty<byte>();
    public byte[] After { get; private set; } = Array.Empty<byte>();
    public string? NativeTopic { get; private set; }
    public MqttQualityOfServiceLevel NativeQuality { get; private set; }

    public Message ReadMessage(MqttApplicationMessage native)
    {
        Interlocked.Increment(ref _calls);
        Before = native.PayloadSegment.ToArray();
        NativeTopic = native.Topic;
        NativeQuality = native.QualityOfServiceLevel;
        var message = mode == "custom"
            ? new Message(new MessageHeader(id, mappedTopic, MessageType.MT_EVENT), new MessageBody(Before.ToArray()))
            : _default.ReadMessage(native);
        message.Body = new MessageBody(new byte[] { mode == "custom" ? (byte)82 : (byte)67, 0 }
            .Concat(message.Body.Memory.ToArray()).ToArray());
        message.Header.Topic = mappedTopic;
        message.Header.Bag["reader-marker"] = "mapped";
        After = native.PayloadSegment.ToArray();
        return message;
    }
}
