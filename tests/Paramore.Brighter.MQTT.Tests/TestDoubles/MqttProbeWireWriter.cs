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

using System.Linq;
using System.Threading;
using MQTTnet;
using MQTTnet.Protocol;
using Paramore.Brighter.MessagingGateway.MQTT;

namespace Paramore.Brighter.MQTT.Tests.TestDoubles;

internal sealed class MqttProbeWireWriter(string mode, string writerTopic) : IAmAWireMessageWriter<MqttApplicationMessage>
{
    private readonly DefaultMqttWireMessageWriter _default = new();
    private int _calls;
    public int Calls => Volatile.Read(ref _calls);

    public MqttApplicationMessage WriteMessage(Message message)
    {
        Interlocked.Increment(ref _calls);
        var body = mode == "custom" ? message.Body.Memory.ToArray()
            : _default.WriteMessage(message).PayloadSegment.ToArray();
        return new MqttApplicationMessageBuilder()
            .WithTopic(writerTopic)
            .WithPayload(new byte[] { mode == "custom" ? (byte)87 : (byte)67, 0 }.Concat(body).ToArray())
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
            .Build();
    }
}
