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
using System.Text;
using MQTTnet;
using Paramore.Brighter.MessagingGateway.MQTT;

namespace Paramore.Brighter.MQTT.Tests.TestDoubles;

/// <summary>Composes default JSON and can fail before publication for consumer recovery checks.</summary>
internal sealed class MqttConsumerSelectedWireWriter(int behavior) : IAmAWireMessageWriter<MqttApplicationMessage>
{
    public bool Recover { get; set; }
    public int Calls { get; private set; }
    public MqttApplicationMessage WriteMessage(Message message)
    {
        Calls++;
        if (!Recover && behavior == 1) throw new FormatException("Selected MQTT writer failed.");
        if (!Recover && behavior == 2) return null!;
        var wire = new DefaultMqttWireMessageWriter().WriteMessage(message);
        wire.PayloadSegment = Encoding.UTF8.GetBytes("selected:" + Encoding.UTF8.GetString(wire.PayloadSegment));
        return wire;
    }
}
