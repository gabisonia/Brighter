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
using Org.Apache.Rocketmq;
using Paramore.Brighter.MessagingGateway.RocketMQ;

namespace Paramore.Brighter.RocketMQ.Tests.TestDoubles;

/// <summary>Fails selected reads until explicitly recovered.</summary>
public sealed class RocketRecoverableWireReader(bool returnNull) : IAmAWireMessageReader<MessageView>
{
    private bool _fail = true;
    /// <summary>Gets the last actual native delivery.</summary>
    public MessageView? LastDelivery { get; private set; }
    /// <summary>Allows later reads to use the default adapter.</summary>
    public void Recover() => _fail = false;
    /// <inheritdoc />
    public Message ReadMessage(MessageView message)
    {
        LastDelivery = message;
        return _fail ? returnNull ? null! : throw new InvalidOperationException("selected RocketMQ reader failure")
            : new DefaultRocketMqWireMessageReader().ReadMessage(message);
    }
}
