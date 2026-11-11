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

#pragma warning disable CS0618 // Verify native pump control signals retained for wire compatibility.

using System.Diagnostics;
using Paramore.Brighter.MessagingGateway.RocketMQ;
using Xunit;

namespace Paramore.Brighter.RocketMQ.Tests.TestDoubles;

/// <summary>Waits for one actual delivery from the isolated mapping topic.</summary>
public static class RocketMappingDelivery
{
    /// <summary>Receives through the selected public synchronous or asynchronous gateway API.</summary>
    public static async Task<Message> ReceiveAsync(RocketMessageConsumer consumer, bool useAsync)
    {
        var elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < TimeSpan.FromSeconds(45))
        {
            var messages = useAsync ? await consumer.ReceiveAsync() : consumer.Receive();
            var message = Assert.Single(messages);
            Assert.NotNull(message);
            if (message.Header.MessageType != MessageType.MT_NONE) return message;
            await Task.Delay(100);
        }
        throw new TimeoutException("The mapping topic did not deliver a message within 45 seconds.");
    }
}

#pragma warning restore CS0618
