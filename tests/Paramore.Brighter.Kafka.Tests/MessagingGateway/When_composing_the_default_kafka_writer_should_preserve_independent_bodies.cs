/*
The MIT License (MIT)

Copyright (c) 2026 gabisonia

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Text;
using Paramore.Brighter.MessagingGateway.Kafka;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

/// <summary>
/// Verifies that composing the default writer preserves independent message bodies.
/// </summary>
public class KafkaDefaultBodyOwnershipTests
{
    /// <summary>
    /// Changing one native body leaves the source message and another mapping untouched.
    /// </summary>
    [Fact]
    public void When_composing_the_default_kafka_writer_should_preserve_independent_bodies()
    {
        // Arrange
        var message = new Message(new MessageHeader(Guid.NewGuid().ToString(), "ownership", MessageType.MT_EVENT),
            new MessageBody("original-body"));
        var writer = new DefaultKafkaWireMessageWriter();

        // Act
        var first = writer.WriteMessage(message);
        var second = writer.WriteMessage(message);
        first.Value[0] = (byte)'X';

        // Assert
        Assert.Equal("original-body", Encoding.UTF8.GetString(message.Body.Memory.Span));
        Assert.Equal("original-body", Encoding.UTF8.GetString(second.Value));
        Assert.NotSame(first.Value, second.Value);
        Assert.NotSame(message.Body.ToByteArray(), first.Value);
    }
}
