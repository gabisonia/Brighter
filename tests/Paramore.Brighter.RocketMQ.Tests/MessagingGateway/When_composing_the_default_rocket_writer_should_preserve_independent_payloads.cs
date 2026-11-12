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

using System.Text;
using Paramore.Brighter.MessagingGateway.RocketMQ;
using Xunit;

namespace Paramore.Brighter.RocketMQ.Tests.MessagingGateway;

/// <summary>Verifies mutable default representations are independent of their input and each other.</summary>
public class RocketDefaultWireMappingTests
{
    /// <summary>Allows body and property composition without changing the source or another output.</summary>
    [Fact]
    public void When_composing_the_default_rocket_writer_should_preserve_independent_payloads()
    {
        //Arrange
        var source = new Message(new MessageHeader(Id.Random(), new RoutingKey("source"), MessageType.MT_EVENT),
            new MessageBody("original-body"));
        source.Header.Bag["marker"] = "original-property";
        var writer = new DefaultRocketMqWireMessageWriter();
        var first = writer.WriteMessage(source);
        var second = writer.WriteMessage(source);

        //Act
        first.Body[0] = (byte)'X';
        first.Properties["marker"] = "changed-property";

        //Assert
        Assert.Equal("original-body", source.Body.Value);
        Assert.Equal("original-body", Encoding.UTF8.GetString(source.Body.Memory.Span));
        Assert.Equal("original-body", Encoding.UTF8.GetString(second.Body));
        Assert.Equal("original-property", source.Header.Bag["marker"]);
        Assert.Equal("original-property", second.Properties["marker"]);
        Assert.NotSame(first.Body, second.Body);
        Assert.NotSame(first.Properties, second.Properties);
    }
}
