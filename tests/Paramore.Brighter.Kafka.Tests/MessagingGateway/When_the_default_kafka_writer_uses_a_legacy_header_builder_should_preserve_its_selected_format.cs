#region Licence

/* The MIT License (MIT)
Copyright © 2026 Irakli Gabisonia

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

using System.Text;
using Paramore.Brighter.Kafka.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.Kafka;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

[Trait("Category", "Kafka")]
[Collection("Kafka")]
public class KafkaLegacyHeaderBuilderWireWriterTests
{
    [Fact]
    public void When_the_default_kafka_writer_uses_a_legacy_header_builder_should_preserve_its_selected_format()
    {
        //Arrange
        byte[] body = [0, 255, 2];
        var source = new Message(new MessageHeader(new Id("legacy-id"), new RoutingKey("legacy-topic"), MessageType.MT_EVENT,
            partitionKey: new Paramore.Brighter.PartitionKey("legacy-key")), new MessageBody(body));
        source.Header.Bag["tenant"] = "south";
        var builder = new KafkaLegacyProfileHeaderBuilder();
        IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>> writer = new DefaultKafkaWireMessageWriter(builder);

        //Act
        var wire = writer.WriteMessage(source);

        //Assert
        Assert.Equal(1, builder.BuildCount);
        Assert.Equal(2, wire.Headers.Count);
        Assert.Equal("legacy-id", Encoding.UTF8.GetString(wire.Headers.GetLastBytes("legacy-id")));
        Assert.Equal("south", Encoding.UTF8.GetString(wire.Headers.GetLastBytes("legacy-tenant")));
        Assert.Equal("legacy-key", wire.Key);
        Assert.Equal(body, wire.Value);
        Assert.Equal("legacy-id", source.Id.Value);
        Assert.Equal(new RoutingKey("legacy-topic"), source.Header.Topic);
        Assert.Equal("south", source.Header.Bag["tenant"]);
        Assert.Equal(body, source.Body.Memory.ToArray());
    }
}
