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
using Paramore.Brighter.MessagingGateway.Kafka;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

[Trait("Category", "Kafka")]
[Collection("Kafka")]
public class KafkaDefaultWireWriterIsolationTests
{
    [Fact]
    public void When_a_default_kafka_writer_is_reused_should_keep_each_native_message_and_header_collection_independent()
    {
        //Arrange
        var first = new Message(new MessageHeader(new Id("first-id"), new RoutingKey("first-topic"), MessageType.MT_EVENT,
            partitionKey: new Paramore.Brighter.PartitionKey("first-key")), new MessageBody(new byte[] { 0, 255, 1 }));
        var second = new Message(new MessageHeader(new Id("second-id"), new RoutingKey("second-topic"), MessageType.MT_EVENT,
            partitionKey: new Paramore.Brighter.PartitionKey("second-key")), new MessageBody(new byte[] { 0, 255, 2 }));
        first.Header.Bag["tenant"] = "first";
        second.Header.Bag["tenant"] = "second";
        IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>> writer = new DefaultKafkaWireMessageWriter();

        //Act
        var firstWire = writer.WriteMessage(first);
        var secondWire = writer.WriteMessage(second);

        //Assert
        Assert.NotSame(firstWire, secondWire);
        Assert.NotSame(firstWire.Headers, secondWire.Headers);
        Assert.Equal("first-id", Encoding.UTF8.GetString(firstWire.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
        Assert.Equal("second-id", Encoding.UTF8.GetString(secondWire.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
        Assert.Equal("first-key", firstWire.Key);
        Assert.Equal("second-key", secondWire.Key);
        Assert.Equal(new byte[] { 0, 255, 1 }, firstWire.Value);
        Assert.Equal(new byte[] { 0, 255, 2 }, secondWire.Value);
        firstWire.Headers.Remove("tenant");
        firstWire.Headers.Add("tenant", Encoding.UTF8.GetBytes("edited-output"));
        Assert.Equal("second", Encoding.UTF8.GetString(secondWire.Headers.GetLastBytes("tenant")));
        Assert.Equal("first", first.Header.Bag["tenant"]);
        Assert.Equal("second", second.Header.Bag["tenant"]);
    }
}
