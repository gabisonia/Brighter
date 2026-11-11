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

using System;
using System.Linq;
using System.Net.Mime;
using System.Text;
using System.Text.Json;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.Kafka;
using Paramore.Brighter.Observability;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

[Trait("Category", "Kafka")]
[Collection("Kafka")]
public class KafkaDefaultWireWriterCompatibilityTests
{
    [Fact]
    public void When_the_default_kafka_writer_maps_a_message_should_preserve_native_content_and_headers()
    {
        //Arrange
        byte[] body = [0, 255, 128, 1];
        var baggage = new Baggage();
        baggage.Add("tenant", "north");
        var source = new Message(new MessageHeader(new Id("wire-id"), new RoutingKey("wire-topic"), MessageType.MT_EVENT,
            timeStamp: new DateTimeOffset(2026, 10, 22, 9, 30, 0, TimeSpan.FromHours(2)),
            contentType: new ContentType("application/octet-stream"), partitionKey: new Paramore.Brighter.PartitionKey("partition-key"),
            correlationId: new Id("correlation"), replyTo: new RoutingKey("reply-topic"), handledCount: 3,
            delayed: TimeSpan.FromMilliseconds(1250),
            traceParent: new TraceParent("00-11111111111111111111111111111111-2222222222222222-01"),
            traceState: new TraceState("vendor=value"), baggage: baggage), new MessageBody(body));
        source.Header.Bag["tenant"] = "north";
        source.Header.Bag["binary"] = new byte[] { 0, 254, 127 };
        source.Header.Bag[Message.ProducerTopicHeaderName] = "local-producer-route";
        var original = JsonSerializer.Serialize(source, JsonSerialisationOptions.Options);
        var legacy = new KafkaDefaultMessageHeaderBuilder().Build(source)
            .Select(header => (header.Key, Convert.ToBase64String(header.GetValueBytes()))).ToArray();
        IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>> writer = new DefaultKafkaWireMessageWriter();

        //Act
        var wire = writer.WriteMessage(source);

        //Assert
        Assert.Equal("partition-key", wire.Key);
        Assert.Equal(body, wire.Value);
        Assert.Equal(legacy, wire.Headers.Select(header => (header.Key, Convert.ToBase64String(header.GetValueBytes()))).ToArray());
        Assert.Equal("wire-id", Encoding.UTF8.GetString(wire.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
        Assert.Equal("wire-id", Encoding.UTF8.GetString(wire.Headers.GetLastBytes(HeaderNames.CLOUD_EVENTS_ID)));
        Assert.Equal("wire-topic", Encoding.UTF8.GetString(wire.Headers.GetLastBytes(HeaderNames.TOPIC)));
        Assert.Equal("3", Encoding.UTF8.GetString(wire.Headers.GetLastBytes(HeaderNames.HANDLED_COUNT)));
        Assert.Equal("1250", Encoding.UTF8.GetString(wire.Headers.GetLastBytes(HeaderNames.DELAYED_MILLISECONDS)));
        Assert.Equal("00-11111111111111111111111111111111-2222222222222222-01",
            Encoding.UTF8.GetString(wire.Headers.GetLastBytes(HeaderNames.CLOUD_EVENTS_TRACE_PARENT)));
        Assert.Equal("vendor=value", Encoding.UTF8.GetString(wire.Headers.GetLastBytes(HeaderNames.CLOUD_EVENTS_TRACE_STATE)));
        Assert.Equal("tenant=north", Encoding.UTF8.GetString(wire.Headers.GetLastBytes(HeaderNames.W3C_BAGGAGE)));
        Assert.Equal(new byte[] { 0, 254, 127 }, wire.Headers.GetLastBytes("binary"));
        Assert.DoesNotContain(wire.Headers, header => header.Key == Message.ProducerTopicHeaderName);
        Assert.Equal(original, JsonSerializer.Serialize(source, JsonSerialisationOptions.Options));
    }
}
