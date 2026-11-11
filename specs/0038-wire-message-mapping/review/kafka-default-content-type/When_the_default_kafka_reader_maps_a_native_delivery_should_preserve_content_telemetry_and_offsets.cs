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
using Confluent.Kafka;
using Paramore.Brighter.MessagingGateway.Kafka;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

[Trait("Category", "Kafka")]
[Collection("Kafka")]
public class KafkaDefaultWireReaderCompatibilityTests
{
    [Fact]
    public void When_the_default_kafka_reader_maps_a_native_delivery_should_preserve_content_telemetry_and_offsets()
    {
        //Arrange
        var native = new ConsumeResult<string, byte[]>
        {
            Topic = "native-topic", Partition = new Partition(2), Offset = new Offset(37),
            Message = new Confluent.Kafka.Message<string, byte[]>
            {
                Key = "native-key", Value = [0, 255, 128, 1],
                Headers = new Headers
                {
                    { HeaderNames.MESSAGE_ID, Encoding.UTF8.GetBytes("legacy-id") },
                    { HeaderNames.CLOUD_EVENTS_ID, Encoding.UTF8.GetBytes("cloud-id") },
                    { HeaderNames.MESSAGE_TYPE, Encoding.UTF8.GetBytes("MT_EVENT") },
                    { HeaderNames.TOPIC, Encoding.UTF8.GetBytes("header-topic") },
                    { HeaderNames.TIMESTAMP, Encoding.UTF8.GetBytes("2026-10-22T09:30:00+02:00") },
                    { HeaderNames.CONTENT_TYPE, Encoding.UTF8.GetBytes("text/plain") },
                    { HeaderNames.CLOUD_EVENTS_DATA_CONTENT_TYPE, Encoding.UTF8.GetBytes("application/octet-stream") },
                    { HeaderNames.PARTITIONKEY, Encoding.UTF8.GetBytes("header-key") },
                    { HeaderNames.HANDLED_COUNT, Encoding.UTF8.GetBytes("3") },
                    { HeaderNames.DELAYED_MILLISECONDS, Encoding.UTF8.GetBytes("1250") },
                    { HeaderNames.CORRELATION_ID, Encoding.UTF8.GetBytes("correlation") },
                    { HeaderNames.REPLY_TO, Encoding.UTF8.GetBytes("reply-topic") },
                    { HeaderNames.CLOUD_EVENTS_TRACE_PARENT, Encoding.UTF8.GetBytes("00-11111111111111111111111111111111-2222222222222222-01") },
                    { HeaderNames.CLOUD_EVENTS_TRACE_STATE, Encoding.UTF8.GetBytes("vendor=value") },
                    { HeaderNames.W3C_BAGGAGE, Encoding.UTF8.GetBytes("tenant=north") },
                    { "tenant", Encoding.UTF8.GetBytes("north") }
                }
            }
        };
        var original = native.Message.Headers.Select(header => (header.Key, Convert.ToBase64String(header.GetValueBytes()))).ToArray();
        IAmAWireMessageReader<ConsumeResult<string, byte[]>> reader = new DefaultKafkaWireMessageReader();

        //Act
        var first = reader.ReadMessage(native);
        var second = reader.ReadMessage(native);

        //Assert
        Assert.NotSame(first, second);
        Assert.NotSame(first.Header.Bag, second.Header.Bag);
        Assert.Equal("cloud-id", first.Id.Value);
        Assert.Equal(new RoutingKey("native-topic"), first.Header.Topic);
#pragma warning disable CS0618 // Verify legacy wire compatibility.
        Assert.Equal(MessageType.MT_EVENT, first.Header.MessageType);
#pragma warning restore CS0618
        Assert.Equal(new Paramore.Brighter.PartitionKey("header-key"), first.Header.PartitionKey);
        Assert.Equal(new byte[] { 0, 255, 128, 1 }, first.Body.Memory.ToArray());
        Assert.Equal(new ContentType("application/octet-stream") { CharSet = "utf-8" }, first.Header.ContentType);
        Assert.Equal(new DateTimeOffset(2026, 10, 22, 7, 30, 0, TimeSpan.Zero), first.Header.TimeStamp);
        Assert.Equal(3, first.Header.HandledCount);
        Assert.Equal(TimeSpan.FromMilliseconds(1250), first.Header.Delayed);
        Assert.Equal(new Id("correlation"), first.Header.CorrelationId);
        Assert.Equal(new RoutingKey("reply-topic"), first.Header.ReplyTo);
        Assert.Equal("00-11111111111111111111111111111111-2222222222222222-01", first.Header.TraceParent?.Value);
        Assert.Equal("vendor=value", first.Header.TraceState?.Value);
        Assert.Equal("tenant=north", first.Header.Baggage.ToString());
        Assert.Equal("north", first.Header.Bag["tenant"]);
        Assert.Equal(native.TopicPartitionOffset, Assert.IsType<TopicPartitionOffset>(first.Header.Bag[HeaderNames.PARTITION_OFFSET]));
        first.Header.Bag["tenant"] = "edited-output";
        Assert.Equal("north", second.Header.Bag["tenant"]);
        Assert.Equal(original, native.Message.Headers.Select(header => (header.Key, Convert.ToBase64String(header.GetValueBytes()))).ToArray());
        Assert.Equal("native-key", native.Message.Key);
        Assert.Equal(new byte[] { 0, 255, 128, 1 }, native.Message.Value);
    }
}
