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

#nullable enable
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Org.Apache.Rocketmq;
using Paramore.Brighter.MessagingGateway.RocketMQ;
using Paramore.Brighter.RocketMQ.Tests.TestDoubles;
using Paramore.Brighter.RocketMQ.Tests.Utils;
using Xunit;

namespace Paramore.Brighter.RocketMQ.Tests.MessagingGateway;

/// <summary>Verifies custom payloads against native broker receipts and redelivery.</summary>
[Trait("Category", "RocketMQ")]
[Collection("RocketMQMessagingGateway")]
public class RocketSelectedWireMappingTests
{
    /// <summary>Preserves routing, telemetry, native settlement and broker-owned retries.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_rocket_endpoints_select_wire_mappings_should_preserve_native_delivery(bool useAsync, bool compose)
    {
        //Arrange
        var connection = GatewayFactory.CreateConnection();
        var topic = new RoutingKey(useAsync ? "rmq_mapping_p" : "rmq_mapping_r");
        var tag = "selected-" + Guid.NewGuid().ToString("N");
        var writer = new RocketSelectedWireWriter(compose);
        var reader = new RocketSelectedWireReader(compose);
        var inactive = new RocketSelectedWireReader(!compose);
        var publication = new RocketMqPublication { Topic = topic, Tag = tag, WireMessageWriter = writer };
        await using var nativeProducer = await GatewayFactory.CreateProducer(connection, publication);
        await using var producer = new RocketMqMessageProducer(connection, nativeProducer, publication);
        var subscription = new RocketSubscription(new SubscriptionName("mapped"), new ChannelName(topic.Value), topic,
            requestType: typeof(Message), consumerGroup: Guid.NewGuid().ToString("N"), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            receiveMessageTimeout: TimeSpan.FromSeconds(1), invisibilityTimeout: TimeSpan.FromSeconds(30),
            filter: new FilterExpression(tag))
        { WireMessageReader = reader };
        var factory = new RocketMessageConsumerFactory(connection);
        await using var consumer = useAsync ? (RocketMessageConsumer)factory.CreateAsync(subscription) : (RocketMessageConsumer)factory.Create(subscription);
        subscription.WireMessageReader = inactive;
        await consumer.PurgeAsync();
        using var telemetry = Sdk.CreateTracerProviderBuilder().Build();
        using var span = new Activity("rocket-mapping").SetIdFormat(ActivityIdFormat.W3C).Start();
        span.TraceStateString = "tenant=mapped";
        producer.Span = span;
        for (var iteration = 0; iteration < 2; iteration++)
        {
            var message = new Message(new MessageHeader(Id.Random(), new RoutingKey("ignored-application-topic"), MessageType.MT_EVENT),
                new MessageBody("mapped-body-π-" + iteration));
            message.Header.Bag["marker"] = "selected-λ";
            message.Header.Bag[HeaderNames.Keys] = "key-" + iteration;
            message.Header.Baggage.Add("tenant", "mapped");
            var bagBefore = JsonSerializer.Serialize(message.Header.Bag);

            //Act
            if (useAsync) await producer.SendAsync(message);
            else producer.Send(message);
            var delivered = await RocketMappingDelivery.ReceiveAsync(consumer, useAsync);

            //Assert
            Assert.Equal(iteration + 1, writer.Calls);
            Assert.Equal(message.Id, delivered.Id);
            Assert.Equal(message.Body.Value, delivered.Body.Value);
            Assert.NotNull(delivered.Header.TraceParent);
            Assert.NotNull(delivered.Header.TraceState);
            Assert.Equal(span.Id, delivered.Header.TraceParent.Value);
            Assert.Equal(span.TraceStateString, delivered.Header.TraceState.Value);
            Assert.Equal(message.Header.Baggage.ToString(), delivered.Header.Baggage.ToString());
            Assert.Equal("selected-λ", delivered.Header.Bag["marker"].ToString());
            var native = Assert.IsType<MessageView>(delivered.Header.Bag["ReceiptHandle"]);
            Assert.Same(reader.LastDelivery, native);
            Assert.Equal(topic.Value, native.Topic);
            Assert.Equal(tag, native.Tag);
            Assert.Contains("key-" + iteration, native.Keys);
            Assert.StartsWith(compose ? "Capped-body-" : "W:", Encoding.UTF8.GetString(native.Body));
            Assert.Equal(compose ? "composed" : "complete", native.Properties["custom-format"]);
            Assert.Equal(Math.Max(0, native.DeliveryAttempt - 1), delivered.Header.HandledCount);
            Assert.Equal(bagBefore, JsonSerializer.Serialize(message.Header.Bag));
            Assert.Equal("mapped-body-π-" + iteration, message.Body.Value);
            Assert.Equal("mapped-body-π-" + iteration, Encoding.UTF8.GetString(message.Body.Memory.Span));
            Assert.Equal(0, inactive.Calls);
            delivered.Body = new MessageBody("retry must retain original native payload");

            //Act
            Assert.True(useAsync ? await consumer.RequeueAsync(delivered, TimeSpan.Zero) : consumer.Requeue(delivered, TimeSpan.Zero));
            var retry = await RocketMappingDelivery.ReceiveAsync(consumer, useAsync);

            //Assert
            var redelivery = Assert.IsType<MessageView>(retry.Header.Bag["ReceiptHandle"]);
            Assert.Equal(native.MessageId, redelivery.MessageId);
            Assert.True(redelivery.DeliveryAttempt > native.DeliveryAttempt);
            Assert.Equal(Math.Max(0, redelivery.DeliveryAttempt - 1), retry.Header.HandledCount);
            Assert.Equal(message.Body.Value, retry.Body.Value);
            Assert.Equal(iteration + 1, writer.Calls);

            //Act
            if (useAsync) await consumer.AcknowledgeAsync(retry);
            else consumer.Acknowledge(retry);

            //Assert
            Assert.Equal(MessageType.MT_NONE, Assert.Single(await consumer.ReceiveAsync()).Header.MessageType);
        }
        Assert.Equal(4, reader.Calls);
    }
}

#pragma warning restore CS0618
