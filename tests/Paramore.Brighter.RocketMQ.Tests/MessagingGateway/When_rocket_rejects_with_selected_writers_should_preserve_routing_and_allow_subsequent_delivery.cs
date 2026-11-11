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
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Org.Apache.Rocketmq;
using Paramore.Brighter.MessagingGateway.RocketMQ;
using Paramore.Brighter.RocketMQ.Tests.TestDoubles;
using Paramore.Brighter.RocketMQ.Tests.Utils;
using Xunit;
using Message = Paramore.Brighter.Message;

namespace Paramore.Brighter.RocketMQ.Tests.MessagingGateway;

/// <summary>Verifies native rejection mappings and captured lazy-producer selection.</summary>
[Trait("Category", "RocketMQ")]
[Collection("RocketMQMessagingGateway")]
public class RocketRejectionWireSelectionTests
{
    /// <summary>Preserves destination and rejection metadata, and allows subsequent mapping recovery.</summary>
    [Theory]
    [InlineData(false, false, "mapped")]
    [InlineData(false, true, "mapped")]
    [InlineData(true, false, "mapped")]
    [InlineData(true, true, "mapped")]
    [InlineData(false, false, "throw")]
    [InlineData(false, true, "throw")]
    [InlineData(true, false, "throw")]
    [InlineData(true, true, "throw")]
    [InlineData(false, false, "null")]
    [InlineData(false, true, "null")]
    [InlineData(true, false, "null")]
    [InlineData(true, true, "null")]
    public async Task When_rocket_rejects_with_selected_writers_should_preserve_routing_and_allow_subsequent_delivery(
        bool useAsync, bool invalid, string mode)
    {
        //Arrange
        var connection = GatewayFactory.CreateConnection();
        var sourceTopic = new RoutingKey("rmq_dlq_source");
        var deadLetter = new RoutingKey("rmq_dlq_target");
        var invalidRoute = new RoutingKey("rmq_dlq_invalid");
        var target = invalid ? invalidRoute : deadLetter;
        var tag = "forward-" + Guid.NewGuid().ToString("N");
        var mapped = new RocketSelectedWireWriter(true);
        var failing = new RocketRecoverableWireWriter(mode == "null");
        IAmAWireMessageWriter<RocketMqWireMessage> selected = mode == "mapped" ? mapped : failing;
        var inactive = new RocketSelectedWireWriter(false);
        var publication = new RocketMqPublication { Topic = sourceTopic, Tag = tag };
        await using var nativeProducer = await GatewayFactory.CreateProducer(connection, publication);
        await using var producer = new RocketMqMessageProducer(connection, nativeProducer, publication);
        var subscription = new RocketSubscription(new SubscriptionName("forward"), new ChannelName(sourceTopic.Value), sourceTopic,
            requestType: typeof(Message), consumerGroup: Guid.NewGuid().ToString("N"),
            messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            receiveMessageTimeout: TimeSpan.FromSeconds(1), invisibilityTimeout: TimeSpan.FromSeconds(30),
            filter: new FilterExpression(tag), deadLetterRoutingKey: deadLetter, invalidMessageRoutingKey: invalidRoute)
        {
            DeadLetterWireMessageWriter = invalid ? inactive : selected,
            InvalidMessageWireMessageWriter = invalid ? selected : inactive
        };
        var factory = new RocketMessageConsumerFactory(connection);
        await using var consumer = useAsync ? (RocketMessageConsumer)factory.CreateAsync(subscription) : (RocketMessageConsumer)factory.Create(subscription);
        subscription.DeadLetterWireMessageWriter = inactive;
        subscription.InvalidMessageWireMessageWriter = inactive;
        await using var observer = await new SimpleConsumer.Builder()
            .SetClientConfig(connection.ClientConfig).SetConsumerGroup(Guid.NewGuid().ToString("N"))
            .SetAwaitDuration(TimeSpan.FromSeconds(1))
            .SetSubscriptionExpression(new Dictionary<string, FilterExpression>
            {
                [target.Value] = new(tag)
            }).Build();
        await using var otherObserver = await new SimpleConsumer.Builder()
            .SetClientConfig(connection.ClientConfig).SetConsumerGroup(Guid.NewGuid().ToString("N"))
            .SetAwaitDuration(TimeSpan.FromSeconds(1))
            .SetSubscriptionExpression(new Dictionary<string, FilterExpression>
            {
                [(invalid ? deadLetter : invalidRoute).Value] = new(tag)
            }).Build();
        using var telemetry = Sdk.CreateTracerProviderBuilder().Build();
        using var span = new Activity("rocket-forward").SetIdFormat(ActivityIdFormat.W3C).Start();
        span.TraceStateString = "tenant=forward";
        producer.Span = span;
        var reason = new MessageRejectionReason(invalid ? RejectionReason.Unacceptable : RejectionReason.DeliveryError, "mapping rejection");
        for (var attempt = 0; attempt < 2; attempt++)
        {
            var message = new Message(new MessageHeader(Id.Random(), sourceTopic, MessageType.MT_EVENT), new MessageBody("mapped-body-π-" + attempt));
            message.Header.Bag["tenant"] = "selected-λ";
            message.Header.Bag[HeaderNames.Tag] = tag;

            //Act
            if (useAsync) await producer.SendAsync(message);
            else producer.Send(message);
            var delivered = await RocketMappingDelivery.ReceiveAsync(consumer, useAsync);
            Assert.Equal(message.Id, delivered.Id);
            // The default reader does not project the native tag into the application bag.
            // Explicitly tag this forwarding operation so native observers isolate the case.
            delivered.Header.Bag[HeaderNames.Tag] = tag;
            Assert.True(useAsync ? await consumer.RejectAsync(delivered, reason) : consumer.Reject(delivered, reason));
            var forwarded = await observer.Receive(4, TimeSpan.FromSeconds(30));
            var waiting = Stopwatch.StartNew();
            while ((mode == "mapped" || attempt > 0) && forwarded.Count == 0 && waiting.Elapsed < TimeSpan.FromSeconds(10))
                forwarded = await observer.Receive(4, TimeSpan.FromSeconds(30));

            //Assert
            if (mode != "mapped" && attempt == 0)
            {
                Assert.Empty(forwarded);
                failing.Recover();
            }
            else
            {
                var native = Assert.Single(forwarded);
                Assert.Equal(target.Value, native.Topic);
                Assert.Equal(tag, native.Tag);
                Assert.Equal(message.Id.Value, native.Properties[HeaderNames.MessageId]);
                Assert.Equal(mode == "mapped" ? "Capped-body-π-" + attempt : message.Body.Value, Encoding.UTF8.GetString(native.Body));
                if (mode == "mapped") Assert.Equal("composed", native.Properties["custom-format"]);
                Assert.Equal(sourceTopic.Value, native.Properties[RejectionMetadataKeyNames.OriginalTopic]);
                Assert.Equal(reason.RejectionReason.ToString(), native.Properties[RejectionMetadataKeyNames.RejectionReason]);
                Assert.Equal("mapping rejection", native.Properties[RejectionMetadataKeyNames.RejectionMessage]);
                Assert.Equal("selected-λ", native.Properties["tenant"]);
                Assert.Equal(span.Id, native.Properties[HeaderNames.TraceParent]);
                Assert.Equal(span.TraceStateString, native.Properties[HeaderNames.TraceState]);
                await observer.Ack(native);
            }
            Assert.Equal(0, inactive.Calls);
            Assert.Empty(await otherObserver.Receive(4, TimeSpan.FromSeconds(30)));
            // Existing rejection settles the source even when the selected mapping fails.
            Assert.True(Assert.Single(useAsync ? await consumer.ReceiveAsync() : consumer.Receive()).IsEmpty);
        }
        if (mode == "mapped") Assert.Equal(2, mapped.Calls);
    }
}
