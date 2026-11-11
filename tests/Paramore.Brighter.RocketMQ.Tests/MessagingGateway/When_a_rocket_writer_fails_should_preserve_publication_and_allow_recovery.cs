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
using System.Text;
using Org.Apache.Rocketmq;
using Paramore.Brighter.MessagingGateway.RocketMQ;
using Paramore.Brighter.RocketMQ.Tests.TestDoubles;
using Paramore.Brighter.RocketMQ.Tests.Utils;
using Xunit;

namespace Paramore.Brighter.RocketMQ.Tests.MessagingGateway;

/// <summary>Verifies failure containment and subsequent native recovery.</summary>
[Trait("Category", "RocketMQ")]
[Collection("RocketMQMessagingGateway")]
public class RocketWireMappingFailureTests
{
    /// <summary>Fails before native publication and recovers on the same producer.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_rocket_writer_fails_should_preserve_publication_and_allow_recovery(bool useAsync, bool returnNull)
    {
        //Arrange
        var connection = GatewayFactory.CreateConnection();
        var topic = new RoutingKey(useAsync ? "rmq_mapping_failure_p" : "rmq_mapping_failure_r");
        var tag = "writer-" + Guid.NewGuid().ToString("N");
        var writer = new RocketRecoverableWireWriter(returnNull);
        var publication = new RocketMqPublication { Topic = topic, Tag = tag, WireMessageWriter = writer };
        await using var nativeProducer = await GatewayFactory.CreateProducer(connection, publication);
        await using var producer = new RocketMqMessageProducer(connection, nativeProducer, publication);
        var nativeConsumer = await new SimpleConsumer.Builder()
            .SetClientConfig(connection.ClientConfig)
            .SetConsumerGroup(Guid.NewGuid().ToString("N"))
            .SetAwaitDuration(TimeSpan.Zero)
            .SetSubscriptionExpression(new Dictionary<string, FilterExpression> { [topic.Value] = new(tag) })
            .Build();
        await using var consumer = new RocketMessageConsumer(nativeConsumer, 1, TimeSpan.FromSeconds(30));
        await consumer.PurgeAsync();
        var message = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT), new MessageBody("writer-recover-π"));

        //Act
        var error = await Record.ExceptionAsync(async () =>
        {
            if (useAsync) await producer.SendAsync(message);
            else producer.Send(message);
        });

        //Assert
        if (returnNull) Assert.IsType<InvalidOperationException>(error);
        else Assert.Same(writer.Error, error);
        Assert.Equal(MessageType.MT_NONE, Assert.Single(await consumer.ReceiveAsync()).Header.MessageType);
        Assert.Equal("writer-recover-π", message.Body.Value);

        //Act
        writer.Recover();
        if (useAsync) await producer.SendAsync(message);
        else producer.Send(message);
        var recovered = await RocketMappingDelivery.ReceiveAsync(consumer, useAsync);

        //Assert
        Assert.Equal(message.Id, recovered.Id);
        Assert.Equal(message.Body.Value, recovered.Body.Value);
        if (useAsync) await consumer.AcknowledgeAsync(recovered);
        else consumer.Acknowledge(recovered);
        Assert.Equal(MessageType.MT_NONE, Assert.Single(await consumer.ReceiveAsync()).Header.MessageType);
    }

    /// <summary>Returns raw rejectable deliveries with authentic receipts and recovers on the same consumer.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_rocket_reader_fails_should_preserve_settlement_and_allow_recovery(bool useAsync, bool returnNull)
    {
        //Arrange
        var connection = GatewayFactory.CreateConnection();
        var topic = new RoutingKey(useAsync ? "rmq_mapping_failure_p" : "rmq_mapping_failure_r");
        var tag = "reader-" + Guid.NewGuid().ToString("N");
        var publication = new RocketMqPublication { Topic = topic, Tag = tag };
        await using var nativeProducer = await GatewayFactory.CreateProducer(connection, publication);
        await using var producer = new RocketMqMessageProducer(connection, nativeProducer, publication);
        var reader = new RocketRecoverableWireReader(returnNull);
        var subscription = new RocketSubscription(new SubscriptionName("failure"), new ChannelName(topic.Value), topic,
            requestType: typeof(Message), consumerGroup: Guid.NewGuid().ToString("N"), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            receiveMessageTimeout: TimeSpan.FromSeconds(1), invisibilityTimeout: TimeSpan.FromSeconds(30),
            filter: new FilterExpression(tag))
        { WireMessageReader = reader };
        var factory = new RocketMessageConsumerFactory(connection);
        await using var consumer = useAsync ? (RocketMessageConsumer)factory.CreateAsync(subscription) : (RocketMessageConsumer)factory.Create(subscription);
        await consumer.PurgeAsync();
        var message = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT), new MessageBody("reader-recover-π"));
        if (useAsync) await producer.SendAsync(message);
        else producer.Send(message);

        //Act
        Message? failed = null;
        var error = await Record.ExceptionAsync(async () => failed = await RocketMappingDelivery.ReceiveAsync(consumer, useAsync));

        //Assert
        Assert.Null(error);
        Assert.NotNull(failed);
        Assert.Equal(MessageType.MT_UNACCEPTABLE, failed.Header.MessageType);
        var native = Assert.IsType<MessageView>(failed.Header.Bag["ReceiptHandle"]);
        Assert.Same(reader.LastDelivery, native);
        Assert.Equal(native.MessageId, failed.Id.Value);
        Assert.Equal(message.Body.Value, failed.Body.Value);
        Assert.Equal(Encoding.UTF8.GetString(native.Body), failed.Body.Value);
        Assert.Equal(Math.Max(0, native.DeliveryAttempt - 1), failed.Header.HandledCount);

        //Act
        Assert.True(useAsync ? await consumer.RejectAsync(failed, new MessageRejectionReason(RejectionReason.Unacceptable))
            : consumer.Reject(failed, new MessageRejectionReason(RejectionReason.Unacceptable)));

        //Assert
        Assert.Equal(MessageType.MT_NONE, Assert.Single(await consumer.ReceiveAsync()).Header.MessageType);

        //Act
        reader.Recover();
        if (useAsync) await producer.SendAsync(message);
        else producer.Send(message);
        var recovered = await RocketMappingDelivery.ReceiveAsync(consumer, useAsync);

        //Assert
        Assert.Equal(message.Id, recovered.Id);
        Assert.Equal(message.Body.Value, recovered.Body.Value);
        if (useAsync) await consumer.AcknowledgeAsync(recovered);
        else consumer.Acknowledge(recovered);
        Assert.Equal(MessageType.MT_NONE, Assert.Single(await consumer.ReceiveAsync()).Header.MessageType);
    }
}

#pragma warning restore CS0618
