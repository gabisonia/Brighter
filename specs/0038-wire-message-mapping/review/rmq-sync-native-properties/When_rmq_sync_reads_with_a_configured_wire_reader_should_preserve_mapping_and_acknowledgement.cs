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
using System.Collections.Generic;
using System.Text;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
[Collection("RMQ")]
public class RmqSyncEndpointReaderTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void When_rmq_sync_reads_with_a_configured_wire_reader_should_preserve_mapping_and_acknowledgement(bool composeDefault)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-reader-{Guid.NewGuid():N}";
        var topic = new RoutingKey("orders");
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        channel.ExchangeDeclare(name, ExchangeType.Direct, durable: false, autoDelete: false);
        var queueCreated = false;
        IAmAMessageConsumerSync? consumer = null;
        try
        {
            channel.QueueDeclare(name, durable: true, exclusive: false, autoDelete: false);
            queueCreated = true;
            channel.QueueBind(name, name, topic.Value);
            var properties = channel.CreateBasicProperties();
            properties.MessageId = composeDefault ? "order-17" : "transport-only";
            properties.ContentType = "text/plain";
            properties.Type = "text/plain";
            properties.Timestamp = new AmqpTimestamp(1700000000);
            properties.DeliveryMode = 2;
            properties.Headers = new Dictionary<string, object>
            {
                ["legacy-id"] = "order-17", ["tenant-id"] = "tenant-42",
                [HeaderNames.HANDLED_COUNT] = 3
            };
            if (composeDefault)
            {
                properties.Headers[HeaderNames.MESSAGE_TYPE] = "MT_EVENT";
                properties.Headers[HeaderNames.TOPIC] = "application-topic";
            }
            channel.ConfirmSelect();
            channel.BasicPublish(name, topic.Value, mandatory: true, basicProperties: properties,
                body: Encoding.UTF8.GetBytes("accepted"));
            channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name)
            };
            var subscription = new RmqSubscription(new SubscriptionName(name), new ChannelName(name), topic,
                requestType: typeof(Command), isDurable: true, makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = new SyncEndpointWireReader(composeDefault)
            };
            consumer = new RmqMessageConsumerFactory(connection).Create(subscription);

            //Act
            var message = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));

            //Assert
            Assert.Equal(new Id("order-17"), message.Id);
            Assert.Equal("accepted", message.Body.Value);
            Assert.Equal(new RoutingKey(composeDefault ? "application-topic" : "mapped-topic"), message.Header.Topic);
#pragma warning disable CS0618 // Verify compatibility of the legacy transport message type.
            Assert.Equal(MessageType.MT_EVENT, message.Header.MessageType);
#pragma warning restore CS0618
            Assert.True(message.Header.Bag.ContainsKey("tenant_id"));
            Assert.Equal("tenant-42", message.Header.Bag["tenant_id"]);
            Assert.False(message.Header.Bag.ContainsKey("tenant-id"));
            Assert.InRange(message.DeliveryTag, 1UL, ulong.MaxValue - 1);
            Assert.False(message.Redelivered);
            Assert.True(message.Persist);
            Assert.Equal(3, message.Header.HandledCount);
            if (composeDefault)
                Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1700000000), message.Header.TimeStamp);

            //Act
            consumer.Acknowledge(message);
            consumer.Dispose();
            consumer = null;

            //Assert
            Assert.Null(channel.BasicGet(name, autoAck: true));
        }
        finally
        {
            try
            {
                consumer?.Dispose();
            }
            finally
            {
                try
                {
                    if (queueCreated) channel.QueueDelete(name, ifUnused: false, ifEmpty: false);
                }
                finally
                {
                    channel.ExchangeDelete(name);
                }
            }
        }
    }
}
