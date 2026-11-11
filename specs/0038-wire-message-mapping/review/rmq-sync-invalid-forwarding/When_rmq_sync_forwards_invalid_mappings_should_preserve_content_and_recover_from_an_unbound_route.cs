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
public class RmqSyncInvalidMappingForwardingTests
{
    [Theory]
    [InlineData("unset", false)]
    [InlineData("unset", true)]
    [InlineData("default", false)]
    [InlineData("default", true)]
    [InlineData("throw", false)]
    [InlineData("throw", true)]
    [InlineData("null", false)]
    [InlineData("null", true)]
    [InlineData("explicit", false)]
    [InlineData("explicit", true)]
    public void When_rmq_sync_forwards_invalid_mappings_should_preserve_content_and_recover_from_an_unbound_route(
        string readerMode, bool unbindBeforeReject)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-invalid-mapping-{Guid.NewGuid():N}";
        var invalidName = name + "-invalid";
        var nativeTopic = new RoutingKey("orders");
        var applicationTopic = new RoutingKey("application-source");
        var expectedTopic = readerMode == "explicit" ? new RoutingKey("mapped-invalid-topic")
            : readerMode is "unset" or "default" ? applicationTopic : nativeTopic;
        var expectedId = readerMode == "explicit" ? "mapped-invalid-id" : "native-id";
        byte[] nativeBody = [0, 1, 255, 128, 42];
        byte[] expectedBody = readerMode == "explicit" ? [7, 128, 0, 255] : nativeBody;
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        channel.ExchangeDeclare(name, ExchangeType.Direct, durable: false, autoDelete: false);
        var sourceCreated = false;
        var invalidCreated = false;
        IAmAMessageConsumerSync? consumer = null;
        try
        {
            channel.QueueDeclare(name, durable: true, exclusive: false, autoDelete: false);
            sourceCreated = true;
            channel.QueueBind(name, name, nativeTopic.Value);
            channel.QueueDeclare(invalidName, durable: true, exclusive: false, autoDelete: false);
            invalidCreated = true;
            channel.QueueBind(invalidName, name, invalidName);
            var properties = channel.CreateBasicProperties();
            properties.MessageId = "native-id";
            properties.ContentType = "application/octet-stream";
            properties.Type = "application/octet-stream";
            properties.Timestamp = new AmqpTimestamp(1700000000);
            properties.DeliveryMode = 2;
            properties.Headers = new Dictionary<string, object>
            {
                [HeaderNames.TOPIC] = applicationTopic.Value,
                [HeaderNames.MESSAGE_TYPE] = "not-a-message-type",
                [HeaderNames.HANDLED_COUNT] = 3
            };
            channel.ConfirmSelect();
            channel.BasicPublish(name, nativeTopic.Value, mandatory: true, basicProperties: properties, body: nativeBody);
            channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name)
            };
            var subscription = new RmqSubscription(new SubscriptionName(name), new ChannelName(name), nativeTopic,
                requestType: typeof(Command), isDurable: true, makeChannels: OnMissingChannel.Validate)
            {
                InvalidMessageRoutingKey = new RoutingKey(invalidName),
                WireMessageReader = readerMode switch
                {
                    "unset" => null,
                    "default" => new DefaultRmqWireMessageReader(),
                    _ => new SyncInvalidDeliveryWireReader(readerMode)
                }
            };
            var factory = new RmqMessageConsumerFactory(connection);
            consumer = factory.Create(subscription);

            //Act
            var message = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));

            //Assert
#pragma warning disable CS0618 // Verify the control signal consumed by the message pump.
            Assert.Equal(MessageType.MT_UNACCEPTABLE, message.Header.MessageType);
#pragma warning restore CS0618
            Assert.Equal(expectedId, message.Id.Value);
            Assert.Equal(expectedTopic, message.Header.Topic);
            Assert.Equal(expectedBody, message.Body.Memory.ToArray());
            Assert.InRange(message.DeliveryTag, 1UL, ulong.MaxValue - 1);
            Assert.False(message.Redelivered);
            Assert.True(message.Persist);
            Assert.Equal(3, message.Header.HandledCount);
            if (readerMode == "explicit") Assert.Equal("retained", message.Header.Bag["mapped-marker"]);
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "Invalid legacy payload");

            if (unbindBeforeReject)
            {
                //Act
                channel.QueueUnbind(invalidName, name, invalidName, null);
                var exception = Record.Exception(() => consumer.Reject(message, reason));

                //Assert
                Assert.IsType<ChannelFailureException>(exception);
                Assert.Equal(expectedTopic, message.Header.Topic);
                Assert.Equal(expectedBody, message.Body.Memory.ToArray());

                //Act
                consumer.Dispose();
                consumer = null;
                var retry = channel.BasicGet(name, autoAck: false);

                //Assert
                Assert.NotNull(retry);
                Assert.Equal("native-id", retry.BasicProperties.MessageId);
                Assert.Equal(nativeBody, retry.Body.ToArray());
                Assert.Equal(nativeTopic.Value, retry.RoutingKey);
                Assert.Equal(properties.ContentType, retry.BasicProperties.ContentType);
                Assert.Equal(properties.Type, retry.BasicProperties.Type);
                Assert.Equal((byte)2, retry.BasicProperties.DeliveryMode);
                Assert.True(retry.Redelivered);
                Assert.NotNull(retry.BasicProperties.Headers);
                Assert.Equal(applicationTopic.Value,
                    Encoding.UTF8.GetString(Assert.IsType<byte[]>(retry.BasicProperties.Headers[HeaderNames.TOPIC])));
                Assert.Equal("not-a-message-type",
                    Encoding.UTF8.GetString(Assert.IsType<byte[]>(retry.BasicProperties.Headers[HeaderNames.MESSAGE_TYPE])));
                Assert.Equal(3, Assert.IsType<int>(retry.BasicProperties.Headers[HeaderNames.HANDLED_COUNT]));
                Assert.Null(channel.BasicGet(invalidName, autoAck: true));

                //Act
                channel.BasicNack(retry.DeliveryTag, multiple: false, requeue: true);
                channel.QueueBind(invalidName, name, invalidName);
                consumer = factory.Create(subscription);
                message = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));

                //Assert
#pragma warning disable CS0618 // Verify the recovered delivery retains its control signal.
                Assert.Equal(MessageType.MT_UNACCEPTABLE, message.Header.MessageType);
#pragma warning restore CS0618
                Assert.Equal(expectedId, message.Id.Value);
                Assert.Equal(expectedTopic, message.Header.Topic);
                Assert.Equal(expectedBody, message.Body.Memory.ToArray());
                Assert.InRange(message.DeliveryTag, 1UL, ulong.MaxValue - 1);
                Assert.True(message.Redelivered);
                Assert.True(message.Persist);
                Assert.Equal(3, message.Header.HandledCount);
            }

            //Act
            var rejected = consumer.Reject(message, reason);
            consumer.Dispose();
            consumer = null;

            //Assert
            Assert.True(rejected);
            Assert.Equal(expectedTopic, message.Header.Topic);
            Assert.Equal(expectedBody, message.Body.Memory.ToArray());
            Assert.Null(channel.BasicGet(name, autoAck: true));
            var forwarded = channel.BasicGet(invalidName, autoAck: true);
            Assert.NotNull(forwarded);
            Assert.Equal(expectedBody, forwarded.Body.ToArray());
            Assert.Equal(expectedId, forwarded.BasicProperties.MessageId);
            Assert.Equal(invalidName, forwarded.RoutingKey);
            Assert.Equal((byte)2, forwarded.BasicProperties.DeliveryMode);
            var headers = forwarded.BasicProperties.Headers;
            Assert.NotNull(headers);
            Assert.Equal(invalidName, Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.TOPIC])));
            Assert.Equal(expectedTopic.Value, Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_TOPIC])));
            Assert.Equal("MT_UNACCEPTABLE", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_TYPE])));
            Assert.Equal("Unacceptable", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.REJECTION_REASON])));
            Assert.Equal("Invalid legacy payload", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.REJECTION_MESSAGE])));
            Assert.Equal(3, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
            if (readerMode == "explicit")
                Assert.Equal("retained", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["mapped-marker"])));
            Assert.Null(channel.BasicGet(invalidName, autoAck: true));
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
                    try
                    {
                        if (sourceCreated) channel.QueueDelete(name, ifUnused: false, ifEmpty: false);
                    }
                    finally
                    {
                        if (invalidCreated) channel.QueueDelete(invalidName, ifUnused: false, ifEmpty: false);
                    }
                }
                finally
                {
                    channel.ExchangeDelete(name);
                }
            }
        }
    }
}
