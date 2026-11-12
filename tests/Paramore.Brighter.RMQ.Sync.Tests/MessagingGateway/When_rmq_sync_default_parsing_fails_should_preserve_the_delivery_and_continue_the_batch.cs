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
using System.Linq;
using System.Text;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
[Collection("RMQ")]
public class RmqSyncDefaultParserFailureBatchTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void When_rmq_sync_default_parsing_fails_should_preserve_the_delivery_and_continue_the_batch(bool configureDefaultReader, bool badFirst)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-default-parser-failure-{Guid.NewGuid():N}";
        var topic = new RoutingKey("orders");
        byte[] failedBody = [0, 1, 255, 128, 42];
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
            channel.ConfirmSelect();
            foreach (var bad in new[] { badFirst, !badFirst })
            {
                var properties = channel.CreateBasicProperties();
                properties.MessageId = bad ? "bad-native" : "good-native";
                properties.ContentType = "application/octet-stream";
                properties.Type = "application/octet-stream";
                properties.Timestamp = new AmqpTimestamp(1700000000);
                properties.DeliveryMode = 2;
                properties.Headers = new Dictionary<string, object>
                {
                    [HeaderNames.HANDLED_COUNT] = 3,
                    [HeaderNames.MESSAGE_TYPE] = bad ? "not-a-message-type" : "MT_EVENT"
                };
                channel.BasicPublish(name, topic.Value, mandatory: true, basicProperties: properties,
                    body: bad ? failedBody : Encoding.UTF8.GetBytes("accepted"));
            }
            channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name)
            };
            var subscription = new RmqSubscription(new SubscriptionName(name), new ChannelName(name), topic,
                requestType: typeof(Command), isDurable: true, bufferSize: 2, makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = configureDefaultReader ? new DefaultRmqWireMessageReader() : null
            };
            consumer = new RmqMessageConsumerFactory(connection).Create(subscription);

            //Act
            Message[] messages = [];
            var exception = Record.Exception(() =>
            {
                messages = consumer.Receive(TimeSpan.FromSeconds(10));
            });

            //Assert
            Assert.Null(exception);
            Assert.Equal(2, messages.Length);
            var failed = messages[badFirst ? 0 : 1];
            var good = messages[badFirst ? 1 : 0];
#pragma warning disable CS0618 // Verify the delivery control signal used by the message pump.
            Assert.Equal(MessageType.MT_UNACCEPTABLE, failed.Header.MessageType);
#pragma warning restore CS0618
            Assert.Equal(new Id("bad-native"), failed.Id);
            Assert.Equal(topic, failed.Header.Topic);
            Assert.Equal(failedBody, failed.Body.Memory.ToArray());
#pragma warning disable CS0618 // Verify the successful delivery remains an application event.
            Assert.Equal(MessageType.MT_EVENT, good.Header.MessageType);
#pragma warning restore CS0618
            Assert.Equal(new Id("good-native"), good.Id);
            Assert.Equal(topic, good.Header.Topic);
            Assert.Equal("accepted", good.Body.Value);
            Assert.Equal(2, messages.Select(message => message.DeliveryTag).Distinct().Count());
            foreach (var message in messages)
            {
                Assert.InRange(message.DeliveryTag, 1UL, ulong.MaxValue - 1);
                Assert.False(message.Redelivered);
                Assert.True(message.Persist);
                Assert.Equal(3, message.Header.HandledCount);
            }

            //Act
            foreach (var message in messages) consumer.Acknowledge(message);
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
