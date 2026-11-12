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
using System.Diagnostics;
using System.Text;
using System.Threading;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
[Collection("RMQ")]
public class RmqSyncInvalidWriterNativeDeadLetterTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void When_rmq_sync_rejection_uses_native_dead_lettering_should_preserve_original_wire_content(
        bool unacceptableWithoutInvalidRoute)
    {
        //Arrange
        byte[] body = [0, 255, 128, 65];
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-native-rejection-{Guid.NewGuid():N}";
        var deadName = name + "-dead";
        var invalidName = name + "-invalid";
        var topic = new RoutingKey("legacy-orders");
        var createdQueues = new List<string>();
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        IAmAMessageConsumerSync? consumer = null;
        channel.ExchangeDeclare(name, ExchangeType.Direct, durable: false, autoDelete: false);
        try
        {
            channel.QueueDeclare(name, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    ["x-dead-letter-exchange"] = name,
                    ["x-dead-letter-routing-key"] = deadName
                });
            createdQueues.Add(name);
            channel.QueueBind(name, name, topic.Value);
            foreach (var destination in new[] { deadName, invalidName })
            {
                channel.QueueDeclare(destination, durable: true, exclusive: false, autoDelete: false);
                createdQueues.Add(destination);
                channel.QueueBind(destination, name, destination);
            }
            var properties = channel.CreateBasicProperties();
            properties.MessageId = "original-native-id";
            properties.ContentType = "application/octet-stream";
            properties.Type = "application/octet-stream";
            properties.Timestamp = new AmqpTimestamp(1700000000);
            properties.DeliveryMode = 2;
            properties.Headers = new Dictionary<string, object>
            {
                [HeaderNames.MESSAGE_TYPE] = "MT_EVENT",
                [HeaderNames.HANDLED_COUNT] = 2,
                ["native-profile"] = "source-profile"
            };
            channel.ConfirmSelect();
            channel.BasicPublish(name, topic.Value, mandatory: true, basicProperties: properties, body: body);
            channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            var factory = new RmqMessageConsumerFactory(new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name)
            });
            var subscription = new RmqSubscription(new SubscriptionName(name), new ChannelName(name), topic,
                requestType: typeof(Command), isDurable: true,
                deadLetterChannelName: new ChannelName(deadName), deadLetterRoutingKey: new RoutingKey(deadName),
                makeChannels: OnMissingChannel.Validate)
            {
                InvalidMessageRoutingKey = unacceptableWithoutInvalidRoute ? null : new RoutingKey(invalidName),
                InvalidMessageWireMessageWriter = new SyncRejectionProfileWireWriter(compose: true) { FailureMode = 1 }
            };
            consumer = factory.Create(subscription);
            var message = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));
            Assert.Equal(2, message.Header.HandledCount);
            message.Header.UpdateHandledCount();
            var reason = new MessageRejectionReason(unacceptableWithoutInvalidRoute
                ? RejectionReason.Unacceptable : RejectionReason.DeliveryError, "Native rejection");

            //Act
            var rejected = consumer.Reject(message, reason);
            consumer.Dispose();
            consumer = null;
            BasicGetResult? forwarded = null;
            var forwardingWait = Stopwatch.StartNew();
            while (forwarded == null && forwardingWait.Elapsed < TimeSpan.FromSeconds(5))
            {
                forwarded = channel.BasicGet(deadName, autoAck: true);
                if (forwarded == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
            }

            //Assert
            Assert.True(rejected);
            Assert.NotNull(forwarded);
            Assert.Equal(body, forwarded.Body.ToArray());
            Assert.Equal("original-native-id", forwarded.BasicProperties.MessageId);
            Assert.Equal("application/octet-stream", forwarded.BasicProperties.ContentType);
            Assert.Equal((byte)2, forwarded.BasicProperties.DeliveryMode);
            Assert.Equal(deadName, forwarded.RoutingKey);
            var headers = forwarded.BasicProperties.Headers;
            Assert.NotNull(headers);
            Assert.Equal("source-profile", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["native-profile"])));
            Assert.Equal(2, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
            Assert.False(headers.ContainsKey("rejection-profile"));
            Assert.True(headers.ContainsKey("x-death"));
            Assert.Null(channel.BasicGet(invalidName, autoAck: true));
            BasicGetResult? remaining = null;
            var settlementWait = Stopwatch.StartNew();
            while (remaining == null && settlementWait.Elapsed < TimeSpan.FromSeconds(2))
            {
                remaining = channel.BasicGet(name, autoAck: true);
                if (remaining == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
            }
            Assert.Null(remaining);
            Assert.Null(channel.BasicGet(deadName, autoAck: true));
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
                    foreach (var queueName in createdQueues)
                        channel.QueueDelete(queueName, ifUnused: false, ifEmpty: false);
                }
                finally
                {
                    channel.ExchangeDelete(name);
                }
            }
        }
    }
}
