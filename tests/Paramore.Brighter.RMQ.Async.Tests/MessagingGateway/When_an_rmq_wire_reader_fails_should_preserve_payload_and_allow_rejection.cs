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
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.RMQ.Async.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
public class RmqWireReaderFailureTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_an_rmq_wire_reader_fails_should_preserve_payload_and_allow_rejection(bool useAsync, bool returnsNull)
    {
        //Arrange
        byte[] body = [0x00, 0xFF, 0x80, 0x41];
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"wire-reader-failure-{Guid.NewGuid():N}";
        var queueName = $"wire-reader-failure-{Guid.NewGuid():N}";
        var topic = new RoutingKey("legacy-orders");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
        await using var channel = await broker.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            timeout.Token);
        IAmAMessageConsumerAsync? consumer = null;

        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct, durable: false,
            autoDelete: false, cancellationToken: timeout.Token);
        try
        {
            await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false,
                autoDelete: false, cancellationToken: timeout.Token);
            await channel.QueueBindAsync(queueName, exchangeName, topic.Value, cancellationToken: timeout.Token);
            var properties = new BasicProperties
            {
                MessageId = "native-message-id",
                ContentType = "application/octet-stream",
                DeliveryMode = DeliveryModes.Persistent
            };
            await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                basicProperties: properties, body: body, cancellationToken: timeout.Token);
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri),
                Exchange = new Exchange(exchangeName)
            };
            var subscription = new RmqSubscription(
                new SubscriptionName(queueName),
                new ChannelName(queueName),
                topic,
                requestType: typeof(Command),
                isDurable: true,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                makeChannels: OnMissingChannel.Validate)
            {
                WireMessageReader = returnsNull
                    ? new NullRmqWireMessageReader()
                    : new ThrowingRmqWireMessageReader()
            };
            var factory = new RmqMessageConsumerFactory(connection);
            consumer = useAsync
                ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));

            //Act
            Message[] messages = [];
            var receiveException = await Record.ExceptionAsync(async () =>
            {
                messages = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
            });

            //Assert
            Assert.Null(receiveException);
            var message = Assert.Single(messages);
            Assert.Equal(MessageType.MT_UNACCEPTABLE, message.Header.MessageType);
            Assert.Equal("native-message-id", message.Id.Value);
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(body, message.Body.Memory.ToArray());
            Assert.True(message.DeliveryTag > 0);
            Assert.False(message.Redelivered);
            Assert.True(message.Persist);

            //Act
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "Invalid legacy payload");
            var rejected = useAsync
                ? await consumer.RejectAsync(message, reason, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(message, reason);
            Assert.True(rejected);
            await consumer.DisposeAsync();
            consumer = null;
            var remaining = await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token);

            //Assert
            Assert.Null(remaining);
        }
        finally
        {
            try
            {
                if (consumer != null)
                    await consumer.DisposeAsync();
            }
            finally
            {
                try
                {
                    await channel.QueueDeleteAsync(queueName, ifUnused: false, ifEmpty: false);
                }
                finally
                {
                    await channel.ExchangeDeleteAsync(exchangeName);
                }
            }
        }
    }
}
