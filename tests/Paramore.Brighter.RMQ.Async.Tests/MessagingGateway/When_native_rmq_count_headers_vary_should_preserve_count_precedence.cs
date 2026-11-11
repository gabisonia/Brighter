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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.RMQ.Async.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
public class RmqHandledCountPrecedenceTests
{
    [Theory]
    [InlineData(false, "missing", 99)]
    [InlineData(false, "integer", 7)]
    [InlineData(false, "text", 7)]
    [InlineData(false, "malformed", 0)]
    [InlineData(false, "unsupported", 0)]
    [InlineData(true, "missing", 99)]
    [InlineData(true, "integer", 7)]
    [InlineData(true, "text", 7)]
    [InlineData(true, "malformed", 0)]
    [InlineData(true, "unsupported", 0)]
    public async Task When_native_rmq_count_headers_vary_should_preserve_count_precedence(
        bool useAsync, string countRepresentation, int expectedCount)
    {
        //Arrange
        const string BODY = "legacy order payload";
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"count-precedence-{Guid.NewGuid():N}";
        var queueName = $"count-precedence-{Guid.NewGuid():N}";
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
                ContentType = "text/plain",
                DeliveryMode = DeliveryModes.Persistent,
                Headers = new Dictionary<string, object?>
                {
                    ["legacy-id"] = Encoding.UTF8.GetBytes("order-17"),
                    ["tenant-id"] = Encoding.UTF8.GetBytes("tenant-42")
                }
            };
            if (countRepresentation != "missing")
            {
                properties.Headers[HeaderNames.HANDLED_COUNT] = countRepresentation switch
                {
                    "integer" => 7,
                    "text" => "7",
                    "malformed" => "not-a-count",
                    "unsupported" => 7L,
                    _ => throw new ArgumentOutOfRangeException(nameof(countRepresentation))
                };
            }
            await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                basicProperties: properties, body: Encoding.UTF8.GetBytes(BODY), cancellationToken: timeout.Token);
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
                WireMessageReader = new CountedLegacyRmqWireMessageReader()
            };
            var factory = new RmqMessageConsumerFactory(connection);
            consumer = useAsync
                ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));

            //Act
            var messages = useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));

            //Assert
            var message = Assert.Single(messages);
            Assert.Equal("order-17", message.Id.Value);
            Assert.Equal(BODY, message.Body.Value);
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(MessageType.MT_EVENT, message.Header.MessageType);
            Assert.Equal("text/plain", message.Header.ContentType?.MediaType);
            Assert.Equal("tenant-42", message.Header.Bag["tenant_id"]);
            Assert.True(message.DeliveryTag > 0);
            Assert.NotEqual(ulong.MaxValue, message.DeliveryTag);
            Assert.False(message.Redelivered);
            Assert.True(message.Persist);
            Assert.Equal(expectedCount, message.Header.HandledCount);

            //Act
            if (useAsync)
                await consumer.AcknowledgeAsync(message, timeout.Token);
            else
                Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Acknowledge(message);
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
