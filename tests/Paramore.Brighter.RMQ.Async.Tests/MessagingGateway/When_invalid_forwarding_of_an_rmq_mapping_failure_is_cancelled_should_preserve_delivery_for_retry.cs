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
public class RmqWireReaderForwardingCancellationTests
{
    [Theory]
    [InlineData("throwing")]
    [InlineData("null")]
    [InlineData("unconfigured-content-type")]
    [InlineData("unconfigured-body-type")]
    [InlineData("default-content-type")]
    [InlineData("default-body-type")]
    public async Task When_invalid_forwarding_of_an_rmq_mapping_failure_is_cancelled_should_preserve_delivery_for_retry(string readerMode)
    {
        //Arrange
        byte[] body = [0x00, 0xFF, 0x80, 0x41];
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"mapping-forwarding-cancel-{Guid.NewGuid():N}";
        var queueName = $"mapping-forwarding-cancel-{Guid.NewGuid():N}";
        var invalidQueueName = $"reader-invalid-{Guid.NewGuid():N}";
        var invalidTopic = new RoutingKey(invalidQueueName);
        var topic = new RoutingKey("legacy-orders");
        var applicationTopic = new RoutingKey("application-topic");
        var expectedTopic = readerMode is "throwing" or "null" ? topic : applicationTopic;
        var malformedBodyType = readerMode.EndsWith("body-type", StringComparison.Ordinal);
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
            await channel.QueueDeclareAsync(invalidQueueName, durable: true, exclusive: false,
                autoDelete: false, cancellationToken: timeout.Token);
            await channel.QueueBindAsync(invalidQueueName, exchangeName, invalidTopic.Value,
                cancellationToken: timeout.Token);
            var properties = new BasicProperties
            {
                MessageId = "native-message-id",
                ContentType = malformedBodyType ? "application/octet-stream" : "not a valid MIME type",
                Type = malformedBodyType ? "not a valid MIME type" : "application/octet-stream",
                DeliveryMode = DeliveryModes.Persistent,
                Headers = new Dictionary<string, object?>
                {
                    [HeaderNames.TOPIC] = Encoding.UTF8.GetBytes(applicationTopic.Value)
                }
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
                messagePumpType: MessagePumpType.Proactor,
                makeChannels: OnMissingChannel.Validate)
            {
                InvalidMessageRoutingKey = invalidTopic,
                WireMessageReader = readerMode switch
                {
                    "throwing" => new ThrowingRmqWireMessageReader(),
                    "null" => new NullRmqWireMessageReader(),
                    "default-content-type" or "default-body-type" => new DefaultRmqWireMessageReader(),
                    _ => null
                }
            };
            var factory = new RmqMessageConsumerFactory(connection);
            consumer = factory.CreateAsync(subscription);

            //Act
            Message[] messages = [];
            var receiveException = await Record.ExceptionAsync(async () =>
            {
                messages = await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token);
            });

            //Assert
            Assert.Null(receiveException);
            var message = Assert.Single(messages);
            Assert.Equal(MessageType.MT_UNACCEPTABLE, message.Header.MessageType);
            Assert.Equal("native-message-id", message.Id.Value);
            Assert.Equal(expectedTopic, message.Header.Topic);
            Assert.Equal(body, message.Body.Memory.ToArray());
            Assert.True(message.DeliveryTag > 0);
            Assert.False(message.Redelivered);
            Assert.True(message.Persist);

            //Act
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "Invalid legacy payload");
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            var forwardingException = await Record.ExceptionAsync(async () =>
                await consumer.RejectAsync(message, reason, cancellation.Token));

            //Assert
            Assert.IsAssignableFrom<OperationCanceledException>(forwardingException);
            Assert.Equal(expectedTopic, message.Header.Topic);
            Assert.Equal(body, message.Body.Memory.ToArray());

            //Act
            await consumer.DisposeAsync();
            consumer = null;
            var retryDelivery = await channel.BasicGetAsync(queueName, autoAck: false,
                cancellationToken: timeout.Token);

            //Assert
            Assert.NotNull(retryDelivery);
            Assert.Equal("native-message-id", retryDelivery.BasicProperties.MessageId);
            Assert.Equal(body, retryDelivery.Body.ToArray());
            Assert.Equal(topic.Value, retryDelivery.RoutingKey);
            Assert.Equal(properties.ContentType, retryDelivery.BasicProperties.ContentType);
            Assert.Equal(properties.Type, retryDelivery.BasicProperties.Type);
            Assert.Equal(DeliveryModes.Persistent, retryDelivery.BasicProperties.DeliveryMode);
            var retryHeaders = retryDelivery.BasicProperties.Headers;
            Assert.NotNull(retryHeaders);
            Assert.Equal(applicationTopic.Value,
                Encoding.UTF8.GetString(Assert.IsType<byte[]>(retryHeaders[HeaderNames.TOPIC])));
            Assert.True(retryDelivery.Redelivered);
            var prematureCopy = await channel.BasicGetAsync(invalidQueueName, autoAck: true,
                cancellationToken: timeout.Token);
            Assert.Null(prematureCopy);

            //Act
            await channel.BasicNackAsync(retryDelivery.DeliveryTag, multiple: false, requeue: true,
                cancellationToken: timeout.Token);
            consumer = factory.CreateAsync(subscription);
            messages = await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token);

            //Assert
            message = Assert.Single(messages);
            Assert.Equal(MessageType.MT_UNACCEPTABLE, message.Header.MessageType);
            Assert.Equal("native-message-id", message.Id.Value);
            Assert.Equal(expectedTopic, message.Header.Topic);
            Assert.Equal(body, message.Body.Memory.ToArray());
            Assert.True(message.Redelivered);
            Assert.True(message.DeliveryTag > 0);
            Assert.True(message.Persist);

            //Act
            var rejected = await consumer.RejectAsync(message, reason, timeout.Token);
            Assert.True(rejected);
            await consumer.DisposeAsync();
            consumer = null;
            var remaining = await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token);

            //Assert
            Assert.Null(remaining);
            Assert.Equal(expectedTopic, message.Header.Topic);
            var forwarded = await channel.BasicGetAsync(invalidQueueName, autoAck: true,
                cancellationToken: timeout.Token);
            Assert.NotNull(forwarded);
            Assert.Equal(body, forwarded.Body.ToArray());
            Assert.Equal("native-message-id", forwarded.BasicProperties.MessageId);
            Assert.Equal(invalidTopic.Value, forwarded.RoutingKey);
            Assert.Equal(DeliveryModes.Persistent, forwarded.BasicProperties.DeliveryMode);
            var headers = forwarded.BasicProperties.Headers;
            Assert.NotNull(headers);
            Assert.Equal(invalidTopic.Value,
                Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.TOPIC])));
            Assert.Equal(expectedTopic.Value,
                Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_TOPIC])));
            Assert.Equal("MT_UNACCEPTABLE",
                Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_TYPE])));
            Assert.Equal("Unacceptable",
                Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.REJECTION_REASON])));
            Assert.Equal("Invalid legacy payload",
                Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.REJECTION_MESSAGE])));
            var extra = await channel.BasicGetAsync(invalidQueueName, autoAck: true,
                cancellationToken: timeout.Token);
            Assert.Null(extra);
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
                    try
                    {
                        await channel.QueueDeleteAsync(queueName, ifUnused: false, ifEmpty: false);
                    }
                    finally
                    {
                        await channel.QueueDeleteAsync(invalidQueueName, ifUnused: false, ifEmpty: false);
                    }
                }
                finally
                {
                    await channel.ExchangeDeleteAsync(exchangeName);
                }
            }
        }
    }
}
