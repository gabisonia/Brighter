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
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
public class RmqDefaultParserMixedBatchTests
{
    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task When_an_rmq_batch_contains_a_default_parser_failure_should_preserve_every_delivery(bool useAsync, bool malformedBodyType, bool failureFirst)
    {
        //Arrange
        byte[] invalidBody = [0x00, 0xFF, 0x80, 0x41];
        byte[] validBody = [0x42, 0x43, 0x44];
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"default-parser-batch-{Guid.NewGuid():N}";
        var queueName = $"default-parser-batch-{Guid.NewGuid():N}";
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
            foreach (var invalid in new[] { failureFirst, !failureFirst })
            {
                var properties = new BasicProperties
                {
                    MessageId = invalid ? "invalid-message-id" : "valid-message-id",
                    ContentType = invalid && !malformedBodyType ? "not a valid MIME type" : "application/octet-stream",
                    Type = invalid && malformedBodyType ? "not a valid MIME type" : "application/octet-stream",
                    DeliveryMode = DeliveryModes.Persistent,
                    Headers = new Dictionary<string, object?>
                    {
                        [HeaderNames.TOPIC] = Encoding.UTF8.GetBytes("application-topic")
                    }
                };
                await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                    basicProperties: properties, body: invalid ? invalidBody : validBody,
                    cancellationToken: timeout.Token);
            }
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
                bufferSize: 2,
                isDurable: true,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                makeChannels: OnMissingChannel.Validate);
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
            Assert.Equal(2, messages.Length);
            var invalidMessage = Assert.Single(messages, message => message.Id.Value == "invalid-message-id");
            var validMessage = Assert.Single(messages, message => message.Id.Value == "valid-message-id");
            Assert.Equal(failureFirst ? invalidMessage : validMessage, messages[0]);
            Assert.Equal(MessageType.MT_UNACCEPTABLE, invalidMessage.Header.MessageType);
            Assert.Equal(MessageType.MT_EVENT, validMessage.Header.MessageType);
            Assert.Equal(invalidBody, invalidMessage.Body.Memory.ToArray());
            Assert.Equal(validBody, validMessage.Body.Memory.ToArray());
            Assert.Equal(new RoutingKey("application-topic"), invalidMessage.Header.Topic);
            Assert.Equal(new RoutingKey("application-topic"), validMessage.Header.Topic);
            Assert.True(invalidMessage.DeliveryTag > 0);
            Assert.True(validMessage.DeliveryTag > 0);
            Assert.NotEqual(invalidMessage.DeliveryTag, validMessage.DeliveryTag);
            Assert.False(invalidMessage.Redelivered);
            Assert.False(validMessage.Redelivered);
            Assert.True(invalidMessage.Persist);
            Assert.True(validMessage.Persist);

            //Act
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "Invalid legacy payload");
            if (useAsync)
            {
                await consumer.AcknowledgeAsync(validMessage, timeout.Token);
                Assert.True(await consumer.RejectAsync(invalidMessage, reason, timeout.Token));
            }
            else
            {
                var syncConsumer = Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer);
                syncConsumer.Acknowledge(validMessage);
                Assert.True(syncConsumer.Reject(invalidMessage, reason));
            }
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
