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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.RMQ.Async.Tests.TestDoubles;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
public class RmqInvalidWireWriterRecoveryTests
{
    [Theory]
    [InlineData(false, false, 0)]
    [InlineData(false, false, 1)]
    [InlineData(false, false, 2)]
    [InlineData(false, false, 3)]
    [InlineData(false, true, 0)]
    [InlineData(false, true, 1)]
    [InlineData(false, true, 2)]
    [InlineData(false, true, 3)]
    [InlineData(true, false, 0)]
    [InlineData(true, false, 1)]
    [InlineData(true, false, 2)]
    [InlineData(true, false, 3)]
    [InlineData(true, true, 0)]
    [InlineData(true, true, 1)]
    [InlineData(true, true, 2)]
    [InlineData(true, true, 3)]
    public async Task When_forwarding_with_a_selected_rmq_invalid_writer_should_preserve_mapping_and_recovery(
        bool useAsync, bool compose, int failureMode)
    {
        //Arrange
        byte[] body = [0x00, 0xFF, 0x80, 0x41];
        const string APPLICATION_ID = "rejected-order-17";
        const string DESCRIPTION = "Cannot process order-17";
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"invalid-writer-{Guid.NewGuid():N}";
        var queueName = $"invalid-writer-source-{Guid.NewGuid():N}";
        var invalidName = $"invalid-writer-destination-{Guid.NewGuid():N}";
        var topic = new RoutingKey("legacy-orders");
        var invalidTopic = new RoutingKey(invalidName);
        var writer = new RejectionProfileRmqWireMessageWriter(compose)
        {
            FailureMode = failureMode is 1 or 2 ? failureMode : 0
        };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
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
            await channel.QueueDeclareAsync(invalidName, durable: true, exclusive: false,
                autoDelete: false, cancellationToken: timeout.Token);
            await channel.QueueBindAsync(invalidName, exchangeName, invalidTopic.Value, cancellationToken: timeout.Token);
            await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                basicProperties: new BasicProperties
                {
                    MessageId = APPLICATION_ID,
                    ContentType = "application/octet-stream",
                    Type = "application/octet-stream",
                    DeliveryMode = DeliveryModes.Persistent,
                    Headers = new Dictionary<string, object?>
                    {
                        [HeaderNames.MESSAGE_TYPE] = "MT_EVENT",
                        [HeaderNames.HANDLED_COUNT] = 2
                    }
                }, body: body, cancellationToken: timeout.Token);
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(exchangeName)
            };
            var subscription = new RmqSubscription(
                new SubscriptionName(queueName), new ChannelName(queueName), topic,
                requestType: typeof(Command), isDurable: true,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                makeChannels: OnMissingChannel.Validate)
            {
                InvalidMessageRoutingKey = invalidTopic,
                InvalidMessageWireMessageWriter = writer,
                RequeueWireMessageWriter = new RecoverableRmqWireMessageWriter(returnNull: false)
            };
            var factory = new RmqMessageConsumerFactory(connection);
            consumer = useAsync ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));
            var message = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10)));
            Assert.Equal(APPLICATION_ID, message.Id.Value);
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(MessageType.MT_EVENT, message.Header.MessageType);
            Assert.Equal(body, message.Body.Memory.ToArray());
            Assert.Equal(2, message.Header.HandledCount);
            message.Header.UpdateHandledCount();
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, DESCRIPTION);
            var before = DateTimeOffset.UtcNow;
            if (failureMode == 3)
                await channel.QueueUnbindAsync(invalidName, exchangeName, invalidTopic.Value,
                    cancellationToken: timeout.Token);

            //Act
            bool rejected = false;
            var exception = await Record.ExceptionAsync(async () =>
            {
                rejected = useAsync
                    ? await consumer.RejectAsync(message, reason, timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(message, reason);
            });

            //Assert
            if (failureMode != 0)
            {
                Assert.NotNull(exception);
                if (failureMode == 1)
                    Assert.IsType<FormatException>(exception);
                if (failureMode == 3)
                    Assert.IsAssignableFrom<PublishException>(exception);
                Assert.False(rejected);
                Assert.Equal(topic, message.Header.Topic);
                Assert.Equal(body, message.Body.Memory.ToArray());
                await consumer.DisposeAsync();
                consumer = null;
                var original = await channel.BasicGetAsync(queueName, autoAck: false, cancellationToken: timeout.Token);
                Assert.NotNull(original);
                Assert.Equal(APPLICATION_ID, original.BasicProperties.MessageId);
                Assert.Equal(body, original.Body.ToArray());
                Assert.True(original.Redelivered);
                Assert.Null(await channel.BasicGetAsync(invalidName, autoAck: true, cancellationToken: timeout.Token));

                //Arrange
                await channel.BasicNackAsync(original.DeliveryTag, multiple: false, requeue: true,
                    cancellationToken: timeout.Token);
                writer.FailureMode = 0;
                if (failureMode == 3)
                    await channel.QueueBindAsync(invalidName, exchangeName, invalidTopic.Value,
                        cancellationToken: timeout.Token);
                consumer = useAsync ? factory.CreateAsync(subscription)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));
                message = Assert.Single(useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10)));
                Assert.Equal(APPLICATION_ID, message.Id.Value);
                Assert.Equal(topic, message.Header.Topic);
                Assert.Equal(body, message.Body.Memory.ToArray());
                Assert.True(message.Redelivered);
                Assert.Equal(2, message.Header.HandledCount);
                message.Header.UpdateHandledCount();

                //Act
                rejected = useAsync
                    ? await consumer.RejectAsync(message, reason, timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(message, reason);
            }
            else
            {
                Assert.Null(exception);
            }

            //Assert
            Assert.True(rejected);
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(body, message.Body.Memory.ToArray());
            await consumer.DisposeAsync();
            consumer = null;
            var forwarded = await channel.BasicGetAsync(invalidName, autoAck: true, cancellationToken: timeout.Token);
            Assert.NotNull(forwarded);
            var headers = forwarded.BasicProperties.Headers;
            Assert.NotNull(headers);
            Assert.True(headers.TryGetValue("rejection-profile", out var profile));
            Assert.Equal("invalid-profile", Encoding.UTF8.GetString(Assert.IsType<byte[]>(profile)));
            Assert.Equal(invalidTopic.Value, forwarded.RoutingKey);
            Assert.Equal(APPLICATION_ID, forwarded.BasicProperties.MessageId);
            Assert.Equal(DeliveryModes.Persistent, forwarded.BasicProperties.DeliveryMode);
            Assert.False(headers.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
            Assert.Equal(message.DeliveryTag.ToString(),
                Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.DELIVERY_TAG])));
            using var proof = JsonDocument.Parse(compose
                ? Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["rejection-proof"]))
                : Encoding.UTF8.GetString(forwarded.Body.Span));
            var metadata = proof.RootElement;
            Assert.Equal(APPLICATION_ID, metadata.GetProperty("id").GetString());
            Assert.Equal(Convert.ToBase64String(body), metadata.GetProperty("payload").GetString());
            Assert.Equal(3, metadata.GetProperty("handledCount").GetInt32());
            Assert.Equal(topic.Value, metadata.GetProperty("originalTopic").GetString());
            Assert.Equal("MT_EVENT", metadata.GetProperty("originalType").GetString());
            Assert.Equal("Unacceptable", metadata.GetProperty("reason").GetString());
            Assert.Equal(DESCRIPTION, metadata.GetProperty("description").GetString());
            Assert.InRange(metadata.GetProperty("timestamp").GetDateTimeOffset(), before, DateTimeOffset.UtcNow);
            if (compose)
            {
                Assert.Equal(body, forwarded.Body.ToArray());
                Assert.Equal(3, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
                Assert.Equal(topic.Value, Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_TOPIC])));
            }
            else
            {
                Assert.Equal("application/vnd.rejected-order+json", forwarded.BasicProperties.ContentType);
            }
            Assert.Null(await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token));
            Assert.Null(await channel.BasicGetAsync(invalidName, autoAck: true, cancellationToken: timeout.Token));
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
                    try
                    {
                        await channel.QueueDeleteAsync(invalidName, ifUnused: false, ifEmpty: false);
                    }
                    finally
                    {
                        await channel.ExchangeDeleteAsync(exchangeName);
                    }
                }
            }
        }
    }
}
