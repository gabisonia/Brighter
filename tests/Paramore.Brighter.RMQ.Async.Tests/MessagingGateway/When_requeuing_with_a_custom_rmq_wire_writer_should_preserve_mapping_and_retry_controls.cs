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
public class RmqCustomRequeueWireWriterTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_requeuing_with_a_custom_rmq_wire_writer_should_preserve_mapping_and_retry_controls(
        bool useAsync, bool hasOriginalId)
    {
        //Arrange
        const string body = "order-17 payload";
        const string nativeId = "native-message-id";
        const string firstId = "first-application-id";
        var expectedOriginalId = hasOriginalId ? firstId : nativeId;
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"custom-requeue-{Guid.NewGuid():N}";
        var queueName = $"custom-requeue-{Guid.NewGuid():N}";
        var topic = new RoutingKey("orders");
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
                MessageId = nativeId,
                ContentType = "application/json",
                Type = "application/json",
                DeliveryMode = DeliveryModes.Persistent,
                Headers = new Dictionary<string, object?>
                {
                    ["tenant_id"] = "tenant-42",
                    [HeaderNames.HANDLED_COUNT] = 2
                }
            };
            if (hasOriginalId)
                properties.Headers[HeaderNames.ORIGINAL_MESSAGE_ID] = firstId;
            await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                basicProperties: properties, body: Encoding.UTF8.GetBytes(body), cancellationToken: timeout.Token);
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
                RequeueWireMessageWriter = new LegacyTenantRmqWireMessageWriter()
            };
            var factory = new RmqMessageConsumerFactory(connection);
            consumer = useAsync
                ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));
            var messages = useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
            var message = Assert.Single(messages);
            Assert.Equal(nativeId, message.Id.Value);
            Assert.Equal(body, message.Body.Value);
            Assert.Equal(2, message.Header.HandledCount);
            Assert.True(message.Persist);
            Assert.True(message.DeliveryTag > 0);
            message.Header.UpdateHandledCount();
            var sourceTag = message.DeliveryTag;
            var sourceBag = new Dictionary<string, object>(message.Header.Bag);

            //Act
            var requeued = useAsync
                ? await consumer.RequeueAsync(message, TimeSpan.Zero, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Requeue(message, TimeSpan.Zero);
            Assert.True(requeued);
            await consumer.DisposeAsync();
            consumer = null;
            var retry = await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token);

            //Assert
            Assert.NotNull(retry);
            Assert.Equal(Encoding.UTF8.GetBytes($"legacy:{body}"), retry.Body.ToArray());
            Assert.Equal("legacy-order", retry.BasicProperties.Type);
            Assert.Equal("application/vnd.legacy.order", retry.BasicProperties.ContentType);
            Assert.Equal(DeliveryModes.Persistent, retry.BasicProperties.DeliveryMode);
            Assert.False(string.IsNullOrEmpty(retry.BasicProperties.MessageId));
            Assert.NotEqual(nativeId, retry.BasicProperties.MessageId);
            Assert.NotEqual($"legacy-{nativeId}", retry.BasicProperties.MessageId);
            Assert.Equal(queueName, retry.RoutingKey);
            var headers = retry.BasicProperties.Headers;
            Assert.NotNull(headers);
            Assert.Equal("tenant-42", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["tenant-id"])));
            Assert.Equal(3, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
            Assert.Equal("1", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.DELIVERY_TAG])));
            Assert.Equal(expectedOriginalId,
                Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_MESSAGE_ID])));
            Assert.False(headers.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
            Assert.Equal(nativeId, message.Id.Value);
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(body, message.Body.Value);
            Assert.Equal(3, message.Header.HandledCount);
            Assert.Equal(sourceTag, message.DeliveryTag);
            Assert.True(message.Persist);
            Assert.Equal(sourceBag.Count, message.Header.Bag.Count);
            foreach (var entry in sourceBag)
                Assert.Equal(entry.Value, message.Header.Bag[entry.Key]);
            var extra = await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token);
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
