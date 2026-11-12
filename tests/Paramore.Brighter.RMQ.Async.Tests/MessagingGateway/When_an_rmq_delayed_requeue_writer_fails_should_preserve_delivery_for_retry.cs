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
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.RMQ.Async.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

[Trait("Category", "RMQNativeDelay")]
[Trait("Requires", "RabbitMQDelayedPlugin")]
[Collection("RMQNativeDelay")]
public class RmqDelayedRequeueWriterRecoveryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_an_rmq_delayed_requeue_writer_fails_should_preserve_delivery_for_retry(
        bool useAsync, bool returnNull)
    {
        //Arrange
        const string body = "order-17 payload";
        const string nativeId = "native-message-id";
        var uri = new Uri(Environment.GetEnvironmentVariable("RMQ_NATIVE_DELAY_URI")
            ?? "amqp://guest:guest@localhost:5673/%2f");
        var delay = TimeSpan.FromSeconds(3);
        var exchangeName = $"failed-delayed-requeue-{Guid.NewGuid():N}";
        var queueName = $"failed-delayed-requeue-{Guid.NewGuid():N}";
        var topic = new RoutingKey("orders");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
        await using var channel = await broker.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            timeout.Token);
        IAmAMessageConsumerAsync? consumer = null;

        await channel.ExchangeDeclareAsync(exchangeName, "x-delayed-message", durable: false,
            autoDelete: false, arguments: new Dictionary<string, object?> { ["x-delayed-type"] = ExchangeType.Direct },
            cancellationToken: timeout.Token);
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
            await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                basicProperties: properties, body: Encoding.UTF8.GetBytes(body), cancellationToken: timeout.Token);
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri),
                Exchange = new Exchange(exchangeName, supportDelay: true)
            };
            var writer = new RecoverableRmqWireMessageWriter(returnNull);
            var subscription = new RmqSubscription(
                new SubscriptionName(queueName),
                new ChannelName(queueName),
                topic,
                requestType: typeof(Command),
                isDurable: true,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                makeChannels: OnMissingChannel.Create)
            {
                RequeueWireMessageWriter = writer
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
                ? await consumer.RequeueAsync(message, delay, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Requeue(message, delay);

            //Assert
            Assert.False(requeued);
            Assert.Equal(nativeId, message.Id.Value);
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(body, message.Body.Value);
            Assert.Equal(3, message.Header.HandledCount);
            Assert.Equal(sourceTag, message.DeliveryTag);
            Assert.True(message.Persist);
            Assert.Equal(sourceBag.Count, message.Header.Bag.Count);
            foreach (var entry in sourceBag)
                Assert.Equal(entry.Value, message.Header.Bag[entry.Key]);
            await consumer.DisposeAsync();
            consumer = null;
            var original = await channel.BasicGetAsync(queueName, autoAck: false, cancellationToken: timeout.Token);
            Assert.NotNull(original);
            Assert.True(original.Redelivered);
            Assert.Equal(Encoding.UTF8.GetBytes(body), original.Body.ToArray());
            Assert.Equal(nativeId, original.BasicProperties.MessageId);
            Assert.Equal(topic.Value, original.RoutingKey);
            Assert.Equal("application/json", original.BasicProperties.Type);
            Assert.Equal("application/json", original.BasicProperties.ContentType);
            Assert.Equal(DeliveryModes.Persistent, original.BasicProperties.DeliveryMode);
            var originalHeaders = original.BasicProperties.Headers;
            Assert.NotNull(originalHeaders);
            Assert.Equal(2, Assert.IsType<int>(originalHeaders[HeaderNames.HANDLED_COUNT]));
            Assert.Equal("tenant-42", Encoding.UTF8.GetString(Assert.IsType<byte[]>(originalHeaders["tenant_id"])));
            Assert.False(originalHeaders.ContainsKey(HeaderNames.ORIGINAL_MESSAGE_ID));
            Assert.False(originalHeaders.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
            Assert.Null(await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token));

            var failedRetryWindow = Stopwatch.StartNew();
            while (failedRetryWindow.Elapsed < delay + TimeSpan.FromMilliseconds(500))
            {
                Assert.Null(await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token));
                await Task.Delay(TimeSpan.FromMilliseconds(50), timeout.Token);
            }

            //Arrange
            await channel.BasicNackAsync(original.DeliveryTag, multiple: false, requeue: true,
                cancellationToken: timeout.Token);
            writer.FailOnWrite = false;
            consumer = useAsync
                ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));
            var recoveredMessages = useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
            var recoveredMessage = Assert.Single(recoveredMessages);
            Assert.Equal(nativeId, recoveredMessage.Id.Value);
            Assert.Equal(body, recoveredMessage.Body.Value);
            Assert.Equal(2, recoveredMessage.Header.HandledCount);
            recoveredMessage.Header.UpdateHandledCount();

            var recoveredBag = new Dictionary<string, object>(recoveredMessage.Header.Bag);
            var recoveredTag = recoveredMessage.DeliveryTag;

            //Act
            var elapsed = Stopwatch.StartNew();
            var recovered = useAsync
                ? await consumer.RequeueAsync(recoveredMessage, delay, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Requeue(recoveredMessage, delay);
            Assert.True(recovered);
            await consumer.DisposeAsync();
            consumer = null;
            BasicGetResult? retry = null;
            while (elapsed.Elapsed < TimeSpan.FromSeconds(10))
            {
                retry = await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token);
                if (retry != null)
                    break;
                await Task.Delay(TimeSpan.FromMilliseconds(50), timeout.Token);
            }
            var observedAfter = elapsed.Elapsed;

            //Assert
            Assert.NotNull(retry);
            Assert.True(observedAfter >= delay);
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
            Assert.Equal(nativeId,
                Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_MESSAGE_ID])));
            Assert.Equal(-delay.TotalMilliseconds, Convert.ToDouble(headers[HeaderNames.DELAY_MILLISECONDS]));
            Assert.Equal(nativeId, recoveredMessage.Id.Value);
            Assert.Equal(topic, recoveredMessage.Header.Topic);
            Assert.Equal(body, recoveredMessage.Body.Value);
            Assert.Equal(3, recoveredMessage.Header.HandledCount);
            Assert.Equal(recoveredTag, recoveredMessage.DeliveryTag);
            Assert.True(recoveredMessage.Persist);
            Assert.Equal(recoveredBag.Count, recoveredMessage.Header.Bag.Count);
            foreach (var entry in recoveredBag)
                Assert.Equal(entry.Value, recoveredMessage.Header.Bag[entry.Key]);
            Assert.Null(await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token));
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
                        await channel.ExchangeDeleteAsync(exchangeName);
                    }
                    finally
                    {
                        await channel.ExchangeDeleteAsync($"{exchangeName}.requeue");
                    }
                }
            }
        }
    }
}
