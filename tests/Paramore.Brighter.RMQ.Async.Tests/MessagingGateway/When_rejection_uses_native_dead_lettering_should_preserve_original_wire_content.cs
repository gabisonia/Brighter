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
public class RmqInvalidWriterNativeDeadLetterTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_rejection_uses_native_dead_lettering_should_preserve_original_wire_content(
        bool useAsync, bool unacceptableWithoutInvalidRoute)
    {
        //Arrange
        byte[] body = [0x00, 0xFF, 0x80, 0x41];
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"native-rejection-{Guid.NewGuid():N}";
        var queueName = $"native-rejection-source-{Guid.NewGuid():N}";
        var deadName = $"native-rejection-dead-{Guid.NewGuid():N}";
        var invalidName = $"native-rejection-invalid-{Guid.NewGuid():N}";
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
            await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    ["x-dead-letter-exchange"] = exchangeName,
                    ["x-dead-letter-routing-key"] = deadName
                }, cancellationToken: timeout.Token);
            await channel.QueueBindAsync(queueName, exchangeName, topic.Value, cancellationToken: timeout.Token);
            foreach (var name in new[] { deadName, invalidName })
            {
                await channel.QueueDeclareAsync(name, durable: true, exclusive: false,
                    autoDelete: false, cancellationToken: timeout.Token);
                await channel.QueueBindAsync(name, exchangeName, name, cancellationToken: timeout.Token);
            }
            await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                basicProperties: new BasicProperties
                {
                    MessageId = "original-native-id",
                    ContentType = "application/octet-stream", Type = "application/octet-stream",
                    DeliveryMode = DeliveryModes.Persistent,
                    Headers = new Dictionary<string, object?>
                    {
                        [HeaderNames.MESSAGE_TYPE] = "MT_EVENT",
                        [HeaderNames.HANDLED_COUNT] = 2,
                        ["native-profile"] = "source-profile"
                    }
                }, body: body, cancellationToken: timeout.Token);
            var factory = new RmqMessageConsumerFactory(new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(exchangeName)
            });
            var subscription = new RmqSubscription(
                new SubscriptionName(queueName), new ChannelName(queueName), topic,
                requestType: typeof(Command), isDurable: true,
                messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                deadLetterChannelName: new ChannelName(deadName), deadLetterRoutingKey: new RoutingKey(deadName),
                makeChannels: OnMissingChannel.Validate)
            {
                InvalidMessageRoutingKey = unacceptableWithoutInvalidRoute ? null : new RoutingKey(invalidName),
                InvalidMessageWireMessageWriter = new RejectionProfileRmqWireMessageWriter(compose: true)
                {
                    FailureMode = 1
                }
            };
            consumer = useAsync ? factory.CreateAsync(subscription)
                : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));
            var message = Assert.Single(useAsync
                ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10)));
            Assert.Equal(2, message.Header.HandledCount);
            message.Header.UpdateHandledCount();
            var reason = new MessageRejectionReason(unacceptableWithoutInvalidRoute
                ? RejectionReason.Unacceptable : RejectionReason.DeliveryError, "Native rejection");

            //Act
            var rejected = useAsync
                ? await consumer.RejectAsync(message, reason, timeout.Token)
                : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(message, reason);
            await consumer.DisposeAsync();
            consumer = null;
            BasicGetResult? forwarded = null;
            var deadline = DateTimeOffset.UtcNow.AddSeconds(5);
            while (forwarded == null && DateTimeOffset.UtcNow < deadline)
            {
                forwarded = await channel.BasicGetAsync(deadName, autoAck: true, cancellationToken: timeout.Token);
                if (forwarded == null)
                    await Task.Delay(TimeSpan.FromMilliseconds(25), timeout.Token);
            }

            //Assert
            Assert.True(rejected);
            Assert.NotNull(forwarded);
            Assert.Equal(body, forwarded.Body.ToArray());
            Assert.Equal("original-native-id", forwarded.BasicProperties.MessageId);
            Assert.Equal("application/octet-stream", forwarded.BasicProperties.ContentType);
            Assert.Equal(DeliveryModes.Persistent, forwarded.BasicProperties.DeliveryMode);
            Assert.Equal(deadName, forwarded.RoutingKey);
            var headers = forwarded.BasicProperties.Headers;
            Assert.NotNull(headers);
            Assert.Equal("source-profile", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["native-profile"])));
            Assert.Equal(2, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
            Assert.False(headers.ContainsKey("rejection-profile"));
            Assert.True(headers.ContainsKey("x-death"));
            Assert.Null(await channel.BasicGetAsync(invalidName, autoAck: true, cancellationToken: timeout.Token));
            Assert.Null(await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token));
            Assert.Null(await channel.BasicGetAsync(deadName, autoAck: true, cancellationToken: timeout.Token));
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
                        await channel.QueueDeleteAsync(deadName, ifUnused: false, ifEmpty: false);
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
}
