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
public class RmqInvalidWireWriterIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_rmq_subscriptions_select_invalid_writers_should_keep_each_consumers_mapping_isolated(
        bool useAsync, bool customFirst)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"invalid-isolation-{Guid.NewGuid():N}";
        var invalidName = $"invalid-isolation-destination-{Guid.NewGuid():N}";
        var queueNames = new[] { $"invalid-source-{Guid.NewGuid():N}", $"invalid-source-{Guid.NewGuid():N}" };
        var customSelections = new[] { customFirst, !customFirst };
        var consumers = new List<IAmAMessageConsumerAsync>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
        await using var channel = await broker.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            timeout.Token);
        var factory = new RmqMessageConsumerFactory(new RmqMessagingGatewayConnection
        {
            AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(exchangeName)
        });

        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct, durable: false,
            autoDelete: false, cancellationToken: timeout.Token);
        try
        {
            await channel.QueueDeclareAsync(invalidName, durable: true, exclusive: false,
                autoDelete: false, cancellationToken: timeout.Token);
            await channel.QueueBindAsync(invalidName, exchangeName, invalidName, cancellationToken: timeout.Token);
            for (var index = 0; index < 2; index++)
            {
                var queueName = queueNames[index];
                await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false,
                    autoDelete: false, cancellationToken: timeout.Token);
                await channel.QueueBindAsync(queueName, exchangeName, queueName, cancellationToken: timeout.Token);
                await channel.BasicPublishAsync(exchangeName, queueName, mandatory: true,
                    basicProperties: new BasicProperties
                    {
                        MessageId = $"independent-id-{index}",
                        ContentType = "text/plain", Type = "text/plain",
                        DeliveryMode = DeliveryModes.Persistent,
                        Headers = new Dictionary<string, object?>
                        {
                            [HeaderNames.MESSAGE_TYPE] = "MT_EVENT",
                            [HeaderNames.HANDLED_COUNT] = 2
                        }
                    }, body: Encoding.UTF8.GetBytes($"independent-payload-{index}"),
                    cancellationToken: timeout.Token);
                var subscription = new RmqSubscription(
                    new SubscriptionName(queueName), new ChannelName(queueName), new RoutingKey(queueName),
                    requestType: typeof(Command), isDurable: true,
                    messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                    makeChannels: OnMissingChannel.Validate)
                {
                    InvalidMessageRoutingKey = new RoutingKey(invalidName),
                    InvalidMessageWireMessageWriter = customSelections[index]
                        ? new RejectionProfileRmqWireMessageWriter(compose: true) : null
                };
                consumers.Add(useAsync ? factory.CreateAsync(subscription)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription)));
            }

            //Act
            for (var index = 0; index < 2; index++)
            {
                var consumer = consumers[index];
                var message = Assert.Single(useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10)));
                Assert.Equal(2, message.Header.HandledCount);
                message.Header.UpdateHandledCount();
                var reason = new MessageRejectionReason(RejectionReason.Unacceptable, "Isolated rejection");
                var rejected = useAsync
                    ? await consumer.RejectAsync(message, reason, timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Reject(message, reason);
                var forwarded = await channel.BasicGetAsync(invalidName, autoAck: true, cancellationToken: timeout.Token);

                //Assert
                Assert.True(rejected);
                Assert.NotNull(forwarded);
                var headers = forwarded.BasicProperties.Headers;
                Assert.NotNull(headers);
                Assert.Equal(customSelections[index], headers.ContainsKey("rejection-profile"));
                Assert.Equal($"independent-id-{index}", forwarded.BasicProperties.MessageId);
                Assert.Equal($"independent-payload-{index}", Encoding.UTF8.GetString(forwarded.Body.Span));
                Assert.Equal(queueNames[index], Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_TOPIC])));
                Assert.Equal(3, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
                Assert.Equal(new RoutingKey(queueNames[index]), message.Header.Topic);
            }
            foreach (var consumer in consumers)
                await consumer.DisposeAsync();
            consumers.Clear();
            foreach (var queueName in queueNames)
                Assert.Null(await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token));
            Assert.Null(await channel.BasicGetAsync(invalidName, autoAck: true, cancellationToken: timeout.Token));
        }
        finally
        {
            try
            {
                foreach (var consumer in consumers)
                    await consumer.DisposeAsync();
            }
            finally
            {
                try
                {
                    await channel.QueueDeleteAsync(queueNames[0], ifUnused: false, ifEmpty: false);
                }
                finally
                {
                    try
                    {
                        await channel.QueueDeleteAsync(queueNames[1], ifUnused: false, ifEmpty: false);
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
