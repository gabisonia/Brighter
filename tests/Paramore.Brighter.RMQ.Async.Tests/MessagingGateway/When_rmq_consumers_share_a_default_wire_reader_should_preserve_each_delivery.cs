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
public class RmqDefaultWireReaderReuseTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_rmq_consumers_share_a_default_wire_reader_should_preserve_each_delivery(bool useAsync)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"default-reader-reuse-{Guid.NewGuid():N}";
        var queueNames = new[] { $"reader-first-{Guid.NewGuid():N}", $"reader-second-{Guid.NewGuid():N}" };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
        await using var channel = await broker.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            timeout.Token);
        var consumers = new List<IAmAMessageConsumerAsync>();
        var received = new List<Message>();
        var reader = new DefaultRmqWireMessageReader();
        var connection = new RmqMessagingGatewayConnection
        {
            AmpqUri = new AmqpUriSpecification(uri),
            Exchange = new Exchange(exchangeName)
        };
        var factory = new RmqMessageConsumerFactory(connection);

        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct, durable: false,
            autoDelete: false, cancellationToken: timeout.Token);
        try
        {
            for (var index = 0; index < queueNames.Length; index++)
            {
                var queueName = queueNames[index];
                var topic = new RoutingKey($"orders-{index}");
                await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false,
                    autoDelete: false, cancellationToken: timeout.Token);
                await channel.QueueBindAsync(queueName, exchangeName, topic.Value, cancellationToken: timeout.Token);
                var properties = new BasicProperties
                {
                    MessageId = $"application-{index}",
                    ContentType = "text/plain",
                    Type = "text/plain",
                    DeliveryMode = index == 0 ? DeliveryModes.Persistent : DeliveryModes.Transient,
                    Headers = new Dictionary<string, object?>
                    {
                        [HeaderNames.MESSAGE_TYPE] = Encoding.UTF8.GetBytes("MT_EVENT"),
                        [HeaderNames.TOPIC] = Encoding.UTF8.GetBytes($"application-topic-{index}"),
                        [HeaderNames.CORRELATION_ID] = Encoding.UTF8.GetBytes($"correlation-{index}"),
                        [HeaderNames.CLOUD_EVENTS_SUBJECT] = Encoding.UTF8.GetBytes($"subject-{index}"),
                        ["tenant_id"] = Encoding.UTF8.GetBytes($"tenant-{index}")
                    }
                };
                await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                    basicProperties: properties, body: Encoding.UTF8.GetBytes($"payload-{index}"),
                    cancellationToken: timeout.Token);
                var subscription = new RmqSubscription(
                    new SubscriptionName(queueName),
                    new ChannelName(queueName),
                    topic,
                    requestType: typeof(Command),
                    isDurable: true,
                    messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                    makeChannels: OnMissingChannel.Validate)
                {
                    WireMessageReader = reader
                };
                consumers.Add(useAsync
                    ? factory.CreateAsync(subscription)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription)));
            }

            //Act
            foreach (var consumer in consumers)
            {
                var messages = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
                received.Add(Assert.Single(messages));
            }

            //Assert
            Assert.NotSame(received[0], received[1]);
            Assert.NotSame(received[0].Header, received[1].Header);
            Assert.NotSame(received[0].Header.Bag, received[1].Header.Bag);
            for (var index = 0; index < received.Count; index++)
            {
                var message = received[index];
                Assert.Equal($"application-{index}", message.Id.Value);
                Assert.Equal($"payload-{index}", message.Body.Value);
                Assert.Equal(new RoutingKey($"application-topic-{index}"), message.Header.Topic);
                Assert.Equal($"correlation-{index}", message.Header.CorrelationId?.Value);
                Assert.Equal($"subject-{index}", message.Header.Subject);
                Assert.Equal($"tenant-{index}", message.Header.Bag["tenant_id"]);
                Assert.Equal(MessageType.MT_EVENT, message.Header.MessageType);
                Assert.Equal("text/plain", message.Header.ContentType?.MediaType);
                Assert.True(message.DeliveryTag > 0);
                Assert.False(message.Redelivered);
                Assert.Equal(index == 0, message.Persist);
            }

            //Act
            received[0].Header.Bag["tenant_id"] = "changed-first-tenant";

            //Assert
            Assert.Equal("tenant-1", received[1].Header.Bag["tenant_id"]);

            //Act
            for (var index = 0; index < received.Count; index++)
            {
                if (useAsync)
                    await consumers[index].AcknowledgeAsync(received[index], timeout.Token);
                else
                    Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumers[index]).Acknowledge(received[index]);
            }
            foreach (var consumer in consumers)
                await consumer.DisposeAsync();
            consumers.Clear();

            //Assert
            foreach (var queueName in queueNames)
            {
                var remaining = await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token);
                Assert.Null(remaining);
            }
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
                    foreach (var queueName in queueNames)
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
