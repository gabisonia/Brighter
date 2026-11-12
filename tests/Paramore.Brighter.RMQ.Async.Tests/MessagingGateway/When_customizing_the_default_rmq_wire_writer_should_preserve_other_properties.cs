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
using System.Text;
using System.Net.Mime;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using Paramore.Brighter.RMQ.Async.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
public class RmqDefaultWireWriterCompositionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_customizing_the_default_rmq_wire_writer_should_preserve_other_properties(bool useAsync)
    {
        //Arrange
        const string body = "order-17 payload";
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"wire-writer-{Guid.NewGuid():N}";
        var topic = new RoutingKey("legacy-orders");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
        await using var channel = await broker.CreateChannelAsync(cancellationToken: timeout.Token);
        IAmAMessageProducerAsync? producer = null;

        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct, durable: false,
            autoDelete: false, cancellationToken: timeout.Token);
        try
        {
            var queue = await channel.QueueDeclareAsync(queue: "", durable: false, exclusive: true,
                autoDelete: true, cancellationToken: timeout.Token);
            await channel.QueueBindAsync(queue.QueueName, exchangeName, topic.Value,
                cancellationToken: timeout.Token);

            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri),
                Exchange = new Exchange(exchangeName),
                PersistMessages = true
            };
            var publication = new RmqPublication
            {
                Topic = topic,
                MakeChannels = OnMissingChannel.Validate,
                WireMessageWriter = new TenantHeaderRmqWireMessageWriter(new DefaultRmqWireMessageWriter())
            };
            var factory = new RmqMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            producer = Assert.IsAssignableFrom<IAmAMessageProducerAsync>(Assert.Single(producers).Value);
            var published = new TaskCompletionSource<PublishConfirmationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.IsAssignableFrom<ISupportPublishConfirmation>(producer).OnMessagePublished +=
                result => published.TrySetResult(result);

            var message = new Message(
                new MessageHeader(new Id("order-17"), topic, MessageType.MT_EVENT,
                    contentType: new ContentType("application/json")),
                new MessageBody(body));
            message.Header.Bag["tenant_id"] = "tenant-42";

            //Act
            if (useAsync)
            {
                await producer.SendAsync(message, timeout.Token);
            }
            else
            {
                Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(message);
            }

            var confirmation = await published.Task.WaitAsync(timeout.Token);
            var delivery = await channel.BasicGetAsync(queue.QueueName, autoAck: true, cancellationToken: timeout.Token);

            //Assert
            Assert.True(confirmation.Success);
            Assert.Equal(message.Id, confirmation.MessageId);
            Assert.Equal(topic, confirmation.Topic);
            Assert.NotNull(delivery);
            Assert.Equal("order-17", delivery.BasicProperties.MessageId);
            Assert.Equal("application/json", delivery.BasicProperties.ContentType);
            Assert.Equal(Encoding.UTF8.GetBytes(body), delivery.Body.ToArray());
            Assert.NotNull(delivery.BasicProperties.Headers);
            var headers = delivery.BasicProperties.Headers;
            Assert.True(headers.TryGetValue("tenant-id", out var tenantId));
            Assert.Equal("tenant-42", Encoding.UTF8.GetString(Assert.IsType<byte[]>(tenantId)));
            Assert.False(headers.ContainsKey("tenant_id"));
            Assert.True(headers.TryGetValue("cloudEvents_id", out var cloudEventId));
            Assert.Equal("order-17", Encoding.UTF8.GetString(Assert.IsType<byte[]>(cloudEventId)));
            Assert.True(headers.TryGetValue("cloudEvents_specversion", out var specVersion));
            Assert.Equal("1.0", Encoding.UTF8.GetString(Assert.IsType<byte[]>(specVersion)));
            Assert.Equal(DeliveryModes.Persistent, delivery.BasicProperties.DeliveryMode);
            var sourceHeader = Assert.Single(message.Header.Bag);
            Assert.Equal("tenant_id", sourceHeader.Key);
            Assert.Equal("tenant-42", sourceHeader.Value);
            Assert.Equal(body, message.Body.Value);
        }
        finally
        {
            try
            {
                if (producer != null)
                {
                    await producer.DisposeAsync();
                }
            }
            finally
            {
                await channel.ExchangeDeleteAsync(exchangeName);
            }
        }
    }
}
