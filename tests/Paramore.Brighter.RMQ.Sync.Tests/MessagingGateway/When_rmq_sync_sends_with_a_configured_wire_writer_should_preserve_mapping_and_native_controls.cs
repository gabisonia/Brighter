#region Licence

/* The MIT License (MIT)
Copyright © 2026 Irakli Gabisonia

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
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
[Collection("RMQ")]
public class RmqSyncEndpointWriterTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_rmq_sync_sends_with_a_configured_wire_writer_should_preserve_mapping_and_native_controls(
        bool useAsyncSend, bool composeDefault)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchange = $"sync-wire-{Guid.NewGuid():N}";
        var topic = new RoutingKey("orders");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var nativeConnection = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = nativeConnection.CreateModel();
        using var traceProvider = Sdk.CreateTracerProviderBuilder().Build();
        using var span = new Activity("sync-wire-publish").SetIdFormat(ActivityIdFormat.W3C).Start();
        channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: false, autoDelete: false);
        try
        {
            var queue = channel.QueueDeclare("", durable: false, exclusive: true, autoDelete: true);
            channel.QueueBind(queue.QueueName, exchange, topic.Value);
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(exchange), PersistMessages = true
            };
            var publication = new RmqPublication
            {
                Topic = topic, MakeChannels = OnMissingChannel.Validate,
                WireMessageWriter = new SyncEndpointWireWriter(composeDefault)
            };
            var producers = new RmqMessageProducerFactory(connection, [publication]).Create();
            using var registry = new ProducerRegistry(producers);
            var producer = Assert.Single(producers).Value;
            producer.Span = span;
            var confirmed = new TaskCompletionSource<PublishConfirmationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.IsAssignableFrom<ISupportPublishConfirmation>(producer).OnMessagePublished += result => confirmed.TrySetResult(result);
            var source = new Message(new MessageHeader(new Id("order-17"), topic, MessageType.MT_EVENT,
                    traceParent: new TraceParent($"00-{ActivityTraceId.CreateRandom()}-{ActivitySpanId.CreateRandom()}-01")),
                new MessageBody("accepted"));
            source.Header.Bag["tenant_id"] = "tenant-42";

            //Act
            if (useAsyncSend)
                await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendAsync(source, timeout.Token);
            else
                Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(source);
            var confirmation = await confirmed.Task.WaitAsync(timeout.Token);
            var native = channel.BasicGet(queue.QueueName, autoAck: true);

            //Assert
            Assert.True(confirmation.Success);
            Assert.Equal(source.Id, confirmation.MessageId);
            Assert.Equal(topic, confirmation.Topic);
            Assert.NotNull(native);
            Assert.Equal(composeDefault ? "order-17" : "legacy-order-17", native.BasicProperties.MessageId);
            Assert.Equal(composeDefault ? "accepted" : "legacy:accepted", Encoding.UTF8.GetString(native.Body.Span));
            Assert.Equal(composeDefault ? "application/json; charset=utf-8" : "application/vnd.legacy.order", native.BasicProperties.ContentType);
            Assert.Equal(composeDefault ? "application/json; charset=utf-8" : "legacy-order", native.BasicProperties.Type);
            Assert.Equal((byte)2, native.BasicProperties.DeliveryMode);
            Assert.True(native.BasicProperties.Headers.ContainsKey("tenant-id"));
            Assert.True(native.BasicProperties.Headers.ContainsKey("telemetry-parent"));
            Assert.Equal("tenant-42", Encoding.UTF8.GetString(Assert.IsType<byte[]>(native.BasicProperties.Headers["tenant-id"])));
            Assert.Equal(span.Id, Encoding.UTF8.GetString(Assert.IsType<byte[]>(native.BasicProperties.Headers["telemetry-parent"])));
            Assert.False(native.BasicProperties.Headers.ContainsKey(HeaderNames.DELIVERY_TAG));
            Assert.False(native.BasicProperties.Headers.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
            if (composeDefault)
                Assert.Equal("order-17", Encoding.UTF8.GetString(Assert.IsType<byte[]>(native.BasicProperties.Headers[HeaderNames.CLOUD_EVENTS_ID])));
            else
                Assert.Equal(2, native.BasicProperties.Headers.Count);
            Assert.Equal("tenant_id", Assert.Single(source.Header.Bag).Key);
            Assert.Equal("tenant-42", source.Header.Bag["tenant_id"]);
            Assert.Equal("accepted", source.Body.Value);
            Assert.Equal(new Id("order-17"), source.Id);
            Assert.Null(channel.BasicGet(queue.QueueName, autoAck: true));
        }
        finally
        {
            channel.ExchangeDelete(exchange);
        }
    }
}
