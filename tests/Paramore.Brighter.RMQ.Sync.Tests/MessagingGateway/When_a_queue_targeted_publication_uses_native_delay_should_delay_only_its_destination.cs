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
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

/// <summary>Verifies native delay and queue isolation for an explicit publication destination.</summary>
[Trait("Category", "RMQNativeDelay")]
[Trait("Requires", "RabbitMQDelayedPlugin")]
[Collection("RMQNativeDelay")]
public class RmqDestinationQueueNativeDelayTests
{
    /// <summary>A queue-targeted publication retains its native delivery delay.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_a_queue_targeted_publication_uses_native_delay_should_delay_only_its_destination(bool useAsync)
    {
        // Arrange
        var name = "destination-delay-" + Guid.NewGuid().ToString("N");
        var target = name + "-target";
        var sibling = name + "-sibling";
        var topic = new RoutingKey("logical-topic");
        var uri = new Uri("amqp://guest:guest@localhost:5673/%2f");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var native = broker.CreateModel();
        var arguments = new Dictionary<string, object> { ["x-delayed-type"] = ExchangeType.Direct };
        native.ExchangeDeclare(name, "x-delayed-message", false, false, arguments);
        native.ExchangeDeclare(name + ".requeue", "x-delayed-message", false, false, arguments);
        try
        {
            native.QueueDeclare(target, false, false, false, null);
            native.QueueDeclare(sibling, false, false, false, null);
            native.QueueBind(target, name, topic.Value);
            native.QueueBind(sibling, name, topic.Value);
            var connection = new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name, supportDelay: true)
            };
            var factory = new RmqMessageProducerFactory(connection, [new RmqPublication
            {
                Topic = topic, DestinationQueue = new ChannelName(target), MakeChannels = OnMissingChannel.Create,
                WireMessageWriter = new DefaultRmqWireMessageWriter()
            }]);
            using var registry = new ProducerRegistry(factory.Create());
            var producer = registry.LookupBy(topic);
            var confirmed = new TaskCompletionSource<PublishConfirmationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.IsAssignableFrom<ISupportPublishConfirmation>(producer).OnMessagePublished += result => confirmed.TrySetResult(result);
            var message = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT), new MessageBody("queue-delayed-payload"));
            var delay = TimeSpan.FromSeconds(2);

            // Act
            var elapsed = Stopwatch.StartNew();
            if (useAsync)
                await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendWithDelayAsync(message, delay, timeout.Token);
            else
                Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).SendWithDelay(message, delay);
            var confirmation = await confirmed.Task.WaitAsync(timeout.Token);
            BasicGetResult? delivered;
            do
            {
                delivered = native.BasicGet(target, true);
                if (delivered is null) await Task.Delay(20, timeout.Token);
            } while (delivered is null);

            // Assert
            Assert.True(elapsed.Elapsed >= delay, $"Delivered after {elapsed.Elapsed}, before {delay} elapsed.");
            Assert.True(confirmation.Success);
            Assert.Equal(message.Id, confirmation.MessageId);
            Assert.Equal(topic, confirmation.Topic);
            Assert.Equal(name + ".requeue", delivered.Exchange);
            Assert.Equal(target, delivered.RoutingKey);
            Assert.Equal(message.Id.Value, delivered.BasicProperties.MessageId);
            Assert.Equal("queue-delayed-payload", Encoding.UTF8.GetString(delivered.Body.Span));
            Assert.Equal(-2000L, Convert.ToInt64(delivered.BasicProperties.Headers!["x-delay"]));
            Assert.Null(native.BasicGet(sibling, true));
            Assert.Null(native.BasicGet(target, true));
            Assert.Equal(topic, message.Header.Topic);
        }
        finally
        {
            native.QueueDelete(target, false, false);
            native.QueueDelete(sibling, false, false);
            native.ExchangeDelete(name + ".requeue");
            native.ExchangeDelete(name);
        }
    }
}
