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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
[Collection("RMQ")]
public class RmqSyncConcurrentReaderTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_rmq_sync_shares_a_reader_across_concurrent_consumers_should_keep_messages_and_delivery_controls_isolated(
        bool compose)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-parallel-reader-{Guid.NewGuid():N}";
        var queues = new[] { name + "-first", name + "-second" };
        using var reads = new Barrier(2);
        var reader = new SyncConcurrentWireReader(compose, reads);
        var factory = new RmqMessageConsumerFactory(new RmqMessagingGatewayConnection
        {
            AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name)
        });
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        var createdQueues = new List<string>();
        var consumers = new List<IAmAMessageConsumerSync>();
        channel.ExchangeDeclare(name, ExchangeType.Direct, durable: false, autoDelete: false);
        try
        {
            channel.ConfirmSelect();
            for (var index = 0; index < 2; index++)
            {
                channel.QueueDeclare(queues[index], durable: true, exclusive: false, autoDelete: false);
                createdQueues.Add(queues[index]);
                channel.QueueBind(queues[index], name, queues[index]);
                var properties = channel.CreateBasicProperties();
                properties.MessageId = $"reader-id-{index}";
                properties.ContentType = "application/octet-stream";
                properties.Type = "application/octet-stream";
                properties.Timestamp = new AmqpTimestamp(1700000000);
                properties.DeliveryMode = (byte)(index == 1 ? 2 : 1);
                properties.Headers = new Dictionary<string, object>
                {
                    [HeaderNames.MESSAGE_TYPE] = "MT_EVENT", [HeaderNames.HANDLED_COUNT] = index + 2, ["member"] = index
                };
                byte[] body = [0, 255, (byte)index];
                channel.BasicPublish(name, queues[index], true, properties, body);
                channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
                consumers.Add(factory.Create(new RmqSubscription(new SubscriptionName(queues[index]),
                    new ChannelName(queues[index]), new RoutingKey(queues[index]), requestType: typeof(Command),
                    isDurable: true, makeChannels: OnMissingChannel.Validate) { WireMessageReader = reader }));
            }

            //Act
            var work = consumers.Select(consumer => Task.Factory.StartNew(
                () => Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10))), CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToArray();
            var messages = await Task.WhenAll(work);

            //Assert
            Assert.Equal(2, reader.ReadCount);
            Assert.Equal(1L, reads.CurrentPhaseNumber);
            Assert.NotSame(messages[0], messages[1]);
            Assert.NotSame(messages[0].Header.Bag, messages[1].Header.Bag);
            for (var index = 0; index < 2; index++)
            {
                var message = messages[index];
                Assert.False(message.IsEmpty);
                Assert.Equal(index, Assert.IsType<int>(message.Header.Bag["reader-member"]));
                Assert.Equal(compose ? $"reader-id-{index}" : $"mapped-reader-id-{index}", message.Id.Value);
                Assert.Equal(new RoutingKey(compose ? queues[index] : "mapped-topic"), message.Header.Topic);
                byte[] body = [0, 255, (byte)index];
                var expectedBody = compose ? body : Encoding.UTF8.GetBytes($"parallel:{Convert.ToBase64String(body)}");
                Assert.Equal(expectedBody, message.Body.Memory.ToArray());
                Assert.Equal(index + 2, message.Header.HandledCount);
                Assert.Equal(index == 1, message.Persist);
                Assert.False(message.Redelivered);
                Assert.InRange(message.DeliveryTag, 1UL, ulong.MaxValue - 1);
                consumers[index].Acknowledge(message);
            }
            foreach (var consumer in consumers) consumer.Dispose();
            foreach (var queue in queues)
            {
                BasicGetResult? remaining = null;
                var wait = Stopwatch.StartNew();
                while (remaining == null && wait.Elapsed < TimeSpan.FromSeconds(2))
                {
                    remaining = channel.BasicGet(queue, autoAck: true);
                    if (remaining == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
                }
                Assert.Null(remaining);
            }
        }
        finally
        {
            try
            {
                foreach (var consumer in consumers) consumer.Dispose();
            }
            finally
            {
                try
                {
                    foreach (var queue in createdQueues) channel.QueueDelete(queue, ifUnused: false, ifEmpty: false);
                }
                finally
                {
                    channel.ExchangeDelete(name);
                }
            }
        }
    }
}
