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
public class RmqSyncConcurrentWriterTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(true, 0)]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 2)]
    public async Task When_rmq_sync_shares_a_writer_across_concurrent_operations_should_keep_native_payloads_and_controls_isolated(
        bool compose, int operation)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-parallel-writer-{Guid.NewGuid():N}";
        var queues = new[] { name + "-first", name + "-second" };
        var destinations = operation == 2 ? new[] { name + "-invalid-first", name + "-invalid-second" } : queues;
        using var operations = new Barrier(2);
        using var writes = new Barrier(2);
        using var callbacks = new Barrier(2);
        var writer = new SyncConcurrentWireWriter(compose, operation != 0, writes, callbacks);
        var connection = new RmqMessagingGatewayConnection
        {
            AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name), PersistMessages = true
        };
        var factory = new RmqMessageConsumerFactory(connection);
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        var createdQueues = new List<string>();
        var consumers = new List<IAmAMessageConsumerSync>();
        var messages = new List<Message>();
        var producers = new List<IAmAMessageProducerSync>();
        ProducerRegistry? registry = null;
        channel.ExchangeDeclare(name, ExchangeType.Direct, durable: false, autoDelete: false);
        try
        {
            channel.ConfirmSelect();
            foreach (var queue in queues.Concat(destinations).Distinct())
            {
                channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
                createdQueues.Add(queue);
                channel.QueueBind(queue, name, queue);
            }
            if (operation == 0)
            {
                var publications = queues.Select(queue => new RmqPublication
                {
                    Topic = new RoutingKey(queue), MakeChannels = OnMissingChannel.Validate, WireMessageWriter = writer
                }).ToArray();
                var registered = new RmqMessageProducerFactory(connection, publications).Create();
                registry = new ProducerRegistry(registered);
                foreach (var queue in queues)
                    producers.Add(Assert.IsAssignableFrom<IAmAMessageProducerSync>(registered.Single(pair => pair.Key.RoutingKey == new RoutingKey(queue)).Value));
            }
            for (var index = 0; index < 2; index++)
            {
                byte[] body = [0, 255, (byte)index];
                var topic = new RoutingKey(queues[index]);
                if (operation == 0)
                {
                    var message = new Message(new MessageHeader(new Id($"parallel-id-{index}"), topic, MessageType.MT_EVENT),
                        new MessageBody(body)) { Persist = true };
                    message.Header.HandledCount = index + 2;
                    message.Header.Bag["member"] = index;
                    messages.Add(message);
                }
                else
                {
                    var properties = channel.CreateBasicProperties();
                    properties.MessageId = $"parallel-id-{index}";
                    properties.ContentType = "application/octet-stream";
                    properties.Type = "application/octet-stream";
                    properties.Timestamp = new AmqpTimestamp(1700000000);
                    properties.DeliveryMode = 2;
                    properties.Headers = new Dictionary<string, object>
                    {
                        [HeaderNames.MESSAGE_TYPE] = "MT_EVENT", [HeaderNames.HANDLED_COUNT] = index + 2, ["member"] = index
                    };
                    channel.BasicPublish(name, queues[index], true, properties, body);
                    channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
                    var subscription = new RmqSubscription(new SubscriptionName(queues[index]), new ChannelName(queues[index]),
                        topic, requestType: typeof(Command), isDurable: true, makeChannels: OnMissingChannel.Validate)
                    {
                        RequeueWireMessageWriter = operation == 1 ? writer : null,
                        InvalidMessageWireMessageWriter = operation == 2 ? writer : null,
                        InvalidMessageRoutingKey = operation == 2 ? new RoutingKey(destinations[index]) : null
                    };
                    var consumer = factory.Create(subscription);
                    consumers.Add(consumer);
                    var received = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));
                    Assert.Equal($"parallel-id-{index}", received.Id.Value);
                    messages.Add(received);
                }
            }

            //Act
            var work = Enumerable.Range(0, 2).Select(index => Task.Factory.StartNew(() =>
            {
                if (!operations.SignalAndWait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Operations did not start together.");
                if (operation == 0)
                {
                    producers[index].Send(messages[index]);
                    return true;
                }
                return operation == 1 ? consumers[index].Requeue(messages[index], TimeSpan.Zero)
                    : consumers[index].Reject(messages[index], new MessageRejectionReason(RejectionReason.Unacceptable, $"member-{index}"));
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToArray();
            var results = await Task.WhenAll(work);

            //Assert
            Assert.All(results, result => Assert.True(result));
            Assert.Equal(2, writer.WriteCount);
            Assert.Equal(2, writer.CallbackCount);
            Assert.Equal(1L, operations.CurrentPhaseNumber);
            Assert.Equal(operation == 0 ? 0L : 1L, writes.CurrentPhaseNumber);
            Assert.Equal(operation == 0 ? 0L : 1L, callbacks.CurrentPhaseNumber);
            foreach (var consumer in consumers) consumer.Dispose();
            registry?.CloseAll();
            var nativeIds = new HashSet<string>();
            for (var index = 0; index < 2; index++)
            {
                BasicGetResult? native = null;
                var wait = Stopwatch.StartNew();
                while (native == null && wait.Elapsed < TimeSpan.FromSeconds(5))
                {
                    native = channel.BasicGet(destinations[index], autoAck: true);
                    if (native == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
                }
                Assert.NotNull(native);
                var headers = native.BasicProperties.Headers;
                Assert.Equal(index, Assert.IsType<int>(headers["writer-member"]));
                Assert.Equal($"parallel-id-{index}", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["writer-id"])));
                Assert.Equal(destinations[index], Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["writer-topic"])));
                byte[] body = [0, 255, (byte)index];
                var expectedBody = compose ? body : Encoding.UTF8.GetBytes($"parallel:{Convert.ToBase64String(body)}");
                Assert.Equal(expectedBody, native.Body.ToArray());
                Assert.Equal(destinations[index], native.RoutingKey);
                Assert.Equal((byte)2, native.BasicProperties.DeliveryMode);
                Assert.Equal(index + 2, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
                Assert.False(headers.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
                if (operation == 0) Assert.False(headers.ContainsKey(HeaderNames.DELIVERY_TAG));
                else Assert.Equal(operation == 1 ? "1" : messages[index].DeliveryTag.ToString(),
                    Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.DELIVERY_TAG])));
                if (operation == 1)
                {
                    Assert.NotEqual($"parallel-id-{index}", native.BasicProperties.MessageId);
                    Assert.Equal($"parallel-id-{index}", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_MESSAGE_ID])));
                }
                else Assert.Equal($"parallel-id-{index}", native.BasicProperties.MessageId);
                Assert.False(string.IsNullOrEmpty(native.BasicProperties.MessageId));
                Assert.True(nativeIds.Add(native.BasicProperties.MessageId));
                Assert.Equal($"parallel-id-{index}", messages[index].Id.Value);
                Assert.Equal(new RoutingKey(queues[index]), messages[index].Header.Topic);
                Assert.Equal(body, messages[index].Body.Memory.ToArray());
                Assert.Equal(index, messages[index].Header.Bag["member"]);
            }
            foreach (var queue in createdQueues)
            {
                BasicGetResult? extra = null;
                var wait = Stopwatch.StartNew();
                while (extra == null && wait.Elapsed < TimeSpan.FromSeconds(2))
                {
                    extra = channel.BasicGet(queue, autoAck: true);
                    if (extra == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
                }
                Assert.Null(extra);
            }
        }
        finally
        {
            try
            {
                foreach (var consumer in consumers) consumer.Dispose();
                registry?.Dispose();
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
