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
using System.Text;
using System.Threading;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
[Collection("RMQ")]
public class RmqSyncRequeueWriterIsolationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void When_rmq_sync_subscriptions_select_requeue_writers_should_keep_each_consumers_mapping_isolated(
        bool customFirst)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-retry-isolation-{Guid.NewGuid():N}";
        var queueNames = new[] { name + "-first", name + "-second" };
        var customSelections = new[] { customFirst, !customFirst };
        var consumers = new List<IAmAMessageConsumerSync>();
        var createdQueues = new List<string>();
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        var factory = new RmqMessageConsumerFactory(new RmqMessagingGatewayConnection
        {
            AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name)
        });
        channel.ExchangeDeclare(name, ExchangeType.Direct, durable: false, autoDelete: false);
        try
        {
            channel.ConfirmSelect();
            for (var index = 0; index < 2; index++)
            {
                var queueName = queueNames[index];
                channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false);
                createdQueues.Add(queueName);
                channel.QueueBind(queueName, name, queueName);
                var properties = channel.CreateBasicProperties();
                properties.MessageId = $"independent-id-{index}";
                properties.ContentType = "text/plain";
                properties.Type = "text/plain";
                properties.Timestamp = new AmqpTimestamp(1700000000);
                properties.DeliveryMode = 2;
                properties.Headers = new Dictionary<string, object>
                {
                    [HeaderNames.MESSAGE_TYPE] = "MT_EVENT",
                    [HeaderNames.HANDLED_COUNT] = 2,
                    ["tenant"] = $"tenant-{index}"
                };
                channel.BasicPublish(name, queueName, mandatory: true, basicProperties: properties,
                    body: Encoding.UTF8.GetBytes($"independent-payload-{index}"));
                channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
                var subscription = new RmqSubscription(new SubscriptionName(queueName),
                    new ChannelName(queueName), new RoutingKey(queueName), requestType: typeof(Command),
                    isDurable: true, makeChannels: OnMissingChannel.Validate)
                {
                    RequeueWireMessageWriter = customSelections[index]
                        ? new SyncRetryProfileWireWriter(compose: true) : null
                };
                consumers.Add(factory.Create(subscription));
            }

            //Act
            for (var index = 0; index < 2; index++)
            {
                var consumer = consumers[index];
                var message = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));
                Assert.Equal(2, message.Header.HandledCount);
                message.Header.UpdateHandledCount();
                var sourceBag = new Dictionary<string, object>(message.Header.Bag);
                var requeued = consumer.Requeue(message, TimeSpan.Zero);
                consumer.Dispose();
                BasicGetResult? retry = null;
                var retryWait = Stopwatch.StartNew();
                while (retry == null && retryWait.Elapsed < TimeSpan.FromSeconds(5))
                {
                    retry = channel.BasicGet(queueNames[index], autoAck: true);
                    if (retry == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
                }

                //Assert
                Assert.True(requeued);
                Assert.NotNull(retry);
                var headers = retry.BasicProperties.Headers;
                Assert.NotNull(headers);
                Assert.Equal(customSelections[index], headers.ContainsKey("retry-profile"));
                Assert.Equal($"independent-payload-{index}", Encoding.UTF8.GetString(retry.Body.Span));
                Assert.False(string.IsNullOrEmpty(retry.BasicProperties.MessageId));
                Assert.NotEqual($"independent-id-{index}", retry.BasicProperties.MessageId);
                Assert.NotEqual("writer-owned-id", retry.BasicProperties.MessageId);
                Assert.Equal(queueNames[index], retry.RoutingKey);
                Assert.Equal((byte)2, retry.BasicProperties.DeliveryMode);
                Assert.Equal($"independent-id-{index}", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_MESSAGE_ID])));
                Assert.Equal(3, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
                Assert.Equal("1", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.DELIVERY_TAG])));
                Assert.False(headers.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
                Assert.Equal($"independent-id-{index}", message.Id.Value);
                Assert.Equal(new RoutingKey(queueNames[index]), message.Header.Topic);
                Assert.Equal(sourceBag.Count, message.Header.Bag.Count);
                foreach (var entry in sourceBag) Assert.Equal(entry.Value, message.Header.Bag[entry.Key]);
                BasicGetResult? extra = null;
                var settlementWait = Stopwatch.StartNew();
                while (extra == null && settlementWait.Elapsed < TimeSpan.FromSeconds(2))
                {
                    extra = channel.BasicGet(queueNames[index], autoAck: true);
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
            }
            finally
            {
                try
                {
                    foreach (var queueName in createdQueues)
                        channel.QueueDelete(queueName, ifUnused: false, ifEmpty: false);
                }
                finally
                {
                    channel.ExchangeDelete(name);
                }
            }
        }
    }
}
