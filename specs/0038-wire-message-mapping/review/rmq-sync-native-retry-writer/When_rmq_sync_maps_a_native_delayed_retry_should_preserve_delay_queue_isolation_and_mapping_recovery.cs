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
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

[Trait("Category", "RMQNativeDelay")]
[Trait("Requires", "RabbitMQDelayedPlugin")]
[Collection("RMQNativeDelay")]
public class RmqSyncNativeRetryWriterRecoveryTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    [InlineData(true, 0)]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    [InlineData(true, 3)]
    public void When_rmq_sync_maps_a_native_delayed_retry_should_preserve_delay_queue_isolation_and_mapping_recovery(
        bool compose, int failureMode)
    {
        //Arrange
        var delay = TimeSpan.FromSeconds(3);
        byte[] body = [0, 255, 128, 65];
        const string NATIVE_ID = "retry-native-id";
        var originalKey = HeaderNames.ORIGINAL_MESSAGE_ID.ToUpperInvariant();
        const string EXPECTED_ORIGINAL = "first-delivery-id";
        var uri = new Uri(Environment.GetEnvironmentVariable("RMQ_NATIVE_DELAY_URI") ?? "amqp://guest:guest@localhost:5673/%2f");
        var name = $"sync-native-retry-writer-{Guid.NewGuid():N}";
        var neighborName = name + "-neighbor";
        var retryExchange = name + ".requeue";
        var topic = new RoutingKey("orders");
        var writer = new SyncRetryProfileWireWriter(compose) { FailureMode = failureMode };
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        channel.ExchangeDeclare(name, "x-delayed-message", durable: false, autoDelete: false,
            arguments: new Dictionary<string, object> { ["x-delayed-type"] = ExchangeType.Direct });
        var sourceCreated = false;
        var neighborCreated = false;
        var retryExchangeCreated = false;
        IAmAMessageConsumerSync? consumer = null;
        try
        {
            channel.QueueDeclare(name, durable: true, exclusive: false, autoDelete: false);
            sourceCreated = true;
            channel.QueueBind(name, name, topic.Value);
            channel.QueueDeclare(neighborName, durable: true, exclusive: false, autoDelete: false);
            neighborCreated = true;
            channel.QueueBind(neighborName, name, topic.Value);
            channel.ExchangeDeclare(retryExchange, "x-delayed-message", durable: false, autoDelete: false,
                arguments: new Dictionary<string, object> { ["x-delayed-type"] = ExchangeType.Direct });
            retryExchangeCreated = true;
            channel.QueueBind(name, retryExchange, name);
            var properties = channel.CreateBasicProperties();
            properties.MessageId = NATIVE_ID;
            properties.ContentType = "application/octet-stream";
            properties.Type = "application/octet-stream";
            properties.Timestamp = new AmqpTimestamp(1700000000);
            properties.DeliveryMode = 2;
            properties.Headers = new Dictionary<string, object>
            {
                [HeaderNames.MESSAGE_TYPE] = "MT_EVENT",
                [HeaderNames.HANDLED_COUNT] = 2,
                ["tenant"] = "retry-tenant"
            };
            properties.Headers[originalKey] = EXPECTED_ORIGINAL;
            channel.ConfirmSelect();
            channel.BasicPublish(name, topic.Value, mandatory: true, basicProperties: properties, body: body);
            channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            BasicGetResult? neighborOriginal = null;
            var neighborSetupWait = Stopwatch.StartNew();
            while (neighborOriginal == null && neighborSetupWait.Elapsed < TimeSpan.FromSeconds(5))
            {
                neighborOriginal = channel.BasicGet(neighborName, autoAck: true);
                if (neighborOriginal == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
            }
            Assert.NotNull(neighborOriginal);
            Assert.Equal(NATIVE_ID, neighborOriginal.BasicProperties.MessageId);
            var factory = new RmqMessageConsumerFactory(new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name, ExchangeType.Direct, durable: false, supportDelay: true)
            });
            var subscription = new RmqSubscription(new SubscriptionName(name), new ChannelName(name), topic,
                requestType: typeof(Command), isDurable: true, makeChannels: OnMissingChannel.Validate)
            {
                RequeueWireMessageWriter = writer,
                InvalidMessageWireMessageWriter = new SyncRetryProfileWireWriter(false) { FailureMode = 1 }
            };
            consumer = factory.Create(subscription);
            var message = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));
            Assert.Equal(NATIVE_ID, message.Id.Value);
            Assert.Equal(body, message.Body.Memory.ToArray());
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(2, message.Header.HandledCount);
            Assert.True(message.Persist);
            Assert.InRange(message.DeliveryTag, 1UL, ulong.MaxValue - 1);
            message.Header.UpdateHandledCount();
            var sourceBag = new Dictionary<string, object>(message.Header.Bag);

            //Act
            var elapsed = Stopwatch.StartNew();
            var requeued = consumer.Requeue(message, delay);

            //Assert
            if (failureMode != 0)
            {
                Assert.False(requeued);
                Assert.Equal(NATIVE_ID, message.Id.Value);
                Assert.Equal(topic, message.Header.Topic);
                Assert.Equal(body, message.Body.Memory.ToArray());
                Assert.Equal(3, message.Header.HandledCount);
                Assert.True(message.Persist);
                Assert.Equal(sourceBag.Count, message.Header.Bag.Count);
                foreach (var entry in sourceBag) Assert.Equal(entry.Value, message.Header.Bag[entry.Key]);
                consumer.Dispose();
                consumer = null;
                BasicGetResult? original = null;
                var recoveryWait = Stopwatch.StartNew();
                while (original == null && recoveryWait.Elapsed < TimeSpan.FromSeconds(2))
                {
                    original = channel.BasicGet(name, autoAck: false);
                    if (original == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
                }
                Assert.NotNull(original);
                Assert.True(original.Redelivered);
                Assert.Equal(NATIVE_ID, original.BasicProperties.MessageId);
                Assert.Equal(body, original.Body.ToArray());
                Assert.Equal(topic.Value, original.RoutingKey);
                Assert.Equal("application/octet-stream", original.BasicProperties.ContentType);
                Assert.Equal("application/octet-stream", original.BasicProperties.Type);
                Assert.Equal((byte)2, original.BasicProperties.DeliveryMode);
                var originalHeaders = original.BasicProperties.Headers;
                Assert.Equal(2, Assert.IsType<int>(originalHeaders[HeaderNames.HANDLED_COUNT]));
                Assert.Equal("retry-tenant", Encoding.UTF8.GetString(Assert.IsType<byte[]>(originalHeaders["tenant"])));
                Assert.False(originalHeaders.ContainsKey("retry-profile"));
                Assert.False(originalHeaders.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
                Assert.True(originalHeaders.Keys.Any(k => k.Equals(HeaderNames.ORIGINAL_MESSAGE_ID, StringComparison.OrdinalIgnoreCase)));
                Assert.Equal(EXPECTED_ORIGINAL, Encoding.UTF8.GetString(Assert.IsType<byte[]>(originalHeaders[originalKey])));
                Assert.Null(channel.BasicGet(name, autoAck: true));

                //Arrange
                channel.BasicNack(original.DeliveryTag, multiple: false, requeue: true);
                writer.FailureMode = 0;
                consumer = factory.Create(subscription);
                message = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));
                Assert.Equal(NATIVE_ID, message.Id.Value);
                Assert.Equal(body, message.Body.Memory.ToArray());
                Assert.True(message.Redelivered);
                Assert.Equal(2, message.Header.HandledCount);
                message.Header.UpdateHandledCount();
                sourceBag = new Dictionary<string, object>(message.Header.Bag);

                //Act
                elapsed.Restart();
                requeued = consumer.Requeue(message, delay);
            }

            //Assert
            Assert.True(requeued);
            Assert.Equal(NATIVE_ID, message.Id.Value);
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(body, message.Body.Memory.ToArray());
            Assert.Equal(3, message.Header.HandledCount);
            Assert.True(message.Persist);
            Assert.Equal(sourceBag.Count, message.Header.Bag.Count);
            foreach (var entry in sourceBag) Assert.Equal(entry.Value, message.Header.Bag[entry.Key]);
            consumer.Dispose();
            consumer = null;
            BasicGetResult? retry = null;
            var retryWait = Stopwatch.StartNew();
            while (retry == null && retryWait.Elapsed < TimeSpan.FromSeconds(5))
            {
                retry = channel.BasicGet(name, autoAck: true);
                if (retry == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
            }
            Assert.NotNull(retry);
            Assert.Equal(name, retry.RoutingKey);
            Assert.False(string.IsNullOrEmpty(retry.BasicProperties.MessageId));
            Assert.NotEqual(NATIVE_ID, retry.BasicProperties.MessageId);
            Assert.NotEqual("writer-owned-id", retry.BasicProperties.MessageId);
            Assert.True(elapsed.Elapsed >= delay, $"Retry arrived after {elapsed.Elapsed}, before {delay}.");
            var headers = retry.BasicProperties.Headers;
            Assert.True(headers.ContainsKey("retry-profile"));
            var expectedBody = compose ? body : Encoding.UTF8.GetBytes($"retry:{Convert.ToBase64String(body)}");
            Assert.Equal(expectedBody, retry.Body.ToArray());
            Assert.Equal(compose ? "application/octet-stream" : "application/vnd.retry-order", retry.BasicProperties.ContentType);
            Assert.Equal((byte)2, retry.BasicProperties.DeliveryMode);
            Assert.Equal("selected-retry", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["retry-profile"])));
            Assert.Equal("retry-tenant", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["retry-tenant"])));
            Assert.Equal(3, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
            Assert.Equal("1", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.DELIVERY_TAG])));
            Assert.Equal(-3000L, Convert.ToInt64(headers[HeaderNames.DELAY_MILLISECONDS]));
            var originalEntry = Assert.Single(headers, h => h.Key.Equals(HeaderNames.ORIGINAL_MESSAGE_ID, StringComparison.OrdinalIgnoreCase));
            Assert.Equal(originalKey, originalEntry.Key);
            Assert.Equal(EXPECTED_ORIGINAL, Encoding.UTF8.GetString(Assert.IsType<byte[]>(originalEntry.Value)));
            BasicGetResult? extra = null;
            BasicGetResult? neighborRetry = null;
            var settlementWait = Stopwatch.StartNew();
            while (extra == null && neighborRetry == null && settlementWait.Elapsed < TimeSpan.FromSeconds(2))
            {
                extra = channel.BasicGet(name, autoAck: true);
                neighborRetry = channel.BasicGet(neighborName, autoAck: true);
                if (extra == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
            }
            Assert.Null(extra);
            Assert.Null(neighborRetry);
        }
        finally
        {
            try
            {
                consumer?.Dispose();
            }
            finally
            {
                try
                {
                    if (sourceCreated) channel.QueueDelete(name, ifUnused: false, ifEmpty: false);
                    if (neighborCreated) channel.QueueDelete(neighborName, ifUnused: false, ifEmpty: false);
                }
                finally
                {
                    if (retryExchangeCreated) channel.ExchangeDelete(retryExchange);
                    channel.ExchangeDelete(name);
                }
            }
        }
    }
}
