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

[Trait("Category", "RMQ")]
[Collection("RMQ")]
public class RmqSyncRequeueWriterRecoveryTests
{
    [Theory]
    [InlineData(false, false, 0)]
    [InlineData(false, false, 1)]
    [InlineData(false, false, 2)]
    [InlineData(false, false, 3)]
    [InlineData(false, true, 0)]
    [InlineData(false, true, 1)]
    [InlineData(false, true, 2)]
    [InlineData(false, true, 3)]
    [InlineData(true, false, 0)]
    [InlineData(true, false, 1)]
    [InlineData(true, false, 2)]
    [InlineData(true, false, 3)]
    [InlineData(true, true, 0)]
    [InlineData(true, true, 1)]
    [InlineData(true, true, 2)]
    [InlineData(true, true, 3)]
    public void When_rmq_sync_selects_a_requeue_writer_should_preserve_native_controls_and_recover_from_mapping_failures(
        bool compose, bool existingOriginal, int failureMode)
    {
        //Arrange
        byte[] body = [0, 255, 128, 65];
        const string NATIVE_ID = "retry-native-id";
        var originalKey = existingOriginal ? HeaderNames.ORIGINAL_MESSAGE_ID.ToUpperInvariant() : HeaderNames.ORIGINAL_MESSAGE_ID;
        var expectedOriginal = existingOriginal ? "first-delivery-id" : NATIVE_ID;
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-retry-writer-{Guid.NewGuid():N}";
        var topic = new RoutingKey("orders");
        var writer = new SyncRetryProfileWireWriter(compose) { FailureMode = failureMode };
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        channel.ExchangeDeclare(name, ExchangeType.Direct, durable: false, autoDelete: false);
        var sourceCreated = false;
        IAmAMessageConsumerSync? consumer = null;
        try
        {
            channel.QueueDeclare(name, durable: true, exclusive: false, autoDelete: false);
            sourceCreated = true;
            channel.QueueBind(name, name, topic.Value);
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
            if (existingOriginal) properties.Headers[originalKey] = expectedOriginal;
            channel.ConfirmSelect();
            channel.BasicPublish(name, topic.Value, mandatory: true, basicProperties: properties, body: body);
            channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            var factory = new RmqMessageConsumerFactory(new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name)
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
            var requeued = consumer.Requeue(message, TimeSpan.Zero);

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
                Assert.Equal(existingOriginal, originalHeaders.Keys.Any(k => k.Equals(HeaderNames.ORIGINAL_MESSAGE_ID, StringComparison.OrdinalIgnoreCase)));
                if (existingOriginal) Assert.Equal(expectedOriginal, Encoding.UTF8.GetString(Assert.IsType<byte[]>(originalHeaders[originalKey])));
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
                requeued = consumer.Requeue(message, TimeSpan.Zero);
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
            Assert.False(headers.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
            var originalEntry = Assert.Single(headers, h => h.Key.Equals(HeaderNames.ORIGINAL_MESSAGE_ID, StringComparison.OrdinalIgnoreCase));
            Assert.Equal(originalKey, originalEntry.Key);
            Assert.Equal(expectedOriginal, Encoding.UTF8.GetString(Assert.IsType<byte[]>(originalEntry.Value)));
            BasicGetResult? extra = null;
            var settlementWait = Stopwatch.StartNew();
            while (extra == null && settlementWait.Elapsed < TimeSpan.FromSeconds(2))
            {
                extra = channel.BasicGet(name, autoAck: true);
                if (extra == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
            }
            Assert.Null(extra);
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
                }
                finally
                {
                    channel.ExchangeDelete(name);
                }
            }
        }
    }
}
