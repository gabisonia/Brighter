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
using System.Text.Json;
using System.Threading;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
[Collection("RMQ")]
public class RmqSyncInvalidWriterRecoveryTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    [InlineData(false, 4)]
    [InlineData(true, 0)]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    [InlineData(true, 3)]
    [InlineData(true, 4)]
    public void When_rmq_sync_selects_an_invalid_writer_should_preserve_mapping_and_recover_from_failures(
        bool compose, int failureMode)
    {
        //Arrange
        byte[] body = [0, 255, 128, 65];
        const string APPLICATION_ID = "rejected-order-17";
        const string DESCRIPTION = "Cannot process order-17";
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-invalid-writer-{Guid.NewGuid():N}";
        var invalidName = name + "-invalid";
        var topic = new RoutingKey("orders");
        var writer = new SyncRejectionProfileWireWriter(compose)
        {
            FailureMode = failureMode is 1 or 2 or 3 ? failureMode : 0
        };
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        channel.ExchangeDeclare(name, ExchangeType.Direct, durable: false, autoDelete: false);
        var sourceCreated = false;
        var invalidCreated = false;
        IAmAMessageConsumerSync? consumer = null;
        try
        {
            channel.QueueDeclare(name, durable: true, exclusive: false, autoDelete: false);
            sourceCreated = true;
            channel.QueueBind(name, name, topic.Value);
            channel.QueueDeclare(invalidName, durable: true, exclusive: false, autoDelete: false);
            invalidCreated = true;
            channel.QueueBind(invalidName, name, invalidName);
            var properties = channel.CreateBasicProperties();
            properties.MessageId = APPLICATION_ID;
            properties.ContentType = "application/octet-stream";
            properties.Type = "application/octet-stream";
            properties.Timestamp = new AmqpTimestamp(1700000000);
            properties.DeliveryMode = 2;
            properties.Headers = new Dictionary<string, object>
            {
                [HeaderNames.MESSAGE_TYPE] = "MT_EVENT",
                [HeaderNames.HANDLED_COUNT] = 2
            };
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
                InvalidMessageRoutingKey = new RoutingKey(invalidName),
                InvalidMessageWireMessageWriter = writer
            };
            consumer = factory.Create(subscription);
            var message = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));
            Assert.Equal(APPLICATION_ID, message.Id.Value);
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(body, message.Body.Memory.ToArray());
            Assert.Equal(2, message.Header.HandledCount);
            message.Header.UpdateHandledCount();
            var reason = new MessageRejectionReason(RejectionReason.Unacceptable, DESCRIPTION);
            var before = DateTimeOffset.UtcNow;
            if (failureMode == 4) channel.QueueUnbind(invalidName, name, invalidName, null);

            //Act
            var rejected = false;
            var exception = Record.Exception(() => rejected = consumer.Reject(message, reason));

            //Assert
            if (failureMode != 0)
            {
                Assert.NotNull(exception);
                if (failureMode == 1) Assert.IsType<FormatException>(exception);
                if (failureMode == 3) Assert.IsType<InvalidOperationException>(exception);
                if (failureMode == 4) Assert.IsType<ChannelFailureException>(exception);
                Assert.False(rejected);
                Assert.Equal(topic, message.Header.Topic);
                Assert.Equal(body, message.Body.Memory.ToArray());

                //Act
                consumer.Dispose();
                consumer = null;
                BasicGetResult? original = null;
                var recoveryWait = Stopwatch.StartNew();
                while (original == null && recoveryWait.Elapsed < TimeSpan.FromSeconds(2))
                {
                    original = channel.BasicGet(name, autoAck: false);
                    if (original == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
                }

                //Assert
                Assert.NotNull(original);
                Assert.Equal(APPLICATION_ID, original.BasicProperties.MessageId);
                Assert.Equal(body, original.Body.ToArray());
                Assert.Equal(topic.Value, original.RoutingKey);
                Assert.True(original.Redelivered);
                Assert.Equal(2, Assert.IsType<int>(original.BasicProperties.Headers[HeaderNames.HANDLED_COUNT]));
                Assert.Null(channel.BasicGet(invalidName, autoAck: true));

                //Arrange
                channel.BasicNack(original.DeliveryTag, multiple: false, requeue: true);
                writer.FailureMode = 0;
                if (failureMode == 4) channel.QueueBind(invalidName, name, invalidName);
                consumer = factory.Create(subscription);
                message = Assert.Single(consumer.Receive(TimeSpan.FromSeconds(10)));
                Assert.Equal(APPLICATION_ID, message.Id.Value);
                Assert.Equal(topic, message.Header.Topic);
                Assert.Equal(body, message.Body.Memory.ToArray());
                Assert.True(message.Redelivered);
                Assert.Equal(2, message.Header.HandledCount);
                message.Header.UpdateHandledCount();

                //Act
                rejected = consumer.Reject(message, reason);
            }
            else
            {
                Assert.Null(exception);
            }

            //Assert
            Assert.True(rejected);
            Assert.Equal(topic, message.Header.Topic);
            Assert.Equal(body, message.Body.Memory.ToArray());
            consumer.Dispose();
            consumer = null;
            var forwarded = channel.BasicGet(invalidName, autoAck: true);
            Assert.NotNull(forwarded);
            var headers = forwarded.BasicProperties.Headers;
            Assert.NotNull(headers);
            Assert.True(headers.TryGetValue("rejection-profile", out var profile));
            Assert.Equal("sync-invalid-profile", Encoding.UTF8.GetString(Assert.IsType<byte[]>(profile)));
            Assert.Equal(invalidName, forwarded.RoutingKey);
            Assert.Equal(compose ? APPLICATION_ID : "mapped-" + APPLICATION_ID, forwarded.BasicProperties.MessageId);
            Assert.Equal((byte)2, forwarded.BasicProperties.DeliveryMode);
            Assert.False(headers.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
            Assert.Equal(message.DeliveryTag.ToString(), Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.DELIVERY_TAG])));
            using var proof = JsonDocument.Parse(compose
                ? Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["rejection-proof"]))
                : Encoding.UTF8.GetString(forwarded.Body.Span));
            var metadata = proof.RootElement;
            Assert.Equal(APPLICATION_ID, metadata.GetProperty("id").GetString());
            Assert.Equal(Convert.ToBase64String(body), metadata.GetProperty("payload").GetString());
            Assert.Equal(3, metadata.GetProperty("handledCount").GetInt32());
            Assert.Equal(topic.Value, metadata.GetProperty("originalTopic").GetString());
            Assert.Equal("MT_EVENT", metadata.GetProperty("originalType").GetString());
            Assert.Equal("Unacceptable", metadata.GetProperty("reason").GetString());
            Assert.Equal(DESCRIPTION, metadata.GetProperty("description").GetString());
            Assert.InRange(metadata.GetProperty("timestamp").GetDateTimeOffset(), before, DateTimeOffset.UtcNow);
            if (compose)
            {
                Assert.Equal(body, forwarded.Body.ToArray());
                Assert.Equal(3, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
                Assert.Equal(topic.Value, Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_TOPIC])));
            }
            else
            {
                Assert.Equal("application/vnd.rejected-order+json", forwarded.BasicProperties.ContentType);
            }
            BasicGetResult? remaining = null;
            var settlementWait = Stopwatch.StartNew();
            while (remaining == null && settlementWait.Elapsed < TimeSpan.FromSeconds(2))
            {
                remaining = channel.BasicGet(name, autoAck: true);
                if (remaining == null) Thread.Sleep(TimeSpan.FromMilliseconds(25));
            }
            Assert.Null(remaining);
            Assert.Null(channel.BasicGet(invalidName, autoAck: true));
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
                    try
                    {
                        if (invalidCreated) channel.QueueDelete(invalidName, ifUnused: false, ifEmpty: false);
                    }
                    finally
                    {
                        channel.ExchangeDelete(name);
                    }
                }
            }
        }
    }
}
