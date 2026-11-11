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
using Paramore.Brighter.RMQ.Async.Tests.TestDoubles;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Async.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
public class RmqRequeueWriterIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_rmq_subscriptions_select_different_requeue_writers_should_keep_their_mapping_isolated(
        bool useAsync, bool customFirst)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"requeue-isolation-{Guid.NewGuid():N}";
        var queueNames = new Dictionary<bool, string>
        {
            [false] = $"default-requeue-{Guid.NewGuid():N}",
            [true] = $"custom-requeue-{Guid.NewGuid():N}"
        };
        bool[] creationOrder = customFirst ? [true, false] : [false, true];
        var consumers = new Dictionary<bool, IAmAMessageConsumerAsync>();
        var sources = new Dictionary<bool, Message>();
        var sourceBags = new Dictionary<bool, Dictionary<string, object>>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var broker = await new ConnectionFactory { Uri = uri }.CreateConnectionAsync(timeout.Token);
        await using var channel = await broker.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            timeout.Token);
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
            foreach (var isCustom in creationOrder)
            {
                var kind = isCustom ? "custom" : "default";
                var queueName = queueNames[isCustom];
                var topic = new RoutingKey($"{kind}-orders");
                await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false,
                    autoDelete: false, cancellationToken: timeout.Token);
                await channel.QueueBindAsync(queueName, exchangeName, topic.Value, cancellationToken: timeout.Token);
                var properties = new BasicProperties
                {
                    MessageId = $"{kind}-native-id",
                    ContentType = "application/json",
                    Type = "application/json",
                    DeliveryMode = DeliveryModes.Persistent,
                    Headers = new Dictionary<string, object?>
                    {
                        ["tenant_id"] = $"{kind}-tenant",
                        [HeaderNames.HANDLED_COUNT] = 2
                    }
                };
                await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                    basicProperties: properties, body: Encoding.UTF8.GetBytes($"{kind} payload"),
                    cancellationToken: timeout.Token);
                var subscription = new RmqSubscription(
                    new SubscriptionName(queueName),
                    new ChannelName(queueName),
                    topic,
                    requestType: typeof(Command),
                    isDurable: true,
                    messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                    makeChannels: OnMissingChannel.Validate);
                if (isCustom)
                    subscription.RequeueWireMessageWriter = new LegacyTenantRmqWireMessageWriter();
                consumers.Add(isCustom, useAsync
                    ? factory.CreateAsync(subscription)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription)));
            }
            foreach (var isCustom in creationOrder)
            {
                var consumer = consumers[isCustom];
                var kind = isCustom ? "custom" : "default";
                var messages = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
                var message = Assert.Single(messages);
                Assert.Equal($"{kind}-native-id", message.Id.Value);
                Assert.Equal($"{kind} payload", message.Body.Value);
                Assert.Equal(2, message.Header.HandledCount);
                Assert.True(message.Persist);
                Assert.True(message.DeliveryTag > 0);
                message.Header.UpdateHandledCount();
                sources.Add(isCustom, message);
                sourceBags.Add(isCustom, new Dictionary<string, object>(message.Header.Bag));
            }

            //Act
            foreach (var isCustom in creationOrder)
            {
                var consumer = consumers[isCustom];
                var requeued = useAsync
                    ? await consumer.RequeueAsync(sources[isCustom], TimeSpan.Zero, timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Requeue(sources[isCustom], TimeSpan.Zero);
                Assert.True(requeued);
                await consumer.DisposeAsync();
                consumers.Remove(isCustom);
            }

            //Assert
            foreach (var isCustom in creationOrder)
            {
                var kind = isCustom ? "custom" : "default";
                var nativeId = $"{kind}-native-id";
                var queueName = queueNames[isCustom];
                var retry = await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token);
                Assert.NotNull(retry);
                var expectedBody = isCustom ? $"legacy:{kind} payload" : $"{kind} payload";
                Assert.Equal(Encoding.UTF8.GetBytes(expectedBody), retry.Body.ToArray());
                Assert.Equal(isCustom ? "legacy-order" : "application/json; charset=utf-8", retry.BasicProperties.Type);
                Assert.Equal(isCustom ? "application/vnd.legacy.order" : "application/json",
                    retry.BasicProperties.ContentType);
                Assert.Equal(DeliveryModes.Persistent, retry.BasicProperties.DeliveryMode);
                Assert.False(string.IsNullOrEmpty(retry.BasicProperties.MessageId));
                Assert.NotEqual(nativeId, retry.BasicProperties.MessageId);
                Assert.NotEqual($"legacy-{nativeId}", retry.BasicProperties.MessageId);
                Assert.Equal(queueName, retry.RoutingKey);
                var headers = retry.BasicProperties.Headers;
                Assert.NotNull(headers);
                Assert.Equal(isCustom, headers.ContainsKey("tenant-id"));
                Assert.Equal(!isCustom, headers.ContainsKey("tenant_id"));
                Assert.Equal($"{kind}-tenant", Encoding.UTF8.GetString(
                    Assert.IsType<byte[]>(headers[isCustom ? "tenant-id" : "tenant_id"])));
                Assert.Equal(3, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
                Assert.Equal("1", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.DELIVERY_TAG])));
                Assert.Equal(nativeId,
                    Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_MESSAGE_ID])));
                Assert.False(headers.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
                Assert.Equal(!isCustom, headers.ContainsKey(HeaderNames.CLOUD_EVENTS_ID));
                if (!isCustom)
                    Assert.Equal(nativeId,
                        Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.CLOUD_EVENTS_ID])));
                var source = sources[isCustom];
                Assert.Equal(nativeId, source.Id.Value);
                Assert.Equal(new RoutingKey($"{kind}-orders"), source.Header.Topic);
                Assert.Equal($"{kind} payload", source.Body.Value);
                Assert.Equal(3, source.Header.HandledCount);
                Assert.True(source.Persist);
                Assert.Equal(sourceBags[isCustom].Count, source.Header.Bag.Count);
                foreach (var entry in sourceBags[isCustom])
                    Assert.Equal(entry.Value, source.Header.Bag[entry.Key]);
                Assert.Null(await channel.BasicGetAsync(queueName, autoAck: true, cancellationToken: timeout.Token));
            }
        }
        finally
        {
            try
            {
                foreach (var consumer in consumers.Values)
                    await consumer.DisposeAsync();
            }
            finally
            {
                try
                {
                    await channel.QueueDeleteAsync(queueNames[false], ifUnused: false, ifEmpty: false);
                }
                finally
                {
                    try
                    {
                        await channel.QueueDeleteAsync(queueNames[true], ifUnused: false, ifEmpty: false);
                    }
                    finally
                    {
                        await channel.ExchangeDeleteAsync(exchangeName);
                    }
                }
            }
        }
    }
}
