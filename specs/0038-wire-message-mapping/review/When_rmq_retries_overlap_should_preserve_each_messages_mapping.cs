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
public class RmqOverlappingRequeueMappingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_rmq_retries_overlap_should_preserve_each_messages_mapping(bool useAsync)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var exchangeName = $"overlapping-requeue-{Guid.NewGuid():N}";
        var queueNames = new[] { $"retry-first-{Guid.NewGuid():N}", $"retry-second-{Guid.NewGuid():N}" };
        var consumers = new List<IAmAMessageConsumerAsync>();
        var sources = new List<Message>();
        var sourceBags = new List<Dictionary<string, object>>();
        var sourceTags = new List<ulong>();
        using var writer = new CoordinatedTenantRmqWireMessageWriter();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
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
            for (var index = 0; index < queueNames.Length; index++)
            {
                var queueName = queueNames[index];
                var topic = new RoutingKey($"orders-{index}");
                await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false,
                    autoDelete: false, cancellationToken: timeout.Token);
                await channel.QueueBindAsync(queueName, exchangeName, topic.Value, cancellationToken: timeout.Token);
                var properties = new BasicProperties
                {
                    MessageId = $"native-{index}",
                    ContentType = "application/json",
                    Type = "application/json",
                    DeliveryMode = index == 0 ? DeliveryModes.Persistent : DeliveryModes.Transient,
                    Headers = new Dictionary<string, object?>
                    {
                        ["tenant_id"] = $"tenant-{index}",
                        [HeaderNames.HANDLED_COUNT] = index + 2,
                        [HeaderNames.CORRELATION_ID] = $"correlation-{index}",
                        [HeaderNames.CLOUD_EVENTS_SUBJECT] = $"subject-{index}"
                    }
                };
                await channel.BasicPublishAsync(exchangeName, topic.Value, mandatory: true,
                    basicProperties: properties, body: Encoding.UTF8.GetBytes($"payload-{index}"),
                    cancellationToken: timeout.Token);
                var subscription = new RmqSubscription(
                    new SubscriptionName(queueName),
                    new ChannelName(queueName),
                    topic,
                    requestType: typeof(Command),
                    isDurable: true,
                    messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
                    makeChannels: OnMissingChannel.Validate)
                {
                    RequeueWireMessageWriter = writer
                };
                var consumer = useAsync
                    ? factory.CreateAsync(subscription)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerAsync>(factory.Create(subscription));
                consumers.Add(consumer);
                var messages = useAsync
                    ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Receive(TimeSpan.FromSeconds(10));
                var message = Assert.Single(messages);
                Assert.Equal($"native-{index}", message.Id.Value);
                Assert.Equal($"payload-{index}", message.Body.Value);
                Assert.Equal(index + 2, message.Header.HandledCount);
                Assert.Equal(index == 0, message.Persist);
                Assert.True(message.DeliveryTag > 0);
                message.Header.UpdateHandledCount();
                sources.Add(message);
                sourceBags.Add(new Dictionary<string, object>(message.Header.Bag));
                sourceTags.Add(message.DeliveryTag);
            }

            //Act
            var retries = new List<Task<bool>>();
            for (var index = 0; index < consumers.Count; index++)
            {
                var consumer = consumers[index];
                var source = sources[index];
                retries.Add(Task.Run(async () => useAsync
                    ? await consumer.RequeueAsync(source, TimeSpan.Zero, timeout.Token)
                    : Assert.IsAssignableFrom<IAmAMessageConsumerSync>(consumer).Requeue(source, TimeSpan.Zero),
                    timeout.Token));
            }
            var results = await Task.WhenAll(retries);
            Assert.All(results, result => Assert.True(result));
            foreach (var consumer in consumers)
                await consumer.DisposeAsync();
            consumers.Clear();

            //Assert
            var retryIds = new HashSet<string>();
            for (var index = 0; index < queueNames.Length; index++)
            {
                var retry = await channel.BasicGetAsync(queueNames[index], autoAck: true,
                    cancellationToken: timeout.Token);
                Assert.NotNull(retry);
                Assert.Equal(Encoding.UTF8.GetBytes($"payload-{index}"), retry.Body.ToArray());
                Assert.Equal("application/json; charset=utf-8", retry.BasicProperties.Type);
                Assert.Equal("application/json", retry.BasicProperties.ContentType);
                Assert.Equal(index == 0 ? DeliveryModes.Persistent : DeliveryModes.Transient,
                    retry.BasicProperties.DeliveryMode);
                var retryId = Assert.IsType<string>(retry.BasicProperties.MessageId);
                Assert.False(string.IsNullOrEmpty(retryId));
                Assert.NotEqual("native-0", retryId);
                Assert.NotEqual("native-1", retryId);
                Assert.True(retryIds.Add(retryId));
                Assert.Equal(queueNames[index], retry.RoutingKey);
                var headers = retry.BasicProperties.Headers;
                Assert.NotNull(headers);
                Assert.False(headers.ContainsKey("tenant_id"));
                Assert.Equal($"tenant-{index}",
                    Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers["tenant-id"])));
                Assert.Equal($"correlation-{index}",
                    Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.CORRELATION_ID])));
                Assert.Equal($"subject-{index}",
                    Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.CLOUD_EVENTS_SUBJECT])));
                Assert.Equal($"native-{index}",
                    Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.CLOUD_EVENTS_ID])));
                Assert.Equal($"native-{index}",
                    Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.ORIGINAL_MESSAGE_ID])));
                Assert.Equal(index + 3, Assert.IsType<int>(headers[HeaderNames.HANDLED_COUNT]));
                Assert.Equal("1", Encoding.UTF8.GetString(Assert.IsType<byte[]>(headers[HeaderNames.DELIVERY_TAG])));
                Assert.False(headers.ContainsKey(HeaderNames.DELAY_MILLISECONDS));
                var source = sources[index];
                Assert.Equal($"native-{index}", source.Id.Value);
                Assert.Equal(new RoutingKey($"orders-{index}"), source.Header.Topic);
                Assert.Equal($"payload-{index}", source.Body.Value);
                Assert.Equal(index + 3, source.Header.HandledCount);
                Assert.Equal(sourceTags[index], source.DeliveryTag);
                Assert.Equal(index == 0, source.Persist);
                Assert.Equal(sourceBags[index].Count, source.Header.Bag.Count);
                foreach (var entry in sourceBags[index])
                    Assert.Equal(entry.Value, source.Header.Bag[entry.Key]);
                Assert.Null(await channel.BasicGetAsync(queueNames[index], autoAck: true,
                    cancellationToken: timeout.Token));
            }
        }
        finally
        {
            try
            {
                foreach (var consumer in consumers)
                    await consumer.DisposeAsync();
            }
            finally
            {
                try
                {
                    await channel.QueueDeleteAsync(queueNames[0], ifUnused: false, ifEmpty: false);
                }
                finally
                {
                    try
                    {
                        await channel.QueueDeleteAsync(queueNames[1], ifUnused: false, ifEmpty: false);
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
