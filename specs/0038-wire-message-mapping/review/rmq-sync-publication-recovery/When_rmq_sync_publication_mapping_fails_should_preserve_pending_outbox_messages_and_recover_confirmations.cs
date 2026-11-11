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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;
using Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;
using Polly.Registry;
using RabbitMQ.Client;
using Xunit;

namespace Paramore.Brighter.RMQ.Sync.Tests.MessagingGateway;

[Trait("Category", "RMQ")]
[Collection("RMQ")]
public class RmqSyncPublicationRecoveryTests
{
    [Theory]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    [InlineData(true, 3)]
    public async Task When_rmq_sync_publication_mapping_fails_should_preserve_pending_outbox_messages_and_recover_confirmations(
        bool useAsync, int failureMode)
    {
        //Arrange
        var uri = new Uri("amqp://guest:guest@localhost:5672/%2f");
        var name = $"sync-publication-recovery-{Guid.NewGuid():N}";
        var topic = new RoutingKey("orders");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var broker = new ConnectionFactory { Uri = uri }.CreateConnection();
        using var channel = broker.CreateModel();
        channel.ExchangeDeclare(name, ExchangeType.Direct, durable: false, autoDelete: false);
        try
        {
            var queue = channel.QueueDeclare("", durable: false, exclusive: true, autoDelete: true);
            channel.QueueBind(queue.QueueName, name, topic.Value);
            var writer = new SyncRecoverablePublicationWireWriter(failureMode);
            var publication = new RmqPublication
            {
                Topic = topic, MakeChannels = OnMissingChannel.Validate, WireMessageWriter = writer
            };
            var factory = new RmqMessageProducerFactory(new RmqMessagingGatewayConnection
            {
                AmpqUri = new AmqpUriSpecification(uri), Exchange = new Exchange(name), PersistMessages = true
            }, [publication]);
            var producers = factory.Create();
            using var confirmationAvailable = new SemaphoreSlim(0);
            using var registry = new ProducerRegistry(producers);
            var producer = Assert.Single(producers).Value;
            var confirmations = new ConcurrentQueue<PublishConfirmationResult>();
            Assert.IsAssignableFrom<ISupportPublishConfirmation>(producer).OnMessagePublished += result =>
            {
                confirmations.Enqueue(result);
                confirmationAvailable.Release();
            };
            using var mappers = new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null);
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(registry,
                new ResiliencePipelineRegistry<string>().AddBrighterDefault(), mappers,
                new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer: null,
                new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
            var context = new RequestContext();
            var failed = new Message(new MessageHeader(new Id("failed-publication"), topic, MessageType.MT_EVENT),
                new MessageBody("retained payload")) { Persist = true };
            failed.Header.Bag["tenant"] = "retained-tenant";
            var healthy = new Message(new MessageHeader(new Id("healthy-publication"), topic, MessageType.MT_EVENT),
                new MessageBody("independent payload")) { Persist = true };

            //Act
            var error = useAsync
                ? await Record.ExceptionAsync(() => Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer)
                    .SendAsync(failed, timeout.Token))
                : Record.Exception(() => Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(failed));

            //Assert
            Assert.NotNull(error);
            if (failureMode == 1) Assert.IsType<FormatException>(error);
            if (failureMode == 3) Assert.IsType<InvalidOperationException>(error);
            Assert.Empty(confirmations);
            Assert.Null(channel.BasicGet(queue.QueueName, autoAck: true));

            //Act
            if (useAsync)
            {
                await mediator.AddToOutboxAsync(failed, context, cancellationToken: timeout.Token);
                await mediator.ClearOutboxAsync([failed.Id], context, cancellationToken: timeout.Token);
            }
            else
            {
                mediator.AddToOutbox(failed, context);
                mediator.ClearOutbox([failed.Id], context);
            }

            //Assert
            var pending = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(failed.Id, pending.Id);
            Assert.Equal(topic, pending.Header.Topic);
            Assert.Equal("retained payload", pending.Body.Value);
            Assert.Equal("retained-tenant", pending.Header.Bag["tenant"]);
            Assert.Empty(confirmations);
            Assert.Null(channel.BasicGet(queue.QueueName, autoAck: true));

            //Act
            if (useAsync)
            {
                await mediator.AddToOutboxAsync(healthy, context, cancellationToken: timeout.Token);
                await mediator.ClearOutboxAsync([healthy.Id], context, cancellationToken: timeout.Token);
            }
            else
            {
                mediator.AddToOutbox(healthy, context);
                mediator.ClearOutbox([healthy.Id], context);
            }
            Assert.True(await confirmationAvailable.WaitAsync(TimeSpan.FromSeconds(10), timeout.Token));

            //Assert
            var firstConfirmation = Assert.Single(confirmations);
            Assert.Equal(healthy.Id, firstConfirmation.MessageId);
            Assert.True(firstConfirmation.Success);
            Assert.Equal(topic, firstConfirmation.Topic);
            var healthySettlement = Stopwatch.StartNew();
            while (outbox.OutstandingMessages(TimeSpan.Zero, context).Any(m => m.Id == healthy.Id)
                && healthySettlement.Elapsed < TimeSpan.FromSeconds(5))
                await Task.Delay(TimeSpan.FromMilliseconds(25), timeout.Token);
            pending = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(failed.Id, pending.Id);
            Assert.Equal("retained payload", pending.Body.Value);
            var nativeHealthy = channel.BasicGet(queue.QueueName, autoAck: true);
            Assert.NotNull(nativeHealthy);
            Assert.Equal(healthy.Id.Value, nativeHealthy.BasicProperties.MessageId);
            Assert.Equal("independent payload", Encoding.UTF8.GetString(nativeHealthy.Body.Span));
            Assert.Equal("recoverable-writer", Encoding.UTF8.GetString(
                Assert.IsType<byte[]>(nativeHealthy.BasicProperties.Headers["publication-profile"])));
            Assert.Equal((byte)2, nativeHealthy.BasicProperties.DeliveryMode);
            Assert.Null(channel.BasicGet(queue.QueueName, autoAck: true));

            //Act
            writer.FailOnWrite = false;
            if (useAsync)
                await mediator.ClearOutboxAsync([failed.Id], context, cancellationToken: timeout.Token);
            else
                mediator.ClearOutbox([failed.Id], context);
            Assert.True(await confirmationAvailable.WaitAsync(TimeSpan.FromSeconds(10), timeout.Token));
            var recoveredSettlement = Stopwatch.StartNew();
            while (outbox.OutstandingMessages(TimeSpan.Zero, context).Any()
                && recoveredSettlement.Elapsed < TimeSpan.FromSeconds(5))
                await Task.Delay(TimeSpan.FromMilliseconds(25), timeout.Token);
            registry.CloseAll();

            //Assert
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            var completed = confirmations.ToArray();
            Assert.Equal(2, completed.Length);
            Assert.Equal(failed.Id, completed[1].MessageId);
            Assert.True(completed[1].Success);
            Assert.Equal(topic, completed[1].Topic);
            var nativeRecovered = channel.BasicGet(queue.QueueName, autoAck: true);
            Assert.NotNull(nativeRecovered);
            Assert.Equal(failed.Id.Value, nativeRecovered.BasicProperties.MessageId);
            Assert.Equal("retained payload", Encoding.UTF8.GetString(nativeRecovered.Body.Span));
            Assert.Equal("recoverable-writer", Encoding.UTF8.GetString(
                Assert.IsType<byte[]>(nativeRecovered.BasicProperties.Headers["publication-profile"])));
            Assert.Equal((byte)2, nativeRecovered.BasicProperties.DeliveryMode);
            Assert.Equal(topic.Value, nativeRecovered.RoutingKey);
            Assert.Equal(new Id("failed-publication"), failed.Id);
            Assert.Equal(topic, failed.Header.Topic);
            Assert.Equal("retained payload", failed.Body.Value);
            Assert.Equal("retained-tenant", failed.Header.Bag["tenant"]);
            Assert.Null(channel.BasicGet(queue.QueueName, autoAck: true));
        }
        finally
        {
            channel.ExchangeDelete(name);
        }
    }
}
