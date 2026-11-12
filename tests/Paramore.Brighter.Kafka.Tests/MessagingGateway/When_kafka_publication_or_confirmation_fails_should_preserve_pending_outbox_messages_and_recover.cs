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
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.Kafka.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.Kafka;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway;

[Trait("Category", "Kafka")]
[Collection("Kafka")]
public class KafkaOutboxRecoveryTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 0)]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    public async Task When_kafka_publication_or_confirmation_fails_should_preserve_pending_outbox_messages_and_recover(
        bool useAsync, int failureMode)
    {
        //Arrange
        var topic = new RoutingKey($"outbox-recovery-{Guid.NewGuid():N}");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic.Value, NumPartitions = 1, ReplicationFactor = 1 }]);
        try
        {
            using var observer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", GroupId = Guid.NewGuid().ToString(),
                EnableAutoCommit = false, AutoOffsetReset = AutoOffsetReset.Earliest
            }).Build();
            var partition = new TopicPartition(topic.Value, new Partition(0));
            observer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));
            var writer = new KafkaRecoverableWireWriter(returnNull: failureMode == 1)
            {
                FailBadMessage = failureMode != 2
            };
            var publication = new KafkaPublication
            {
                Topic = topic, MakeChannels = OnMissingChannel.Validate, NumPartitions = 1, ReplicationFactor = 1,
                MessageTimeoutMs = 10000, RequestTimeoutMs = 5000, WireMessageWriter = writer
            };
            var producers = new KafkaMessageProducerFactory(new KafkaMessagingGatewayConfiguration
            {
                Name = "outbox-recovery", BootStrapServers = ["localhost:9092"]
            }, [publication]).Create();
            using var confirmationAvailable = new SemaphoreSlim(0);
            using var registry = new ProducerRegistry(producers);
            var producer = Assert.Single(producers).Value;
            var confirmations = new ConcurrentQueue<PublishConfirmationResult>();
            var failConfirmation = failureMode == 2;
            if (useAsync)
                Assert.IsAssignableFrom<ISupportPublishConfirmationAsync>(producer).OnMessagePublishedAsync += result =>
                {
                    confirmations.Enqueue(result);
                    confirmationAvailable.Release();
                    return Volatile.Read(ref failConfirmation) && result.MessageId == new Id("bad-id")
                        ? Task.FromException(new InvalidOperationException("Confirmation subscriber unavailable."))
                        : Task.CompletedTask;
                };
            else
                Assert.IsAssignableFrom<ISupportPublishConfirmation>(producer).OnMessagePublished += result =>
                {
                    confirmations.Enqueue(result);
                    confirmationAvailable.Release();
                    if (Volatile.Read(ref failConfirmation) && result.MessageId == new Id("bad-id"))
                        throw new InvalidOperationException("Confirmation subscriber unavailable.");
                };
            using var mappers = new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null);
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(registry,
                new ResiliencePipelineRegistry<string>().AddBrighterDefault(), mappers,
                new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), tracer: null,
                new FindPublicationByPublicationTopicOrRequestType(), outbox: outbox);
            var context = new RequestContext();
            byte[] badBody = [0, 255, 128, 3];
            byte[] goodBody = [0, 255, 128, 4];
            var bad = new Message(new MessageHeader(new Id("bad-id"), topic, MessageType.MT_EVENT,
                partitionKey: new Paramore.Brighter.PartitionKey("source-key")), new MessageBody(badBody));
            bad.Header.Bag["tenant"] = "retained-tenant";
            var good = new Message(new MessageHeader(new Id("good-id"), topic, MessageType.MT_EVENT,
                partitionKey: new Paramore.Brighter.PartitionKey("source-key")), new MessageBody(goodBody));

            //Act
            if (failureMode != 2)
            {
                var mappingError = useAsync
                    ? await Record.ExceptionAsync(() => Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer)
                        .SendAsync(bad, timeout.Token))
                    : Record.Exception(() => Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(bad));
                Assert.NotNull(mappingError);
            }
            if (useAsync)
            {
                await mediator.AddToOutboxAsync(bad, context, cancellationToken: timeout.Token);
                await mediator.ClearOutboxAsync([bad.Id], context, cancellationToken: timeout.Token);
            }
            else
            {
                mediator.AddToOutbox(bad, context);
                mediator.ClearOutbox([bad.Id], context);
            }
            if (failureMode == 2)
                Assert.True(await confirmationAvailable.WaitAsync(TimeSpan.FromSeconds(10), timeout.Token));

            //Assert
            var pending = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(new Id("bad-id"), pending.Id);
            Assert.Equal(topic, pending.Header.Topic);
            Assert.Equal(badBody, pending.Body.Memory.ToArray());
            Assert.Equal("retained-tenant", pending.Header.Bag["tenant"]);
            Assert.Equal(failureMode == 2 ? 1 : 0, confirmations.Count);
            Assert.Equal(failureMode == 2 ? 1L : 0L,
                observer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High.Value);
            if (failureMode == 2)
            {
                var first = Assert.Single(confirmations);
                Assert.True(first.Success);
                Assert.Equal(new Id("bad-id"), first.MessageId);
                Assert.Equal(topic, first.Topic);
                var nativeFailedCallback = observer.Consume(timeout.Token);
                Assert.Equal("mapped-bad-id", nativeFailedCallback.Message.Key);
                Assert.Equal("wire-bad-id", Encoding.UTF8.GetString(nativeFailedCallback.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
                Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(badBody)), nativeFailedCallback.Message.Value);
            }

            //Act
            if (useAsync)
            {
                await mediator.AddToOutboxAsync(good, context, cancellationToken: timeout.Token);
                await mediator.ClearOutboxAsync([good.Id], context, cancellationToken: timeout.Token);
            }
            else
            {
                mediator.AddToOutbox(good, context);
                mediator.ClearOutbox([good.Id], context);
            }
            Assert.True(await confirmationAvailable.WaitAsync(TimeSpan.FromSeconds(10), timeout.Token));
            var healthySettlement = Stopwatch.StartNew();
            while (outbox.OutstandingMessages(TimeSpan.Zero, context).Any(message => message.Id == good.Id)
                && healthySettlement.Elapsed < TimeSpan.FromSeconds(5))
                await Task.Delay(TimeSpan.FromMilliseconds(25), timeout.Token);

            //Assert
            var healthyConfirmation = confirmations.Last();
            Assert.Equal(new Id("good-id"), healthyConfirmation.MessageId);
            Assert.True(healthyConfirmation.Success);
            Assert.Equal(topic, healthyConfirmation.Topic);
            pending = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal(new Id("bad-id"), pending.Id);
            Assert.Equal(badBody, pending.Body.Memory.ToArray());
            var nativeGood = observer.Consume(timeout.Token);
            Assert.Equal("mapped-good-id", nativeGood.Message.Key);
            Assert.Equal("wire-good-id", Encoding.UTF8.GetString(nativeGood.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(goodBody)), nativeGood.Message.Value);

            //Act
            writer.FailBadMessage = false;
            Volatile.Write(ref failConfirmation, false);
            if (useAsync)
                await mediator.ClearOutboxAsync([bad.Id], context, cancellationToken: timeout.Token);
            else
                mediator.ClearOutbox([bad.Id], context);
            Assert.True(await confirmationAvailable.WaitAsync(TimeSpan.FromSeconds(10), timeout.Token));
            var recoveredSettlement = Stopwatch.StartNew();
            while (outbox.OutstandingMessages(TimeSpan.Zero, context).Any()
                && recoveredSettlement.Elapsed < TimeSpan.FromSeconds(5))
                await Task.Delay(TimeSpan.FromMilliseconds(25), timeout.Token);
            registry.Dispose();

            //Assert
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            var completed = confirmations.ToArray();
            Assert.Equal(failureMode == 2 ? 3 : 2, completed.Length);
            Assert.All(completed, result => Assert.True(result.Success));
            Assert.Equal(new Id("bad-id"), completed.Last().MessageId);
            Assert.Equal(topic, completed.Last().Topic);
            Assert.Equal(failureMode == 2 ? 3L : 2L,
                observer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High.Value);
            var nativeRecovered = observer.Consume(timeout.Token);
            Assert.Equal("mapped-bad-id", nativeRecovered.Message.Key);
            Assert.Equal("wire-bad-id", Encoding.UTF8.GetString(nativeRecovered.Message.Headers.GetLastBytes(HeaderNames.MESSAGE_ID)));
            Assert.Equal(Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(badBody)), nativeRecovered.Message.Value);
            Assert.Equal(new Id("bad-id"), bad.Id);
            Assert.Equal(topic, bad.Header.Topic);
            Assert.Equal(new Paramore.Brighter.PartitionKey("source-key"), bad.Header.PartitionKey);
            Assert.Equal(badBody, bad.Body.Memory.ToArray());
            Assert.Equal("retained-tenant", bad.Header.Bag["tenant"]);
            Assert.Equal(new Id("good-id"), good.Id);
            Assert.Equal(goodBody, good.Body.Memory.ToArray());
        }
        finally
        {
            await admin.DeleteTopicsAsync([topic.Value]);
        }
    }
}
