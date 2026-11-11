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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Paramore.Brighter.MessagingGateway.Postgres;
using Paramore.Brighter.PostgresSQL.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.PostgresSQL.Tests.MessagingGateway;

/// <summary>Verifies native row ownership and recovery when selected mappings fail.</summary>
[Trait("Category", "PostgresSql")]
public class PostgresWireMappingFailureTests
{
    /// <summary>A failed writer inserts no row and the same producer can recover.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_postgres_writer_fails_should_preserve_rows_and_allow_recovery(bool useAsync, bool returnNull)
    {
        //Arrange
        var helper = new PostgresSqlTestHelper();
        helper.SetupDatabase();
        var configuration = helper.Configuration;
        var topic = new RoutingKey("writer-failure-" + Guid.NewGuid().ToString("N"));
        var writer = new PostgresRecoverableWireWriter(returnNull);
        var connection = new PostgresMessagingGatewayConnection(configuration);
        var factory = new PostgresMessageProducerFactory(connection,
            [new PostgresPublication { Topic = topic, WireMessageWriter = writer }]);
        using var registry = new ProducerRegistry(useAsync ? await factory.CreateAsync() : factory.Create());
        var producer = (PostgresMessageProducer)registry.LookupBy(topic);
        var message = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT), new MessageBody("recover-π"));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var observer = new NpgsqlConnection(configuration.ConnectionString);
        await observer.OpenAsync(timeout.Token);
        try
        {
            //Act
            var error = await Record.ExceptionAsync(async () =>
            {
                if (useAsync) await producer.SendAsync(message, timeout.Token);
                else producer.Send(message);
            });

            //Assert
            if (returnNull) Assert.IsType<InvalidOperationException>(error);
            else Assert.Same(writer.Failure, error);
            using var count = observer.CreateCommand();
            count.CommandText = $"SELECT COUNT(*) FROM \"{configuration.QueueStoreTable}\"";
            Assert.Equal(0L, await count.ExecuteScalarAsync(timeout.Token));
            Assert.Equal("recover-π", message.Body.Value);

            //Act
            writer.Recover();
            if (useAsync) await producer.SendAsync(message, timeout.Token);
            else producer.Send(message);

            //Assert
            Assert.Equal(1L, await count.ExecuteScalarAsync(timeout.Token));
            using var payload = observer.CreateCommand();
            payload.CommandText = $"SELECT content::text FROM \"{configuration.QueueStoreTable}\"";
            var recovered = new DefaultPostgresWireMessageReader().ReadMessage((string)(await payload.ExecuteScalarAsync(timeout.Token))!);
            Assert.Equal(message.Id, recovered.Id);
            Assert.Equal(message.Body.Value, recovered.Body.Value);
        }
        finally
        {
            using var cleanup = observer.CreateCommand();
            cleanup.CommandText = $"DROP TABLE IF EXISTS \"{configuration.QueueStoreTable}\"";
            await cleanup.ExecuteNonQueryAsync();
        }
    }

    /// <summary>A failed reader returns a rejectable message carrying the actual native receipt.</summary>
    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task When_a_postgres_reader_fails_should_preserve_settlement_and_allow_recovery(bool useAsync, bool returnNull, bool large)
    {
        //Arrange
        var helper = new PostgresSqlTestHelper(true);
        helper.SetupDatabase();
        var configuration = helper.Configuration;
        var topic = new RoutingKey("reader-failure-" + Guid.NewGuid().ToString("N"));
        var reader = new PostgresRecoverableWireReader(returnNull);
        var connection = new PostgresMessagingGatewayConnection(configuration);
        var factory = new PostgresMessageProducerFactory(connection, [new PostgresPublication { Topic = topic }]);
        using var registry = new ProducerRegistry(useAsync ? await factory.CreateAsync() : factory.Create());
        var producer = (PostgresMessageProducer)registry.LookupBy(topic);
        var subscription = new PostgresSubscription(new SubscriptionName("failure"), new ChannelName(topic.Value), topic,
            typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor, tableWithLargeMessage: large) { WireMessageReader = reader };
        var consumers = new PostgresConsumerFactory(connection);
        await using var consumer = useAsync ? (PostgresMessageConsumer)consumers.CreateAsync(subscription) : (PostgresMessageConsumer)consumers.Create(subscription);
        var message = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT), new MessageBody("reader-recover-π"));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var observer = new NpgsqlConnection(configuration.ConnectionString);
        await observer.OpenAsync(timeout.Token);
        try
        {
            if (useAsync) await producer.SendAsync(message, timeout.Token);
            else producer.Send(message);
            using var inspect = observer.CreateCommand();
            inspect.CommandText = $"SELECT id FROM \"{configuration.QueueStoreTable}\"";
            var rowId = (long)(await inspect.ExecuteScalarAsync(timeout.Token))!;

            //Act
            Message[]? messages = null;
            var error = await Record.ExceptionAsync(async () => messages = useAsync
                ? await consumer.ReceiveAsync(cancellationToken: timeout.Token) : consumer.Receive());

            //Assert
            Assert.Null(error);
            var failed = Assert.Single(messages!);
            Assert.Equal(MessageType.MT_UNACCEPTABLE, failed.Header.MessageType);
            Assert.Equal(rowId.ToString(), failed.Id.Value);
            Assert.True(failed.Header.Bag.TryGetValue("ReceiptHandle", out var receipt));
            Assert.Equal(rowId, Assert.IsType<long>(receipt));
            Assert.Contains("reader-recover-π", new DefaultPostgresWireMessageReader().ReadMessage(failed.Body.Value).Body.Value);

            //Act
            Assert.True(useAsync ? await consumer.RejectAsync(failed, new MessageRejectionReason(RejectionReason.Unacceptable), timeout.Token)
                : consumer.Reject(failed, new MessageRejectionReason(RejectionReason.Unacceptable)));

            //Assert
            using var count = observer.CreateCommand();
            count.CommandText = $"SELECT COUNT(*) FROM \"{configuration.QueueStoreTable}\"";
            Assert.Equal(0L, await count.ExecuteScalarAsync(timeout.Token));

            //Act
            reader.Recover();
            if (useAsync) await producer.SendAsync(message, timeout.Token);
            else producer.Send(message);
            var recovered = Assert.Single(useAsync ? await consumer.ReceiveAsync(cancellationToken: timeout.Token) : consumer.Receive());

            //Assert
            Assert.Equal(message.Id, recovered.Id);
            Assert.Equal(message.Body.Value, recovered.Body.Value);
            if (useAsync) await consumer.AcknowledgeAsync(recovered, timeout.Token);
            else consumer.Acknowledge(recovered);
            Assert.Equal(0L, await count.ExecuteScalarAsync(timeout.Token));
        }
        finally
        {
            using var cleanup = observer.CreateCommand();
            cleanup.CommandText = $"DROP TABLE IF EXISTS \"{configuration.QueueStoreTable}\"";
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
