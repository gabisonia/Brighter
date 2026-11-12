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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using System.Text.Json;
using Paramore.Brighter.MessagingGateway.Postgres;
using Paramore.Brighter.PostgresSQL.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.PostgresSQL.Tests.MessagingGateway;

/// <summary>Exercises selected rejection mappings through native PostgreSQL rows and public consumers.</summary>
[Trait("Category", "PostgresSql")]
public class PostgresRejectionWireSelectionTests
{
    /// <summary>Captures forwarding selection, contains failures and permits subsequent messages.</summary>
    [Theory]
    [InlineData(false, false, "mapped", false)]
    [InlineData(false, false, "mapped", true)]
    [InlineData(false, true, "mapped", false)]
    [InlineData(false, true, "mapped", true)]
    [InlineData(true, false, "mapped", false)]
    [InlineData(true, false, "mapped", true)]
    [InlineData(true, true, "mapped", false)]
    [InlineData(true, true, "mapped", true)]
    [InlineData(false, false, "throw", false)]
    [InlineData(false, false, "throw", true)]
    [InlineData(false, true, "throw", false)]
    [InlineData(false, true, "throw", true)]
    [InlineData(true, false, "throw", false)]
    [InlineData(true, false, "throw", true)]
    [InlineData(true, true, "throw", false)]
    [InlineData(true, true, "throw", true)]
    [InlineData(false, false, "null", false)]
    [InlineData(false, false, "null", true)]
    [InlineData(false, true, "null", false)]
    [InlineData(false, true, "null", true)]
    [InlineData(true, false, "null", false)]
    [InlineData(true, false, "null", true)]
    [InlineData(true, true, "null", false)]
    [InlineData(true, true, "null", true)]
    public async Task When_postgres_rejects_with_selected_writers_should_preserve_routing_and_allow_subsequent_delivery(
        bool useAsync, bool invalid, string mode, bool binary)
    {
        //Arrange
        var helper = new PostgresSqlTestHelper(binary);
        helper.SetupDatabase();
        var configuration = helper.Configuration;
        var source = new RoutingKey("source-" + Guid.NewGuid().ToString("N"));
        var deadLetter = new RoutingKey(source.Value + "-dead");
        var invalidRoute = new RoutingKey(source.Value + "-invalid");
        var target = invalid ? invalidRoute : deadLetter;
        var table = $"\"public\".\"{configuration.QueueStoreTable}\"";
        var mapped = new PostgresSelectedWireWriter(true);
        var failing = new PostgresRecoverableWireWriter(mode == "null");
        IAmAWireMessageWriter<string> selected = mode == "mapped" ? mapped : failing;
        var inactive = new PostgresSelectedWireWriter(false);
        var subscription = new PostgresSubscription(new SubscriptionName("forward"), new ChannelName(source.Value), source,
            typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor,
            deadLetterRoutingKey: deadLetter, invalidMessageRoutingKey: invalidRoute)
        {
            DeadLetterWireMessageWriter = invalid ? inactive : selected,
            InvalidMessageWireMessageWriter = invalid ? selected : inactive
        };
        var connection = new PostgresMessagingGatewayConnection(configuration);
        var publications = new PostgresMessageProducerFactory(connection, [new PostgresPublication { Topic = source }]);
        using var registry = new ProducerRegistry(useAsync ? await publications.CreateAsync() : publications.Create());
        var factory = new PostgresConsumerFactory(connection);
        await using var consumer = useAsync ? (PostgresMessageConsumer)factory.CreateAsync(subscription) : (PostgresMessageConsumer)factory.Create(subscription);
        subscription.DeadLetterWireMessageWriter = inactive;
        subscription.InvalidMessageWireMessageWriter = inactive;
        var producer = (PostgresMessageProducer)registry.LookupBy(source);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var observer = new NpgsqlConnection(configuration.ConnectionString);
        await observer.OpenAsync(timeout.Token);
        var reason = new MessageRejectionReason(invalid ? RejectionReason.Unacceptable : RejectionReason.DeliveryError, "mapping rejection");
        try
        {
            for (var attempt = 0; attempt < 2; attempt++)
            {
                var message = new Message(new MessageHeader(Id.Random(), source, MessageType.MT_EVENT), new MessageBody("forward-body-π-" + attempt));
                message.Header.Bag["tenant"] = "selected-λ";

                //Act
                if (useAsync) await producer.SendAsync(message, timeout.Token);
                else producer.Send(message);
                var delivered = Assert.Single(useAsync ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(5), timeout.Token) : consumer.Receive());
                Assert.Equal(message.Id, delivered.Id);
                Assert.True(useAsync ? await consumer.RejectAsync(delivered, reason, timeout.Token) : consumer.Reject(delivered, reason));

                //Assert
                using var query = observer.CreateCommand();
                query.CommandText = $"SELECT queue, content::text FROM {table}";
                await using (var rows = await query.ExecuteReaderAsync(timeout.Token))
                {
                    if (mode != "mapped" && attempt == 0)
                    {
                        // Rejection deletes the source even when forwarding fails; no destination is written.
                        Assert.False(await rows.ReadAsync(timeout.Token));
                        failing.Recover();
                    }
                    else
                    {
                        Assert.True(await rows.ReadAsync(timeout.Token));
                        Assert.Equal(target.Value, rows.GetString(0));
                        var payload = rows.GetString(1);
                        using var document = JsonDocument.Parse(payload);
                        var content = document.RootElement;
                        if (mode == "mapped")
                        {
                            Assert.True(content.TryGetProperty("composed", out var composed));
                            content = composed;
                        }
                        var decoded = new DefaultPostgresWireMessageReader().ReadMessage(content.GetRawText());
                        Assert.Equal(message.Id, decoded.Id);
                        Assert.Equal(message.Body.Value, decoded.Body.Value);
                        Assert.Equal(target, decoded.Header.Topic);
                        Assert.Equal(source.Value, decoded.Header.Bag["originalTopic"].ToString());
                        Assert.Equal(reason.RejectionReason.ToString(), decoded.Header.Bag["rejectionReason"].ToString());
                        Assert.Equal("mapping rejection", decoded.Header.Bag["rejectionMessage"].ToString());
                        Assert.Equal("selected-λ", decoded.Header.Bag["tenant"].ToString());
                        Assert.False(await rows.ReadAsync(timeout.Token));
                    }
                }
                Assert.Equal(0, inactive.Calls);
                using var clear = observer.CreateCommand();
                clear.CommandText = $"DELETE FROM {table}";
                await clear.ExecuteNonQueryAsync(timeout.Token);
            }
            if (mode == "mapped") Assert.Equal(2, mapped.Calls);
        }
        finally
        {
            using var cleanup = observer.CreateCommand();
            cleanup.CommandText = $"DROP TABLE IF EXISTS {table}";
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
