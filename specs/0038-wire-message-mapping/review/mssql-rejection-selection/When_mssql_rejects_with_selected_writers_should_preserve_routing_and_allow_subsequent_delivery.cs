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
using Microsoft.Data.SqlClient;
using Paramore.Brighter.MessagingGateway.MsSql;
using Paramore.Brighter.MSSQL.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.MSSQL.Tests.MessagingGateway;

/// <summary>Exercises selected rejection mappings through native SQL rows and public consumers.</summary>
[Trait("Category", "MSSQL")]
public class MsSqlRejectionWireSelectionTests
{
    /// <summary>Captures forwarding selection, contains failures and permits subsequent messages.</summary>
    [Theory]
    [InlineData(false, false, "mapped")]
    [InlineData(false, true, "mapped")]
    [InlineData(true, false, "mapped")]
    [InlineData(true, true, "mapped")]
    [InlineData(false, false, "throw")]
    [InlineData(false, true, "throw")]
    [InlineData(true, false, "throw")]
    [InlineData(true, true, "throw")]
    [InlineData(false, false, "null")]
    [InlineData(false, true, "null")]
    [InlineData(true, false, "null")]
    [InlineData(true, true, "null")]
    public async Task When_mssql_rejects_with_selected_writers_should_preserve_routing_and_allow_subsequent_delivery(
        bool useAsync, bool invalid, string mode)
    {
        //Arrange
        var helper = new MsSqlTestHelper(tableName: "forward_" + Guid.NewGuid().ToString("N"));
        helper.SetupQueueDb();
        var configuration = helper.QueueConfiguration;
        var source = new RoutingKey("source-" + Guid.NewGuid().ToString("N"));
        var deadLetter = new RoutingKey(source.Value + "-dead");
        var invalidRoute = new RoutingKey(source.Value + "-invalid");
        var target = invalid ? invalidRoute : deadLetter;
        var mapped = new MsSqlSelectedWireWriter(true);
        var failing = new MsSqlRecoverableWireWriter(mode == "null");
        IAmAWireMessageWriter<string> selected = mode == "mapped" ? mapped : failing;
        var inactive = new MsSqlSelectedWireWriter(false);
        var subscription = new MsSqlSubscription(new SubscriptionName("forward"), new ChannelName(source.Value), source,
            typeof(Message), deadLetterRoutingKey: deadLetter, invalidMessageRoutingKey: invalidRoute)
        {
            DeadLetterWireMessageWriter = invalid ? inactive : selected,
            InvalidMessageWireMessageWriter = invalid ? selected : inactive
        };
        var factory = new MsSqlMessageConsumerFactory(configuration);
        await using var consumer = useAsync ? (MsSqlMessageConsumer)factory.CreateAsync(subscription) : (MsSqlMessageConsumer)factory.Create(subscription);
        subscription.DeadLetterWireMessageWriter = inactive;
        subscription.InvalidMessageWireMessageWriter = inactive;
        using var producer = new MsSqlMessageProducer(configuration, new MsSqlPublication { Topic = source });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var observer = new SqlConnection(configuration.ConnectionString);
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
                query.CommandText = $"SELECT Topic, Payload FROM [{configuration.QueueStoreTable}]";
                await using (var rows = await query.ExecuteReaderAsync(timeout.Token))
                {
                    if (mode != "mapped" && attempt == 0)
                    {
                        // Receipt atomically deleted the source; failed forwarding writes no row.
                        Assert.False(await rows.ReadAsync(timeout.Token));
                        failing.Recover();
                    }
                    else
                    {
                        Assert.True(await rows.ReadAsync(timeout.Token));
                        Assert.Equal(target.Value, rows.GetString(0));
                        var payload = rows.GetString(1);
                        if (mode == "mapped") Assert.StartsWith("C:", payload);
                        var decoded = new DefaultMsSqlWireMessageReader().ReadMessage(mode == "mapped" ? payload[2..] : payload);
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
                clear.CommandText = $"DELETE FROM [{configuration.QueueStoreTable}]";
                await clear.ExecuteNonQueryAsync(timeout.Token);
            }
            if (mode == "mapped") Assert.Equal(2, mapped.Calls);
        }
        finally
        {
            using var cleanup = observer.CreateCommand();
            cleanup.CommandText = $"DROP TABLE IF EXISTS [{configuration.QueueStoreTable}]";
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
