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
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.Postgres;
using Paramore.Brighter.PostgresSQL.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.PostgresSQL.Tests.MessagingGateway;

/// <summary>Verifies JSON mapping through native PostgreSQL factories and delivery controls.</summary>
[Trait("Category", "PostgresSql")]
public class PostgresSelectedWireMappingTests
{
    /// <summary>Maps every send/read path while preserving physical row ownership.</summary>
    [Theory]
    [InlineData(false, false, false, false)]
    [InlineData(false, false, false, true)]
    [InlineData(false, false, true, false)]
    [InlineData(false, false, true, true)]
    [InlineData(false, true, false, false)]
    [InlineData(false, true, false, true)]
    [InlineData(false, true, true, false)]
    [InlineData(false, true, true, true)]
    [InlineData(true, false, false, false)]
    [InlineData(true, false, false, true)]
    [InlineData(true, false, true, false)]
    [InlineData(true, false, true, true)]
    [InlineData(true, true, false, false)]
    [InlineData(true, true, false, true)]
    [InlineData(true, true, true, false)]
    [InlineData(true, true, true, true)]
    public async Task When_postgres_endpoints_select_wire_mappings_should_preserve_native_delivery_controls(
        bool useAsync, bool binary, bool large, bool compose)
    {
        //Arrange
        var helper = new PostgresSqlTestHelper(binary);
        helper.SetupDatabase();
        var configuration = helper.Configuration;
        var topic = new RoutingKey("mapped-" + Guid.NewGuid().ToString("N"));
        var logicalTopic = new RoutingKey("logical-" + Guid.NewGuid().ToString("N"));
        var writer = new PostgresSelectedWireWriter(compose);
        var reader = new PostgresSelectedWireReader(compose);
        var inactiveWriter = new PostgresSelectedWireWriter(!compose);
        var inactiveReader = new PostgresSelectedWireReader(!compose);
        var connection = new PostgresMessagingGatewayConnection(configuration);
        var factory = new PostgresMessageProducerFactory(connection,
        [
            new PostgresPublication { Topic = topic, WireMessageWriter = writer },
            new PostgresPublication { Topic = new RoutingKey("inactive"), WireMessageWriter = inactiveWriter }
        ]);
        var producers = useAsync ? await factory.CreateAsync() : factory.Create();
        using var registry = new ProducerRegistry(producers);
        var producer = (PostgresMessageProducer)registry.LookupBy(topic);
        var subscription = new PostgresSubscription(new SubscriptionName("mapped"), new ChannelName(topic.Value), topic,
            typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor, visibleTimeout: TimeSpan.FromHours(1), tableWithLargeMessage: large)
        {
            WireMessageReader = reader, RequeueWireMessageWriter = writer
        };
        var consumers = new PostgresConsumerFactory(connection);
        await using var consumer = useAsync
            ? (PostgresMessageConsumer)consumers.CreateAsync(subscription)
            : (PostgresMessageConsumer)consumers.Create(subscription);
        subscription.WireMessageReader = inactiveReader;
        subscription.RequeueWireMessageWriter = inactiveWriter;
        using var telemetry = Sdk.CreateTracerProviderBuilder().Build();
        using var span = new Activity("postgres-mapping").SetIdFormat(ActivityIdFormat.W3C).Start();
        span.TraceStateString = "tenant=mapped";
        producer.Span = span;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var observer = new NpgsqlConnection(configuration.ConnectionString);
        await observer.OpenAsync(timeout.Token);
        try
        {
            foreach (var delayed in new[] { false, true })
            {
                var message = new Message(new MessageHeader(Id.Random(), logicalTopic, MessageType.MT_EVENT, handledCount: 7),
                    new MessageBody("mapped-body-π-" + Guid.NewGuid()));
                message.Header.Bag["marker"] = "selected-λ";
                message.Header.Baggage.Add("tenant", "mapped");
                var bodyBefore = message.Body.Value;
                var bagBefore = JsonSerializer.Serialize(message.Header.Bag, JsonSerialisationOptions.Options);
                var beforeCalls = writer.Calls;

                //Act
                if (delayed)
                {
                    if (useAsync) await producer.SendWithDelayAsync(message, TimeSpan.FromSeconds(1), timeout.Token);
                    else producer.SendWithDelay(message, TimeSpan.FromSeconds(1));
                }
                else
                {
                    if (useAsync) await producer.SendAsync(message, timeout.Token);
                    else producer.Send(message);
                }

                //Assert
                Assert.Equal(beforeCalls + 1, writer.Calls);
                Assert.Equal(0, inactiveWriter.Calls);
                Assert.Equal(bodyBefore, message.Body.Value);
                Assert.Equal(bagBefore, JsonSerializer.Serialize(message.Header.Bag, JsonSerialisationOptions.Options));
                long rowId;
                using (var inspect = observer.CreateCommand())
                {
                    inspect.CommandText = $"SELECT id, queue, content::text, visible_timeout > CURRENT_TIMESTAMP FROM \"{configuration.QueueStoreTable}\"";
                    using var rows = await inspect.ExecuteReaderAsync(timeout.Token);
                    Assert.True(await rows.ReadAsync(timeout.Token));
                    rowId = rows.GetInt64(0);
                    Assert.Equal(topic.Value, rows.GetString(1));
                    Assert.Contains(compose ? "composed" : "marker", rows.GetString(2));
                    Assert.Equal(delayed, rows.GetBoolean(3));
                    Assert.False(await rows.ReadAsync(timeout.Token));
                }
                if (delayed)
                {
                    var hidden = useAsync ? await consumer.ReceiveAsync(cancellationToken: timeout.Token) : consumer.Receive();
                    Assert.True(Assert.Single(hidden).IsEmpty);
                    await Task.Delay(TimeSpan.FromSeconds(1.1), timeout.Token);
                }
                var delivered = Assert.Single(useAsync ? await consumer.ReceiveAsync(cancellationToken: timeout.Token) : consumer.Receive());
                Assert.True(delivered.Header.Bag.TryGetValue("ReceiptHandle", out var receipt));
                Assert.Equal(rowId, Assert.IsType<long>(receipt));
                Assert.Equal(message.Id, delivered.Id);
                Assert.Equal(logicalTopic, delivered.Header.Topic);
                Assert.Equal(bodyBefore, delivered.Body.Value);
                Assert.Equal(span.Id, delivered.Header.TraceParent.Value);
                Assert.Equal(span.TraceStateString, delivered.Header.TraceState.Value);
                Assert.Equal(message.Header.Baggage.ToString(), delivered.Header.Baggage.ToString());
                Assert.Equal(7, delivered.Header.HandledCount);
                Assert.Equal("selected-λ", delivered.Header.Bag["marker"].ToString());
                Assert.Equal(0, inactiveReader.Calls);
                Assert.True(Assert.Single(useAsync ? await consumer.ReceiveAsync(cancellationToken: timeout.Token) : consumer.Receive()).IsEmpty);
                delivered.Body = new MessageBody("retry-π-" + message.Id.Value);

                //Act
                Assert.True(useAsync ? await consumer.RequeueAsync(delivered, TimeSpan.Zero, timeout.Token) : consumer.Requeue(delivered));

                //Assert
                Assert.Equal(beforeCalls + 2, writer.Calls);
                var retry = Assert.Single(useAsync ? await consumer.ReceiveAsync(cancellationToken: timeout.Token) : consumer.Receive());
                Assert.True(retry.Header.Bag.TryGetValue("ReceiptHandle", out var retryReceipt));
                Assert.Equal(rowId, Assert.IsType<long>(retryReceipt));
                Assert.Equal(message.Id, retry.Id);
                Assert.Equal(delivered.Body.Value, retry.Body.Value);
                Assert.Equal(0, inactiveWriter.Calls);

                //Act
                if (useAsync) await consumer.AcknowledgeAsync(retry, timeout.Token);
                else consumer.Acknowledge(retry);

                //Assert
                using var count = observer.CreateCommand();
                count.CommandText = $"SELECT COUNT(*) FROM \"{configuration.QueueStoreTable}\"";
                Assert.Equal(0L, await count.ExecuteScalarAsync(timeout.Token));
            }
            Assert.Equal(4, reader.Calls);
        }
        finally
        {
            using var cleanup = observer.CreateCommand();
            cleanup.CommandText = $"DROP TABLE IF EXISTS \"{configuration.QueueStoreTable}\"";
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
