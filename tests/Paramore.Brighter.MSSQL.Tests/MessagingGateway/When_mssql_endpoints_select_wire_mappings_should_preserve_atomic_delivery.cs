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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.MsSql;
using Paramore.Brighter.MSSQL.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.MSSQL.Tests.MessagingGateway;

/// <summary>Verifies endpoint mappings with SQL Server's atomic read-and-delete queue.</summary>
[Trait("Category", "MSSQL")]
public class MsSqlSelectedWireMappingTests
{
    /// <summary>Maps native payloads and retries while preserving row metadata and atomic removal.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_mssql_endpoints_select_wire_mappings_should_preserve_atomic_delivery(bool useAsync, bool compose)
    {
        //Arrange
        var helper = new MsSqlTestHelper(tableName: "mapping_" + Guid.NewGuid().ToString("N"));
        var configuration = helper.QueueConfiguration;
        var topic = new RoutingKey("mapped-" + Guid.NewGuid().ToString("N"));
        var writer = new MsSqlSelectedWireWriter(compose);
        var reader = new MsSqlSelectedWireReader(compose);
        var inactiveWriter = new MsSqlSelectedWireWriter(!compose);
        var inactiveReader = new MsSqlSelectedWireReader(!compose);
        var factory = new MsSqlMessageProducerFactory(configuration,
        [
            new MsSqlWirePublication { Topic = topic, WireMessageWriter = writer },
            new MsSqlWirePublication { Topic = new RoutingKey("inactive"), WireMessageWriter = inactiveWriter }
        ]);
        using var registry = new ProducerRegistry(useAsync ? await factory.CreateAsync() : factory.Create());
        var producer = (MsSqlMessageProducer)registry.LookupBy(topic);
        var subscription = new MsSqlSubscription(new SubscriptionName("mapped"), new ChannelName(topic.Value), topic,
            typeof(Message), messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
        {
            WireMessageReader = reader, RequeueWireMessageWriter = writer
        };
        var consumers = new MsSqlMessageConsumerFactory(configuration);
        await using var consumer = useAsync ? (MsSqlMessageConsumer)consumers.CreateAsync(subscription) : (MsSqlMessageConsumer)consumers.Create(subscription);
        subscription.WireMessageReader = inactiveReader;
        subscription.RequeueWireMessageWriter = inactiveWriter;
        using var telemetry = Sdk.CreateTracerProviderBuilder().Build();
        using var span = new Activity("mssql-mapping").SetIdFormat(ActivityIdFormat.W3C).Start();
        span.TraceStateString = "tenant=mapped";
        producer.Span = span;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var observer = new SqlConnection(configuration.ConnectionString);
        await observer.OpenAsync(timeout.Token);
        try
        {
            for (var iteration = 0; iteration < 2; iteration++)
            {
                var message = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT, handledCount: 7),
                    new MessageBody("mapped-body-π-" + iteration));
                message.Header.Bag["marker"] = "selected-λ";
                message.Header.Baggage.Add("tenant", "mapped");
                var bagBefore = JsonSerializer.Serialize(message.Header.Bag, JsonSerialisationOptions.Options);
                var beforeCalls = writer.Calls;

                //Act
                if (useAsync) await producer.SendAsync(message, timeout.Token);
                else producer.Send(message);

                //Assert
                Assert.Equal(beforeCalls + 1, writer.Calls);
                Assert.Equal(0, inactiveWriter.Calls);
                Assert.Equal("mapped-body-π-" + iteration, message.Body.Value);
                Assert.Equal(bagBefore, JsonSerializer.Serialize(message.Header.Bag, JsonSerialisationOptions.Options));
                long rowId;
                using (var inspect = observer.CreateCommand())
                {
                    inspect.CommandText = $"SELECT Id, Topic, MessageType, Payload FROM [{configuration.QueueStoreTable}]";
                    using var rows = await inspect.ExecuteReaderAsync(timeout.Token);
                    Assert.True(await rows.ReadAsync(timeout.Token));
                    rowId = rows.GetInt64(0);
                    Assert.Equal(topic.Value, rows.GetString(1));
                    Assert.Equal(typeof(Message).FullName, rows.GetString(2));
                    Assert.StartsWith(compose ? "C:" : "W:", rows.GetString(3));
                    Assert.False(await rows.ReadAsync(timeout.Token));
                }

                //Act
                var delivered = Assert.Single(useAsync ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(5), timeout.Token) : consumer.Receive());

                //Assert
                Assert.Equal(message.Id, delivered.Id);
                Assert.Equal(topic, delivered.Header.Topic);
                Assert.Equal(message.Body.Value, delivered.Body.Value);
                Assert.Equal(span.Id, delivered.Header.TraceParent.Value);
                Assert.Equal(span.TraceStateString, delivered.Header.TraceState.Value);
                Assert.Equal(message.Header.Baggage.ToString(), delivered.Header.Baggage.ToString());
                Assert.Equal(7, delivered.Header.HandledCount);
                Assert.Equal("selected-λ", delivered.Header.Bag["marker"].ToString());
                Assert.Equal(0, inactiveReader.Calls);
                using var count = observer.CreateCommand();
                count.CommandText = $"SELECT COUNT_BIG(*) FROM [{configuration.QueueStoreTable}]";
                Assert.Equal(0L, await count.ExecuteScalarAsync(timeout.Token));
                delivered.Body = new MessageBody("retry-π-" + iteration);

                //Act
                Assert.True(useAsync ? await consumer.RequeueAsync(delivered, TimeSpan.Zero, timeout.Token) : consumer.Requeue(delivered));

                //Assert
                Assert.Equal(beforeCalls + 2, writer.Calls);
                using (var inspectRetry = observer.CreateCommand())
                {
                    inspectRetry.CommandText = $"SELECT Id, Payload FROM [{configuration.QueueStoreTable}]";
                    using var rows = await inspectRetry.ExecuteReaderAsync(timeout.Token);
                    Assert.True(await rows.ReadAsync(timeout.Token));
                    Assert.True(rows.GetInt64(0) > rowId);
                    Assert.StartsWith(compose ? "C:" : "W:", rows.GetString(1));
                    Assert.False(await rows.ReadAsync(timeout.Token));
                }
                var retry = Assert.Single(useAsync ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(5), timeout.Token) : consumer.Receive());
                Assert.Equal(message.Id, retry.Id);
                Assert.Equal(delivered.Body.Value, retry.Body.Value);
                Assert.Equal(0, inactiveWriter.Calls);
                Assert.Equal(0L, await count.ExecuteScalarAsync(timeout.Token));

                //Act
                if (useAsync) await consumer.AcknowledgeAsync(retry, timeout.Token);
                else consumer.Acknowledge(retry);

                //Assert
                Assert.Equal(0L, await count.ExecuteScalarAsync(timeout.Token));
            }
            Assert.Equal(4, reader.Calls);
        }
        finally
        {
            using var cleanup = observer.CreateCommand();
            cleanup.CommandText = $"DROP TABLE IF EXISTS [{configuration.QueueStoreTable}]";
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
