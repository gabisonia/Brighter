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
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Paramore.Brighter.AWS.Tests.Helpers;
using Paramore.Brighter.AWS.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Xunit;

namespace Paramore.Brighter.AWS.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsOverlappingWireWriterTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_sends_overlap_should_preserve_each_messages_wire_attributes(bool useAsync)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Wire-Writer-Overlap-{Guid.NewGuid():N}");
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        IAmAMessageProducerAsync? producer = null;
        OverlappingSqsWireMessageWriter? writer = null;
        var sends = new List<Task>();

        try
        {
            var queue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new() { ["VisibilityTimeout"] = "30" }
            }, timeout.Token);
            writer = new OverlappingSqsWireMessageWriter(
                new TenantHeaderSqsWireMessageWriter(new DefaultSqsWireMessageWriter(queue.QueueUrl)));
            var publication = new SqsPublication(
                new ChannelName(queue.QueueUrl),
                findQueueBy: QueueFindBy.Url,
                makeChannels: OnMissingChannel.Validate)
            {
                Topic = new RoutingKey(queueName),
                WireMessageWriter = writer
            };
            var factory = new SqsMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            producer = Assert.IsAssignableFrom<IAmAMessageProducerAsync>(Assert.Single(producers).Value);
            var first = new Message(
                new MessageHeader(new Id("overlap-first"), publication.Topic, MessageType.MT_EVENT,
                    handledCount: 1, subject: "First subject"),
                new MessageBody("first payload"));
            first.Header.Bag["tenant_id"] = "tenant-one";
            var second = new Message(
                new MessageHeader(new Id("overlap-second"), publication.Topic, MessageType.MT_EVENT,
                    handledCount: 4, subject: "Second subject"),
                new MessageBody("second payload"));
            second.Header.Bag["tenant_id"] = "tenant-two";
            var messages = new[] { first, second };

            //Act
            sends.Add(SendOnDedicatedThread(producer, first, useAsync, timeout.Token));
            var firstReady = await Task.WhenAny(writer.FirstMapped, sends[0]).WaitAsync(timeout.Token);
            if (firstReady == sends[0])
                await sends[0];
            await writer.FirstMapped.WaitAsync(timeout.Token);
            Assert.False(sends[0].IsCompleted);
            sends.Add(SendOnDedicatedThread(producer, second, useAsync, timeout.Token));
            await Task.WhenAll(sends).WaitAsync(timeout.Token);

            var received = new List<Amazon.SQS.Model.Message>();
            var elapsed = Stopwatch.StartNew();
            while (received.Count < 2 && elapsed.Elapsed < TimeSpan.FromSeconds(10))
            {
                var response = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = queue.QueueUrl,
                    MessageAttributeNames = ["All"],
                    WaitTimeSeconds = 1,
                    MaxNumberOfMessages = 2
                }, timeout.Token);
                received.AddRange(response.Messages ?? []);
            }

            //Assert
            Assert.Equal(2, received.Count);
            Assert.Equal(2, received.Select(message => message.MessageAttributes["id"].StringValue).Distinct().Count());
            Assert.Equal("overlap-first", first.Id.Value);
            Assert.Equal("overlap-second", second.Id.Value);
            Assert.Equal("first payload", first.Body.Value);
            Assert.Equal("second payload", second.Body.Value);
            Assert.Equal("First subject", first.Header.Subject);
            Assert.Equal("Second subject", second.Header.Subject);
            Assert.Equal(1, first.Header.HandledCount);
            Assert.Equal(4, second.Header.HandledCount);
            Assert.Equal("tenant-one", first.Header.Bag["tenant_id"]);
            Assert.Equal("tenant-two", second.Header.Bag["tenant_id"]);
            foreach (var message in messages)
            {
                var native = Assert.Single(received,
                    item => item.MessageAttributes["id"].StringValue == message.Id.Value);
                Assert.Equal(message.Body.Value, native.Body);
                Assert.Equal(message.Header.Subject, native.MessageAttributes["subject"].StringValue);
                Assert.Equal(queue.QueueUrl, native.MessageAttributes["topic"].StringValue);
                var tenant = Assert.IsType<string>(message.Header.Bag["tenant_id"]);
                Assert.Equal(tenant, native.MessageAttributes["tenant-id"].StringValue);
                using var cloudEvents = JsonDocument.Parse(native.MessageAttributes["cloudeventheaders"].StringValue);
                Assert.Equal(message.Id.Value, cloudEvents.RootElement.GetProperty("id").GetString());
                using var bag = JsonDocument.Parse(native.MessageAttributes["bag"].StringValue);
                Assert.Equal(tenant, bag.RootElement.GetProperty("tenant_id").GetString());
                Assert.Equal(message.Header.HandledCount.ToString(), bag.RootElement.GetProperty("handled-count").GetString());
                var sourceAttribute = Assert.Single(message.Header.Bag);
                Assert.Equal("tenant_id", sourceAttribute.Key);
                Assert.Equal(tenant, sourceAttribute.Value);
            }
        }
        finally
        {
            writer?.ReleaseFirstMapping();
            try
            {
                await Task.WhenAll(sends);
            }
            finally
            {
                writer?.Dispose();
                try
                {
                    if (producer != null)
                        await producer.DisposeAsync();
                }
                finally
                {
                    await reaper.ReapAsync();
                }
            }
        }
    }

    private static Task SendOnDedicatedThread(
        IAmAMessageProducerAsync producer, Message message, bool useAsync, CancellationToken cancellationToken)
    {
        return Task.Factory.StartNew(async () =>
        {
            if (useAsync)
                await producer.SendAsync(message, cancellationToken);
            else
                Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(message);
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
    }
}
