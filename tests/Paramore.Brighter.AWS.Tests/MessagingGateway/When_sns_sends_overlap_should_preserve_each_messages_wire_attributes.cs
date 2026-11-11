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
public class SnsOverlappingWireWriterTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_sns_sends_overlap_should_preserve_each_messages_wire_attributes(bool useAsync)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var topicName = reaper.TrackTopic($"Wire-Sns-Overlap-{Guid.NewGuid():N}");
        var queueName = reaper.TrackQueue($"Wire-Sns-Overlap-{Guid.NewGuid():N}");
        var clients = new AWSClientFactory(connection);
        using var sqs = clients.CreateSqsClient();
        using var sns = clients.CreateSnsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        IAmAMessageProducerAsync? producer = null;
        OverlappingSnsWireMessageWriter? writer = null;
        var sends = new List<Task>();

        try
        {
            var topic = await sns.CreateTopicAsync(topicName, timeout.Token);
            var queue = await sqs.CreateQueueAsync(queueName, timeout.Token);
            var queueAttributes = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = queue.QueueUrl,
                AttributeNames = ["QueueArn"]
            }, timeout.Token);
            var queueArn = queueAttributes.Attributes["QueueArn"];
            var policy = JsonSerializer.Serialize(new
            {
                Version = "2012-10-17",
                Statement = new[]
                {
                    new
                    {
                        Effect = "Allow",
                        Principal = new { Service = "sns.amazonaws.com" },
                        Action = "sqs:SendMessage",
                        Resource = queueArn,
                        Condition = new
                        {
                            ArnEquals = new Dictionary<string, string> { ["aws:SourceArn"] = topic.TopicArn }
                        }
                    }
                }
            });
            await sqs.SetQueueAttributesAsync(new SetQueueAttributesRequest
            {
                QueueUrl = queue.QueueUrl,
                Attributes = new() { ["Policy"] = policy }
            }, timeout.Token);
            await sns.SubscribeAsync(new Amazon.SimpleNotificationService.Model.SubscribeRequest
            {
                TopicArn = topic.TopicArn,
                Protocol = "sqs",
                Endpoint = queueArn,
                Attributes = new() { ["RawMessageDelivery"] = "false" }
            }, timeout.Token);

            writer = new OverlappingSnsWireMessageWriter(
                new TenantHeaderSnsWireMessageWriter(new DefaultSnsWireMessageWriter(topic.TopicArn)));
            var publication = new SnsPublication(TopicFindBy.Arn, topic.TopicArn)
            {
                Topic = new RoutingKey(topicName),
                WireMessageWriter = writer
            };
            var factory = new SnsMessageProducerFactory(connection, [publication]);
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
                var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = queue.QueueUrl,
                    WaitTimeSeconds = 1,
                    MaxNumberOfMessages = 2
                }, timeout.Token);
                received.AddRange(response.Messages ?? []);
            }

            //Assert
            Assert.Equal(2, received.Count);
            Assert.Equal(2, received.Select(ReadApplicationId).Distinct().Count());
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
                var native = Assert.Single(received, item => ReadApplicationId(item) == message.Id.Value);
                using var envelope = JsonDocument.Parse(native.Body);
                var root = envelope.RootElement;
                Assert.Equal(message.Body.Value, root.GetProperty("Message").GetString());
                Assert.Equal(message.Header.Subject, root.GetProperty("Subject").GetString());
                Assert.Equal(topic.TopicArn, root.GetProperty("TopicArn").GetString());
                var attributes = root.GetProperty("MessageAttributes");
                Assert.Equal(topic.TopicArn, attributes.GetProperty("topic").GetProperty("Value").GetString());
                var tenant = Assert.IsType<string>(message.Header.Bag["tenant_id"]);
                Assert.Equal("String", attributes.GetProperty("tenant-id").GetProperty("Type").GetString());
                Assert.Equal(tenant, attributes.GetProperty("tenant-id").GetProperty("Value").GetString());
                using var cloudEvents = JsonDocument.Parse(
                    attributes.GetProperty("cloudeventheaders").GetProperty("Value").GetString()!);
                Assert.Equal(message.Header.Type.Value, cloudEvents.RootElement.GetProperty("type").GetString());
                Assert.Equal(message.Header.SpecVersion, cloudEvents.RootElement.GetProperty("specversion").GetString());
                Assert.Equal(message.Header.Subject, cloudEvents.RootElement.GetProperty("subject").GetString());
                using var bag = JsonDocument.Parse(attributes.GetProperty("bag").GetProperty("Value").GetString()!);
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

    private static string ReadApplicationId(Amazon.SQS.Model.Message message)
    {
        using var envelope = JsonDocument.Parse(message.Body);
        return envelope.RootElement.GetProperty("MessageAttributes").GetProperty("id").GetProperty("Value").GetString()!;
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
