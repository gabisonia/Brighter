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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Paramore.Brighter.AWS.V4.Tests.Helpers;
using Paramore.Brighter.AWS.V4.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;
using Xunit;

namespace Paramore.Brighter.AWS.V4.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SnsFifoWireWriterTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_publishing_with_a_custom_sns_wire_writer_should_preserve_fifo_controls(bool useAsync, bool rawDelivery)
    {
        //Arrange
        const string BODY = "{\"orderId\":\"order-17\"}";
        const string GROUP = "orders-tenant-42";
        const string DEDUPLICATION_ID = "order-17-publication";
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var topicName = reaper.TrackTopic($"Wire-Sns-Fifo-{Guid.NewGuid():N}.fifo");
        var queueName = reaper.TrackQueue($"Wire-Sns-Fifo-{Guid.NewGuid():N}.fifo");
        var clients = new AWSClientFactory(connection);
        using var sqs = clients.CreateSqsClient();
        using var sns = clients.CreateSnsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        IAmAMessageProducerAsync? producer = null;

        try
        {
            var topic = await sns.CreateTopicAsync(new Amazon.SimpleNotificationService.Model.CreateTopicRequest
            {
                Name = topicName,
                Attributes = new() { ["FifoTopic"] = "true", ["ContentBasedDeduplication"] = "false" }
            }, timeout.Token);
            var queue = await sqs.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new() { ["FifoQueue"] = "true", ["ContentBasedDeduplication"] = "false" }
            }, timeout.Token);
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
                Attributes = new() { ["RawMessageDelivery"] = rawDelivery ? "true" : "false" }
            }, timeout.Token);

            var publication = new SnsPublication(TopicFindBy.Arn, topic.TopicArn)
            {
                Topic = new RoutingKey(topicName),
                WireMessageWriter = new LegacyTenantSnsWireMessageWriter()
            };
            var factory = new SnsMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            producer = Assert.IsAssignableFrom<IAmAMessageProducerAsync>(Assert.Single(producers).Value);
            var message = new Message(
                new MessageHeader(Id.Random(), publication.Topic, MessageType.MT_EVENT, subject: "Original subject", partitionKey: GROUP),
                new MessageBody(BODY));
            message.Header.Bag["tenant_id"] = "tenant-42";
            message.Header.Bag["messageDeduplicationId"] = DEDUPLICATION_ID;

            //Act
            if (useAsync)
                await producer.SendAsync(message, timeout.Token);
            else
                Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(message);

            var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                MessageAttributeNames = ["All"],
                MessageSystemAttributeNames = ["All"],
                WaitTimeSeconds = 10,
                MaxNumberOfMessages = 1
            }, timeout.Token);

            //Assert
            var received = Assert.Single(response.Messages ?? []);
            Assert.Equal(GROUP, received.Attributes["MessageGroupId"]);
            Assert.Equal(DEDUPLICATION_ID, received.Attributes["MessageDeduplicationId"]);
            if (rawDelivery)
            {
                Assert.Equal($"legacy:{BODY}", received.Body);
                var attribute = Assert.Single(received.MessageAttributes);
                Assert.Equal("tenant-id", attribute.Key);
                Assert.Equal("String", attribute.Value.DataType);
                Assert.Equal("tenant-42", attribute.Value.StringValue);
            }
            else
            {
                using var envelope = JsonDocument.Parse(received.Body);
                Assert.Equal($"legacy:{BODY}", envelope.RootElement.GetProperty("Message").GetString());
                Assert.Equal("Legacy order", envelope.RootElement.GetProperty("Subject").GetString());
                Assert.Equal(topic.TopicArn, envelope.RootElement.GetProperty("TopicArn").GetString());
                var attribute = Assert.Single(envelope.RootElement.GetProperty("MessageAttributes").EnumerateObject());
                Assert.Equal("tenant-id", attribute.Name);
                Assert.Equal("String", attribute.Value.GetProperty("Type").GetString());
                Assert.Equal("tenant-42", attribute.Value.GetProperty("Value").GetString());
            }
            Assert.Equal(BODY, message.Body.Value);
            Assert.Equal("Original subject", message.Header.Subject);
            Assert.Equal(new PartitionKey(GROUP), message.Header.PartitionKey);
            Assert.Equal(2, message.Header.Bag.Count);
            Assert.Equal("tenant-42", message.Header.Bag["tenant_id"]);
            Assert.Equal(DEDUPLICATION_ID, message.Header.Bag["messageDeduplicationId"]);
        }
        finally
        {
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
