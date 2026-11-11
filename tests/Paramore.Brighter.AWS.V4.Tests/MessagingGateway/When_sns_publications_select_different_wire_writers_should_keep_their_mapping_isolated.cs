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
public class SnsWireWriterPublicationIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_sns_publications_select_different_wire_writers_should_keep_their_mapping_isolated(
        bool useAsync, bool customFirst)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var clients = new AWSClientFactory(connection);
        using var sqs = clients.CreateSqsClient();
        using var sns = clients.CreateSnsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var producers = new Dictionary<ProducerKey, IAmAMessageProducer>();
        var endpoints = new List<(SnsPublication Publication, string QueueUrl, Message Message, bool Custom)>();

        try
        {
            foreach (var custom in customFirst ? new[] { true, false } : new[] { false, true })
            {
                var label = custom ? "custom" : "default";
                var topicName = reaper.TrackTopic($"Wire-Sns-{label}-{Guid.NewGuid():N}");
                var queueName = reaper.TrackQueue($"Wire-Sns-{label}-{Guid.NewGuid():N}");
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
                var publication = new SnsPublication(TopicFindBy.Arn, topic.TopicArn)
                {
                    Topic = new RoutingKey(topicName),
                    WireMessageWriter = custom
                        ? new TenantHeaderSnsWireMessageWriter(new DefaultSnsWireMessageWriter(topic.TopicArn))
                        : null
                };
                var message = new Message(
                    new MessageHeader(new Id($"{label}-application"), publication.Topic, MessageType.MT_EVENT,
                        handledCount: custom ? 4 : 1, subject: $"{label} subject"),
                    new MessageBody($"{label} payload"));
                message.Header.Bag["tenant_id"] = $"{label}-tenant";
                endpoints.Add((publication, queue.QueueUrl, message, custom));
            }
            var factory = new SnsMessageProducerFactory(connection,
                [endpoints[0].Publication, endpoints[1].Publication]);
            producers = useAsync ? await factory.CreateAsync() : factory.Create();

            //Act
            foreach (var endpoint in endpoints)
            {
                var producer = producers[new ProducerKey(endpoint.Publication.Topic, endpoint.Publication.Type)];
                if (useAsync)
                    await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer)
                        .SendAsync(endpoint.Message, timeout.Token);
                else
                    Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(endpoint.Message);
            }

            //Assert
            Assert.Equal(2, producers.Count);
            foreach (var endpoint in endpoints)
            {
                var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = endpoint.QueueUrl,
                    WaitTimeSeconds = 10,
                    MaxNumberOfMessages = 10
                }, timeout.Token);
                var native = Assert.Single(response.Messages ?? []);
                using var envelope = JsonDocument.Parse(native.Body);
                var root = envelope.RootElement;
                Assert.Equal(endpoint.Publication.TopicArn, root.GetProperty("TopicArn").GetString());
                var attributes = root.GetProperty("MessageAttributes");
                Assert.Equal(endpoint.Publication.TopicArn, attributes.GetProperty("topic").GetProperty("Value").GetString());
                var label = endpoint.Custom ? "custom" : "default";
                Assert.Equal($"{label}-application", attributes.GetProperty("id").GetProperty("Value").GetString());
                Assert.Equal($"{label} payload", root.GetProperty("Message").GetString());
                Assert.Equal($"{label} subject", root.GetProperty("Subject").GetString());
                if (endpoint.Custom)
                {
                    Assert.Equal("String", attributes.GetProperty("tenant-id").GetProperty("Type").GetString());
                    Assert.Equal("custom-tenant", attributes.GetProperty("tenant-id").GetProperty("Value").GetString());
                    Assert.IsType<TenantHeaderSnsWireMessageWriter>(endpoint.Publication.WireMessageWriter);
                }
                else
                {
                    Assert.False(attributes.TryGetProperty("tenant-id", out _));
                    Assert.Null(endpoint.Publication.WireMessageWriter);
                }
                using var cloudEvents = JsonDocument.Parse(
                    attributes.GetProperty("cloudeventheaders").GetProperty("Value").GetString()!);
                Assert.Equal($"{label} subject", cloudEvents.RootElement.GetProperty("subject").GetString());
                using var bag = JsonDocument.Parse(attributes.GetProperty("bag").GetProperty("Value").GetString()!);
                Assert.Equal($"{label}-tenant", bag.RootElement.GetProperty("tenant_id").GetString());
                Assert.Equal(endpoint.Custom ? "4" : "1", bag.RootElement.GetProperty("handled-count").GetString());
                Assert.Equal($"{label}-application", endpoint.Message.Id.Value);
                Assert.Equal($"{label} payload", endpoint.Message.Body.Value);
                Assert.Equal($"{label} subject", endpoint.Message.Header.Subject);
                Assert.Equal(endpoint.Custom ? 4 : 1, endpoint.Message.Header.HandledCount);
                var sourceAttribute = Assert.Single(endpoint.Message.Header.Bag);
                Assert.Equal("tenant_id", sourceAttribute.Key);
                Assert.Equal($"{label}-tenant", sourceAttribute.Value);
            }
        }
        finally
        {
            try
            {
                foreach (var producer in producers.Values)
                    await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).DisposeAsync();
            }
            finally
            {
                await reaper.ReapAsync();
            }
        }
    }
}
