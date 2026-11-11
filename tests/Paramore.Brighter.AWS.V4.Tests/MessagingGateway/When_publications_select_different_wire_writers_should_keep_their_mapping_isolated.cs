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
public class SqsWireWriterPublicationIsolationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_publications_select_different_wire_writers_should_keep_their_mapping_isolated(
        bool useAsync, bool customFirst)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var defaultQueueName = reaper.TrackQueue($"Wire-Default-Isolation-{Guid.NewGuid():N}");
        var customQueueName = reaper.TrackQueue($"Wire-Custom-Isolation-{Guid.NewGuid():N}");
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var producers = new Dictionary<ProducerKey, IAmAMessageProducer>();

        try
        {
            var defaultQueue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = defaultQueueName
            }, timeout.Token);
            var customQueue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = customQueueName
            }, timeout.Token);
            var defaultPublication = new SqsPublication(
                new ChannelName(defaultQueue.QueueUrl),
                findQueueBy: QueueFindBy.Url,
                makeChannels: OnMissingChannel.Validate)
            {
                Topic = new RoutingKey(defaultQueueName)
            };
            var customPublication = new SqsPublication(
                new ChannelName(customQueue.QueueUrl),
                findQueueBy: QueueFindBy.Url,
                makeChannels: OnMissingChannel.Validate)
            {
                Topic = new RoutingKey(customQueueName),
                WireMessageWriter = new TenantHeaderSqsWireMessageWriter(
                    new DefaultSqsWireMessageWriter(customQueue.QueueUrl))
            };
            var defaultMessage = new Message(
                new MessageHeader(new Id("default-application"), defaultPublication.Topic, MessageType.MT_EVENT,
                    subject: "Default subject"),
                new MessageBody("default payload"));
            defaultMessage.Header.Bag["tenant_id"] = "default-tenant";
            var customMessage = new Message(
                new MessageHeader(new Id("custom-application"), customPublication.Topic, MessageType.MT_EVENT,
                    subject: "Custom subject"),
                new MessageBody("custom payload"));
            customMessage.Header.Bag["tenant_id"] = "custom-tenant";
            SqsPublication[] publications = customFirst
                ? [customPublication, defaultPublication]
                : [defaultPublication, customPublication];
            var factory = new SqsMessageProducerFactory(connection, publications);
            producers = useAsync ? await factory.CreateAsync() : factory.Create();

            //Act
            foreach (var publication in publications)
            {
                var message = publication == customPublication ? customMessage : defaultMessage;
                var producer = producers[new ProducerKey(publication.Topic, publication.Type)];
                if (useAsync)
                    await Assert.IsAssignableFrom<IAmAMessageProducerAsync>(producer).SendAsync(message, timeout.Token);
                else
                    Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(message);
            }

            //Assert
            Assert.Equal(2, producers.Count);
            foreach (var publication in publications)
            {
                var custom = publication == customPublication;
                var queueUrl = custom ? customQueue.QueueUrl : defaultQueue.QueueUrl;
                var message = custom ? customMessage : defaultMessage;
                var tenant = custom ? "custom-tenant" : "default-tenant";
                var response = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = queueUrl,
                    MessageAttributeNames = ["All"],
                    WaitTimeSeconds = 10,
                    MaxNumberOfMessages = 10
                }, timeout.Token);
                Assert.NotNull(response.Messages);
                var native = Assert.Single(response.Messages);
                Assert.Equal(queueUrl, native.MessageAttributes["topic"].StringValue);
                Assert.Equal(message.Id.Value, native.MessageAttributes["id"].StringValue);
                Assert.Equal(message.Body.Value, native.Body);
                Assert.Equal(message.Header.Subject, native.MessageAttributes["subject"].StringValue);
                if (custom)
                    Assert.Equal(tenant, native.MessageAttributes["tenant-id"].StringValue);
                else
                    Assert.False(native.MessageAttributes.ContainsKey("tenant-id"));
                using var cloudEvents = JsonDocument.Parse(native.MessageAttributes["cloudeventheaders"].StringValue);
                Assert.Equal(message.Id.Value, cloudEvents.RootElement.GetProperty("id").GetString());
                using var bag = JsonDocument.Parse(native.MessageAttributes["bag"].StringValue);
                Assert.Equal(tenant, bag.RootElement.GetProperty("tenant_id").GetString());
                var sourceAttribute = Assert.Single(message.Header.Bag);
                Assert.Equal("tenant_id", sourceAttribute.Key);
                Assert.Equal(tenant, sourceAttribute.Value);
            }
            Assert.Null(defaultPublication.WireMessageWriter);
            Assert.IsType<TenantHeaderSqsWireMessageWriter>(customPublication.WireMessageWriter);
            Assert.Equal("default-application", defaultMessage.Id.Value);
            Assert.Equal("custom-application", customMessage.Id.Value);
            Assert.Equal("default payload", defaultMessage.Body.Value);
            Assert.Equal("custom payload", customMessage.Body.Value);
            Assert.Equal("Default subject", defaultMessage.Header.Subject);
            Assert.Equal("Custom subject", customMessage.Header.Subject);
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
