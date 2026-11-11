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

using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Paramore.Brighter.AWS.V4.Tests.Helpers;
using Paramore.Brighter.AWS.V4.Tests.TestDoubles;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;
using Xunit;

namespace Paramore.Brighter.AWS.V4.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsCustomWireWriterTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_sending_with_a_custom_wire_writer_should_use_its_native_attributes(bool useAsync)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Wire-Writer-{Guid.NewGuid():N}");
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        IAmAMessageProducerAsync? producer = null;

        try
        {
            var queue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName
            }, timeout.Token);

            var publication = new SqsPublication(
                new ChannelName(queue.QueueUrl),
                findQueueBy: QueueFindBy.Url,
                makeChannels: OnMissingChannel.Validate)
            {
                Topic = new RoutingKey(queueName),
                WireMessageWriter = new LegacyTenantSqsWireMessageWriter()
            };
            var factory = new SqsMessageProducerFactory(connection, [publication]);
            var producers = useAsync ? await factory.CreateAsync() : factory.Create();
            producer = Assert.IsAssignableFrom<IAmAMessageProducerAsync>(Assert.Single(producers).Value);
            var message = new Message(
                new MessageHeader(Id.Random(), publication.Topic, MessageType.MT_EVENT),
                new MessageBody("{\"orderId\":\"order-17\"}"));
            message.Header.Bag["tenant_id"] = "tenant-42";

            //Act
            if (useAsync)
            {
                await producer.SendAsync(message, timeout.Token);
            }
            else
            {
                Assert.IsAssignableFrom<IAmAMessageProducerSync>(producer).Send(message);
            }

            var response = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                MessageAttributeNames = ["All"],
                WaitTimeSeconds = 10,
                MaxNumberOfMessages = 1
            }, timeout.Token);

            //Assert
            Assert.NotNull(response.Messages);
            var received = Assert.Single(response.Messages);
            Assert.Equal(message.Body.Value, received.Body);
            Assert.NotNull(received.MessageAttributes);
            var attribute = Assert.Single(received.MessageAttributes);
            Assert.Equal("tenant-id", attribute.Key);
            Assert.Equal("String", attribute.Value.DataType);
            Assert.Equal("tenant-42", attribute.Value.StringValue);
            var originalAttribute = Assert.Single(message.Header.Bag);
            Assert.Equal("tenant_id", originalAttribute.Key);
            Assert.Equal("tenant-42", originalAttribute.Value);
        }
        finally
        {
            try
            {
                if (producer != null)
                {
                    await producer.DisposeAsync();
                }
            }
            finally
            {
                await reaper.ReapAsync();
            }
        }
    }
}
