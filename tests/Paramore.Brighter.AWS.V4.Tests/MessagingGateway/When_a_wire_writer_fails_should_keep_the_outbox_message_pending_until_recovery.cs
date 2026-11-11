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
using System.Transactions;
using Amazon.SQS.Model;
using Paramore.Brighter.AWS.V4.Tests.Helpers;
using Paramore.Brighter.AWS.V4.Tests.TestDoubles;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;
using Polly.Registry;
using Xunit;

namespace Paramore.Brighter.AWS.V4.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsWireWriterOutboxFailureTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_a_wire_writer_fails_should_keep_the_outbox_message_pending_until_recovery(bool useAsync)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var reaper = new AwsTestResourceReaper(connection);
        var queueName = reaper.TrackQueue($"Wire-Writer-Outbox-{Guid.NewGuid():N}");
        using var client = new AWSClientFactory(connection).CreateSqsClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        try
        {
            var queue = await client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = queueName
            }, timeout.Token);
            var writer = new SwitchableSqsWireMessageWriter(new DefaultSqsWireMessageWriter(queue.QueueUrl));
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
            using var registry = new ProducerRegistry(producers);
            var outbox = new InMemoryOutbox(TimeProvider.System);
            using var mediator = new OutboxProducerMediator<Message, CommittableTransaction>(
                registry,
                new ResiliencePipelineRegistry<string>().AddBrighterDefault(),
                new MessageMapperRegistry(new SimpleMessageMapperFactory(_ => null!), null),
                new EmptyMessageTransformerFactory(),
                new EmptyMessageTransformerFactoryAsync(),
                tracer: null,
                new FindPublicationByPublicationTopicOrRequestType(),
                outbox: outbox);
            var context = new RequestContext();
            var message = new Message(
                new MessageHeader(new Id("outbox-application"), publication.Topic, MessageType.MT_EVENT,
                    subject: "Outbox subject"),
                new MessageBody("outbox payload"));
            message.Header.Bag["tenant_id"] = "outbox-tenant";
            if (useAsync)
                await mediator.AddToOutboxAsync(message, context, cancellationToken: timeout.Token);
            else
                mediator.AddToOutbox(message, context);

            //Act
            if (useAsync)
                await mediator.ClearOutboxAsync([message.Id], context, cancellationToken: timeout.Token);
            else
                mediator.ClearOutbox([message.Id], context);

            //Assert
            var pending = Assert.Single(outbox.OutstandingMessages(TimeSpan.Zero, context));
            Assert.Equal("outbox-application", pending.Id.Value);
            Assert.Equal("outbox payload", pending.Body.Value);
            Assert.Equal("outbox-tenant", pending.Header.Bag["tenant_id"]);
            var beforeRecovery = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                WaitTimeSeconds = 1,
                MaxNumberOfMessages = 1
            }, timeout.Token);
            Assert.Empty(beforeRecovery.Messages ?? []);

            //Act
            writer.ThrowOnWrite = false;
            if (useAsync)
                await mediator.ClearOutboxAsync([message.Id], context, cancellationToken: timeout.Token);
            else
                mediator.ClearOutbox([message.Id], context);
            var afterRecovery = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queue.QueueUrl,
                WaitTimeSeconds = 10,
                MaxNumberOfMessages = 10,
                MessageAttributeNames = ["All"]
            }, timeout.Token);

            //Assert
            Assert.Empty(outbox.OutstandingMessages(TimeSpan.Zero, context));
            var native = Assert.Single(afterRecovery.Messages ?? []);
            Assert.Equal("outbox-application", native.MessageAttributes["id"].StringValue);
            Assert.Equal("outbox payload", native.Body);
            Assert.Equal("Outbox subject", native.MessageAttributes["subject"].StringValue);
            Assert.Equal(queue.QueueUrl, native.MessageAttributes["topic"].StringValue);
            Assert.Equal("outbox-application", message.Id.Value);
            Assert.Equal("outbox payload", message.Body.Value);
            Assert.Equal("Outbox subject", message.Header.Subject);
            var sourceAttribute = Assert.Single(message.Header.Bag);
            Assert.Equal("tenant_id", sourceAttribute.Key);
            Assert.Equal("outbox-tenant", sourceAttribute.Value);
        }
        finally
        {
            await reaper.ReapAsync();
        }
    }
}
