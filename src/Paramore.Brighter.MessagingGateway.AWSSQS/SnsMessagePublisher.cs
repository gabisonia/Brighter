#region Licence

/* The MIT License (MIT)
Copyright © 2022 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;

namespace Paramore.Brighter.MessagingGateway.AWSSQS;

/// <summary>
/// Publishes messages with native SNS content and gateway-owned delivery controls.
/// </summary>
public class SnsMessagePublisher
{
    private readonly string _topicArn;
    private readonly AmazonSimpleNotificationServiceClient _client;
    private readonly IAmAWireMessageWriter<SnsWireMessage> _wireMessageWriter;

    /// <summary>
    /// Initializes a publisher using the default wire mapping.
    /// </summary>
    /// <param name="topicArn">The destination topic ARN.</param>
    /// <param name="client">The SNS client.</param>
    public SnsMessagePublisher(string topicArn, AmazonSimpleNotificationServiceClient client)
        : this(topicArn, client, null)
    {
    }

    /// <summary>
    /// Initializes a publisher with an optional wire message writer.
    /// </summary>
    /// <param name="topicArn">The destination topic ARN.</param>
    /// <param name="client">The SNS client.</param>
    /// <param name="wireMessageWriter">The writer, or null to use the default mapping.</param>
    public SnsMessagePublisher(string topicArn, AmazonSimpleNotificationServiceClient client,
        IAmAWireMessageWriter<SnsWireMessage>? wireMessageWriter)
    {
        _topicArn = topicArn;
        _client = client;
        _wireMessageWriter = wireMessageWriter ?? new DefaultSnsWireMessageWriter(topicArn);
    }

    /// <summary>
    /// Publishes the configured wire content with the message's FIFO controls.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <returns>The native message ID, or null if SNS did not accept the publication.</returns>
    public async Task<string?> PublishAsync(Message message)
    {
        var publishRequest = CreatePublishRequest(message);
        
        var response = await _client.PublishAsync(publishRequest);
        if (response.HttpStatusCode is System.Net.HttpStatusCode.OK or System.Net.HttpStatusCode.Created
            or System.Net.HttpStatusCode.Accepted)
        {
            return response.MessageId;
        }

        return null;
    }

    private PublishRequest CreatePublishRequest(Message message)
    {
        var wireMessage = _wireMessageWriter.WriteMessage(message);
        var publishRequest = new PublishRequest(_topicArn, wireMessage.Body, wireMessage.Subject)
        {
            MessageAttributes = wireMessage.MessageAttributes
        };
        
        ConfigureFifoSettings(message, publishRequest);

        return publishRequest;
    }

    private static void ConfigureFifoSettings(Message message, PublishRequest request)
    {
        if (PartitionKey.IsNullOrEmpty(message.Header.PartitionKey))
        {
            return;
        }
        
        request.MessageGroupId = message.Header.PartitionKey;
        if (message.Header.Bag.TryGetValue(HeaderNames.DeduplicationId, out var deduplicationId))
        {
            request.MessageDeduplicationId = (string)deduplicationId;
        }
    }

}
