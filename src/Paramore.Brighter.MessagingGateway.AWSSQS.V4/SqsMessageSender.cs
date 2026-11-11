using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Paramore.Brighter.Logging;

namespace Paramore.Brighter.MessagingGateway.AWSSQS.V4;

/// <summary>
/// Class responsible for sending a message to a SQS
/// </summary>
public partial class SqsMessageSender
{
    private static readonly ILogger s_logger = ApplicationLogging.CreateLogger<SqsMessageSender>();
    private static readonly TimeSpan s_maxDelay = TimeSpan.FromSeconds(900);
    
    private readonly string _queueUrl;
    private readonly AmazonSQSClient _client;
    private readonly IAmAWireMessageWriter<SqsWireMessage> _wireMessageWriter;

    /// <summary>
    /// Initialize the <see cref="SqsMessageSender"/>
    /// </summary>
    /// <param name="queueUrl">The destination queue URL.</param>
    /// <param name="client">The SQS Client</param>
    public SqsMessageSender(string queueUrl, AmazonSQSClient client)
        : this(queueUrl, client, null)
    {
    }

    /// <summary>
    /// Initializes a sender with an optional custom wire message writer.
    /// </summary>
    /// <param name="queueUrl">The destination queue URL.</param>
    /// <param name="client">The SQS client.</param>
    /// <param name="wireMessageWriter">The writer, or null to use the default mapping.</param>
    public SqsMessageSender(string queueUrl, AmazonSQSClient client,
        IAmAWireMessageWriter<SqsWireMessage>? wireMessageWriter)
    {
        _queueUrl = queueUrl;
        _client = client;
        _wireMessageWriter = wireMessageWriter ?? new DefaultSqsWireMessageWriter(queueUrl);
    }
    
    /// <summary>
    /// Sending message via SQS
    /// </summary>
    /// <param name="message">The message.</param>
    /// <param name="delay">The delay in ms. 0 is no delay. Defaults to 0</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> that cancels the Publish operation</param>
    /// <returns>The message id.</returns>
    public async Task<string?> SendAsync(Message message, TimeSpan? delay, CancellationToken cancellationToken)
    {
        var request = CreateSendMessageRequest(message, delay);

        var response = await _client.SendMessageAsync(request, cancellationToken);
        if (response.HttpStatusCode is HttpStatusCode.OK or HttpStatusCode.Created
            or HttpStatusCode.Accepted)
        {
            return response.MessageId;
        }

        return null;
    }

    private SendMessageRequest CreateSendMessageRequest(Message message, TimeSpan? delay)
    {
        var wireMessage = _wireMessageWriter.WriteMessage(message);
        var request = new SendMessageRequest
        {
            QueueUrl = _queueUrl,
            MessageBody = wireMessage.Body,
            MessageAttributes = wireMessage.MessageAttributes
        };

        SetMessageDelay(request, delay);
        SetFifoQueueProperties(request, message);

        return request;
    }

    private static void SetMessageDelay(SendMessageRequest request, TimeSpan? delay)
    {
        delay ??= TimeSpan.Zero;
        if (delay > TimeSpan.Zero)
        {
            if (delay.Value > s_maxDelay)
            {
                delay = s_maxDelay;
                Log.DelaySetToMaximum(s_logger, delay);
            }

            request.DelaySeconds = (int)delay.Value.TotalSeconds;
        }
    }

    private static void SetFifoQueueProperties(SendMessageRequest request, Message message)
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

    private static partial class Log
    {
        [LoggerMessage(LogLevel.Warning, "Set delay from {CurrentDelay} to 15min (SQS support up to 15min)")]
        public static partial void DelaySetToMaximum(ILogger logger, TimeSpan? currentDelay);
    }
}
