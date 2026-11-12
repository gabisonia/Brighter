using System.Diagnostics;
using Google.Cloud.PubSub.V1;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Tasks;

namespace Paramore.Brighter.MessagingGateway.GcpPubSub;

/// <summary>
/// A message producer for Google Cloud Pub/Sub, responsible for sending messages to a specific topic.
/// It implements synchronous, asynchronous, and bulk sending, and supports delayed messaging via a scheduler.
/// </summary>
/// <param name="client">The Google <see cref="PublisherClient"/> used to publish messages.</param>
/// <param name="publication">The publication this producer sends to.</param>
/// <param name="instrumentation">The instrumentation options for the producer's spans.</param>
/// <param name="enableMessageOrdering">
/// Whether <paramref name="client"/> was built with message ordering enabled. When <c>null</c>, the publication's
/// <see cref="GcpPublication.EnableMessageOrdering"/> is used. A message's partition key is sent as the Pub/Sub ordering
/// key only when ordering is enabled, as the client refuses an ordering key otherwise.
/// </param>
public class GcpMessageProducer(
    PublisherClient client,
    GcpPublication publication,
    InstrumentationOptions instrumentation = InstrumentationOptions.None,
    bool? enableMessageOrdering = null)
    : IAmAMessageProducerAsync, IAmAMessageProducerSync, IAmABulkMessageProducerAsync
{
    private readonly IAmAWireMessageWriter<PubsubMessage> _wireMessageWriter =
        publication.WireMessageWriter ?? new DefaultGcpPubSubWireMessageWriter();
    private readonly bool _enableMessageOrdering = enableMessageOrdering ?? publication.EnableMessageOrdering;

    /// <summary>
    /// Gets the publication configuration for this producer.
    /// </summary>
    public Publication Publication => publication;

    /// <summary>
    /// Gets or sets the OpenTelemetry/DiagnosticSource span associated with this producer operation.
    /// </summary>
    public Activity? Span { get; set; }

    /// <summary>
    /// Gets or sets the message scheduler used for delayed messages.
    /// </summary>
    public IAmAMessageScheduler? Scheduler { get; set; }

    /// <summary>
    /// Asynchronously sends a message without a delay.
    /// </summary>
    /// <remarks>
    /// Wire mapping failures propagate before publication. A null writer result raises
    /// <see cref="InvalidOperationException"/>. The same producer can be used after the writer recovers.
    /// Cancellation is checked before mapping and before native submission. Once submitted,
    /// publication is awaited to completion; cancellation cannot recall the message.
    /// </remarks>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    /// <exception cref="OperationCanceledException">The caller's token was cancelled before mapping or native submission.</exception>
    public Task SendAsync(Message message, CancellationToken cancellationToken = default)
    {
        return SendWithDelayAsync(message, null, cancellationToken);
    }

    /// <summary>
    /// Asynchronously sends a message with an optional delay.
    /// If a delay is specified, it schedules the message using the configured scheduler.
    /// </summary>
    /// <remarks>
    /// For immediate sends, wire mapping failures propagate before publication. A null writer result
    /// raises <see cref="InvalidOperationException"/>. Delayed mapping occurs when the scheduler dispatches.
    /// Immediate sends check cancellation before mapping and before native submission. Once submitted,
    /// publication is awaited to completion; cancellation cannot recall the message.
    /// </remarks>
    /// <param name="message">The message to send.</param>
    /// <param name="delay">The time span to delay the message sending. If null or zero, the message is sent immediately.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous send or schedule operation.</returns>
    /// <exception cref="OperationCanceledException">The caller's token was cancelled before immediate mapping or native submission.</exception>
    /// <exception cref="InvalidOperationException">Thrown if a delay is specified but the scheduler is not an <see cref="IAmAMessageSchedulerAsync"/>.</exception>
    public async Task SendWithDelayAsync(Message message, TimeSpan? delay, CancellationToken cancellationToken = default)
    {
        if (delay == null || delay == TimeSpan.Zero)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Write instrumentation event for the producer
            BrighterTracer.WriteProducerEvent(Span, MessagingSystem.PubSub, message, instrumentation);
            
            // Convert the Brighter message to a Google Pub/Sub message
            var pubSubMessage = CreateWireMessage(message);
            cancellationToken.ThrowIfCancellationRequested();

            // Publish the message to Google Cloud Pub/Sub
            await client.PublishAsync(pubSubMessage);
        }
        else if (Scheduler is IAmAMessageSchedulerAsync scheduler)
        {
            // Schedule the message for delayed delivery
            await scheduler.ScheduleAsync(message, delay.Value, cancellationToken);
        }
        else
        {
            throw new InvalidOperationException("Scheduler must be an IAmAMessageSchedulerAsync to support delayed messages.");
        }
    }

    /// <summary>
    /// Synchronously sends a message without a delay.
    /// </summary>
    /// <remarks>
    /// Wire mapping failures propagate before publication. A null writer result raises
    /// <see cref="InvalidOperationException"/>. The same producer can be used after the writer recovers.
    /// </remarks>
    /// <param name="message">The message to send.</param>
    public void Send(Message message)
    {
        SendWithDelay(message, null);
    }

    /// <summary>
    /// Synchronously sends a message with an optional delay.
    /// It wraps the asynchronous method using <see cref="BrighterAsyncContext.Run"/>.
    /// </summary>
    /// <remarks>
    /// For immediate sends, wire mapping failures propagate before publication. A null writer result
    /// raises <see cref="InvalidOperationException"/>. Delayed mapping occurs when the scheduler dispatches.
    /// </remarks>
    /// <param name="message">The message to send.</param>
    /// <param name="delay">The time span to delay the message sending. If null or zero, the message is sent immediately.</param>
    public void SendWithDelay(Message message, TimeSpan? delay)
    {
        // Execute the asynchronous send in a way that blocks the calling thread (synchronous wrapper)
        BrighterAsyncContext.Run(() => SendWithDelayAsync(message, delay));
    }

    /// <summary>
    /// Creates batches from a collection of messages. For Google Pub/Sub, a simple batching strategy
    /// where all messages are grouped into a single logical batch is used for simplicity in this implementation,
    /// as the underlying Pub/Sub client handles the actual efficient batching.
    /// </summary>
    /// <param name="messages">The messages to batch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns an enumerable of message batches.</returns>
    public ValueTask<IEnumerable<IAmAMessageBatch>> CreateBatchesAsync(IEnumerable<Message> messages,
        CancellationToken cancellationToken)
    {
        // Simple batching: treat all messages as one logical batch
        return new ValueTask<IEnumerable<IAmAMessageBatch>>([new MessageBatch(messages)]);
    }

    /// <summary>
    /// Asynchronously sends a batch of messages.
    /// Note: The current implementation iterates and publishes messages individually.
    /// The Google Pub/Sub <see cref="PublisherClient"/> handles internal batching for performance.
    /// </summary>
    /// <remarks>
    /// Wire mapping failures propagate before publishing the affected message. A null writer result
    /// raises <see cref="InvalidOperationException"/>. Earlier messages in the batch may already be published.
    /// Cancellation is checked before mapping each message and before its native submission.
    /// Once submitted, publication is awaited to completion; cancellation cannot recall that message.
    /// </remarks>
    /// <param name="batch">The message batch to send.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous bulk send operation.</returns>
    /// <exception cref="OperationCanceledException">The caller's token was cancelled before mapping a message or submitting it.</exception>
    /// <exception cref="InvalidOperationException">Thrown if the batch is not of type <see cref="MessageBatch"/>.</exception>
    public async Task SendAsync(IAmAMessageBatch batch, CancellationToken cancellationToken)
    {
        if (batch is not MessageBatch messageBatch)
        {
            throw new InvalidOperationException("Batch must be a message batch");
        }

        // Iterate through the messages in the batch and publish each one.
        // The underlying Google PublisherClient will buffer and batch these calls efficiently.
        foreach (var message in messageBatch.Content)
        {
            cancellationToken.ThrowIfCancellationRequested();

            BrighterTracer.WriteProducerEvent(Span, MessagingSystem.PubSub, message, instrumentation);
            var pubSubMessage = CreateWireMessage(message);
            cancellationToken.ThrowIfCancellationRequested();
            await client.PublishAsync(pubSubMessage);
        }
    }

    private PubsubMessage CreateWireMessage(Message message)
    {
        var native = _wireMessageWriter.WriteMessage(message)
            ?? throw new InvalidOperationException("The wire message writer returned null.");
        native.OrderingKey = _enableMessageOrdering ? message.Header.PartitionKey : string.Empty;
        return native;
    }

    /// <summary>
    /// Disposes of the producer resources synchronously.
    /// </summary>
    public void Dispose()
    {
        client.DisposeAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Disposes of the producer resources asynchronously.
    /// </summary>
    /// <returns>A ValueTask representing the asynchronous disposal.</returns>
    public async ValueTask DisposeAsync()
    {
        await client.DisposeAsync();
    }
}
