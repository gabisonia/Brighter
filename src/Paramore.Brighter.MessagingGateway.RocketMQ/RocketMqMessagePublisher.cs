using System;

namespace Paramore.Brighter.MessagingGateway.RocketMQ;

/// <summary>
/// Turns a Brighter message into the RocketMQ message that will be sent for it.
/// </summary>
public static class RocketMqMessagePublisher
{
    private static readonly DefaultRocketMqWireMessageWriter s_defaultWireMessageWriter = new();

    /// <summary>
    /// Builds the RocketMQ message for <paramref name="message"/>.
    /// </summary>
    /// <param name="message">The Brighter message to send.</param>
    /// <param name="publication">The publication, which supplies the topic, topic type and default tag.</param>
    /// <param name="delay">
    /// How long delivery should be held back, or null for none. Only honoured on a
    /// <see cref="TopicType.Delay"/> topic, or when non-zero.
    /// </param>
    /// <param name="timeProvider">The clock a delivery timestamp is measured from.</param>
    /// <returns>The message to hand to a RocketMQ <c>Producer</c>.</returns>
    public static Org.Apache.Rocketmq.Message CreateRocketMqMessage(
        Message message,
        RocketMqPublication publication,
        TimeSpan? delay,
        TimeProvider timeProvider)
    {
        var wire = (publication.WireMessageWriter ?? s_defaultWireMessageWriter).WriteMessage(message)
            ?? throw new InvalidOperationException("The RocketMQ wire message writer returned null.");
        var builder = new Org.Apache.Rocketmq.Message.Builder()
            .SetBody(wire.Body)
            .SetTopic(publication.Topic!.Value);
        foreach (var property in wire.Properties)
        {
            builder.AddProperty(property.Key, property.Value);
        }

        if (publication.TopicType == TopicType.Delay || delay.HasValue && delay.Value != TimeSpan.Zero)
        {
            delay ??= TimeSpan.Zero;
            builder
                .SetDeliveryTimestamp(timeProvider.GetUtcNow().Add(delay.Value).UtcDateTime);
        }

        if (publication.TopicType == TopicType.Fifo || !PartitionKey.IsNullOrEmpty(message.Header.PartitionKey))
        {
            builder.SetMessageGroup(message.Header.PartitionKey.Value);
        }

        if (message.Header.Bag.TryGetValue(HeaderNames.Keys, out var keys))
        {
            if (keys is string[] keysArray)
            {
                builder.SetKeys(keysArray);
            }
            else if (keys is string keyString)
            {
                builder.SetKeys(keyString);
            }
        }
        else
        {
            builder.SetKeys(message.Id);
        }

        if (message.Header.Bag.TryGetValue(HeaderNames.Tag, out var tag) && tag is string tagString)
        {
            builder.SetTag(tagString);
        }
        else if (!string.IsNullOrEmpty(publication.Tag))
        {
            builder.SetTag(publication.Tag);
        }

        return builder.Build();
    }

}
