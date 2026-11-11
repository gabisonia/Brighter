using Google.Cloud.PubSub.V1;

namespace Paramore.Brighter.MessagingGateway.GcpPubSub;

/// <summary>
/// Represents Google Cloud Pub/Sub specific configuration for a message publication (a topic).
/// This extends the core Brighter <see cref="Publication"/> with GCP-specific settings.
/// </summary>
public class GcpPublication : Publication
{
    /// <summary>
    /// Gets or sets the native message writer used for individual and bulk sends.
    /// </summary>
    /// <value>The writer, or null to use the default Pub/Sub mapping.</value>
    /// <remarks>
    /// A null value selects <see cref="DefaultGcpPubSubWireMessageWriter"/>.
    /// The writer owns the message data and attributes. The gateway sets the native ordering key
    /// from the source partition key when the publisher client enables ordering, or clears it otherwise.
    /// </remarks>
    public IAmAWireMessageWriter<PubsubMessage>? WireMessageWriter { get; set; }

    /// <summary>
    /// Gets or sets the attributes for the Google Cloud Pub/Sub Topic.
    /// This includes settings such as the ProjectId, Topic Name, and potentially more advanced configurations.
    /// </summary>
    public TopicAttributes? TopicAttributes { get; set; }
    
    /// <summary>
    /// Gets or sets a value indicating whether messages published to the topic are delivered in the order they were published,
    /// provided they were published with an ordering key.
    /// </summary>
    /// <remarks>
    /// Brighter sends a message's partition key as the Pub/Sub ordering key only when the publisher client has message
    /// ordering enabled, either by this property or by a <see cref="PublisherClientConfiguration"/>, as the Google client
    /// refuses an ordering key otherwise. The default writer also includes the partition key in an attribute, so a consumer
    /// receives it whether or not the publication is ordered. Custom writers control their own attributes.
    /// </remarks>
    public bool EnableMessageOrdering { get; set; }

    /// <summary>
    /// Gets or sets an <see cref="Action"/> to allow advanced customization of the <see cref="PublisherClientBuilder"/>.
    /// This is used to configure the client that publishes messages to the topic, for scenarios like
    /// setting custom client options, retries, or deadlines.
    /// </summary>
    /// <remarks>
    /// Runs after the connection's <see cref="GcpMessagingGatewayConnection.PublisherConfiguration"/>, so this configuration
    /// wins where both set the same property. Assigning a new <c>Settings</c> replaces any the connection's configuration
    /// set. If <see cref="EnableMessageOrdering"/> is set, Brighter enables message ordering after both have run.
    /// </remarks>
    public Action<PublisherClientBuilder>? PublisherClientConfiguration { get; set; }
}


/// <summary>
/// Represents Google Cloud Pub/Sub specific configuration for a message publication, strongly typed to a request type.
/// This allows for easy association between a Brighter <typeparamref name="T"/> and its publication settings.
/// </summary>
/// <typeparam name="T">The type of <see cref="IRequest"/> associated with this publication.</typeparam>
public class GcpPublication<T> : GcpPublication where T: IRequest
{
    /// <summary>
    /// Initializes a new instance of the <see cref="GcpPublication{T}"/> class,
    /// automatically setting the <see cref="Publication.RequestType"/> to <typeparamref name="T"/>.
    /// </summary>
    public GcpPublication()
    {
        RequestType = typeof(T);
    }
}
