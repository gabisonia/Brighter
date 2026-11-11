# Configuring wire message mapping

Select a wire writer on a publication and a wire reader on a subscription before
creating endpoints. Leave both unset to retain the default Brighter format. The
shared contracts customize the native payload and application metadata; gateways
retain routing, confirmations, receipts, locks and settlement.

The following endpoint settings exist in the local implementation. Their presence
does not close the remaining validation and scheduler/forwarding gates in the
[completion plan](completion-plan.md).

| Gateway | Publication writer | Subscription reader | Wire types |
| --- | --- | --- | --- |
| SQS, AWS SDK v3/v4 | `SqsPublication.WireMessageWriter` | `SqsSubscription.WireMessageReader` | `SqsWireMessage` / SDK SQS `Message` |
| SNS, AWS SDK v3/v4 | `SnsPublication.WireMessageWriter` | Receiving SQS subscription | `SnsWireMessage` / SDK SQS `Message` |
| RabbitMQ Async/Sync | `RmqPublication.WireMessageWriter` | `RmqSubscription.WireMessageReader` | Gateway-specific `RmqWireMessage` / `BasicDeliverEventArgs` |
| Kafka | `KafkaPublication.WireMessageWriter` | `KafkaSubscription.WireMessageReader` | Confluent `Message<string, byte[]>` / `ConsumeResult<string, byte[]>` |
| Azure Service Bus | `AzureServiceBusPublication.WireMessageWriter` | `AzureServiceBusSubscription.WireMessageReader` | `ServiceBusMessage` / `ServiceBusReceivedMessage` |
| GCP Pub/Sub | `GcpPublication.WireMessageWriter` | `GcpPubSubSubscription.PullWireMessageReader` or `StreamWireMessageReader` | `PubsubMessage` / `ReceivedMessage` or `GcpStreamMessage` |
| MQTT | `MqttPublication.WireMessageWriter` | `MqttSubscription.WireMessageReader` | `MqttApplicationMessage` |
| Redis | `RedisMessagePublication.WireMessageWriter` | `RedisSubscription.WireMessageReader` | `string` |
| RocketMQ | `RocketMqPublication.WireMessageWriter` | `RocketSubscription.WireMessageReader` | `RocketMqWireMessage` / `MessageView` |
| SQL Server | `MsSqlPublication.WireMessageWriter` | `MsSqlSubscription.WireMessageReader` | `string` |
| PostgreSQL | `PostgresPublication.WireMessageWriter` | `PostgresSubscription.WireMessageReader` | `string` |

The typed composition API is the chosen alternative to a general mapping DSL;
see the [configuration design review](configuration-design-review.md). Native SDK
types belong to the gateway's package version. A writer or reader for one SDK variant
cannot be assigned to another variant merely because its type names look similar.

SDK V3 now exposes the corresponding configuration in
`Paramore.Brighter.MessagingGateway.AWSSQS`, using its own SDK V3 wire carriers
and native attribute/message types. Its replacement/default-composition examples
are in the [SDK V3 test batch](review/aws-v3-mapping/README.md). Reader failure
handling, mixed batches and raw/envelope preservation are now covered by the
[SDK V3 reader tests](review/aws-v3-reader-failures/README.md). The
[lifecycle tests](review/aws-v3-lifecycle/README.md) also cover mapping isolation,
reuse, overlapping sends, FIFO controls, writer recovery and dispatch telemetry.
The scoped SDK v3/v4 mapping parity now includes callback-pump recovery; broader release gates remain separate. Both SDK variants preserve readable
rejection metadata and stamped count when another field fails default parsing.
Ordinary messages retain broker-count precedence, and receipt handles come from
the current native delivery. The [regression review](review/aws-malformed-rejection-count/README.md)
records the focused and full-suite validation status.

## Reuse the default mapping

Compose `DefaultSqsWireMessageWriter` when an integration needs a few additional or renamed attributes. The queue URL passed to this writer supplies the default `topic` attribute; the publication still determines the actual destination.

```csharp
using MessageAttributeValue = Amazon.SQS.Model.MessageAttributeValue;
using Paramore.Brighter;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;

public sealed class TenantWireWriter(string queueUrl)
    : IAmAWireMessageWriter<SqsWireMessage>
{
    private readonly DefaultSqsWireMessageWriter _defaultWriter = new(queueUrl);

    public SqsWireMessage WriteMessage(Message message)
    {
        var wireMessage = _defaultWriter.WriteMessage(message);
        wireMessage.MessageAttributes["tenant-id"] = new MessageAttributeValue
        {
            DataType = "String",
            StringValue = (string)message.Header.Bag["tenant_id"]
        };
        return wireMessage;
    }
}
```

Configure it on the publication using a resolved queue URL:

```csharp
var publication = new SqsPublication(
    new ChannelName(queueUrl),
    findQueueBy: QueueFindBy.Url,
    makeChannels: OnMissingChannel.Validate)
{
    Topic = new RoutingKey("orders"),
    WireMessageWriter = new TenantWireWriter(queueUrl)
};
```

The example requires a string `tenant_id` in the bag. It adds a native attribute while retaining the serialized bag and default headers. To move a value rather than duplicate it, modify the serialized `bag` attribute in the returned wire message; do not remove it from the source `Message.Header.Bag`.

## Replace the mapping

Implement `IAmAWireMessageWriter<SqsWireMessage>` without calling the default writer to select the entire outgoing body and application attributes. Return a new `SqsWireMessage` containing the native SQS attribute dictionary. This supports legacy formats that must omit Brighter or CloudEvents metadata.

The sender applies the configured queue URL, delay, message group and deduplication ID independently of that payload. Existing producer APIs and the two-argument `SqsMessageSender` constructor continue to use the default mapping when no custom writer is configured.

## Move telemetry outside CloudEvents

A complete replacement writer can read `Message.Header.TraceParent`, `TraceState`
and `Baggage` at dispatch and put them in the receiver's chosen native fields.
The paired reader restores those three header properties before consumer tracing.
Do not capture telemetry when constructing the converter or depositing the message
in the outbox: the publish span can differ from the deposit span.

The executable SDK V4 examples are the
[custom writer](../../tests/Paramore.Brighter.AWS.V4.Tests/TestDoubles/NonCloudEventsSqsWireMessageWriter.cs)
and [paired reader](../../tests/Paramore.Brighter.AWS.V4.Tests/TestDoubles/NonCloudEventsSqsWireMessageReader.cs).
Configure them on `SqsPublication.WireMessageWriter` and
`SqsSubscription.WireMessageReader`, respectively, selecting the same
`telemetryInBody` value. These examples expect all three telemetry values; adapt
missing-field handling to the external system's contract.

| Representation | Traceparent | Tracestate | Baggage | Application payload |
| --- | --- | --- | --- | --- |
| Custom native attributes | `telemetry-parent` | `telemetry-state` | `telemetry-baggage` | Original body |
| JSON body | `parent` property | `state` property | `baggage` property | `payload` property |

Both formats retain application identity in `legacy-id` and omit CloudEvents
attributes entirely. Body rewriting is explicit: this JSON example expects text
and is not a generic wrapper for compressed or encrypted bytes. The existing
receipt handle, delivery count and acknowledgement remain gateway responsibilities.

For SNS, the executable [writer](../../tests/Paramore.Brighter.AWS.V4.Tests/TestDoubles/NonCloudEventsSnsWireMessageWriter.cs)
uses the same two representations through `SnsPublication.WireMessageWriter`.
Configure the [delivery reader](../../tests/Paramore.Brighter.AWS.V4.Tests/TestDoubles/NonCloudEventsSnsDeliveryWireMessageReader.cs)
on the receiving `SqsSubscription`, using the matching `telemetryInBody` value and
SNS subscription's `rawDelivery` setting. For enveloped delivery, this example
unwraps the inner message and attributes before delegating content conversion;
it leaves the original native receipt for the consumer to manage. Both SDKs have
native-wire, Reactor/Proactor handler-context and acknowledgement evidence for
raw and enveloped SNS delivery. The examples require their documented text/JSON
fields; they are not a general binary-envelope parser.

For RabbitMQ Async, use the executable
[writer](../../tests/Paramore.Brighter.RMQ.Async.Tests/TestDoubles/NonCloudEventsRmqWireMessageWriter.cs)
on `RmqPublication.WireMessageWriter` and the
[reader](../../tests/Paramore.Brighter.RMQ.Async.Tests/TestDoubles/NonCloudEventsRmqWireMessageReader.cs)
on `RmqSubscription.WireMessageReader`, with the same `telemetryInBody` selection.
Native AMQP headers contain UTF-8 bytes; the JSON alternative keeps the application
text in `payload`. The gateway retains persistence, delivery and confirmation
metadata. The SDK's publish-sequence header can coexist with the custom headers.
The [integration test](../../tests/Paramore.Brighter.RMQ.Async.Tests/MessagingGateway/When_dispatching_rmq_non_cloud_events_telemetry_should_reach_the_application_handler.cs)
observes native output independently, then runs the actual Reactor/Proactor and
checks handler telemetry and automatic acknowledgement. These examples require
the expected fields and do not define a general binary-message contract.

## Read legacy messages

Implement the shared reader contract with the native AWS SDK v4 message type:

```csharp
public sealed class LegacyOrderReader(RoutingKey topic)
    : IAmAWireMessageReader<Amazon.SQS.Model.Message>
{
    public Message ReadMessage(Amazon.SQS.Model.Message nativeMessage)
    {
        var header = new MessageHeader(
            new Id(nativeMessage.MessageAttributes["legacy-id"].StringValue),
            topic,
            MessageType.MT_EVENT,
            traceParent: new TraceParent(nativeMessage.MessageAttributes["traceparent"].StringValue));
        header.Bag["tenant_id"] = nativeMessage.MessageAttributes["tenant-id"].StringValue;
        return new Message(header, new MessageBody(nativeMessage.Body));
    }
}
```

Set the reader on the subscription passed to `SqsMessageConsumerFactory` or the channel factory:

```csharp
var subscription = new SqsSubscription(
    new SubscriptionName("legacy-orders"),
    new ChannelName(queueUrl),
    ChannelType.PointToPoint,
    new RoutingKey("orders"),
    requestType: typeof(OrderCommand),
    messagePumpType: MessagePumpType.Proactor,
    findQueueBy: QueueFindBy.Url,
    makeChannels: OnMissingChannel.Validate)
{
    WireMessageReader = new LegacyOrderReader(new RoutingKey("orders"))
};
```

`OrderCommand` is the application's request type. Use `Reactor` for synchronous pipelines. The reader maps application fields; the consumer attaches the actual SQS receipt handle and resolves the native delivery count using Brighter's existing rejection-copy rules. The first delivery normally has handled count zero. A reader need not copy the receipt handle, and a supplied value cannot replace the broker's value.

The custom reader currently receives the original SQS delivery. If SNS raw delivery is disabled, the body still contains the SNS envelope; custom readers must interpret it themselves. Leaving the property unset retains the existing raw/enveloped parsing. `DefaultSqsWireMessageReader` selects the existing parser explicitly: use `rawMessageDelivery: true` for raw SQS body/attributes, or `false` for an SNS envelope. Raw and envelope composition have dedicated coverage. The envelope test supplies an independently constructed SNS-shaped body and verifies the inner application message and real acknowledgement.

If a custom reader throws or returns null, the consumer logs the mapping failure and returns an `MT_UNACCEPTABLE` message with the receipt handle and delivery count preserved. Rejecting that message without configured rejection channels deletes the source delivery. The failure result retains the original native body and uses the native SQS message ID because application mapping did not produce an ID. With default rejection writing, a configured invalid-message queue receives that body and fallback ID; successful forwarding removes the source delivery.

When default parsing fails, `DefaultSqsWireMessageReader` also preserves the native body, receipt and delivery count. It retains a parsed application ID when available and otherwise uses the native SQS ID. Malformed-envelope tests cover both ordinary consumption and explicit reader configuration, including forwarding to an invalid-message queue and deletion of the source delivery.

Raw-parser failure tests supply a valid application ID and topic followed by an invalid content type. They verify that the parsed application identity and native body survive rejection and forwarding, through both ordinary and explicitly configured readers.

The mixed-batch test combines two valid legacy messages with one custom-reader failure. It verifies distinct receipt handles, independent acknowledgement/rejection and deletion of all source deliveries. Both modes exercised a single receive containing all three messages in the local emulator.

A successfully parsed rejection envelope retains its inner application body and metadata. It is not reconstructed as a parser failure merely because its message type is unacceptable. Dedicated tests cover the application ID, tracing, recorded rejection count and real acknowledgement through both ordinary and explicitly configured readers.

## Configure AWS rejection forwarding

Both AWS SDK gateways expose separate subscription settings for Brighter-managed
invalid-message and dead-letter publication. Configure them before creating the
consumer:

```csharp
subscription.InvalidMessageRoutingKey = new RoutingKey("invalid-orders");
subscription.DeadLetterRoutingKey = new RoutingKey("dead-orders");
subscription.InvalidMessageWireMessageWriter = new TenantWireWriter(invalidQueueUrl);
subscription.DeadLetterWireMessageWriter = new TenantWireWriter(deadLetterQueueUrl);
```

Here the resolved queue URLs correspond to the named destination queues.
`TenantWireWriter` is the composing example above and requires `tenant_id` in the
received bag. A complete replacement can instead implement
`IAmAWireMessageWriter<SqsWireMessage>` directly. Leave either property null to
retain that route's default format.

The gateway selects the destination and refreshes rejection metadata before
invoking its writer. Invalid-message fallback to a dead-letter queue uses the
dead-letter writer. Supplying a writer without its route does not create a route,
and these properties do not configure broker-managed redrive or normal producer
publications. The destination's reader must understand the chosen representation;
the incoming reader does not supply an inverse mapping.

If a rejection writer throws or returns null, the gateway releases the source for
redelivery without publishing a default fallback. Its existing `Reject` contract
returns true after handling that failure; this is not proof that forwarding
succeeded. A recovered writer can forward a fresh delivery through the same
consumer. Publish and source deletion remain separate operations.

The [approved native integration cases](review/aws-rejection-writers/README.md)
cover all three routes, replacement/composition and throwing/null recovery.
The [FIFO and consumer-isolation cases](review/aws-rejection-controls/README.md)
also pass in both SDKs and frameworks: native group/deduplication IDs survive
forwarding, failure recovery retains the source, and consumers from one factory
retain their own writer selections. These are emulator results; publication and
source deletion remain separate operations.

## Quartz with RabbitMQ Async

Quartz callbacks return through the command processor and the configured RabbitMQ
publication writer. The executable
[native dispatch example](../../tests/Paramore.Brighter.RMQ.Async.Tests/MessagingGateway/When_quartz_dispatches_scheduled_work_should_use_the_configured_rmq_wire_writer.cs)
uses `QuartzSchedulerFactory` with the production Quartz job and scheduler handlers.
For message scheduling, attach the factory-created scheduler to the producer and
use `SendWithDelay` or `SendWithDelayAsync` on an exchange without native delay.
For request scheduling, use the factory's request scheduler with
`RequestSchedulerType.Post`. Both paths select `RmqPublication.WireMessageWriter`
when publishing after the job fires.

The example verifies native output, exact source preservation and dispatch-time
traceparent for AMQP-header and JSON-body mappings. Use a positive delay: the
separate zero-delay timestamp rejection remains recorded in the scheduler audit.
The executable [writer-recovery example](../../tests/Paramore.Brighter.RMQ.Async.Tests/MessagingGateway/When_a_quartz_rmq_writer_fails_should_retain_the_message_until_outbox_recovery.cs)
shows throwing/null writers leaving the message pending in the outbox. After the
writer recovers, `ClearOutbox`/`ClearOutboxAsync` publishes it with a fresh producer
span beneath the recovery activity's outbox-clear span. The callback completes
successfully because the outbox retains the failed publication for later clearing.
These examples use in-memory stores; database persistence and automatic retry
policies remain separate validation work.

## AWS scheduler destinations

With `UseMessageTopicAsTarget = false`, set `SchedulerTopicOrQueue` to the queue
or topic consumed by your scheduler callback handler. It may differ from the
business destination in `Message.Header.Topic`. Both AWS SDK implementations
now resolve the callback destination from that configuration and preserve the
original business message inside `FireAwsScheduler`.

With `UseMessageTopicAsTarget = true`, an existing business endpoint remains the
native Scheduler target. This path serializes the native request at scheduling
time and does not invoke the publication's wire writer at dispatch. Selecting a
callback destination is a prerequisite for running application mapping later;
the native-callback tests verify this dispatch path in both SDKs and sending modes.

For custom business wire mapping, configure callback mode explicitly:

```csharp
var schedulerFactory = new AwsSchedulerFactory(awsConnection, schedulerRoleArn)
{
    UseMessageTopicAsTarget = false,
    SchedulerTopicOrQueue = new RoutingKey("brighter-scheduler-callbacks"),
    MakeRole = OnMissingRole.Assume
};
```

Provision that callback queue and configure its consumer to read Brighter's default
wire representation with `AwsSchedulerFiredMapper` and `AwsSchedulerFiredHandler`.
The handler routes the contained message through `FireSchedulerMessageHandler`
and the command processor. Select your custom writer on the business publication;
the producer invokes it with the trace context of that later dispatch. The callback
queue and business endpoint can be different. Use the namespace matching your
AWS SDK version and grant the scheduler role delivery access to the callback queue.

The native-callback tests receive an actual emulator-generated callback and invoke
the registered handlers through a command processor. They validate custom SQS/SNS
output, dispatch-time traceparent/tracestate/baggage, an empty outbox after success,
and the unchanged original scheduling object. Automatic callback-pump settlement,
writer-failure redelivery and real AWS IAM enforcement remain separate checks.

Native scheduled SNS/SQS messages now write the `timestamp` attribute using
invariant round-trip formatting, preserving fractional seconds and the offset.
Brighter's reader accepts that value. Integrations that parse the previous
culture-dependent timestamp text should accept the ISO round-trip representation.

## Runtime requirements

- For direct publication, mapping runs at send time after the producer's existing trace-context propagation. Read tracing fields from the supplied message when selecting their wire locations. SQS and SNS scheduled dispatch through Brighter’s in-memory scheduler have native-wire validation. Other scheduled paths remain under validation. AWS scheduler native targets (`UseMessageTopicAsTarget`) currently serialize their own requests without consulting publication writers.
- Readers must return a fresh Brighter message and must not mutate their native input. A reader instance may be shared by concurrent consumers.
- A writer instance can serve multiple concurrent sends. Keep per-message state in local variables and return fresh attributes and body data. Do not mutate the supplied message, including its bag.
- The default SQS and SNS writers serialize a copy of the bag and stamps the handled count in that copy. They retain filtering of local-only headers.
- Let outbound mapping errors propagate. Returning an unrelated fallback payload could acknowledge a send that did not represent the intended message.
- Keep the resulting payload within SQS limits. The default format can already use all ten available attributes when optional metadata is present, so adding attributes may require removing or regrouping others. See [AWS message metadata](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html).
- Body changes must match the receiving system. In particular, do not treat compressed or encrypted body content as ordinary JSON.

## Verification scope

The integration tests use the public publication and producer factory, inspect the native queue message independently, and cover both sync and async sending. One test replaces all attributes; another composes the default mapping and confirms that existing metadata and the source bag are preserved. The validation log records observed failures before the fix, supported-target builds, and full regression results.

The reader test publishes native legacy attributes independently and checks mapped identity, tenant metadata, tracing, delivery count and actual deletion after acknowledgement. Three temporary production mutations establish that the test detects ignored configuration and missing delivery metadata.

The throwing-reader test additionally checks the failure signal and native receipt, then verifies actual deletion after rejection. Mutations verify that incorrect message classification and lost receipt metadata are detected.

Global production readiness still requires tracing lifecycle, remaining failure-path, concurrency, and transport tasks in [the implementation plan](tasks.md).

## RabbitMQ Async custom writing

The RabbitMQ Async gateway also supports publication-selected writing through `RmqPublication.WireMessageWriter`. The writer implements `IAmAWireMessageWriter<RmqWireMessage>`, returning fresh `RabbitMQ.Client.BasicProperties` and a `ReadOnlyMemory<byte>` body. The Async package supports both synchronous and asynchronous producer entry points.

```csharp
using System.Collections.Generic;
using System.Text;
using Paramore.Brighter;
using Paramore.Brighter.MessagingGateway.RMQ.Async;
using RabbitMQ.Client;

public sealed class LegacyRabbitWriter : IAmAWireMessageWriter<RmqWireMessage>
{
    public RmqWireMessage WriteMessage(Message message)
    {
        return new RmqWireMessage
        {
            Body = Encoding.UTF8.GetBytes(message.Body.Value),
            Properties = new BasicProperties
            {
                MessageId = $"legacy-{message.Id.Value}",
                ContentType = "application/json",
                Headers = new Dictionary<string, object?>
                {
                    ["tenant-id"] = message.Header.Bag["tenant_id"]
                }
            }
        };
    }
}
```

Configure the writer on the publication:

```csharp
var publication = new RmqPublication
{
    Topic = new RoutingKey("orders"),
    WireMessageWriter = new LegacyRabbitWriter()
};
```

The example re-encodes a textual body; binary payloads should use bytes directly. The producer factory selects the publication's writer. The gateway still chooses the exchange/routing key, applies its persistence setting, and tracks confirmation against the original Brighter ID and topic. RabbitMQ.Client adds its own publish-sequence header; application-header replacement does not remove that SDK bookkeeping. The gateway discards a writer-supplied delay header before applying its own delay, so an immediate send emits no `x-delay`. The gateway also discards writer-supplied delivery tags and emits only the source message's tag when one exists. The SDK replaces a writer-supplied publish-sequence header with its own sequence number. Both sending modes preserve that control and confirm the original Brighter identity; disabling SDK confirmation tracking makes the header-ownership test fail. The immediate-send test checks the native header on a direct exchange. A separate test uses the delayed-message plugin and confirms a requested three-second delay overrides a custom writer's 60-second header, while custom body/properties and original confirmation identity are preserved.

An unset writer uses `DefaultRmqWireMessageWriter` and preserves existing serialization. Consumer retry/rejection selection, recovery and Sync endpoint mapping are described below; current validation limits are tracked in the [completion plan](completion-plan.md). The integration test deliberately changes the wire ID and body, observes them through the native SDK, and verifies the confirmation still identifies the original message.

## Reuse the default RabbitMQ mapping

Compose `DefaultRmqWireMessageWriter` to change selected fields while preserving Brighter and CloudEvents metadata:

```csharp
public sealed class TenantRabbitWriter : IAmAWireMessageWriter<RmqWireMessage>
{
    private readonly DefaultRmqWireMessageWriter _defaultWriter = new();

    public RmqWireMessage WriteMessage(Message message)
    {
        var wireMessage = _defaultWriter.WriteMessage(message);
        var headers = wireMessage.Properties.Headers!;
        headers["tenant-id"] = headers["tenant_id"];
        headers.Remove("tenant_id");
        return wireMessage;
    }
}
```

Set `WireMessageWriter = new TenantRabbitWriter()` on the publication. The example requires a `tenant_id` bag entry and renames it in the fresh native header dictionary. It leaves the source message untouched. The default writer preserves existing AMQP header-type validation and local-header filtering; its body retains the source's read-only memory representation. If a custom writer changes body bytes or mutable nested header values, create replacement values rather than modifying data shared with the source.

The composition test verifies both sending modes through the producer factory, including native metadata and original confirmation identity. The publisher also uses the extracted default for its existing requeue serialization, while retaining control of generated message IDs, original-message metadata and retry routing.

## Reuse the default SQS reader

A custom reader can rename selected bag entries after default parsing:

```csharp
public sealed class TenantSqsReader : IAmAWireMessageReader<Amazon.SQS.Model.Message>
{
    private readonly DefaultSqsWireMessageReader _defaultReader = new(rawMessageDelivery: true);

    public Message ReadMessage(Amazon.SQS.Model.Message nativeMessage)
    {
        var message = _defaultReader.ReadMessage(nativeMessage);
        if (message.Header.Bag.TryGetValue("tenant-id", out var tenantId))
        {
            message.Header.Bag["tenant_id"] = tenantId;
            message.Header.Bag.Remove("tenant-id");
        }
        return message;
    }
}
```

Set `WireMessageReader = new TenantSqsReader()` on the subscription. The example preserves default identity, body, tracing, content type and other metadata while moving a tenant bag value. Missing tenant metadata is left unchanged. The consumer attaches its actual receipt and resolves delivery counts as for other configured readers.

The default reader creates an independent parser on every call and preserves successful parsing. It enriches parser failures with the native payload and delivery metadata needed for rejection. It also supplies ordinary consumption when no custom reader is configured. Raw JSON reading retains the existing `charset=utf-8` normalization. The integration test publishes an independent native fixture, checks the selectively mapped result and confirms actual deletion after acknowledgement.

## SNS custom writing

Implement `IAmAWireMessageWriter<SnsWireMessage>` to replace the SNS body, optional subject and application attributes. `SnsWireMessage.MessageAttributes` uses `Amazon.SimpleNotificationService.Model.MessageAttributeValue`, which differs from the SQS SDK type.

```csharp
public sealed class LegacySnsWriter : IAmAWireMessageWriter<SnsWireMessage>
{
    public SnsWireMessage WriteMessage(Message message) => new()
    {
        Body = $"legacy:{message.Body.Value}",
        Subject = "Legacy order",
        MessageAttributes = new()
        {
            ["tenant-id"] = new()
            {
                DataType = "String",
                StringValue = (string)message.Header.Bag["tenant_id"]
            }
        }
    };
}

var publication = new SnsPublication(TopicFindBy.Arn, topicArn)
{
    Topic = new RoutingKey("orders"),
    WireMessageWriter = new LegacySnsWriter()
};
```

The factory passes this writer to the publisher for both sync and async sends. The gateway resolves the destination and applies message group/deduplication controls from the original message. Custom FIFO writing is validated with raw and enveloped delivery. Scheduled dispatch through Brighter’s in-memory scheduler is validated with native envelope observation; AWS native scheduler targets remain separate work.

An unset writer retains the existing default mapping, and the original two-argument `SnsMessagePublisher` constructor remains available. `DefaultSnsWireMessageWriter` supplies default publication and is available for composition. It stamps the handled count into a filtered copy of the bag and leaves the source unchanged.

The integration test observes the SNS envelope through an independent native SQS subscription. It verifies the custom body, subject, exclusive tenant attribute, destination and unchanged source message. The reader receives the native delivery representation selected by the subscription's raw-delivery configuration; customizing the publisher does not remove an SNS envelope.

## Reuse the default SNS mapping

Pass the resolved topic ARN to `DefaultSnsWireMessageWriter` to preserve the default `topic` attribute. The publisher separately owns the actual destination. A custom writer can add selected native attributes to the fresh result:

```csharp
public sealed class TenantSnsWriter(string topicArn) : IAmAWireMessageWriter<SnsWireMessage>
{
    private readonly DefaultSnsWireMessageWriter _defaultWriter = new(topicArn);

    public SnsWireMessage WriteMessage(Message message)
    {
        var wireMessage = _defaultWriter.WriteMessage(message);
        wireMessage.MessageAttributes["tenant-id"] = new()
        {
            DataType = "String",
            StringValue = (string)message.Header.Bag["tenant_id"]
        };
        return wireMessage;
    }
}
```

Set `WireMessageWriter = new TenantSnsWriter(topicArn)` on `SnsPublication`. This preserves the body, subject, identity, CloudEvents metadata and serialized bag while adding a native tenant attribute. Default mapping creates fresh native attributes and serializes the handled count without changing the source bag. Existing local-header filtering remains in effect.

The composition test observes the real SNS envelope independently in both sending modes. It verifies default metadata, the added attribute, wire handled count and unchanged source message. A temporary omission of the native ID causes both cases to fail at their ID-presence assertion.

## Read legacy RabbitMQ Async deliveries

Set `RmqSubscription.WireMessageReader` before creating a consumer through `RmqMessageConsumerFactory`. The reader implements `IAmAWireMessageReader<BasicDeliverEventArgs>` from `RabbitMQ.Client.Events` and returns a fresh Brighter message. It maps the native body, properties and headers to application content and metadata. Implementations must support concurrent calls and must not mutate the native delivery.

The gateway overwrites `DeliveryTag`, `Redelivered` and `Persist` with broker values after mapping. Acknowledge the returned message through its consumer. Both public factory paths capture the subscription's selected reader when constructing the consumer. An unset reader retains the existing parser, and existing public consumer constructors remain available.

When the native canonical handled-count header exists, the gateway also attaches
its value using the default parser. Without that header, it retains the reader's
count. Direct retries write that header, so a replacement reader can map only
application content without resetting the retry count.
The default parser also preserves a canonical native count when malformed
properties cause it to return an unacceptable message.

The legacy-reader integration test provides a complete example in `tests/Paramore.Brighter.RMQ.Async.Tests/MessagingGateway/When_receiving_a_legacy_rmq_message_should_map_content_and_preserve_acknowledgement.cs`. A throwing or null-returning custom reader produces an unacceptable message with the original body bytes, native message ID and routing key. The gateway stamps broker delivery metadata so the result can be rejected through the consumer. Rejection without an invalid-message route is covered. Mixed-batch tests also verify that valid and failed custom mappings retain their own delivery tags and payloads in either delivery order, and can be settled independently. Default-parser failures also preserve raw bytes and already parsed application ID/topic, with broker metadata for rejection. Malformed native content-type and type properties are covered in both factory paths. Mixed default-parser batch tests cover both malformed native MIME properties and both delivery orders, preserving and settling each result independently. For throwing/null readers, a configured `InvalidMessageRoutingKey` receives the exact payload, native ID and rejection metadata. Successful forwarding acknowledges the source and restores the original message topic. If the invalid queue binding is missing, mandatory forwarding fails and preserves the source delivery and original topic. Recovery tests inspect the redelivered native bytes and identity, repair the binding, then verify successful forwarding and settlement through a new consumer. Default-parser failures also forward exact bytes and native identity, with the parsed application topic in the original-topic header even when it differs from the broker routing key. Both unconfigured and explicit default readers are covered. If forwarding a default-parser failure fails, the original delivery retains its malformed properties, bytes and application-topic header. Recovery tests repair the route, receive through a new consumer and verify successful forwarding and source settlement. If cancellation is already requested before `RejectAsync` starts, it reports cancellation and preserves the source delivery for a later attempt with an uncancelled token. This does not make cancellation during publication atomic: successful forwarding followed by failed acknowledgement can still result in a duplicate on redelivery.


## Reuse the default RabbitMQ reader

Compose `DefaultRmqWireMessageReader` to customize a parsed field while preserving the ordinary format:

```csharp
public sealed class TenantRmqReader : IAmAWireMessageReader<BasicDeliverEventArgs>
{
    private readonly DefaultRmqWireMessageReader _defaultReader = new();

    public Message ReadMessage(BasicDeliverEventArgs nativeMessage)
    {
        var message = _defaultReader.ReadMessage(nativeMessage);
        if (message.Header.Bag.TryGetValue("tenant-id", out var tenantId))
        {
            message.Header.Bag["tenant_id"] = tenantId;
            message.Header.Bag.Remove("tenant-id");
        }
        return message;
    }
}
```

Use `RabbitMQ.Client.Events` for `BasicDeliverEventArgs` and `Paramore.Brighter.MessagingGateway.RMQ.Async` for the default reader. Set `WireMessageReader = new TenantRmqReader()` on the subscription before creating the consumer. The default reader delegates to the same parser used by an unconfigured consumer. The gateway supplies broker delivery metadata and owns acknowledgement and rejection.

The composition integration test checks the renamed tenant key, ordinary fields and successful acknowledgement using independently published native messages. A shared-reader test verifies that sequential deliveries through two consumers retain independent messages, headers and bags, including independent acknowledgement. Configured default-reader failure tests also cover malformed native MIME properties through direct configuration and composition. They verify exact body bytes, parsed identity/topic, broker metadata and rejection. Overlapping-read tests coordinate two consumers entering a shared composing reader, then verify independent parsed results and unchanged native bodies, properties and headers. The test does not prescribe a particular interleaving inside the default parser.


## Configure RabbitMQ invalid-message forwarding

Set `RmqSubscription.InvalidMessageWireMessageWriter` before creating consumers.
It accepts an `IAmAWireMessageWriter<RmqWireMessage>` independently of the retry
writer and ordinary producer publications. Null retains the default format.

```csharp
subscription.InvalidMessageRoutingKey = new RoutingKey("invalid-orders");
subscription.InvalidMessageWireMessageWriter = new LegacyRabbitWriter();
```

`LegacyRabbitWriter` is the textual example above and expects `tenant_id` in the
received bag. For binary payloads, use `message.Body.Memory`. A composing writer
can call `DefaultRmqWireMessageWriter` and alter selected properties or headers.
The destination must understand the selected format; an incoming reader does not
imply its inverse writer.

Brighter supplies rejection metadata and sets the invalid topic before calling
the writer. The gateway retains the native exchange/routing key, source
persistence, delivery tag, immediate-send delay cleanup and confirmed mandatory
publication. Different consumers retain their own selected writers.

`Reject` and `RejectAsync` propagate a writer or publication exception, restore
the original application topic and leave the source unacknowledged. After
recovery, a fresh delivery can be forwarded successfully. Publication and source
acknowledgement are separate operations; an acknowledgement failure after a
confirmed forward can produce a duplicate.

The setting does not change broker-managed dead-lettering. Delivery errors, and
unacceptable messages without an invalid route, use native rejection and the
queue's DLX configuration. The broker transfers the original wire body and
properties without executing an application writer. The selected writer does not
create a missing invalid route or configure scheduler-backed retries.

## Customize RabbitMQ immediate requeue

Configure the consumer's retry mapping independently of producer publications:

```csharp
var subscription = new RmqSubscription(
    new SubscriptionName("orders"),
    new ChannelName("orders"),
    new RoutingKey("orders"),
    requestType: typeof(Order),
    isDurable: true)
{
    RequeueWireMessageWriter = new LegacyRabbitWriter()
};
```

The example reuses `LegacyRabbitWriter` from the publication example. Supply the
application's request type and ensure the received message contains the fields
that writer reads. The factory captures the writer when it creates the consumer;
set it before creating consumers. An unset writer retains the default format.
Consumers created through the same factory retain their own writer selections,
so default and custom subscriptions can coexist in either creation order.
The subscription's `WireMessageReader` must understand the retry representation.
Pair a complete replacement writer with a compatible reader; composing the default
writer can preserve the ordinary reader's format.

For immediate `Requeue` and `RequeueAsync`, the writer controls native body and
application properties. The gateway generates a retry message ID and supplies
source persistence, handled count, delivery marker and original-message ID.
The original ID remains stable across retries, even if the writer omits it.
The gateway removes delay metadata for immediate retries and acknowledges the
original delivery after publication. Serialization leaves the source unchanged.

Native delayed requeue uses the same direct publication path. The requested delay
replaces a writer-supplied delay, and queue-specific routing confines the retry to
the original queue even when sibling queues share the application topic. Targeted
native-delay tests verify custom content and retry controls on that path.
Scheduler-backed retry retains its existing scheduler publication mapping.
`RequeueWireMessageWriter` does not configure scheduler-backed retries or
invalid-message forwarding. For canonical retry-header names, gateway values
replace writer-supplied handled count, original ID, delivery marker and delay.
Before assigning retry original identity, the gateway removes writer-supplied
case variants of the original-ID header. It retains an existing source header's
spelling and value, or emits the canonical header with the source ID for a first
retry. Overlapping retry tests verify independent outputs from a shared composing
default writer. The helper serializes conversion before overlapping publication;
custom writers must still be safe for their callers' concurrency. Other header-name
normalization remains in the implementation plan.

If the writer throws or returns an invalid null result during immediate or native delayed requeue,
`Requeue` and `RequeueAsync` return false and leave the original unacknowledged.
The original becomes available for redelivery when the consumer is disposed.
A subsequent consumer can retry after the mapping problem is resolved. Writers
must still return a valid, fresh representation and must not mutate the source.
Native delayed recovery retains the requested delay and retry metadata.
For scheduler-backed retries, configure `RmqSubscription.ScheduledRequeuePublicationTopic` and register the desired publication writer under that topic and the original CloudEvents type. The consumer captures the selector and schedules a private message copy while retaining the original queue destination. [Native callback evidence](review/rmq-scheduled-selection/README.md) covers selection and queue isolation; [Quartz recovery](review/quartz-rmq-writer-recovery/README.md) covers failed dispatch followed by explicit outbox recovery. Scheduling acceptance alone does not guarantee successful later publication.


## RabbitMQ Sync endpoint mapping

Use the types in `Paramore.Brighter.MessagingGateway.RMQ.Sync` and configure
`RmqPublication.WireMessageWriter` and `RmqSubscription.WireMessageReader` before
creating producers/consumers through their existing factories. Null selects the
existing default format. The existing Task-based producer send wrapper is covered;
the Sync package does not add an async consumer or async factory.

RabbitMQ Client 6 creates native properties through a channel. A Sync writer
returns `RmqWireMessage.Body` and an optional
`Action<IBasicProperties> ConfigureProperties`, rather than constructing the
client's internal properties class. The gateway creates fresh native properties,
invokes the callback once during sending, then applies persistence and delivery
controls. The callback should only populate the supplied object and must not retain
it. Return fresh output per message and capture values during WriteMessage.

Compose the default format by wrapping its callback:

```csharp
public sealed class SyncTenantWriter : IAmAWireMessageWriter<RmqWireMessage>
{
    private readonly DefaultRmqWireMessageWriter _defaultWriter = new();

    public RmqWireMessage WriteMessage(Message message)
    {
        var wire = _defaultWriter.WriteMessage(message);
        var configureDefault = wire.ConfigureProperties;
        var tenant = message.Header.Bag["tenant_id"];
        wire.ConfigureProperties = properties =>
        {
            configureDefault?.Invoke(properties);
            properties.Headers["tenant-id"] = tenant;
        };
        return wire;
    }
}
```

For complete replacement, return a new RmqWireMessage containing the body and a
callback that fills the chosen native properties/headers. The gateway retains the
exchange/routing key, persistence, delay/delivery-tag control and original Brighter
confirmation identity. The [replacement/composition writer](../../tests/Paramore.Brighter.RMQ.Sync.Tests/TestDoubles/SyncEndpointWireWriter.cs)
and [reader](../../tests/Paramore.Brighter.RMQ.Sync.Tests/TestDoubles/SyncEndpointWireReader.cs)
are exercised through the public factories with independent native observation.
The reader can compose DefaultRmqWireMessageReader; the consumer then applies
broker delivery metadata and any canonical handled count.

Configured reader exceptions and null results are handled per delivery. The
consumer returns an unacceptable message retaining native ID, routing key, exact
body and broker metadata while continuing the batch. Four native cases cover both
failure modes in both positions and verify that both deliveries can be acknowledged;
five mutations guard the behavior. This does not establish invalid-message forwarding.

Default-parser failures also retain native body bytes, delivery metadata and handled
count, alongside the ID/topic parsed before failure. The [four cases](review/rmq-sync-default-parser-failures/README.md)
cover unset/explicit default readers and both positions in a mixed batch, including
independent acknowledgement of both deliveries. The [ten forwarding/recovery cases](review/rmq-sync-invalid-forwarding/README.md)
exercise explicit unacceptable custom results, throw/null fallback and default
parser failures. Custom results keep their mapped ID, topic, bytes and marker;
the gateway replaces delivery controls with native values. An unbound invalid route
reports failure, restores the topic and leaves the native source available. Repairing
the binding allows forwarding through a new consumer. Native observation checks
forwarded bytes, identity and rejection metadata. The approved
[bounded observation](review/rmq-sync-settlement-observation/README.md) checks source
recovery/absence for up to two seconds after disposal. It detects all ten omitted
acknowledgements in all seven mutation runs, including three additional runs per
framework. This does not make forwarding and acknowledgement atomic. Custom
invalid-writer selection uses `RmqSubscription.InvalidMessageWireMessageWriter`.
Null retains the default writer; a custom writer can replace or compose it for
unacceptable-message forwarding. The factory captures each consumer's selection.
The writer receives rejection metadata and the invalid route. The gateway controls
routing, persistence, delay/tag headers and confirmed forwarding before source
acknowledgement. It replaces the writer's tag with the current native delivery tag.
The original message topic is restored after success or failure.

The [fourteen cases](review/rmq-sync-invalid-writer/README.md) cover replacement and
composition, writer throw/null, callback failure and an unbound invalid route.
Failures retain the original native source; repairing the writer or binding permits
forwarding through a new consumer. Consumers from one factory retain independent
selections. Delivery-error rejection and unacceptable rejection without an invalid
route bypass the writer and preserve native dead-letter content. All cases pass
with nine mutation checks and both full Sync regressions. Forwarding and source
acknowledgement remain separate operations, so failure after publication can still
produce a duplicate.

The [six publication recovery cases](review/rmq-sync-publication-recovery/README.md)
cover writer throw/null and native-properties callback failure through `Send` and
its compatibility `SendAsync` wrapper. Mapping failure surfaces to direct callers
and leaves the outbox entry pending after retry exhaustion. A healthy message can
still publish with its own confirmation identity; repairing the strategy allows
the original pending message to dispatch with its retained ID, topic and content.
The test uses a real producer and in-memory outbox. This does not establish outage
atomicity or exactly-once delivery. Both focused and full suites pass with five
mutation checks. `RmqSubscription.RequeueWireMessageWriter` now selects mapping for direct consumer
retries. Null uses the default writer. Configure it before creating the consumer;
the factory captures each consumer's selection independently of invalid-message
and publication writers. The gateway supplies the fresh native ID, source-queue
routing, persistence, current handled count and retry delivery marker. Immediate
retry removes writer-supplied delay. Original-ID variants are removed using ordinal
case-insensitive comparison; the source's existing key/value is preserved, or the
current source ID is added under the canonical key.

The [eighteen immediate-retry cases](review/rmq-sync-requeue-writer/README.md) cover
replacement/composition, first/subsequent identity, throw/null/callback failure
recovery and per-consumer isolation. Mapping failure returns false and leaves the
original delivery unacknowledged; repairing the strategy permits recovery through
a new consumer. The source message and bag remain unchanged by gateway mapping.
A selected reader must understand the chosen retry representation. Publication and
acknowledgement remain separate operations, so consumers must tolerate duplicates.
Scheduler-backed retries retain the scheduler's publication mapping. The [eight native-delay cases](review/rmq-sync-native-retry-writer/README.md) validate
replacement/composition and mapping-failure recovery with the native delay plugin.
The gateway replaces the writer's delay with the requested interval and routes the
retry to its original queue, without delivering it to a neighbor sharing the same
application topic. Native inspection checks the elapsed delay, consumed delay header,
identity, persistence, count, delivery marker and exact mapped payload. This selected
mapping batch uses a direct application exchange; existing default-mapping tests
cover topic/fanout separately. The [eight concurrency cases](review/rmq-sync-concurrent-mapping/README.md) pass
initially and after restoration on both frameworks. The approved
[completed-properties cache amendment](review/rmq-sync-concurrent-cache-amendment/README.md)
produces all four intended isolation failures, completing twelve mutations with
34 expected failures. After restoration, all eight focused cases pass again and
both full Sync suites retain exactly 291 passes and one existing skip. No production
changes were needed. The [Kafka default adapters](review/kafka-default-wire-mapping/README.md) are now
implemented and validated with the approved [charset correction](review/kafka-default-content-type/README.md).
All four focused cases pass on both frameworks, ten mutations produce their intended
failures, and both complete Kafka suites pass 269 cases with no failures/skips,
retaining all 265 baseline outcomes at that checkpoint. Both RabbitMQ packages now have selected scheduler retry coverage; Kafka retry/dead-letter selection is described below. See the [completion plan](completion-plan.md) for current release gates.


## Kafka default mapping adapters

`DefaultKafkaWireMessageWriter` implements
`IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>`. It accepts an optional
`IKafkaMessageHeaderBuilder` and uses the existing default builder when omitted.
The existing `KafkaPublication.MessageHeaderBuilder` still feeds that strategy on
the normal publishing path. The adapter maps native key, body and headers; the
gateway retains topic selection and delivery operations.

`DefaultKafkaWireMessageReader` implements
`IAmAWireMessageReader<ConsumeResult<string, byte[]>>`. It delegates to the existing
public KafkaMessageCreator, preserving its header precedence, native offset and
malformed-message behavior. Both adapters can be called or composed directly.
The default writer copies the body, and the default header builder copies byte-array
header values, so composition can edit those outputs without changing the source.
A supplied header builder retains responsibility for independent mutable values.
Reader input remains read-only; copy native body or header arrays before editing them.

The reader retains existing UTF-8 charset normalization. The
[endpoint selection batch](review/kafka-endpoint-wire-mapping/README.md) adds the
configuration below; its eight cases pass on both frameworks. Eight mutations
produce 32 intended failures, and both complete Kafka suites pass 277 cases.

## Kafka endpoint selection

Set `KafkaPublication.WireMessageWriter` to an
`IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>`. The producer captures
the strategy at construction. It maps after dispatch tracing, so a custom external
trace header can carry the current span. A selected writer replaces key/body/header
mapping and may compose DefaultKafkaWireMessageWriter. Omitting it retains the
existing MessageHeaderBuilder behavior. The gateway retains the send topic and
uses the original source identity for confirmations from selected writers.

Set `KafkaSubscription.WireMessageReader` to an
`IAmAWireMessageReader<ConsumeResult<string, byte[]>>`. Both consumer factory paths
capture that selection. The reader may change application identity/topic/body and
telemetry. The gateway supplies the actual consumed TopicPartitionOffset for
acknowledgement, overriding a mapped offset. Omitting the reader retains the default
parser. Existing public constructors are unchanged.

Strategies must support their endpoints' concurrency, retain independent outputs
and preserve source/native input. Selection does not infer a retry or rejection
writer from a reader. Configured reader exceptions and null results return an
unacceptable message for the failed delivery. Its native body, topic, key and offset
are retained, along with metadata readable by the default parser. The message
remains unacceptable even when default parsing succeeds; later good deliveries
continue through the selected reader. Acknowledgement can commit both positions.

The [eight reader-failure cases](review/kafka-reader-failures/README.md) pass on both
frameworks. Nine mutations produce 72 intended failures; both complete Kafka suites
pass 285 cases. Default parsing now preserves the native delivery when required-header
validation fails: the result is unacceptable with its body, readable identity/topic,
key, handled count, trace, custom headers and native offset. This applies to unset
and explicit default readers. The [eight required-header cases](review/kafka-default-parser-failures/README.md)
pass on both frameworks; eight mutations produce 60 intended failures, and both full
Kafka suites pass 293 cases with no failures/skips.

Malformed content type now yields an unacceptable message while the creator continues
reading other fields. The payload, identity, topic, key, count, trace, custom headers
and native offset remain available. Missing content type keeps its previous plain-text
default; successful header precedence is unchanged. The [eight MIME-exception cases](review/kafka-parser-exceptions/README.md)
pass on both frameworks; nine mutations produce 68 intended failures, and both full
Kafka suites pass 301 cases with no failures/skips.

A selected reader may deliberately return an unacceptable result. The gateway keeps
that result's selected identity, body, topic, key, handled count, trace and custom
headers, while replacing its offset with the actual native delivery offset. Subsequent
messages still use the selected reader and both results can be acknowledged. The
[eight explicit-unacceptable cases](review/kafka-explicit-unacceptable/README.md) pass
on both frameworks without production changes; ten mutations produce 80 intended
failures and both full suites pass 309 cases with no failures/skips.

A selected writer that throws or returns null surfaces a send error without publishing
that attempt or reporting it as successfully published. A healthy message can still
publish through the same producer. After repairing the strategy, the same failed
message can be retried with its original identity and binary body intact. Native
records use the selected key, wire identity and body; successful confirmations retain
source identity. The [eight writer-recovery cases](review/kafka-writer-recovery/README.md)
pass on both frameworks without production changes. Nine mutations produce 72 intended
failures; both full suites pass 317 cases with no failures/skips. This validates direct
caller-driven retry, not automatic outbox recovery or broker failure.

Outbox clearing also preserves a pending message after writer retry exhaustion.
A healthy message can dispatch while the selected writer still fails the original.
Repairing the writer and clearing the retained entry publishes its mapped content
and marks it dispatched through the real broker confirmation. A throwing confirmation
subscriber registered before the mediator has different consequences: native publication
has already succeeded, but the mediator callback is skipped and the entry stays pending.
Later healthy confirmations still run. Repairing the subscriber and clearing the entry
publishes it again, so this recovery can deliver duplicates. The
[six outbox/subscriber cases](review/kafka-outbox-recovery/README.md) cover both API modes
with an in-memory outbox and real Kafka delivery. Seven mutations produce 40 intended
failures; both full suites pass 323 cases. Persistent outboxes and automatic sweepers
are not established by these cases.

A shared factory keeps default and custom endpoint mappings independent in both
creation orders and API modes. Native publication bodies, keys and wire IDs remain
separate, while confirmations retain source IDs. Consumers in independent groups
retain their own default/selected mapping, receive real native offsets and flush
acknowledgements independently on disposal. The
[eight isolation cases](review/kafka-factory-isolation/README.md) pass without
production changes; eight mutations produce 32 intended failures and both complete
suites pass 331 cases. Concurrent calls and configuration changes after construction
remain separate contracts.

Duplicate and null-valued custom headers now retain the native payload, parsed
identity/topic/key/count/trace, offset and readable entries before and after the error.
The default reader preserves its existing unacceptable classification. The first
successfully read duplicate value remains in the bag; no representation is introduced
for null values. The protected virtual custom-header hook is unchanged. The
[sixteen custom-header cases](review/kafka-custom-header-failures/README.md) pass on
both frameworks, ten mutations produce 144 intended failures, and both complete
suites pass 347 cases. Other parsing paths may still use the outer failure fallback.

Set `KafkaSubscription.InvalidMessageWireMessageWriter` to map deliveries forwarded
to an explicitly configured invalid-message channel. It accepts the existing
`IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>` contract. The factory
captures the selection per consumer, and the lazy invalid-channel producer invokes
it after rejection metadata is populated. The writer may replace the wire format or
compose `DefaultKafkaWireMessageWriter`. Null retains default serialization; public
consumer constructors remain unchanged. This setting does not select publication,
retry or dead-letter writers.

The gateway retains destination and source-offset ownership. Reject/RejectAsync
restore the source topic in finally, including for default rejection. The
[four selected-writer cases](review/kafka-invalid-writer/README.md) verify native
body/key/wire identity, original-topic/reason/tenant mapping and committed source
offsets after disposal. Nine mutations produce 34 intended failures; both full
suites pass 351 cases. Reject/RejectAsync now return false and restore the captured
source offset when forwarding throws, without buffering its acknowledgement.
The [four writer-recovery cases](review/kafka-invalid-writer-recovery/README.md)
verify no invalid publication or source commit after failure and successful
redelivery/forwarding after repairing a throwing/null-returning writer. Eight
mutations produce 32 intended failures; both full suites pass 355 cases.

Reject/RejectAsync now wait for a confirmation tied to the individual send before
acknowledging the source. A negative broker confirmation returns false and preserves
the source offset, as do send/wait exceptions. Confirmation waits are bounded to five
seconds; the async path also links caller cancellation. Public send/delay signatures
and normal queued publication-confirmation behavior remain unchanged.
The [two broker-failure cases](review/kafka-invalid-broker-recovery/README.md) verify
native size rejection, no failed-source commit and recovery after reducing the
selected writer's output. Eight mutations produce 16 intended failures; both full
suites pass 357 cases. Missing-confirmation timeout, cancellation and other rejection
routes need separate coverage. Forwarding and acknowledgement are not atomic.
The [four offset-barrier cases](review/kafka-failed-forward-offset-barrier/README.md)
verify that a later acknowledgement after a failed forward cannot commit past that
failed source record, including during disposal. Commits are capped at the earliest
failed source offset per partition. The subsequent
[four same-consumer recovery cases](review/kafka-failed-forward-same-consumer-recovery/README.md)
verify that successful retry releases the matching limit and preserves later
acknowledgements through final disposal. Affected partitions remember the highest
acknowledgement observed by the commit reducer, including offsets beyond a pending
failure, after those offsets have been removed from the pending bag. Eight mutations produce 32 intended failures; both full suites
pass 365 cases for that single-failure batch. The subsequent
[four multiple-failure cases](review/kafka-multiple-failed-forwards/README.md) verify
partial recovery in either order without skipping unresolved records. Every failed
native offset is retained per partition, successful retry removes only its matching
offset, and commit reduction stops at the earliest remaining failure. Later
acknowledgements remain available after recovery. Eight mutations produce 32 intended
failures; both full suites pass 369 cases at that checkpoint. The
[four partition-isolation cases](review/kafka-partition-forward-isolation/README.md)
are validated, followed by native in-flight forwarding, cancellation, confirmation
timeout, broker recovery and selected retry/dead-letter cases. The current full
matrix has 412 passes per framework. These results do not resolve the retained
[native close stall](review/kafka-native-close-diagnostic/README.md).
Forwarding and acknowledgement remain separate operations; a false rejection
result does not itself schedule an automatic pump retry. The gateway exports no
bulk-send or transaction lifecycle; the [applicability review](review/kafka-default-body-ownership/README.md#batch-and-transaction-applicability)
records that boundary. Kafka and global readiness remain subject to the completion plan.

## SQL Server rejection mappings

Set `MsSqlSubscription.DeadLetterWireMessageWriter` and
`InvalidMessageWireMessageWriter` before creating the consumer to select its
consumer-created forwarding producers. Each setting defaults to the standard JSON
writer and is captured at consumer creation. The configured dead-letter or invalid
routing key still determines the native destination. A writer must preserve any
identity, rejection metadata or trace fields required by its receiving reader.

SQL Server's queue atomically reads and deletes a row on receipt. If a forwarding
writer throws or returns null, no destination row is inserted; the existing consumer
logs the failure and settles rejection. That original row cannot be retried after
receipt. A recovered writer can handle subsequent messages. This mapping option does
not add transactional receive-and-forward or restore already deleted source rows.

For scheduler-backed retries, set
`MsSqlSubscription.ScheduledRequeuePublicationTopic` to a registered publication's
`Topic`. Configure the retry writer on that publication. This selects dispatch
configuration while retaining the message's logical topic as its native SQL
queue destination. Consumers capture the setting on creation and use a private
message copy for scheduling. Leaving it unset preserves the existing selector;
zero-delay retries continue to use `RequeueWireMessageWriter`.

## PostgreSQL rejection mappings

Set `PostgresSubscription.DeadLetterWireMessageWriter` and
`InvalidMessageWireMessageWriter` before creating the consumer. Each selection is
captured before its lazy forwarding producer is created and defaults to standard
JSON. Writers must return valid JSON for the configured JSON/JSONB column.

Rejection retains its existing finally-block deletion of the source row, including
when a forwarding writer throws or returns null. The failure is logged and rejection
settles; a recovered writer can process subsequent messages. This option does not
make receive-and-forward transactional or restore the deleted source message.

### RocketMQ rejection forwarding

Set `DeadLetterWireMessageWriter` and `InvalidMessageWireMessageWriter` on
`RocketSubscription` to select the writer for the corresponding consumer-created
forwarding producer. Selection is captured when the consumer is created. Null
configuration retains the default writer; a configured writer returning null is
a mapping failure.

RocketMQ's existing rejection flow acknowledges the original even when forwarding
fails. These properties do not provide recovery of that acknowledged message.
The native acceptance cases verify recovery with a fresh subsequent message.

### MQTT consumer-created retry and rejection publications

`MqttSubscription.RequeueWireMessageWriter`, `DeadLetterWireMessageWriter` and
`InvalidMessageWireMessageWriter` select the writers for the consumer's immediate
retry and rejection producers. The factory captures each selection; null preserves
the default JSON format. Immediate retry mapping failure propagates, allowing the
caller to retry the same in-memory message. Rejection preserves its existing behavior:
forwarding errors are logged and the call returns true. Native protocol acknowledgement
is owned by MQTTnet, so a failed rejection does not promise recovery of the original.

For a positive scheduled retry, set `ScheduledRequeuePublicationTopic` to a registered
publication lookup topic. Register that publication with the original CloudEvents type
and the same native topic prefix. The callback resolves the registered producer and
its writer; `RequeueWireMessageWriter` applies only to immediate retry. The selector
is placed on a private message copy. Logical topic, caller payload and metadata remain
unchanged, and dispatch creates fresh telemetry. A null selector retains ordinary
publication lookup. See the native scheduled retry test for an executable example.

### Kafka dead-letter and scheduled retry mapping

`KafkaSubscription.DeadLetterWireMessageWriter` selects the consumer-created
DLQ producer independently of invalid-message and immediate retry writers. The
factory captures it before lazy producer creation. Null preserves the default wire
format. Mapping failure returns false, leaves the source offset uncommitted and
allows redelivery/recovery through the existing confirmed-forwarding path.

`KafkaSubscription.ScheduledRequeuePublicationTopic` selects a registered publication
for positive-delay retries. Register that topic and the original CloudEvents type
with the desired writer using the typed producer registry. The selector is local to
a copied scheduled message; native Kafka publication still uses the original logical
topic. Immediate retries use `RequeueWireMessageWriter`. Null retains ordinary lookup.
Existing resend bookkeeping clears native offset/header duplicates; after successful
schedule registration the original offset is acknowledged. This is not a durable
scheduler or atomic broker/outbox guarantee. Callback dispatch adds fresh telemetry.

### GCP Brighter-managed rejection mapping

`GcpPubSubSubscription.DeadLetterWireMessageWriter` and
`InvalidMessageWireMessageWriter` select independent writers for Brighter-created
rejection publications. Both pull and streaming factories capture these values.
A null selection preserves default mapping. When no invalid-message destination is
configured, unacceptable messages use the dead-letter destination and its writer.
When both routes share a topic, their cached producers still retain separate writers.

The writer receives stamped rejection metadata. A throwing/null writer preserves
the existing failure policy: pull releases the original ack ID and streaming nacks
its callback. Recovery receives the original native delivery again. This does not
make forwarding and acknowledgement atomic. Native broker dead-letter policies and
visibility/nack retries do not execute these Brighter-created publication writers.

## Azure Service Bus queue retry mapping

For `RequeueSchedulerMode.Native`, configure the `AzureServiceBusPublication`
passed to `AzureServiceBusConsumerFactory` with `WireMessageWriter`. The factory
captures that writer when creating each queue/forwarding consumer. For
`RequeueSchedulerMode.Global` with the Azure scheduler, configure
`AzureServiceBusSchedulerFactory.RequeueWireMessageWriter` before creating the
scheduler. A directly constructed `AzureServiceBusRequeueScheduler` accepts an
init-only `WireMessageWriter`. These settings accept
`IAmAWireMessageWriter<ServiceBusMessage>`; a reader does not imply a writer.

Mapping receives a private message copy after current retry trace propagation.
The selected writer owns body and application metadata; the scheduler restores
deterministic retry ID, original application/CloudEvents identity, source session
and partition, and handled count. A throwing or null writer result fails before
creating the retry sender or completing the source. The same channel can retry
successfully after the writer recovers. Completion follows accepted scheduling.

Leaving the retry writer null preserves the existing mapping and original binary
body. Explicit default-writer composition follows that writer's body encoding.
Global scheduler callback envelopes retain their existing serialization. Direct
topic abandon and native dead-letter operations retain broker-owned content.

Twelve public-factory cases pass on .NET 9 and .NET 10 using in-memory SDK I/O.
Hosted scheduled delivery/session validation remains an open acceptance gate;
these tests do not establish native broker timing or duplicate detection.

## GCP and RocketMQ delayed publication

GCP delayed sends use the configured scheduler and return through ordinary producer
lookup at callback time. Register the publication under the message topic and
CloudEvents type; its selected writer runs with the actual dispatch telemetry.

RocketMQ DELAY topics use native broker scheduling and map before submission.
Other topic types delegate positive delays to the configured scheduler and map when
the callback dispatches. Both paths preserve native destination, keys and tags.
The new native tests cover default and composing writers through both sending modes.
These timer-based observations do not establish durable storage or automatic recovery
for an arbitrary scheduler configuration.
