# Customize native message formats

Local draft for Brighter Docs. Endpoint mapping is implemented across the gateways
listed below. Full-feature validation and remaining scheduler/forwarding paths are
in progress; this page has not been published. Current evidence and limitations are
tracked in the [completion plan](../completion-plan.md).

Use a wire writer when another system needs a different native body or header
layout. Use a wire reader when a publisher sends a format Brighter does not
normally understand. Application message mappers still convert between application
requests and Brighter messages.

Leave the settings unset to use the default Brighter format.

| Gateway | Outgoing setting | Incoming setting |
| --- | --- | --- |
| AWS SDK V3/V4 SQS | `SqsPublication.WireMessageWriter` | `SqsSubscription.WireMessageReader` |
| AWS SDK V3/V4 SNS | `SnsPublication.WireMessageWriter` | Read through the receiving SQS subscription |
| RabbitMQ Async | `RmqPublication.WireMessageWriter` | `RmqSubscription.WireMessageReader` |
| RabbitMQ Sync | `RmqPublication.WireMessageWriter` | `RmqSubscription.WireMessageReader` |
| Kafka | `KafkaPublication.WireMessageWriter` | `KafkaSubscription.WireMessageReader` |
| Azure Service Bus | `AzureServiceBusPublication.WireMessageWriter` | `AzureServiceBusSubscription.WireMessageReader` |
| GCP Pub/Sub | `GcpPublication.WireMessageWriter` | `GcpPubSubSubscription.PullWireMessageReader` / `StreamWireMessageReader` |
| MQTT | `MqttPublication.WireMessageWriter` | `MqttSubscription.WireMessageReader` |
| Redis | `RedisMessagePublication.WireMessageWriter` | `RedisSubscription.WireMessageReader` |
| RocketMQ | `RocketMqPublication.WireMessageWriter` | `RocketSubscription.WireMessageReader` |
| SQL Server | `MsSqlPublication.WireMessageWriter` | `MsSqlSubscription.WireMessageReader` |
| PostgreSQL | `PostgresPublication.WireMessageWriter` | `PostgresSubscription.WireMessageReader` |

The AWS variants use their respective SDK-native types and namespaces:
`Paramore.Brighter.MessagingGateway.AWSSQS` for V3 and
`Paramore.Brighter.MessagingGateway.AWSSQS.V4` for V4. RabbitMQ Sync uses its
`Paramore.Brighter.MessagingGateway.RMQ.Sync` namespace and a callback-based outgoing
properties contract described below. Configure settings before creating producers
and consumers.

## Replace the SQS format

A writer returns application content. The gateway supplies the destination,
delay and FIFO controls separately.

```csharp
using Paramore.Brighter;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;

public sealed class LegacyOrderWriter : IAmAWireMessageWriter<SqsWireMessage>
{
    public SqsWireMessage WriteMessage(Message message) => new()
    {
        Body = message.Body.Value,
        MessageAttributes = new()
        {
            ["legacy-id"] = new()
            {
                DataType = "String",
                StringValue = message.Id.Value
            },
            ["tenant-id"] = new()
            {
                DataType = "String",
                StringValue = (string)message.Header.Bag["tenant_id"]
            }
        }
    };
}
```

Set `WireMessageWriter = new LegacyOrderWriter()` on an `SqsPublication`.
The example requires a string `tenant_id` bag entry and emits only the two
application attributes shown. Add any trace fields that the receiving system
requires, reading them from the supplied message when `WriteMessage` runs.

For SNS, implement `IAmAWireMessageWriter<SnsWireMessage>`. Its result contains
`Body`, optional `Subject` and SNS-native `MessageAttributes`. SNS and SQS use
different SDK attribute types.

## Mapping telemetry for a non-CloudEvents receiver

Read `message.Header.TraceParent?.Value`, `message.Header.TraceState?.Value` and
`message.Header.Baggage.ToString()` inside `WriteMessage`. For example, store them
in native string attributes named `telemetry-parent`, `telemetry-state` and
`telemetry-baggage`, or in a JSON body with a separate application payload field.
A complete replacement writer can omit CloudEvents attributes entirely.

The matching reader sets `MessageHeader.TraceParent` and `TraceState`, then loads
the received baggage string with `header.Baggage.LoadBaggage(value)`. Configure
both ends to use the same representation and define missing-field behavior for
your external system. Preserve application identity as part of that contract.

Take these values from the message at dispatch. Do not capture a deposit-time
trace in the converter constructor: outbox clearing can create a different publish
span. The consumer attaches receipt metadata independently of your reader, and
Brighter's tracing uses the restored message headers.

For SNS, select the writer on `SnsPublication` and the reader on the receiving
`SqsSubscription`. Match the reader to the SNS subscription's raw-delivery setting.
With raw delivery disabled, your reader receives an SNS envelope containing the
inner message and attributes and must interpret it before restoring telemetry.
The consumer still owns the outer delivery receipt. Local tests cover both wire
locations, raw/enveloped delivery and real Reactor/Proactor handlers in both AWS
SDKs, including native acknowledgement after processing.

For RabbitMQ Async, configure the writer on `RmqPublication.WireMessageWriter`
and the paired reader on `RmqSubscription.WireMessageReader`. Custom AMQP headers
can carry UTF-8 telemetry bytes, or a JSON body can carry telemetry beside the
application text payload. Restore all three telemetry fields before returning the
message from the reader. Native delivery controls and the SDK confirmation
sequence remain gateway-owned. Local tests verify independent native observation,
Reactor/Proactor handler context and acknowledgement after closing the consumer.
Scheduler-backed retry mapping remains separate unfinished work.

## Change one part of the default format

Compose a default writer when the receiver understands Brighter's format and
only a small change is needed:

```csharp
public sealed class TenantAttributeWriter(string queueUrl)
    : IAmAWireMessageWriter<SqsWireMessage>
{
    private readonly DefaultSqsWireMessageWriter _defaultWriter = new(queueUrl);

    public SqsWireMessage WriteMessage(Message message)
    {
        var wire = _defaultWriter.WriteMessage(message);
        wire.MessageAttributes["tenant-id"] = new()
        {
            DataType = "String",
            StringValue = (string)message.Header.Bag["tenant_id"]
        };
        return wire;
    }
}
```

The queue URL supplies the default `topic` attribute. The publication still owns
the destination. This example adds an attribute while keeping the existing bag.
Check the provider's attribute limits before adding fields.

`DefaultSnsWireMessageWriter(topicArn)` and `DefaultRmqWireMessageWriter` provide
the same composition approach for their respective transports.

## Read a legacy SQS message

```csharp
public sealed class LegacyOrderReader(RoutingKey topic)
    : IAmAWireMessageReader<Amazon.SQS.Model.Message>
{
    public Message ReadMessage(Amazon.SQS.Model.Message native)
    {
        var header = new MessageHeader(
            new Id(native.MessageAttributes["legacy-id"].StringValue),
            topic,
            MessageType.MT_EVENT);
        header.Bag["tenant_id"] = native.MessageAttributes["tenant-id"].StringValue;
        return new Message(header, new MessageBody(native.Body));
    }
}
```

Set `WireMessageReader = new LegacyOrderReader(new RoutingKey("orders"))` on the
SQS subscription. The consumer attaches the actual receipt handle and resolves
the delivery count. The reader maps application content.

When SNS raw delivery is disabled, a custom reader receives the SNS envelope
inside the original SQS body. Compose
`DefaultSqsWireMessageReader(rawMessageDelivery: false)` to reuse envelope parsing.
Use `true` for raw body and attributes.

## AWS invalid-message and dead-letter formats

Set `SqsSubscription.InvalidMessageWireMessageWriter` or
`SqsSubscription.DeadLetterWireMessageWriter` before creating the consumer.
Both accept the corresponding SDK's `IAmAWireMessageWriter<SqsWireMessage>`.
An unset setting retains the default format for that route.

The gateway supplies refreshed rejection metadata and selects the native
destination. Invalid-message fallback to a dead-letter destination uses the
dead-letter writer. These settings do not create routes or alter broker-managed
redrive. The reader at the destination must understand the selected format.

If the writer throws or returns null, SQS rejection handling releases the source
for redelivery and publishes no default fallback. The existing `Reject` return
value is true after that failure is handled; it does not prove forwarding
succeeded. A fresh delivery can be forwarded after writer recovery.

Standard and FIFO routing/recovery have integration coverage against the AWS
emulator. FIFO group and deduplication IDs are retained, and consumers from one
factory retain their own writer selections. Publication and source deletion are
separate operations; these tests do not establish all managed-service failure modes.

## RabbitMQ Async content and retry mapping

A RabbitMQ writer implements `IAmAWireMessageWriter<RmqWireMessage>`.
Return fresh `RabbitMQ.Client.BasicProperties` and a `ReadOnlyMemory<byte>` body.
The gateway retains routing, persistence, delay and confirmation tracking.
A custom wire message ID does not replace the Brighter ID used for confirmation.
RabbitMQ.Client also replaces a writer-supplied publish-sequence header with its
own confirmation sequence number.

A RabbitMQ reader implements
`IAmAWireMessageReader<RabbitMQ.Client.Events.BasicDeliverEventArgs>`.
Compose `DefaultRmqWireMessageReader` to reuse default parsing. The consumer
supplies the actual delivery tag, redelivery flag and persistence.
When a canonical native handled-count header exists, its parsed value overrides
the reader's count. Without that header, the reader's count is retained.

Set `RmqSubscription.RequeueWireMessageWriter` to customize direct consumer
republishing for immediate or native delayed retries. This is independent of
the producer publication's writer. The gateway generates retry identity and
applies handled count, original identity, persistence and delay.

The subscription retry writer does not configure scheduler-backed retries or
invalid-message forwarding. Scheduler-backed retries retain their existing mapping.
Ensure the subscription reader understands the format produced by direct retries.

## RabbitMQ Async invalid-message formats

Set `RmqSubscription.InvalidMessageWireMessageWriter` to customize forwarding to
`InvalidMessageRoutingKey`. It uses the same `RmqWireMessage` contract as ordinary
publication and direct retry writing, but its selection is independent. Configure
it before creating consumers. Null retains the default format.

The writer receives rejection metadata and the invalid destination topic. The
gateway retains routing, persistence, delivery controls and mandatory confirmed
publication. A mapping or publication failure propagates an exception, restores
the source application topic and leaves the original unacknowledged. A recovered
writer or route can forward a fresh delivery. Publication and acknowledgement
remain separate operations, so duplicates are possible after acknowledgement
failure.

Broker-managed dead-lettering does not invoke this writer. Delivery errors, and
unacceptable messages without an invalid route, follow the queue's native DLX
configuration and retain the original wire representation.

## Scheduling scope

Direct producer sends use the configured writer, including transport-native
delays supported by that producer. SQS and SNS dispatch through Brighter’s
in-memory scheduler have native-wire validation, including dispatch-time trace
propagation. Quartz message/request callbacks also have native RabbitMQ Async
validation through the configured publication writer, covering both sending modes,
custom headers/JSON body, original payload/identity, native controls and actual
producer traceparent. The application scheduling objects remain unchanged.
Quartz writer failures also have native recovery coverage: throwing/null writers
leave original outbox data pending, then explicit clearing produces one confirmed
publication with a recovery activity → clear span → publish span trace chain.
This recovery evidence uses in-memory stores and a positive delay. Separate process-restart
probes validate Hangfire/PostgreSQL, Quartz/PostgreSQL JobStoreTX/System.Text.Json and
TickerQ/EF Core SQLite. Those results do not establish other storage configurations,
arbitrary crash recovery or automatic business retry policies. The separate zero-delay
Quartz timestamp rejection is unchanged.

The AWS scheduler's `UseMessageTopicAsTarget` mode serializes an SNS or SQS
request itself. It does not use the publication's wire writer. Custom wire
mapping is not currently supported on that native scheduler target path.

Callback mode (`UseMessageTopicAsTarget = false`) routes to the configured
`SchedulerTopicOrQueue`, which can differ from the business destination. The
callback retains the original message for later application dispatch. Stored
destination selection and native callback-to-writer dispatch are validated in
both SDKs. The callback queue uses Brighter's default wire format and
`AwsSchedulerFiredMapper`/`AwsSchedulerFiredHandler`; the contained message then
passes through `FireSchedulerMessageHandler` and the command processor to the
business publication's selected writer. That writer sees the traceparent,
tracestate and baggage of the actual business dispatch.

Native emulator callback-pump tests also run the public mapper, ServiceActivator
Proactor and scheduler handlers. A business writer failure leaves the original
outbox entry pending while the callback queue is settled; explicit outbox clearing
recovers it through the selected writer with fresh telemetry. This is not automatic
business redelivery or hosted AWS IAM validation.

Native scheduled messages use invariant round-trip timestamp text to preserve
fractional seconds. Consumers must accept that ISO representation rather than
depending on the previous culture-specific formatting.

## Mapping lifetime and failures

Reader and writer instances may be shared by concurrent operations. Keep
per-message state in local variables and return independent results. Do not
mutate the supplied message or native delivery.

Default RabbitMQ writing retains the source body's read-only memory. Allocate
replacement bytes when changing the body. Likewise, replace mutable nested
header values instead of modifying objects shared with the source.

Mapping errors during ordinary publication fail the send. A throwing or
null-returning custom reader produces an unacceptable message retaining native
payload and delivery metadata for rejection.

If a direct RabbitMQ retry writer fails, requeue reports false and preserves
the original delivery. A later attempt can succeed after the mapping is repaired.
Forwarding and source acknowledgement remain separate operations; applications
must tolerate redelivery.


## RabbitMQ Sync endpoint mapping

The Sync package uses RabbitMQ Client 6. Its RmqWireMessage contains the body and
an optional `Action<IBasicProperties> ConfigureProperties` callback. The gateway
creates native properties through its existing channel and calls the callback once
before applying persistence and delivery controls. Fill only the supplied object;
do not retain it. Return fresh output for each mapping.

To compose DefaultRmqWireMessageWriter, capture its ConfigureProperties callback
and invoke it before adding custom fields in your own callback. For replacement,
return a new wire body and callback. Configure the reader through
RmqSubscription.WireMessageReader; DefaultRmqWireMessageReader supports composition.
Existing consumer constructors remain compatible. The Task-based producer send
wrapper is supported; async factories/consumers remain unsupported in this package.

Six endpoint cases validate both frameworks with native broker observation. Four
additional cases verify that a configured reader exception/null result becomes an
unacceptable delivery with native identity, exact body and acknowledgement metadata,
while the other member of a mixed batch remains available. Both deliveries can be
acknowledged after mapping. Four further cases cover default-parser failures for
unset/explicit readers in either batch position: the failed message retains native
bytes, delivery metadata, handled count and the ID/topic parsed before failure.
Explicit custom unacceptable results retain their mapped ID, topic, body and bag;
the gateway still applies native delivery controls. Forwarding cases cover those
results and parser/reader failures, including repairing an unbound invalid route
and recovering the original delivery through a new consumer. The source-absence
verification now observes for two seconds after disposal and detects all ten
omitted acknowledgements in seven mutation runs across both frameworks. This is
bounded evidence, not atomic forwarding and acknowledgement. Sync
`RmqSubscription.InvalidMessageWireMessageWriter` selects a custom writer for
unacceptable-message forwarding; null preserves the default. Writers may replace
or compose the default format and receive rejection metadata before mapping. The
factory captures the choice per consumer. The gateway preserves routing,
persistence and confirmation/acknowledgement controls, replaces writer-owned tags
with the native message tag, and restores the original topic. Writer throw/null,
callback failure and unbound-route recovery are covered. Native dead-lettering
bypasses this writer and preserves original wire content. Publication writer
throw/null and callback failure leave outbox messages pending; healthy messages
retain correct confirmation identity and repaired mapping permits recovery of the
original pending message. These cases use a real producer and in-memory outbox
through sync and compatibility async APIs. `RmqSubscription.RequeueWireMessageWriter` selects direct retry mapping per consumer;
null preserves the default. The gateway owns routing, persistence, fresh native ID,
handled count, delivery/delay metadata and original-ID preservation. Immediate
retry replacement/composition, mapping-failure recovery and independent consumer
selection are covered. Source values remain unchanged by gateway mapping. Custom
readers must understand the selected representation. Publication and acknowledgement
are separate operations; consumers must tolerate duplicates. Scheduler-backed retries
retain scheduler publication mapping. Native delayed retry replacement/composition and mapping-failure recovery are
covered with the native plugin and direct topology. Requested delay and source-queue
routing override writer values; a neighbor on the same application topic receives
no retry during bounded observation. Eight concurrent mapping cases pass on both frameworks, exercising shared safe
strategies across separate endpoints and preserving the producer send lock. Twelve named mutations produce 34 intended failures, including a completed-properties
cache mutation. All eight cases and both full Sync suites pass after restoration.
Scheduler-backed retry now captures `ScheduledRequeuePublicationTopic` for registered
publication selection while retaining the source queue destination. Native callbacks
validate that selection in both RabbitMQ packages. These scoped tests do not establish
whole-feature production readiness. The local Kafka default adapters and endpoint selections are implemented.
`KafkaPublication.WireMessageWriter` selects native key/body/header mapping after
dispatch tracing. Omitting it preserves the existing MessageHeaderBuilder.
`KafkaSubscription.WireMessageReader` selects application mapping; the gateway
supplies the actual offset for acknowledgement. Selected-writer confirmations retain
the source identity. Both complete Kafka suites pass 277 cases; the endpoint batch
adds eight cases and eight mutations with 32 intended failures. Configured reader
exceptions and null results now produce unacceptable messages retaining native body,
topic, key and offset, plus readable default metadata. Good deliveries continue
through the configured reader. Eight reader-failure cases and nine mutations with
72 intended failures validate this path; both complete suites now pass 285 cases.
Required-header validation failures also retain the native body, readable metadata,
custom headers and offset, for both unset and explicit default readers. Eight more
cases and eight mutations with 60 intended failures validate this path; both full
suites pass 293 cases with no failures/skips. MIME parsing exceptions now retain the
native delivery and independently readable metadata while leaving the result unacceptable.
Missing content type keeps its existing default. Eight additional cases and nine mutations
with 68 intended failures validate this recovery; both full suites pass 301 cases.
Explicit unacceptable results from a selected reader retain its application mapping,
while the gateway normalizes the native offset. Eight characterisation cases and ten
mutations with 80 intended failures validate this behavior without production changes;
both full suites pass 309 cases with no failures/skips. Selected writer exceptions
and null results surface a send error without publishing that attempt. The producer
remains usable; the original message can be retried after repairing the writer, and
confirmations retain source identity. Eight characterisation cases and nine mutations
with 72 intended failures validate direct caller-driven recovery without production
changes; both full suites pass 317 cases with no failures/skips. Outbox clearing
retains the original pending entry after writer retry exhaustion and can recover it
after the writer is repaired. A throwing confirmation subscriber before the mediator
can leave an already-published entry pending; recovery republishes it, allowing duplicate
delivery. Later healthy confirmations still work. Six cases with an in-memory outbox
and real Kafka, plus seven mutations with 40 intended failures, validate these paths
without production changes; both full suites pass 323 cases. Shared factories also
keep default and custom publication/subscription mappings independent in both creation
orders and API modes, including source confirmation IDs, native offsets and separate
group acknowledgement commits. Eight isolation cases and eight mutations producing
32 intended failures validate this without production changes; both complete suites
pass 331 cases. Duplicate/null-valued custom-header failures now preserve payload,
parsed metadata, native offset and readable entries before and after the error while
retaining the default reader's unacceptable classification. The first readable
duplicate value remains. Sixteen cases and ten mutations producing 144 intended
failures validate this; both complete suites pass 347 cases.
`KafkaSubscription.InvalidMessageWireMessageWriter` now selects a writer for an
explicit invalid-message channel. Rejection metadata is supplied before mapping;
the gateway retains destination/source acknowledgement and restores the source
topic afterward. Null preserves default serialization; retry and dead-letter writers
are separate. Four successful-forwarding cases and nine mutations producing 34
intended failures validate this path; both full suites pass 351 cases. Forwarding
exceptions now return false and restore the source offset without acknowledgement.
Four additional writer-recovery cases and eight mutations producing 32 intended
failures verify no commit on mapping failure and redelivery after repair; both full
suites pass 355 cases. Rejection also now awaits a per-send persistence confirmation
before acknowledging, with a five-second confirmation bound. Negative confirmations
return false and preserve the source offset. Two broker-size-rejection recovery
cases and eight mutations producing 16 intended failures validate this path; both
full suites pass 357 cases for that batch. The subsequent
[offset-barrier batch](../review/kafka-failed-forward-offset-barrier/README.md) prevents
sequential later acknowledgements from committing past the earliest failed forward
on a partition, including at disposal. Four cases and eight mutations producing 32
intended failures validate recreation-based recovery; both full suites pass 361 cases.
The subsequent [same-consumer recovery batch](../review/kafka-failed-forward-same-consumer-recovery/README.md)
releases the matching limit after successful retry and retains later acknowledgements.
Affected partitions remember the highest acknowledgement observed during commit
reduction. Four cases and eight mutations producing 32 intended failures validate
single-failure recovery; both full suites pass 365 cases at that checkpoint. The
[multiple-failure batch](../review/kafka-multiple-failed-forwards/README.md) now retains
every outstanding failed native offset per partition and releases only successful
retries. Commit reduction retains later acknowledgements but stops at the earliest
remaining failure. Four cases and eight mutations producing 32 intended failures
verify partial recovery in either order; both full suites pass 369 cases at that
checkpoint. Subsequent partition-isolation, in-flight forwarding, cancellation,
confirmation-timeout, broker-recovery and selected retry/dead-letter packages bring
the current matrix to 412 passes per framework. The retained native close stall
remains unresolved. Forwarding and acknowledgement are not atomic, and a false
rejection result does not itself schedule automatic pump recovery. No bulk-send or
transaction lifecycle is exported by this gateway. Persistent-outbox and arbitrary
crash-recovery behavior is not established by these scoped checks. These local drafts
are not published documentation.

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
