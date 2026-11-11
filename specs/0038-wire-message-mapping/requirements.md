# Wire message mapping requirements

## Active delivery scope

The user requires full implementation of #4170. The earlier AWS SDK V4/RabbitMQ
Async partial boundary is superseded as a completion criterion. All gateway rows
and applicable dispatch/retry/forwarding paths below remain required. Existing
validated work is preserved; exclusions describe current limitations to resolve,
not permission to finish with missing integrations.

The motivating acceptance scenario must move dispatch-time traceparent, tracestate
and baggage to custom attributes or the body, restore them through a configured
reader, and preserve consumer tracing and settlement. Default wire formats remain
compatible. The issue prefers a mapping DSL but allows alternatives; configuration
usability and the final design rationale must be reviewed before completion.

## Behavior

1. Each gateway accepts a custom wire writer on its publication and a custom wire reader on its subscription. Selection is per endpoint, not global mutable state.
2. Omitted configuration preserves existing wire formats, parsing fallbacks, public constructors, and supported sync and async operations.
3. Applications can implement a complete mapping or reuse the default mapping and alter selected fields. Custom readers can consume messages that the default reader cannot parse.
4. Writers control application metadata and body representation. Readers restore Brighter headers and body before tracing extraction and the application mapper run.
5. Writing occurs after producer trace propagation, on actual dispatch from the outbox or scheduler. Mapping must not bake dispatch-time tracing into the deposited message.
6. Gateways retain transport operations: destination resolution, acknowledgement tokens, delivery counts, retry bookkeeping, confirmation correlation, scheduling, and connection management. A custom mapping must not accidentally disable these operations.
7. A writer creates a fresh native representation without mutating the source body or bag. Readers and writers shared across messages must not store per-message mutable state on an instance.
8. Outbound mapping failures fail the send and leave the outbox entry undispatched. Inbound mapping failures follow each gateway's invalid-message path while retaining the delivery identity needed for rejection or acknowledgement.
9. Preserve provider-specific limits and validation. Optional custom fields cannot silently overwrite gateway-owned control values or be silently dropped to fit a limit.
10. Apply the feature to all gateway rows below. Completion of AWS alone does not complete this specification.

## Shared contracts established by the first increment

These synchronous contracts perform in-memory conversion; the producer and consumer continue to own asynchronous I/O. The AWS SDK V4 and RabbitMQ Async implementations validate these names and carrier types for the approved first increment.

```csharp
public interface IAmAWireMessageWriter<out TWireMessage>
{
    TWireMessage WriteMessage(Message message);
}

public interface IAmAWireMessageReader<in TWireMessage>
{
    Message ReadMessage(TWireMessage message);
}
```

The contracts belong in `Paramore.Brighter` and have no broker SDK dependencies. Concrete publication and subscription properties use the appropriate closed generic type. Keep the existing nongeneric base configuration types compatible; introduce typed configuration in gateways that currently accept only `Publication` where needed.

Use native SDK payload types where they separate content from delivery operations. Use a small gateway-specific carrier where the SDK combines payload with an operation or splits payload across arguments. For SQS, `SqsWireMessage` contains `Body` and `MessageAttributes`; `SqsMessageSender` adds queue URL, delay, and FIFO controls. Default writers can receive immutable transport configuration at construction, including the queue URL needed by the existing SQS topic attribute.

Default implementations should support composition. The first default SQS writer is sealed; applications alter its fresh result rather than override private serialization helpers. Avoid making every existing parsing helper public. Existing static conversion exports should delegate to default implementations without breaking their callers.

## Current SQS reader composition design

`DefaultSqsWireMessageReader(bool rawMessageDelivery = true)` exposes the existing raw/SNS-envelope parsers through the shared reader contract. Callers compose the returned message to customize selected fields. The ordinary no-custom-reader path uses the same implementation, preserving current fallback behavior. Each read creates a new internal parser because the envelope parser retains per-message attributes; a reusable reader must not share that state across deliveries. Parser helpers remain internal.

The custom-reader consumer boundary still owns receipt/count metadata and converts thrown/null results into rejectable failures. Raw and SNS-envelope composition have dedicated coverage through the consumer factory and native SDK observation. The default-reader boundary enriches parser failures with native payload and delivery metadata, falling back to the native ID when no application ID was parsed. Malformed-envelope rejection has dedicated coverage through both ordinary and explicitly configured readers. Successfully parsed messages already carry receipt metadata and retain their parsed fields, including routed rejection messages. Public constructor signatures are unchanged.

## RabbitMQ Async reader boundary

`RmqSubscription.WireMessageReader` optionally accepts
`IAmAWireMessageReader<RabbitMQ.Client.Events.BasicDeliverEventArgs>`. Both public
consumer-factory paths carry the subscription selection to the consumer.
Existing public consumer constructors retain their signatures and default
behavior; an internal constructor carries the immutable reader selection.
A null selection uses the shared default parser in `RmqMessageCreator`.

The native delivery already contains content and metadata, so no new carrier
is needed. `PullConsumer` already copies the SDK's pooled body before buffering
it; that ownership remains unchanged. Conversion is synchronous, must not
mutate the native delivery and must return a fresh Brighter message. The
consumer stamps `DeliveryTag`, `Redelivered` and `Persist` from the broker after
custom mapping. It also attaches the canonical handled count when that native
header exists, using the default parser's rules. Without the header, the reader's
count remains. A reader cannot replace acknowledgement identity or delivery
state with fabricated values. Application identity, body, content type, topic
and bag values remain reader responsibilities.

Successful legacy mapping and acknowledgement work through real RabbitMQ in
sync/async paths. Throwing or null-returning custom readers produce unacceptable
messages with native identity, routing key, exact body bytes and broker delivery
metadata. Rejection without an invalid-message route settles the original
delivery. With an invalid-message route, rejection forwards exact payload bytes
and native identity with rejection metadata, then acknowledges the source.
If forwarding fails because the route is missing, the source remains available
with exact bytes and identity. The original topic is restored, and forwarding
can be retried after repairing the route. Mixed batches retain both valid and failed custom mappings in delivery
order, with independent broker tags. Default-parser failures preserve the native
canonical handled count as well as raw body
bytes and already parsed application ID/topic, and receive broker delivery
metadata for rejection. Invalid forwarding preserves those exact bytes and
records the parsed application topic in the original-topic header, even when
it differs from the broker routing key. This holds for both unconfigured and
explicit default readers. If invalid forwarding fails, the original default-parser
delivery retains its malformed properties, body and application-topic header.
Repairing the route allows a new consumer to parse the same failure and forward
it successfully. Mixed default-parser batches retain valid and failed deliveries
in either order, with independent broker tags and settlement.
Cancellation requested before asynchronous invalid forwarding starts preserves
the source delivery and reports cancellation for custom-reader and default-parser
failures. A later attempt with an uncancelled token can forward and settle it.
Cancellation during publication is not atomic; a confirmed forward followed by
failed acknowledgement can still produce a duplicate. Further redelivery cases
and custom republish semantics remain separate required increments. Existing default
receive and republish behavior must continue to pass full regression. This
boundary adds no writer, scheduler or outbox persistence changes.

### Default-reader composition

A parameterless, stateless `DefaultRmqWireMessageReader` implements
`IAmAWireMessageReader<BasicDeliverEventArgs>` and delegates to the existing
`RmqMessageCreator`. Applications can compose it to rename selected parsed bag
keys while retaining default content and metadata. Each call returns a fresh
message without mutating the native delivery. The consumer retains ownership
of delivery operations and stamps broker metadata after configured mapping.
Existing public constructors, subscription configuration and the unconfigured
parser path stay compatible. The composition test covers selective tenant-header
renaming and acknowledgement through both public factory paths. A shared-reader
test verifies independent sequential deliveries and acknowledgement across two
consumers. Explicit default-reader configuration and composition preserve failed
parsing results, including exact payload bytes, parsed identity/topic and delivery
metadata for rejection. Overlapping-read tests verify independent parsed results
and unchanged native bodies, properties and headers. Their barrier coordinates
entry to the composing reader without prescribing parser instruction order.

## RabbitMQ Async invalid-message mapping

`RmqSubscription.InvalidMessageWireMessageWriter` selects consumer-owned invalid
forwarding independently of retry and publication writers. Both factory modes
capture the setting per consumer, including null/default selections. Public
consumer constructors remain unchanged.

The writer receives the application payload, handled count and refreshed rejection
metadata. The gateway retains routing, persistence, delivery controls, mandatory
publication and confirmation tracking. Mapping or publication failure propagates
an exception, restores the source topic and leaves the original unacknowledged.
Recovery must retain native identity and exact bytes, then permit successful
forwarding and source settlement. There is no default fallback on mapping failure.

The broker's native dead-letter path is separate: the original wire content is
transferred by the broker, without invoking an application writer. The setting
must not intercept delivery errors or unacceptable messages without an invalid
route. Forwarding and acknowledgement are not atomic.

## RabbitMQ Async requeue mapping

`RmqSubscription.RequeueWireMessageWriter` optionally selects an
`IAmAWireMessageWriter<RmqWireMessage>` for direct consumer retry publication.
The consumer captures this selection through either public factory. Existing
public constructor signatures remain unchanged and select default mapping.
A consumer has no producer publication from which to select its retry writer.
The selected reader must understand the writer's retry representation.

Writer selection belongs to each consumer. Creating a second consumer through
the same factory must not overwrite the first consumer's choice. Four cases
verify default and custom mappings in both creation orders and both factory
modes, with independent native observation and source settlement. Two further
cases verify independent native outputs while retries share a composing default
writer. A barrier retains both results before publication, after serial conversion
inside the helper. Simultaneous converter execution remains a separate validation
requirement.

The writer supplies body and application properties. The gateway replaces the
native message ID, applies source persistence and handled count, and supplies
retry delivery, delay and original-message metadata. An existing original ID
is retained across retries even when the writer omits that header. Serialization
must not mutate the source message or bag. Immediate requeue is verified through
independent native observation and source settlement in sync and async modes.

Native delayed requeue uses the same direct publication path. Dedicated cases
verify that the requested three-second delay replaces a writer-supplied sixty-second
delay, preserves custom content and retry controls, and reaches only the source
queue. Scheduler-backed retries retain their existing scheduler publication
mapping. This property does not configure that path or invalid-message forwarding.
Scheduler-backed retry semantics remain separate work. Invalid forwarding has
its own explicit selection, described below.

Throwing or invalid null-returning writers make immediate and native delayed requeue return false.
The original broker delivery remains available with its native identity, payload
and properties. Recovery through a new consumer can publish one custom retry and
settle the original. Native delayed recovery also verifies the requested delay
and observes no extra delivery during a bounded window after mapping fails.
This coverage does not establish atomic publication and acknowledgement, the
absence of arbitrarily late duplicates or scheduler-backed recovery.

For canonical retry-header names, gateway values take precedence over custom
writer values. Eight cases cover handled count, original-ID chain, delivery marker
and delay across first/subsequent retries, both factory paths and immediate/native
delayed delivery.

Retry original-ID handling removes writer-supplied key variants using ordinal
case-insensitive comparison before writing one authoritative source identity.
If the source already has an original ID, retain its key spelling and value.
Otherwise use the canonical key and current source message ID. Four immediate
retry cases verify this behavior through both factory paths and first/subsequent
retries. This rule applies only to original-ID bookkeeping; it does not normalize
all AMQP header names or resolve multiple original-ID entries in the source bag.

## Gateway coverage

SNS publishing now supports `SnsPublication.WireMessageWriter` with `IAmAWireMessageWriter<SnsWireMessage>`. The carrier contains the body, nullable subject and SNS SDK message attributes. SNS and SQS attribute types belong to different SDK namespaces, so each gateway keeps its own native content carrier. Topic ARN resolution, message groups, deduplication and scheduling remain gateway responsibilities. The approved test uses native SNS-to-SQS observation of the custom mapping; `DefaultSnsWireMessageWriter(topicArn)` now supplies ordinary mapping and composition. It writes the wire handled count into a filtered bag copy without mutating the source.

| Gateway packages | Existing serialization boundary | Delivery behavior to preserve |
| --- | --- | --- |
| AWSSQS and AWSSQS.V4 | SQS sender, SNS publisher, raw and inline SQS creators | Receipt handle, FIFO group and deduplication, native receive count, SNS envelope handling |
| RMQ.Sync and RMQ.Async | Publisher builds properties and body; creator reads deliveries | Delivery tag, publisher confirms, mandatory returns, immediate and delayed republish, original message identity |
| Kafka | Header builder and publisher; message creator | Existing `MessageHeaderBuilder`, keys, offsets, consumer commits, delivery reports, transactions and batch paths |
| AzureServiceBus | Message publisher and creator | Locks, sessions, partition keys, scheduled messages, TTL, delivery count and batch size |
| GcpPubSub | Parser used by producer and pull/stream consumers | Ack IDs, ordering, delivery attempts, pull and streaming subscriptions |
| MQTT | Publisher serializes a Brighter message into payload; creator reverses it | Topic prefix, QoS, retained-message behavior and payload encoding |
| Redis | Publisher and creator use a string envelope | Queue selection, rejection/requeue behavior and existing framing |
| RocketMQ | Publisher builds native message; consumer reads message views | Topic, group, keys, tags, delivery timestamp and acknowledgement identity |
| MsSql | SQL queue serializes its generic payload as JSON | Row identity, locking, visibility and deletion |
| Postgres | Producer serializes payload; consumer reads it | Queue schema, row identity, locking, visibility and deletion |

The SQL gateway extension is for queue payload serialization. It does not change the persistence format of Brighter inboxes or outboxes. In-memory messaging has no native wire encoding; retain its existing behavior and use it for core lifecycle tests where appropriate.

## Decisions to resolve through early tests

- Validate the shared contract against AWS and RabbitMQ before extending all gateways. RabbitMQ needs a body/properties carrier and explicit handling of republish control headers.
- Preserve Kafka's existing header builder when no custom writer is supplied. An explicitly supplied full writer owns its headers; document that precedence and test both paths.
- Validate SNS envelope semantics. The initial custom SQS reader receives the original native delivery before envelope normalization; the default-reader composition design must make raw/enveloped parsing reusable and define malformed-envelope behavior. Preserve native receipt metadata outside the configurable parser.
- Define the outbound profile for consumer-created DLQ and invalid-message producers. A reader alone cannot infer its inverse; preserve current default behavior and provide explicit outbound configuration where custom rejection formats are required.
- Audit delivery confirmations that recover Brighter IDs from native headers. Retain internal correlation even when the user changes the wire ID representation.
- Treat JSON body rewriting as explicit mapping. Compressed or encrypted payloads require a compatible custom mapping; defaults must preserve bytes.

## Alternatives

A fluent C# DSL can later express common renames and field moves over the same contracts. It is not required for the first version. Delegates alone offer less guidance for default reuse. Application message transforms do not own native serialization and cannot implement this feature by themselves.

## AWS scheduler destination boundary

In callback mode, the native schedule targets `SchedulerTopicOrQueue`, even when
the original message targets another business queue or topic. The callback
retains that original message, including identity, business topic, payload and
telemetry, and retains the scheduling API's sync/async dispatch choice. Native
target mode continues to address the original business endpoint. Stored schedule
selection is validated separately from callback execution and dispatch-time wire
mapping; it does not establish the latter.

Application mapping at AWS callback dispatch uses `UseMessageTopicAsTarget = false`
and a distinct callback consumer with the default wire reader, AWS mapper and
scheduler handlers. The business publication selects the custom writer; it sees
the actual business producer trace context after callback delivery. This explicit
configuration preserves existing native target behavior for applications that
use it. Native mode does not execute application code inside AWS. Callback failure
recovery remains a separate completion requirement. Native SNS serialization now
keeps a generated correlation ID local to the wire request, leaving the source
unchanged; a supplied correlation ID remains unchanged on both source and wire.
Both SDKs and sending modes have stored-schedule regression evidence for this
contract. Global source preservation across other providers remains required.

## MQTT publication writer boundary

MqttPublication.WireMessageWriter optionally selects an
IAmAWireMessageWriter<MqttApplicationMessage>. A null selection and existing base
Publication callers use the reusable DefaultMqttWireMessageWriter, preserving the
complete Brighter Message JSON encoding. Applications can compose this default
writer or replace its payload representation.

The gateway applies its configured topic prefix and the source message's header
topic after conversion, then sets QoS AtLeastOnce. Writers cannot redirect sends
or reduce that QoS through the returned native message. Writers must return fresh
native messages, leave the source unchanged and support concurrent use when shared.
Public producer/publisher entry points remain compatible; writer selection travels
between them through internal overloads. Native observation covers sync/async
legacy, custom and composed payloads, routing, QoS and source preservation.

Writer exceptions propagate as their original instance. A null writer result fails
with InvalidOperationException before native publication. Bounded throw/null tests
observe only the next valid message through the same producer, followed by a 250 ms
window with no additional delivery. They establish recovery without silent default
fallback for those cases, not transactional outbox behavior or unlimited absence
of duplicates. Retention, cancellation and wider failure/lifecycle requirements
remain separate.

## MQTT subscription reader boundary

MqttSubscription.WireMessageReader optionally selects an
IAmAWireMessageReader<MqttApplicationMessage>. Both public consumer factories capture
the selection per consumer through an internal constructor; existing public
constructors retain their signatures and default behavior. Mapping runs synchronously
inside the SDK callback. A non-null mapped message enters the existing buffer.

Unconfigured subscriptions keep MqttMessageCreator's JSON parsing and log-and-drop
handling. DefaultMqttWireMessageReader exposes the same parser for composition,
returning a fresh decoded message or throwing FormatException when no message can be
decoded. Readers own application identity, topic, body and metadata and must not
mutate native deliveries. The SDK retains protocol acknowledgement ownership.

Native success-path validation covers both factory modes, legacy/custom/composed
mapping, payload preservation and one configured invocation. Bounded native tests
also cover a configured InvalidOperationException or null result followed by a valid
delivery through the same consumer, without default fallback or a placeholder.
The existing null guard and observed SDK dispatch provide that behavior; no new
Brighter exception boundary is introduced. Explicit default-reader failure
composition, other exception types and wider lifecycle requirements remain open.

## Redis publication writer boundary

RedisMessagePublication.WireMessageWriter optionally selects an
IAmAWireMessageWriter<string>. A null selection uses DefaultRedisWireMessageWriter,
which creates a fresh RedisMessagePublisher per conversion and preserves the current
header/body string envelope. Applications can replace the stored string or compose
the default writer. Existing public producer signatures remain compatible.

Both immediate send paths choose the writer before incrementing the message counter
or storing/fanning out the result. Writer exceptions propagate as their original
instance; a null result throws InvalidOperationException before any counter, storage
or queue mutation. The gateway retains source-header topic routing,
storage keys, counters, configured expiry and queue delivery. Writers must leave
source messages unchanged and support concurrent calls when shared. This requirement
does not establish concurrent safety for the producer's existing mutable routing state.

Native validation covers two sequential messages in sync/async legacy/custom/composed
modes, exact stored strings, source-topic routing distinct from publication routing,
counter advancement, ordered delivery to two recipient queues, positive configured
expiry and source preservation. The compatibility oracle is the existing public
serializer. Four sync/async × throw/null cases additionally establish an absent
counter/payload and empty queue after mapping failure, followed by successful
same-producer recovery at native ID 1 with the exact legacy envelope and one queue
entry. Arbitrary binary encoding, native failures after partial publication,
transaction/outbox behavior, consumer retry/rejection, cancellation, scheduling,
concurrent calls and wider lifecycle behavior remain separate.

## Redis subscription reader boundary

RedisSubscription.WireMessageReader optionally selects an IAmAWireMessageReader<string>.
Both public factories capture the configured reader per consumer through an internal
constructor. Existing public consumer constructor signatures remain unchanged and
select DefaultRedisWireMessageReader. The default adapter composes RedisMessageCreator,
retaining its header/body decoding and sentinel behavior. Missing/empty stored
payload handling before conversion remains unchanged.

The reader supplies application identity, topic, body and metadata. The gateway
retains destructive native queue removal, native ID tracking and the existing
in-flight acknowledgement gate. Native tests cover two sequential non-empty strings
through both factory modes, legacy/custom/composed mappings, an application topic
distinct from the native topic, and exact mapped IDs/bodies/markers. A receive before
acknowledgement is blocked without consuming the next queued ID; acknowledgement
allows the next message and eventual empty receive. Stored inputs remain unchanged.

This does not add native settlement or promise redelivery after a process crash.
Configured-reader exceptions and null results become rejectable failure messages,
retaining the stored string, native queue ID and source topic. The conversion
boundary surrounds only the configured mapping call; unconfigured default parsing
and native Redis connection/command error handling retain their existing behavior.
Four sync/async × throw/null cases establish exact single-line Unicode content and
identity preservation, native invalid-channel publication on explicit rejection,
and successful subsequent delivery through the same consumer. The failed delivery
does not occupy the acknowledgement gate.

Eight sync/async boundary cases additionally establish that unconfigured malformed
JSON retains its JsonException, an invalid envelope prefix retains the empty-message
sentinel, and a configured reader composing the default parser converts malformed
JSON into a failure preserving the complete multiline input. A native WRONGTYPE
queue command error remains a ChannelFailureException with a RedisException inner
error; the configured reader is not called. After each outcome (and repair of the
owned wrong-type key), the same consumer receives and acknowledges a valid message.

Arbitrary multiline/binary rejection round trips, other parser and native errors,
native invalid-channel failure recovery, retry mapping, cancellation, connection recovery
and concurrency remain open.

## Redis immediate requeue writer boundary

RedisSubscription.RequeueWireMessageWriter optionally selects an
IAmAWireMessageWriter<string> for immediate retries. Both consumer factories capture
the selection per consumer; null uses DefaultRedisWireMessageWriter. Existing public
consumer constructor signatures retain the default behavior. This selection is
independent of the subscription reader and producer publication writer.

Both immediate paths map and validate the writer result before storing the replacement
payload, enqueueing its existing native ID and removing the in-flight entry. Writer
exceptions propagate unchanged; null throws InvalidOperationException. Mapping
failure preserves the previous payload, queue/counter state and in-flight entry,
allowing the same message to be retried through the same consumer.

Ten sync/async legacy/custom/composed/throw/null cases establish exact native strings,
source-topic storage despite a different application topic, source-only queue delivery,
native ID reuse, unchanged counter, configured expiry and source-message preservation.
Native inspection removes the retried entry before a separate valid legacy delivery
proves gate release. This does not claim default-reader compatibility with arbitrary
custom formats.

Four sync/async × legacy-first/custom-first cases additionally establish sequential
endpoint isolation. Three consumers share one factory and retain their captured
legacy/custom/composed selections after subscription settings change. Two distinct
retries per endpoint preserve exact native strings and ordering without accumulated
serializer output; the replacement writer is never called.

Delayed retries retain their scheduler's existing mapping path. Native failures after
partial progress, atomic visibility to competing consumers, concurrent mapping,
cancellation, custom retry-reader compatibility and remaining rejection paths remain
separate implementation and validation work.

## Redis rejection writer boundary

RedisSubscription.InvalidMessageWireMessageWriter and DeadLetterWireMessageWriter
independently select IAmAWireMessageWriter<string> mappings for Brighter-managed
forwarding. Both factories capture the selections per consumer; null uses the
default envelope. Existing public consumer constructors remain compatible and use
default writers. The producer's internal mapped-send path shares native publication
with ordinary sends without remapping the rejection.

The selected writer receives the destination topic and refreshed rejection metadata.
Mapping errors propagate as their original instance; null throws
InvalidOperationException. Before native publication, mapping failure restores the
source application topic and retains its in-flight entry so the same message can
be rejected again. Refreshed rejection metadata may remain after failure.

Twenty sync/async × invalid/dead-letter × legacy/custom/composed/throw/null cases
establish native destination selection, exact stored strings, counter/queue/expiry
controls, unchanged source payloads, rejection metadata, inactive-writer isolation
and same-consumer recovery. Both routes are configured in these cases. The successful
forward retains application ID/body/marker and releases the in-flight gate.

Native publication exceptions retain their existing logged/true-result behavior and
release the in-flight entry because Redis already popped the original delivery.
This is distinct from mapping failure and does not promise reliable redelivery.
Twelve additional sync/async cases characterize default/custom DLQ fallback,
absent/ineligible destinations and a noninteger native counter through each route.
Fallback uses the DLQ selection and leaves the invalid writer unused. Without an
eligible destination, neither writer runs; no-channel rejection leaves metadata
untouched, while DeliveryError with only an invalid channel refreshes metadata
without publishing. Native INCR failure runs mapping once, publishes no payload,
leaves the counter unchanged and releases the consumer for its next source message.
The existing true return value does not guarantee forwarding on native failure.

Four approved sync/async × endpoint-order cases establish sequential rejection
selection capture and repeated payload isolation in focused runs (48 native forwards).
Both route selections are captured before subscription settings are replaced; the
replacement writer is unused and default/composed serialization does not accumulate
previous content. Three mutations produce twelve intended failures. Initial single-process
full runs hit the ServiceStack quota. The user-approved paced regression plan then
passed all 178 identities once per framework (174 passes/four existing skips), retaining
all prior outcomes without code/dependency/test/timeout changes or a license. This
establishes batched regression coverage; concurrent safety and a passing single-process
full run are not established.

Fallback mapping failure, other native failure windows,
delayed retry mapping, cancellation, concurrency and arbitrary multiline/binary
forwarding remain separate validation work.

## Redis subscription-selected scheduled retry boundary

RedisSubscription.ScheduledRequeuePublicationTopic optionally selects the registered
publication used for positive-delay retries. Both consumer factory paths capture
this value at creation. Null retains ordinary scheduled publication lookup; immediate
retries continue using RequeueWireMessageWriter. The existing public consumer
constructor remains compatible.

For an explicit selection, the consumer creates a private message/header/body wrapper,
copies the header bag, content types and baggage, and stores the publication lookup
as the existing local ProducerTopicHeaderName string. It preserves application topic,
event type, ID, handled count, delay, metadata and persistence flag. Dispatch-time
trace propagation must not mutate the caller's message. The retry publication must
be registered with the original event type; no writer delegate is serialized into
the scheduler job.

RedisMessagePublication.DestinationTopic optionally overrides the native Redis
storage/counter topic without changing the logical topic supplied to the writer.
RedisMessagePublication.ChannelName optionally selects one native destination queue;
null retains the native topic's recipient-set fan-out. Configure a retry publication
with the source native topic and source queue when retrying only that subscription.
The gateway retains ownership of native IDs, storage, counters and expiry. Default
wire serialization excludes the local producer lookup key.

The four approved native cases cover sync/async × custom/default mapping through the
real in-memory scheduler and dispatch pipeline. Behavioral RED and initial GREEN are
verified; complete mutation/final regression evidence is tracked in
[the review package](review/redis-scheduled-retry-selection/README.md). Durable job
persistence, multi-endpoint isolation, invalid configuration and submission/callback
failure or cancellation remain separate validation work.

## PostgreSQL wire mapping boundary

Publications select `IAmAWireMessageWriter<string>` for the existing JSON/JSONB content
column. The writer must return valid JSON. Queue name, row identity, visibility and
locking remain gateway-owned. Null selects `DefaultPostgresWireMessageWriter`; custom
writers can compose its fresh JSON result. Writer exceptions propagate unchanged and
null results fail before insertion. Outbox serialization is independent of this boundary.

Subscriptions capture `WireMessageReader` and `RequeueWireMessageWriter` when consumers
are created. The reader receives JSON text and supplies application fields. The gateway
replaces any returned receipt handle with the actual native row ID. Thrown/null custom
reads become rejectable messages retaining raw JSON and that ID. The unconfigured reader
keeps existing parser behavior, including streaming for large messages. Configuring a
custom reader materializes the JSON text for that synchronous mapping interface.

Native retry updates the same row using the captured requeue writer while SQL controls
visibility. Null selects default JSON serialization. The selected reader must understand
that retry format. Default adapters support composition without changing SQL schemas.

### SQL Server initial integration boundary — 2026-11-07

Public writer/reader selection and default composition now preserve native atomic
read-and-delete, row metadata, immediate requeue and failure recovery. Twelve focused
cases, twenty mutation failures and full 380-pass/four-skip suites on both frameworks
validate the repository's ARM64 SQL fixture. The separate amd64 SQL Server 2022 crash
and remaining scheduled/rejection combinations are still release gates; initial
integration is not whole-issue acceptance. See [evidence](review/mssql-wire-mapping/README.md).
