# User defined wire message mapping

Allow applications to control how a Brighter `Message` maps to and from each messaging gateway's native format. Existing applications retain the current format without configuration. Full implementation is the active goal. AWS SDK V4 and RabbitMQ Async are the validated first increment, with the remaining integrations and path limitations still to resolve.

Source: [Brighter issue 4170](https://github.com/BrighterCommand/Brighter/issues/4170).

- [Requirements and proposed contracts](requirements.md)
- [Implementation tasks](tasks.md)
- [Validation plan and results](validation.md)
- [Wire mapping usage](usage.md)
- [Writer API decision](../../docs/adr/0086-configurable-wire-message-writers.md)

## Working state

Base: upstream master `64ba97097199c708974195f55fbdca0ce3e8487c`.
Local branch: `feat/4170-wire-message-mapping`.
All work remains uncommitted and unpushed. No commits or pushes are authorized.

Work resumed by applying the named stash `WIP #4170 wire-message mapping 2026-10-18`.
The saved stash and backup remain intact. The user's latest instruction supersedes
the [partial release boundary](release-boundary-proposal.md): complete the entire
feature. The [active completion checklist](tasks.md#full-implementation-completion-gates)
tracks the remaining work. No full-feature readiness claim is made.

## Implemented behavior

The shared reader and writer contracts allow transport-specific native formats. Publication and subscription configuration reaches the public production factories and both sync and async operations where implemented.

### AWS SDK V4

- SQS publications select custom native body and attribute mapping. `DefaultSqsWireMessageWriter` supplies ordinary serialization and reusable composition without source-bag mutation.
- SNS publications select custom body, subject and attributes. `DefaultSnsWireMessageWriter` supplies ordinary serialization and reusable composition without source-bag mutation.
- SNS custom writing preserves gateway-owned FIFO group and deduplication IDs in raw and enveloped delivery.
- SQS subscriptions select custom readers. `DefaultSqsWireMessageReader` exposes raw and SNS-envelope parsing for ordinary consumption and composition.
- The consumer preserves native receipt handles and normalizes broker delivery counts independently of custom mapping.
- Throwing or null custom readers produce rejectable messages with native payload and identity. Invalid-message forwarding preserves that payload and acknowledgement remains possible.
- Default-parser failures preserve native payload and receipt metadata. Parsed application IDs and successfully parsed rejection envelopes retain their identity and representation.
- Mixed batches preserve valid deliveries alongside throwing or null custom-reader results and default raw/envelope-parser failures. Their tests verify forwarding, acknowledgement and source deletion.
- Subscription-selected invalid-message and dead-letter writers configure Brighter-managed rejection publication independently. Fallback uses the dead-letter writer; throwing/null writers preserve the source for recovery. FIFO forwarding retains the native group and deduplication IDs, and consumers created by one factory retain independent writer selections.

### RabbitMQ Async

Publication-selected writing supplies native body and properties while preserving original confirmation identity and gateway-owned persistence. `DefaultRmqWireMessageWriter` supplies ordinary publication and requeue serialization and supports composition.

Canonical native handled counts take precedence after custom reading. When that
header is absent, the reader's count remains. The default parser also preserves
the native count when parsing fails. Immediate and native delayed retry tests
consume two successive custom retries and verify counts one and two.

Subscription-selected readers map native deliveries through both factory paths. The consumer replaces reader-supplied delivery tag, redelivery and persistence values with broker metadata. Existing constructors remain available, and successful default parsing retains its format. Throwing or null-returning custom readers return unacceptable messages preserving native identity, routing key, body bytes and delivery metadata for rejection. Mixed-batch tests cover valid and failed custom mappings together, in both delivery orders. Default-parser failures preserve raw bytes, parsed identity/topic and broker delivery metadata for rejection. Mixed default-parser batches also preserve valid and failed deliveries together in either order. `DefaultRmqWireMessageReader` exposes the existing parser for composition, allowing selected bag changes while retaining standard fields and delivery operations.

`RmqSubscription.RequeueWireMessageWriter` selects direct consumer retry mapping independently of producer publications. Immediate and native delayed requeue preserve gateway-owned identity, persistence, handled count and original-message metadata without mutating the source. Native delayed retry applies the requested delay and targets only the original queue. Immediate retry writer failures return false and leave the original delivery available; a later retry succeeds after writer recovery. Scheduler-backed retries retain their existing mapping.

The publisher discards writer-supplied delay headers before applying gateway delay. It clears fabricated delivery tags while preserving source tags. Tests cover immediate sending, native delayed delivery, default composition and these delivery controls.

`RmqSubscription.InvalidMessageWireMessageWriter` separately configures invalid
forwarding. Both factories retain each consumer's selection. Mapping or mandatory
publication failure restores the application topic and leaves the original
unacknowledged. Native dead-lettering remains broker-owned. The
[24-case batch](review/rmq-invalid-writer/README.md), seven mutations and both full
regressions pass: 536 passed and two existing skips per framework.

### AWS SDK V3 — integration in progress

SQS and SNS publications now select SDK V3-specific wire writers, with reusable
default writers for composition. SQS subscriptions select native SDK V3 readers
through both factory modes. Successful custom reading retains gateway-owned
receipt and delivery-count metadata. Existing public constructors remain intact.

The [first integration batch](review/aws-v3-mapping/README.md) covers replacement
and composed writers/readers. The [reader-failure batch](review/aws-v3-reader-failures/README.md)
now preserves native payload and settlement data for throwing/null readers and
default-parser failures. Mixed batches retain every delivery, and valid rejection
envelopes retain their inner content and metadata. Native rejection and requeue
checks pass through both factory modes.

The [rejection-writer batch](review/aws-rejection-writers/README.md) adds separate
invalid-message/dead-letter writer selections in both SDKs. All 72 cases pass on
both frameworks, and all ten mutations fail as intended. Restored focused checks
pass; those full regressions pass 555 cases each. The subsequent
[FIFO/isolation batch](review/aws-rejection-controls/README.md) passes all four
full suites with 603 cases each, verifies all ten mutations, and requires no
production change.

The [39 lifecycle and compatibility cases](review/aws-v3-lifecycle/README.md)
pass on both frameworks and catch all 15 named mutations. They cover mapping
isolation/reuse, overlapping sends, FIFO controls, writer failure recovery,
outbox/scheduled dispatch tracing, non-CloudEvents SQS telemetry and constructor
compatibility. The subsequent malformed rejection-count fix brings both complete
SDK V3 suites to 519 passing cases. SDK V3 parity and full-feature
readiness remain open.

## Validation checkpoint

The latest completed full regressions are:

| Suite | Result per framework |
| --- | --- |
| AWS V4 Release, .NET 9 and .NET 10 | 619 passed, 0 skipped, 0 failed |
| AWS V3 Release, .NET 9 and .NET 10 | 619 passed, 0 skipped, 0 failed |
| AWS Scheduler V3 and V4 Release, .NET 9 and .NET 10 | 74 passed, 0 skipped, 0 failed |
| Quartz, .NET 9 and .NET 10 | 53 passed, 0 skipped, 0 failed |
| Core, .NET 9 and .NET 10 | 1,716 passed, 7 existing skips, 0 failed |
| RabbitMQ Async, .NET 9 / RabbitMQ 4.2 and .NET 10 / RabbitMQ 4.3 | 556 passed, 2 existing skips, 0 failed |

RabbitMQ full runs include mutual TLS and native delay. Final AWS gateway runs use Floci 2.3.0 with isolated account namespaces. Full scheduler runs use separate Floci 2.3.0 containers with the default test account to avoid the emulator's SNS account-scope limitation. All runs inhibit idle sleep. The earlier full solution builds in Debug and Release after restoring missing assets remain valid checkpoints. An external legacy SQS constructor call compiles on netstandard2.0, net8.0, net9.0 and net10.0.

Each completed behavior has a verified named mutation or initial behavioral failure. Mutation code is restored before regression. Commands, logs, framework results and environment limitations are recorded in [validation.md](validation.md).

## Scoped review outcome

The [release review](release-review.md) records public API compatibility, default
wire formats, gateway delivery controls, mapping isolation and lifecycle evidence.
The non-CloudEvents SQS telemetry cases validate all three telemetry fields in
custom attributes or the body, consumer tracing through the public tracer API and
acknowledgement. They pass their named mutations and both full AWS V4 suites.
The subsequent [real-pump batch](review/aws-pump-telemetry/README.md) validates
Reactor and Proactor application dispatch in both AWS SDKs, including handler
trace/state/baggage and source acknowledgement. All eight mutations and six full
AWS/Core suites pass; no production change is needed. The subsequent
[SNS pump batch](review/sns-pump-telemetry/README.md) validates raw/enveloped SNS
telemetry in attributes/body through both pumps and SDKs. All ten mutations and
six full AWS/Core suites pass. The [RabbitMQ pump batch](review/rmq-pump-telemetry/README.md)
now validates both wire locations and both pumps: seven mutations produce 24
intended failures, both full RabbitMQ suites pass 540 cases with two existing
skips, and both Core suites retain 1,716 passes/seven skips. Scheduler and remaining
transport paths stay open.

The constructor ambiguity and RabbitMQ retry-count defects found during review
are corrected and covered by regression tests. SQS outbox dispatch and SQS/SNS
in-memory scheduled dispatch serialize the actual producer trace context.

The fixture synchronization changes pass their named mutations, 140 focused
executions and subsequent full suites. The historical empty original-ID read was
not reproduced; its exact cause remains unknown. Overlap tests and source review
support independent converter outputs, without claiming exhaustive thread
interleavings or safety for application-provided converters.

The earlier scoped review is a checkpoint; full-feature readiness remains open.
AWS tests use an emulator; no real AWS deployment or remote CI pass is claimed.
The reader/writer ADR text is finalized for this increment and remains proposed
for upstream acceptance. The external documentation draft remains unpublished.

## Remaining global scope

AWS custom scheduler mappings use callback destinations; automatic callback-pump failure recovery remains open. The RabbitMQ retry writer
does not configure scheduler-backed retries; invalid forwarding now has its own
validated setting. Other schedulers and gateways remain unimplemented or unvalidated. These current
limitations are active work under the restored full scope.
The [task list](tasks.md) retains the remaining work.

The [32 malformed-copy regression cases](review/aws-malformed-rejection-count/README.md)
reproduced loss of stamped handled count in both SDK versions. The local fix
retains parsed bag/count through parser failures, and count assertions now pass.
The approved timestamp correction compares the exact UTC instant in the existing
`DateTime` representation. All cases pass on both frameworks and all six mutations
fail as intended. All four full regressions pass 519 cases each with zero
failures/skips. The subsequent [AWS rejection-writer batch](review/aws-rejection-writers/README.md)
passes all four full regressions with 555 cases each. The subsequent
[96 FIFO/isolation cases](review/aws-rejection-controls/README.md) pass on both
frameworks; all four full suites pass 603 cases, and all ten mutations fail at
their intended assertions. The [24 RabbitMQ invalid-writer cases](review/rmq-invalid-writer/README.md) are
now implemented and validated, with 536 passing cases and two existing skips in
each full suite. The [eight real-pump telemetry cases](review/aws-pump-telemetry/README.md)
now pass all mutation and regression checks; all four AWS suites pass 607 cases.
The [sixteen scheduler destination-selection cases](review/aws-scheduler-destinations/README.md)
pass on both frameworks, with 24 intended mutation failures. All four scheduler
suites pass 70 cases after correcting native timestamp precision; all four
gateway suites retain 607 passing cases at that checkpoint. The subsequent
[eight native callback mapping cases](review/aws-native-callback-mapping/README.md)
pass on both frameworks; eight mutations produce 16 intended failures. All four
full AWS suites now pass 611 cases, and both Core suites retain 1,716 passes and
seven existing skips. No production source changes were needed for callback
mapping. The [eight native SNS source-preservation cases](review/aws-native-source-preservation/README.md)
now reproduce and fix source mutation while preserving native wire correlation.
Six mutations produce 16 intended failures; all four Scheduler suites pass 74
cases and all four gateway suites retain 611 at that checkpoint. The [sixteen SNS
pump telemetry cases](review/sns-pump-telemetry/README.md) now pass all checks:
ten mutations produce 80 intended failures, all four AWS suites pass 619 cases,
and both Core suites retain 1,716 passes/seven skips. The [four RabbitMQ pump
telemetry cases](review/rmq-pump-telemetry/README.md) now pass on both frameworks;
seven mutations produce 24 intended failures, both full RabbitMQ suites pass 540
cases with two existing skips and both Core suites retain their preceding outcomes.
No production correction was needed for that RabbitMQ batch. The
[Quartz completion batch](review/quartz-job-completion/README.md), with its
[approved positive delay](review/quartz-job-positive-delay/README.md), now
reproduces and fixes the false job failure after successful dispatch. The fix adds
two early returns. Four mutations produce nine intended failures, and both full
Quartz suites pass 53 cases while retaining all 48 baseline outcomes. Zero-delay
timestamp rejection remains a separate recorded finding. The
[eight native Quartz/RabbitMQ cases](review/quartz-rmq-wire-dispatch/README.md)
now pass both frameworks: four mutations produce 24 intended failures, both full
RabbitMQ suites pass 548 cases with two existing skips, and both full Quartz suites
retain 53 passes. All production source files remain unchanged in that batch.
The [eight scheduled writer-recovery cases](review/quartz-rmq-writer-recovery/README.md)
pass with the approved [trace-chain amendment](review/quartz-rmq-recovery-trace-chain/README.md).
Three mutations produce 24 intended failures. Both full RabbitMQ suites pass 556
cases/two existing skips, both Quartz suites retain 53 passes and both Core suites
retain 1,716 passes/seven existing skips. All 1,277 production source files remain
unchanged. The [six RabbitMQ Sync endpoint cases](review/rmq-sync-endpoint-mapping/README.md)
now pass with the approved [native-properties callback API](review/rmq-sync-native-properties/README.md).
Eight mutations produce 26 intended assertion failures. Both full Sync suites pass
219 cases/one existing skip and retain every baseline outcome. The subsequent
[four mixed reader-failure cases](review/rmq-sync-reader-failures/README.md) reproduce
and fix batch loss after a configured reader throws or returns null. Five mutations
produce 20 intended failures; both full Sync suites pass 223 cases/one existing skip
and retain every preceding outcome. Only the Sync consumer changes in that batch.
The [four default-parser failure cases](review/rmq-sync-default-parser-failures/README.md)
reproduce and fix native body, handled-count and delivery-metadata loss. Four
mutations produce 12 intended failures; final full Sync suites pass 227 cases/one
existing skip each, retaining every preceding outcome. An initial .NET 9 run hit a
broker storage I/O error during queue binding; the affected case passed three
rechecks per framework before the full reruns. Only the parser and default-reader
XML documentation change. The [ten forwarding/recovery cases](review/rmq-sync-invalid-forwarding/README.md)
pass on both frameworks without production changes. Both full suites pass 237 cases
with one existing skip each and retain all prior outcomes. Six mutation runs produce
42 intended failures, but an earlier omit-source-ack run missed one case. The
[settlement-observation amendment](review/rmq-sync-settlement-observation/README.md)
now passes initial/restored checks on both frameworks, all six mutations (42
intended failures) and six additional acknowledgement-omission runs (60 intended
failures, no misses). Both full suites retain exactly 237 passes/one existing skip.
The two-second observation is bounded; the historical miss remains recorded.
All 1,280 production files are unchanged in the observation increment. The approved
[fourteen invalid-writer cases](review/rmq-sync-invalid-writer/README.md) now pass
with the approved [native-tag correction](review/rmq-sync-invalid-writer-native-tag/README.md).
Nine mutations produce 22 intended failures. Both complete Sync suites pass 251
cases/one existing skip and retain every preceding outcome. Three production files
add subscription/factory/consumer selection; the other 1,277 files and dependencies
are unchanged. Initial incorrect-assertion evidence is retained. The subsequent
[six publication recovery cases](review/rmq-sync-publication-recovery/README.md)
pass initially and after restoration on both frameworks without production changes.
Five mutations produce 26 intended failures; full suites pass 257 cases/one existing
skip each, retaining every preceding outcome. All 1,280 production files and
dependencies are unchanged in that increment. The subsequent
[eighteen immediate-retry cases](review/rmq-sync-requeue-writer/README.md) pass on both
frameworks after implementation and restoration. Fourteen mutations produce 60
intended failures. Both full suites pass 275 cases/one existing skip, retaining all
prior outcomes. Four Sync files add per-consumer retry selection and authoritative
native controls; other production files and dependencies are unchanged. The subsequent
[eight native-delay cases](review/rmq-sync-native-retry-writer/README.md) pass with
the eighteen immediate-retry cases on both frameworks, initially and after mutation
restoration and assertion-style cleanup. Seven mutations produce 20 intended
failures. Both full suites pass 283 cases/one existing skip and retain every prior
outcome. All production files and dependencies remain unchanged in that increment;
the xUnit assertion-style warnings are resolved. The [eight concurrency cases](review/rmq-sync-concurrent-mapping/README.md) pass
initially and after restoration on both frameworks. The approved
[completed-properties cache amendment](review/rmq-sync-concurrent-cache-amendment/README.md)
produces all four intended isolation failures, completing twelve mutations with
34 expected failures. After restoration, all eight focused cases pass again and
both full Sync suites retain exactly 291 passes and one existing skip. No production
changes were needed. The [Kafka default adapters](review/kafka-default-wire-mapping/README.md) are now
implemented and validated with the approved [charset correction](review/kafka-default-content-type/README.md).
All four focused cases pass on both frameworks, ten mutations produce their intended
failures, and both complete Kafka suites pass 269 cases with no failures/skips,
retaining all 265 baseline outcomes. Scheduler-backed retry, remaining Kafka coverage
and full #4170 implementation remain open.


The [eight Kafka endpoint cases](review/kafka-endpoint-wire-mapping/README.md) now
pass on both frameworks. Publication/subscription mapping selection preserves
custom dispatch trace, original confirmation identity and native acknowledgement
offsets. Eight mutations produce 32 intended failures; both complete Kafka suites
pass 277 cases with no failures/skips and retain all 269 preceding outcomes.
The [eight reader-failure cases](review/kafka-reader-failures/README.md) now reproduce
and fix configured throw/null failures on both frameworks. Nine mutations produce
72 intended failures; both complete Kafka suites pass 285 cases with no failures/skips
and retain all 277 preceding outcomes. Only the consumer changes; approved tests and
dependencies are unchanged. The [eight required-header failure cases](review/kafka-default-parser-failures/README.md)
now reproduce and fix payload/metadata loss on both frameworks. Eight mutations
produce 60 intended failures; both complete suites pass 293 with no failures/skips,
retaining all 285 prior outcomes. Only KafkaMessageCreator changes in that increment.
The [eight content-type parser cases](review/kafka-parser-exceptions/README.md)
now reproduce and fix MIME-exception payload/metadata loss on both frameworks.
Nine mutations produce 68 intended failures; both complete suites pass 301 with no
failures/skips, retaining all 293 prior outcomes. Only KafkaMessageCreator changes
in that increment. The [eight explicit-unacceptable cases](review/kafka-explicit-unacceptable/README.md)
pass initially and after restoration on both frameworks without production changes.
Ten mutations produce 80 intended failures; both full suites pass 309 with no failures/skips,
retaining all 301 prior outcomes. The [eight writer-recovery cases](review/kafka-writer-recovery/README.md)
pass initially and after restoration on both frameworks without production changes.
Nine mutations produce 72 intended failures; both full suites pass 317 with no failures/skips,
retaining all 309 prior outcomes. The [six outbox/subscriber recovery cases](review/kafka-outbox-recovery/README.md)
pass initially and after restoration on both frameworks without production changes.
Seven mutations produce 40 intended failures; both full suites pass 323 with no failures/skips,
retaining all 317 prior outcomes. The [eight factory-isolation cases](review/kafka-factory-isolation/README.md)
pass initially after mechanical setup corrections and after restoration on both frameworks.
Eight mutations produce 32 intended failures; both full suites pass 331 with no failures/skips,
retaining all 323 prior outcomes. No production changes were needed. The
[sixteen custom-header cases](review/kafka-custom-header-failures/README.md) reproduce
and fix payload/metadata loss from duplicate and null-valued custom headers on both
frameworks. Ten mutations produce 144 intended failures; both full suites pass 347
with no failures/skips, retaining all 331 prior outcomes. Only KafkaMessageCreator
changes; approved assertions and dependencies remain unchanged. The
[four invalid-message writer cases](review/kafka-invalid-writer/README.md) now pass
on both frameworks with per-subscription selection, mapped rejection metadata,
source-topic restoration and native source acknowledgement. Nine mutations produce
34 intended failures; both full suites pass 351 with no failures/skips, retaining
all 347 prior outcomes. Only subscription/factory/consumer source changes; approved
files and dependencies are unchanged. The
[four rejection recovery cases](review/kafka-invalid-writer-recovery/README.md)
now reproduce and fix false success and lost source offsets after selected-writer
exceptions/null results. Eight mutations produce 32 intended failures; both full
suites pass 355 cases, retaining all 351 prior outcomes. Only KafkaMessageConsumer
changes; the approved test, reused helper and dependencies remain unchanged. The
[two broker-failure cases](review/kafka-invalid-broker-recovery/README.md) now reproduce
and fix false success after negative native delivery confirmations. Rejection waits
for a per-send confirmation before acknowledging the source. Eight mutations produce
16 intended failures; both final full suites pass 357 cases, retaining all 355 prior
outcomes. Only consumer/producer source changes; the helper and dependencies remain
unchanged. The test's SDK enum spelling is corrected without changing its intended
assertion or line numbers. The
[four offset-barrier cases](review/kafka-failed-forward-offset-barrier/README.md)
now reproduce and fix later acknowledgements committing past a failed forward.
That batch capped commits at the earliest failed source offset for the consumer
lifetime and verified recreation-based recovery. Eight mutations produced 32
intended failures; both full suites passed 361 cases, retaining all 357 prior outcomes.
The [four same-consumer recovery cases](review/kafka-failed-forward-same-consumer-recovery/README.md)
now reproduce and fix the stuck limit after a successful retry. Rejection releases
only the matching limit, limited commits retain later acknowledgements, and the
consumer remembers the highest acknowledgement on affected partitions. Eight
mutations produce 32 intended failures; both full suites pass 365 cases, retaining
all 361 prior outcomes. Only consumer source changes; the approved test, reused
helper and dependencies are unchanged. The [four multiple-failure cases](review/kafka-multiple-failed-forwards/README.md)
now reproduce and fix skipped unresolved records after partial recovery. The consumer
retains every outstanding failed native offset per partition and releases only the
successfully recovered record. Commit reduction preserves later acknowledgements
and stops at the earliest remaining failure. Eight mutations produce 32 intended
failures; both full suites pass 369 cases, retaining all 365 prior outcomes. Only
consumer source changes; approved test/helper and dependencies are unchanged.
The next [four partition-isolation cases](review/kafka-partition-forward-isolation/README.md)
remain outside compiled projects, uncompiled/unrun. Partition isolation, concurrent
commits/rebalance and automatic pump recovery remain unverified or unfinished. Other malformed headers,
remaining failure/recovery paths, retry mapping, transactions, batches, concurrency
and the global #4170 gates remain open.
