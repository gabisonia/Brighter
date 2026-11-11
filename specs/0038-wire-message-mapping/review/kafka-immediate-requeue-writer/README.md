# Kafka immediate requeue writer selection — validated batch

Status: approved, implemented and validated. Both full Kafka suites pass 389 cases; final results follow the original reviewed proposal below.

## Reviewed gap and public contract

Before this batch, `KafkaSubscription` exposed `InvalidMessageWireMessageWriter`,
but had no retry writer selection. `KafkaMessageConsumer.EnsureRequeueProducer`
created a default publication without a writer. An application receiving an
external wire format could not select that format for an immediate retry through
its subscription. The approved design and validation plan below are retained;
completed results follow them.

Add a nullable `RequeueWireMessageWriter` selection using the existing
`IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>` contract, matching
the separate retry selection already available in RabbitMQ. Carry it through the
Kafka factory to the lazily created retry producer. Null retains the default
writer; the invalid-message writer must not become the retry writer implicitly.
Existing consumer constructor signatures need not change.

## Eight approved cases

The theory covers sync/async × custom/default selection × null/zero delay. It uses
the real Kafka broker, public factory and public consumer. The existing
`KafkaBrokerRejectedWireWriter` is reused unchanged with `Oversized = false`;
despite its name it is a general strategy writer that returns deterministic key,
ID and binary-body mappings without inspecting rejection metadata.

Each case publishes and receives one source record, requeues it, closes the
consumer, and independently checks with the native Kafka client:

- Exactly one retry is appended to the source partition, at the next offset.
- Custom selection produces the chosen key, wire ID and mapped binary body;
  default selection preserves the default key, ID, raw body and tenant header.
- The retry writer runs exactly once when selected and never when absent.
- The invalid writer is never invoked and the invalid topic remains empty.
- Source ID, topic, partition key, handled count, tenant and binary body remain
  intact. Successful requeue does not need to retain the source-offset bag entry.
- The group's committed offset advances past the source record only, leaving the
  new retry unacknowledged.

There is one proposed test file and no new helper. It was kept outside compiled
projects until approval, then copied unchanged for validation.

## Approved validation plan

After approval, copy the test unchanged into the Kafka test project. The initial
build is expected to report the missing public property. That compilation failure
is API-gap evidence, not behavioral RED. Add only the nullable subscription
property as a compilation scaffold, without factory/consumer wiring; then run the
tests on both frameworks. The four custom-selection cases should fail because
the writer was not invoked; all four default cases should pass. Investigate any
different failure before implementing forwarding of the selection.

Wire the selection with the minimum production changes and XML documentation.
Run these named mutations independently, checking the corresponding assertions
and restoring every mutation:

1. Omit factory forwarding of retry selection — selected writer invocation.
2. Omit retry publication writer selection — selected writer invocation.
3. Use the invalid writer for retries — retry/invalid writer invocation.
4. Substitute source key for the mapped key — native retry key.
5. Substitute source ID for the mapped wire ID — native retry ID.
6. Substitute source bytes for the mapped body — native retry body.
7. Omit source acknowledgement on requeue — committed source offset.
8. Commit past the new retry as well — exact committed offset.

If a variant is not applicable to the eventual minimal implementation, document
the equivalent defect used; do not modify approved assertions to suit a mutation.
Build all Kafka library targets and run both complete Kafka suites, retaining all
381 preceding outcomes. Preserve approved test/helper bytes and record exact
failures, results and remaining gaps. No commit or push is included.

## Scope limits

This batch adds immediate retry selection. It does not establish retry writer
exception/null recovery, broker rejection or missing-confirmation behavior,
concurrent lazy-producer initialization, cross-consumer isolation, scheduler-backed
delays or consumer-pump retry behavior. In particular, current immediate requeue
uses send plus flush; successful broker tests must not be presented as proof that
negative delivery confirmations prevent source acknowledgement.

The repository's `.agent_instructions/testing.md` requires “STOP after writing the
test and ASK FOR APPROVAL” and “DO NOT run tests without approval.”


## Kafka immediate retry writer selection validated

The approved eight retry-writer cases
cover sync/async calls, null/zero delay and selected/default mapping through the
public subscription and factory. The initial compilation confirms the missing
`KafkaSubscription.RequeueWireMessageWriter` property. With only that nullable
property added as a scaffold, all four selected-writer cases fail because the
writer is never called; all four default cases pass on both frameworks. This
provides behavioral RED independently of the initial compilation failure.

The subscription now exposes the nullable selection, the factory captures it on
the consumer, and the lazy retry producer uses it for its publication. Null
preserves default mapping. Retry selection is independent of invalid-message
selection; no existing constructor signatures or dependencies change.

All eight cases pass on .NET 9 and .NET 10 before and after mutation restoration.
Eight mutations produce 44 intended assertion failures: missing factory/producer
selection, use of the invalid writer, source key/ID/body substituted for mapped
values, omitted source acknowledgement, and committing past the new retry. Native
Kafka observations verify the exact retry offset and contents, an empty invalid
topic, and a commit that advances past the source only.

Both full Kafka suites pass **389 cases, zero failures and zero skips**, preserving
all 381 preceding outcomes. All four library targets and both test targets build
with no new warning diagnostics. Only KafkaSubscription, KafkaMessageConsumerFactory
and KafkaMessageConsumer change relative to this batch's production baseline.
The approved test and reused helper remain byte-identical; all mutations are
restored. No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-writer/`, including the initial
compile diagnostics, behavioral RED/green/restored/full TRX files, mutation logs,
source snapshots, build logs and verification.json. Optimized RED stack traces
point to line 125 while the invocation-count assertion is on line 126; recorded
failure messages confirm expected 1 versus actual 0. The initial .NET 9 harness
stopped on that line-number check; its four intended failures and four default
passes are retained and verified independently.

This closes immediate retry selection only. Writer exception/null recovery,
negative delivery confirmations, missing confirmations, cancellation,
scheduler-backed dispatch, concurrent lazy initialization and consumer-pump retry
behavior remain unverified or unfinished. In particular, send-plus-flush must not
be treated as proof that failed forwarding prevents source acknowledgement.
Global production readiness remains open.
