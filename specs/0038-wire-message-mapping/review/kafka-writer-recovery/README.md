# Kafka writer failure and recovery

Status: approved and validated as characterisation. All eight cases pass initially
and after mutation restoration on both frameworks without production changes.
Nine named mutations produce 72 intended assertion failures. Both complete Kafka
suites pass 317 cases with no failures/skips, retaining all 309 prior outcomes.
Both approved files, all 1,282 production files and dependencies remain unchanged.
No commit or push was made.

A selected writer that throws or returns null must surface a send error without
publishing that attempt or reporting it as successfully published. Its failure must
not poison the producer: a healthy message must still publish, and the original
failed message can be retried after the strategy is repaired. Confirmations must
retain source identity even when the selected writer changes identity on the wire.

The [eight cases](When_a_kafka_writer_fails_should_surface_the_error_and_recover_publication_and_confirmations.cs)
combine sync/async send, throwing/null-returning writer, and failure before/after a
healthy message. The [strategy](KafkaRecoverableWireWriter.cs) fails only the bad
message while enabled. Healthy and recovered sends use distinct mapped keys,
wire identities and encoded binary bodies. A native consumer observes the real broker.
Async cases subscribe through the async confirmation interface; sync cases use the
sync interface. No mock, request handler, internal access or dependency is added.

After the failed attempt, capture the original body before retry. Send the healthy
message, if needed, while the failure is still enabled; repair the strategy and retry
the same failed Message instance through the same producer. Dispose the registry to
flush delivery reports and drain queued confirmations before assertions. Require a
surfaced error, successful recovery, unchanged original body, three mapping calls,
and exactly two successful source-ID confirmations. Failure confirmations are allowed;
this does not impose whether a mapping error also raises a failed confirmation.

Query the actual partition high watermark after disposal: exactly two records must
exist. Then assert both native records' mapped keys, wire identities and encoded bodies,
plus original source identity/topic/key/body. This detects a failed-attempt publication
without relying on a short empty-poll delay. Topics are unique and cleaned up on failure;
broker calls and consumption are bounded. This covers caller-driven retry, not outbox
sweeper recovery or broker delivery errors.

## Validation after approval

Copy both files unchanged into MessagingGateway and TestDoubles. These are expected
to CHARACTERISE existing behavior. Preserve initial outcomes on both frameworks.
If a real failure appears, preserve it and implement only this reviewed contract.
Whether initially green or fixed, prove the following named production mutations
independently on .NET 9, selecting all eight cases:

| Mutation | Expected failures | Required assertion |
| --- | ---: | --- |
| Swallow the mapping error without publishing | 8 | Mapping error is not null |
| Raise a successful confirmation for the failed attempt, then rethrow | 8 | Exactly two successful confirmations |
| Publish an extra default-mapped failed attempt without a callback, then rethrow | 8 | Native high watermark equals two |
| Replace successful selected bodies with the original binary body | 8 | Exact mapped healthy native body |
| Replace successful selected keys with the source key | 8 | Exact mapped healthy native key |
| Replace successful selected wire IDs with source IDs | 8 | Exact healthy wire ID |
| Confirm wire identity instead of original source identity | 8 | Successful source-ID set equality |
| Latch the producer into a fatal state after mapping failure | 8 | Recovery error is null |
| Clear the source body when mapping fails, then rethrow | 8 | Original failed-body snapshot equality |

Require 72 intended failures across nine mutations. Preserve the selected writer's
invocations except where the fatal-state mutation prevents a later send. For failure
mutations, cover both a thrown exception and a null result in both publication methods;
keep the original send error visible except for the explicit swallow mutation.
The extra-publication mutation must not add a confirmation, so the watermark assertion
is reached. For the identity mutation, preserve successful status and callback count.
Do not accept compile errors, callback timeouts or unrelated failures as proof.

Restore production bytes and normal optimized builds. Rerun focused cases on both
frameworks, then both complete Kafka suites, retaining every prior outcome. Check all
supported targets, warnings, source/dependency scope, approved-file identity, links,
whitespace, HEAD and stash. Preserve evidence and stop task-owned infrastructure.
Do not commit or push.

Callback exceptions, pending-outbox recovery, broker failure/cancellation behavior,
rejection forwarding, factory isolation, malformed headers, retry writer selection,
transactions/batches and concurrency remain separate work. Kafka and the
[global #4170 gates](../../tasks.md) remain incomplete.


Evidence is under `/private/tmp/brighter-4170-kafka-writer-recovery/`. The subsequent
[six outbox/subscriber recovery cases](../kafka-outbox-recovery/README.md) are now
validated without production changes: seven mutations with 40 intended failures and
323 passes in both full Kafka suites.
