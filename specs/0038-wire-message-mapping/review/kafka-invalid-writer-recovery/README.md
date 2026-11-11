# Kafka invalid-message writer failure recovery

Status: approved, implemented and validated. All four cases fail at False, line
118, before the fix on both frameworks. All four pass after the fix and again after
mutation restoration. Eight mutations produce 32 intended assertion failures.
Both full Kafka suites pass 355 cases with no failures/skips, retaining all 351
prior outcomes. Only KafkaMessageConsumer changes. The approved test and reused
helper are unchanged. No commit or push was made.

The [test](When_kafka_invalid_writer_fails_should_keep_source_uncommitted_and_recover_after_redelivery.cs)
combines sync/async receive/reject with a selected writer that throws or returns null.
It reuses the existing
[KafkaRecoverableWireWriter](../../../../tests/Paramore.Brighter.Kafka.Tests/TestDoubles/KafkaRecoverableWireWriter.cs)
unchanged. No mock, helper, handler, registration or dependency is added.

A failed forward must return false, retain the source body/identity/topic/key/count,
tenant and actual acknowledgement offset, and publish no invalid record. Disposing
the consumer flushes any buffered acknowledgements; a native observer must still
see an unset source-group commit. This checks durable settlement, not only the
method result or the in-memory offset collection.

After repairing the same selected writer, a fresh consumer in the same group must
receive the original native record again. Rejection then succeeds, publishes exactly
one invalid record with the selected body/key/wire identity, and commits the original
source offset on disposal. Topic/group names are unique, receives/native operations
are bounded and cleanup runs on assertion failure. A 90-second token covers both
consumer lifetimes; each receive loop is bounded to 30 seconds.

Before the fix, the rejection catch returned true after forwarding errors while
metadata cleanup removed the source offset. All four initial cases reproduced
this at `Assert.False`, line 118, on both frameworks. Recovery in the same consumer,
concurrent partitions, broker failure, cancellation, default/dead-letter forwarding,
factory isolation and duplicate-delivery policy remain separate required work.
Forwarding and source acknowledgement are not atomic.

## Validation after approval

Copy the test unchanged into MessagingGateway and run both frameworks. Implement
only this failure/recovery contract, then independently prove these mutations:

| Mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Report a failed mapping as successful rejection | 4 | False, line 118 |
| Store source acknowledgement despite mapping failure | 4 | Equal unset committed offset after disposal, line 140 |
| Omit restoration of the failed delivery's native offset | 4 | True, line 129 |
| Clear the failed delivery's binary body | 4 | Equal, line 125 |
| Replace selected recovered body with original bytes | 4 | Equal native body, line 150 |
| Replace selected recovered key with source key | 4 | Equal native key, line 148 |
| Replace selected recovered wire identity with source identity | 4 | Equal native ID, line 149 |
| Omit successful recovered rejection acknowledgement | 4 | Equal committed source offset, line 151 |

Require 32 intended failures across eight mutations. Failure-only mutations must
preserve the false result and source data except the named defect. Premature-ack
proof must flush on disposal before inspecting native commits. Recovery mapping
mutations retain strategy invocation and actual native delivery. Compilation errors,
missing deliveries, unrelated exceptions and timeouts are not mutation proof.

Restore production bytes and normal optimized builds. Rerun focused cases on both
frameworks, then both full Kafka suites, expecting 355 passes each with all 351
preceding outcomes retained. Build all four library targets and both test targets.
Check source/dependency scope, approved-file/helper identity, warning diagnostics,
documentation links, whitespace, HEAD and stash. Preserve evidence and stop task-owned
services. No commit or push. Kafka and global #4170 production readiness remain open.


## Implementation and evidence

Reject/RejectAsync now restore the captured native offset and return false when
forwarding throws, without buffering a source acknowledgement. Source-topic
restoration remains in finally. Successful forwarding retains the existing
acknowledgement path. Public signatures and dependency versions are unchanged.
The catch behavior applies to both rejection routes; this batch proves selected
invalid-channel writer exceptions/null results. Broker delivery callbacks, default
and dead-letter failures, cancellation and later offsets in the same partition
are not covered by this result. Reactor/Proactor currently ignore the false rejection
result for unacceptable messages and continue receiving. Kafka commits the highest
acknowledged offset per partition, so a later acknowledgement can skip an earlier
failed forward. Automatic pump recovery is therefore not established by these tests.
Forwarding and acknowledgement are not atomic.

All eight .NET 9 mutations reached their specified assertions, four failures each.
Optimized builds and focused runs were restored before the complete suites. All
four library targets and both test targets build with no new warning diagnostics.
The other 1,281 production files and 268 dependency entries are unchanged. The
approved test and reused helper are byte-identical; HEAD and stash are unchanged.
Evidence is under `/private/tmp/brighter-4170-kafka-invalid-writer-recovery/`, including
RED/green/restored/full TRX, mutation TRX/logs, source snapshots, manifests, builds
and verification.json. Task-owned Kafka/schema services are stopped after verification.

The subsequent [two broker-failure cases](../kafka-invalid-broker-recovery/README.md)
are now implemented and validated: rejection waits for per-send persistence
confirmation, eight mutations produce 16 intended failures, and both final full
suites pass 357 cases. Later-offset and automatic pump recovery remain open. Kafka and global
#4170 production readiness remain open.
