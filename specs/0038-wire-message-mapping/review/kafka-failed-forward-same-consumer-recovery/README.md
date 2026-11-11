# Kafka failed-forward recovery on the same consumer

Status: approved, implemented and validated. All four cases reproduce the stuck
commit at Equal, line 190, on .NET 9 and .NET 10. All four pass after the fix and
mutation restoration. Eight mutations produce 32 intended failures. Both complete
Kafka suites pass 365 cases with no failures/skips, retaining all 361 prior outcomes.
Only KafkaMessageConsumer changes. No commit or push.

The [test](When_a_failed_kafka_forward_is_retried_should_resume_commits_on_the_same_consumer.cs)
combines sync/async consumers with commit batch sizes 1 and 10. Two source records
are read before the first is rejected. Native preflight confirms the invalid topic
rejects oversized records; the selected writer initially returns 4,096 bytes against
a 1,024-byte topic limit. Rejection must return false and retain source metadata,
binary content and native offset. Acknowledging the later record must not commit
past the failed record. The native observer accepts either an unset commit or a
nonnegative offset at/before that failed record.

The selected writer is repaired and the same original delivery is rejected again
on the SAME consumer, without recreation or another acknowledgement of the later
record. Successful forwarding must preserve source metadata and produce exactly one
native invalid record with selected body, key and wire identity. Disposing the
consumer flushes buffered acknowledgements. The source commit must reach the later
record plus one: successful recovery must release the failed-forward limit AND
retain the later acknowledgement across a limited commit. Batch size 1 schedules an immediate flush; size 10
exercises buffered settlement. The native pre-recovery observation accepts an unset
commit because flushing is asynchronous; it does not prove the background flush
finished before retry. Final disposal makes the final commit assertion deterministic.

This is an explicit caller retry through public consumer interfaces. It does not
prove automatic pump retry, several failed records, concurrent commits, rebalance,
partition isolation, cancellation, missing confirmations or other rejection routes.
Before this change, the consumer kept its failed-forward limit for its lifetime
and drained later acknowledgements during limited commits. All four initial cases
fail at Equal, line 190, on both frameworks. A retry must make safe progress without discarding the
acknowledgement already supplied by the caller.

The test reuses [KafkaBrokerRejectedWireWriter](../../../../tests/Paramore.Brighter.Kafka.Tests/TestDoubles/KafkaBrokerRejectedWireWriter.cs)
unchanged. No helper, handler, registration, dependency, mock or testing API is added.
Topics/groups are unique, native requests and receives are bounded, a 90-second
cancellation token bounds asynchronous operations, and finally cleanup disposes the
consumer and deletes topics even after assertion failures.

## Validation after approval

Copy the test unchanged into MessagingGateway. Observe behavioral RED on both
frameworks before implementing only safe same-consumer recovery. Preserve failed
source coordinates independently of selected wire identity. Do not release a limit
merely because an unrelated later record is acknowledged. Retain the earlier
recreation-based offset-barrier and all prior failure tests.

| Mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Keep the failed-forward limit after successful retry | 4 | Equal final committed later offset, line 190 |
| Omit ordinary acknowledgement storage, retaining successful rejection acknowledgement | 4 | Equal final committed later offset, line 190 |
| Report failed broker forwarding as successful rejection | 4 | False, line 147 |
| Omit failed source offset restoration | 4 | True, line 155 |
| Clear failed source binary body | 4 | Equal, line 153 |
| Replace recovered native body with source bytes | 4 | Equal, line 184 |
| Replace recovered native key with source key | 4 | Equal, line 182 |
| Replace recovered native wire ID with source identity | 4 | Equal, line 183 |

Require 32 intended failures across eight mutations. Output mutations apply only to
the smaller repaired native record, preserving the initial broker rejection. Omit
ordinary acknowledgements only; retain successful rejection settlement so the final
commit stops at the first record plus one instead of the later record plus one.
Compilation failures, setup errors, missing deliveries and unrelated timeouts are
not proof. If initially green, the named persistent-limit mutation must demonstrate
that the final offset assertion detects the defect.

Restore production bytes and optimized builds; rerun focused cases on both frameworks
and both complete Kafka suites (365 each, retaining all 361). Build all library/test
targets and verify unchanged approved test/helper, source scope, dependencies,
warnings, HEAD/stash, documentation links and whitespace. Preserve logs/TRX and stop
only task-owned services. No commit or push. Global #4170 readiness remains open.


## Implementation and evidence

Successful rejection now conditionally removes the failed limit matching that
native source partition and offset. A later record's successful rejection cannot
remove a different earlier record's limit. For partitions that have failed forwarding,
the consumer remembers the highest acknowledgement observed by the commit reducer,
including acknowledgements beyond the failed record. They remain in this per-partition
record after removal from the pending bag.
After recovery, a lower acknowledgement for the retried record therefore cannot
replace the later acknowledgement already observed. Normal partitions retain their
existing path. Public signatures and producer/publisher code are unchanged.
The highest-offset bookkeeping lasts for the consumer lifetime and uses one entry
per affected partition. Existing commit serialization remains in place.

The first implementation also reinserted capped offsets into the pending bag.
Review found that unnecessary: the highest-offset record already retains that
progress, and reinsertion would also put revoked-partition offsets back in the bag.
The reinsertion was removed. Both earlier full 365-case suites finished before stop
requests reached them; no test container was stopped. Their results, snapshots and
mutation evidence are archived under pre-retention-review/. Focused tests, all eight
mutations, optimized builds and both full suites were repeated against the final
version. Only the repeated results establish final validation.

This batch verifies one failed record followed by a later acknowledgement, recovery
on the same consumer, and disposal flushing for batch sizes 1/10. The pre-recovery
observation accepts unset commits because flushing is asynchronous; it does not
prove that the background flush completed before retry. Final broker state after
disposal proves the required outcome for each run. This is not a controlled
concurrency/rebalance test. At this checkpoint only the earliest failure per partition was retained:
recovering it could remove protection for later failed records. The subsequent
multiple-failure batch below closes that sequential partial-recovery gap.
Automatic pump recovery is also unfinished; pumps still ignore false rejection
results. Initialization failure, cancellation, missing confirmations and other
rejection routes remain separately unverified.

Evidence is under `/private/tmp/brighter-4170-kafka-failed-forward-same-consumer-recovery/`:
initial RED, green/restored/full logs and TRX; eight mutation logs/TRX;
mutations.json; original/implementation snapshots; source/dependency manifests;
all-target build logs; and verification.json. Approved test bytes and the reused
helper hash are unchanged. Every prior outcome is retained by exact name on both
frameworks. All four library targets and both test targets build without new warning
diagnostics. Only KafkaMessageConsumer changes among 1,282 production files; the
other 1,281 and all 268 dependency entries are unchanged. No source file is added,
all mutation bytes are restored, and HEAD/stash are unchanged. Task-owned services
are stopped after verification.

The subsequent [four multiple-failure cases](../kafka-multiple-failed-forwards/README.md)
now retain every outstanding failed native offset and release only successful
retries. Eight mutations produce 32 intended failures; both full suites pass 369
cases, retaining all 365 prior outcomes. This supersedes the single-failure limit
above. Partition isolation, concurrent commits/rebalance and automatic pump recovery
remain open. Kafka and global #4170 production readiness remain open.
