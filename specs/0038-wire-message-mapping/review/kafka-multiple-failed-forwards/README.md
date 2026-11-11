# Kafka partial recovery with several failed forwards

Status: approved, implemented and validated. All four cases reproduce skipped
unresolved records at Equal, line 189, on .NET 9 and .NET 10. All four pass after
the fix and mutation restoration. Eight mutations produce 32 intended failures.
Both full Kafka suites pass 369 cases without failures/skips, retaining all 365
prior outcomes. Only KafkaMessageConsumer changes. No commit or push.

The [test](When_some_failed_kafka_forwards_recover_should_commit_only_up_to_the_remaining_failure.cs)
combines sync/async consumers with two recovery orders. Four native source records
are received before rejection. The first three fail forwarding because the selected
writer produces 4,096 bytes for an invalid topic limited to 1,024 bytes; a native
preflight verifies that broker limit. Failure must return false and preserve source
metadata, binary content and actual native offsets. The fourth record is acknowledged.

After repairing the writer, the first two failed records are retried on the same
consumer, either earliest first or second first. The third failure remains unresolved.
Disposal must flush a source commit exactly at that third record: earlier successful
work makes progress, while the unresolved record remains available. This intentionally
requires partial progress, beyond the earlier barrier test's acceptance of an unset
commit. It must not keep an already-resolved earlier limit or discard all limits when
one failed forward succeeds. The invalid topic must contain exactly two records in
retry order, with selected native body, key and wire identity.

A new consumer in the same group must receive the third source record and then the
fourth again. Forwarding the third and acknowledging the fourth must commit the last
native offset plus one. Exactly three invalid records must exist, with the final
record carrying the third source's selected native representation. The fourth source
record is redelivered because its earlier acknowledgement could not safely cross the
remaining gap. Native commit observations happen after disposal, avoiding dependence
on when the background flush runs.

Before this change, only the earliest failed offset per partition was retained.
Recovering it removed the partition's limit even when later failures remained.
Initial RED on both frameworks is Equal, line 189, in all four cases: the commit
advances to the fourth record plus one instead of stopping at the third failed record.

The cases reuse [KafkaBrokerRejectedWireWriter](../../../../tests/Paramore.Brighter.Kafka.Tests/TestDoubles/KafkaBrokerRejectedWireWriter.cs)
unchanged. They use public factory/consumer APIs and the native broker, with unique
topics/groups, bounded requests and receive loops, a 120-second cancellation token,
and finally cleanup. No helper, handler, dependency, registration or testing API is
added. These cases do not prove concurrent failures/commits, partition isolation,
rebalance, automatic pump retry, missing confirmations or other rejection routes.

## Validation after approval

Copy the test unchanged into MessagingGateway and observe behavioral RED on both
frameworks before implementation. Retain every outstanding failed native offset per
partition, remove only the successfully recovered record, and commit only as far as
the earliest unresolved record. Preserve the prior single-failure recovery and
recreation-based tests. Do not infer settlement coordinates from selected wire IDs.

| Mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Clear all failed-record limits for the partition when any retry succeeds | 4 | Equal partial committed offset, line 189 |
| Keep recovered records in the failed set | 4 | Equal partial committed offset, line 189 |
| Omit ordinary acknowledgement storage, retaining rejection acknowledgement | 4 | Equal final committed offset, line 235 |
| Report failed broker forwarding as successful rejection | 4 | False, line 144 |
| Omit failed native offset restoration | 4 | True, line 152 |
| Replace recovered native body with source bytes | 4 | Equal, line 186 |
| Replace recovered native key with source key | 4 | Equal, line 184 |
| Replace recovered native wire ID with source identity | 4 | Equal, line 185 |

Require 32 intended failures across eight mutations. Native-output mutations apply
only to the smaller repaired record, preserving the initial size failure. Omitting
ordinary acknowledgements still permits partial progress to the third source record,
but after recreation only successful rejection settles that record, so the final
commit falls short of the fourth record plus one. Compilation failures, missing
deliveries and unrelated timeouts are not proof. If initially green, use the named
clear-all-limits mutation to demonstrate the partial-commit guard.

Restore production bytes and optimized builds, rerun focused cases and both complete
Kafka suites (369 each, retaining all 365), and build every library/test target.
Verify test/helper identity, source scope, dependencies, warnings, HEAD/stash,
documentation links and whitespace. Preserve logs/TRX and stop only task-owned
services. No commit or push. Global #4170 production readiness remains open.


## Implementation and evidence

The consumer now retains a sorted set of outstanding failed native offsets for
each affected partition. Successful rejection removes only that source offset;
the partition's failed set is removed when empty. Commit reduction caps its
highest observed acknowledgement at the earliest remaining failure. The existing
highest-acknowledgement record preserves later progress after pending offsets drain.
Recovering the first two failures, in either order, therefore moves the commit
exactly to the third failure, never past it.

A short shared lock protects the failed sets and highest-acknowledgement metadata.
Native broker calls, confirmation waits and commit operations run outside this lock.
Failure/recovery bookkeeping releases the lock before acknowledgement can trigger
a flush. Existing commit serialization remains in place. No capped offsets are
reinserted into the pending bag, and no public signatures or producer/publisher
implementation change. Memory tracks outstanding failures plus one highest-offset
entry per affected partition for the consumer lifetime.

This verifies sequential partial recovery within one partition and recovery after
recreation. It does not prove concurrent failure registration versus native commits,
partition reassignment or automatic pump recovery. A metadata lock alone does not
make forwarding and settlement atomic. Highest-offset lifetime across rebalances,
initialization failures, cancellation, missing confirmations and other routes remain
separate concerns. Pumps still ignore false rejection results.

Evidence is under `/private/tmp/brighter-4170-kafka-multiple-failed-forwards/`:
initial RED, green/restored/full logs and TRX; eight mutation logs/TRX and report;
original/implementation snapshots; source/dependency manifests; all-target builds;
and verification.json. Approved test bytes and the reused helper hash are unchanged.
All 365 previous outcomes are retained by exact name on both frameworks. All four
library targets and both test targets build without new warning diagnostics. Only
KafkaMessageConsumer changes among 1,282 production files; the other 1,281 and all
268 dependency entries are unchanged. No source file is added, mutation bytes are
restored, and HEAD/stash remain unchanged. Task-owned services are stopped after
verification.

The next [four partition-isolation cases](../kafka-partition-forward-isolation/README.md)
remain outside compiled projects, uncompiled/unrun pending review. Kafka and global
#4170 production readiness remain open.
