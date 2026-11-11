# Kafka failed-forward isolation between partitions

Status: approved and added unchanged to the compiled Kafka test project. All four
CHARACTERISE cases pass initially and after mutation restoration on .NET 9 and
.NET 10. Eight mutations produce 32 intended assertion failures on .NET 9.
All Kafka library and test targets build. Both full regression suites pass 373
cases with no failures/skips, retaining all 369 previous outcomes.
No production correction is required by this batch. This follows the
[multiple-failure recovery batch](../kafka-multiple-failed-forwards/README.md).

The [test](When_a_failed_kafka_forward_recovers_should_preserve_other_partition_failures_and_progress.cs)
combines sync/async consumption with recovery of partition zero or one. Each source
partition receives two explicitly targeted native records. Both first records fail
forwarding because the selected writer emits 4,096 bytes for an invalid topic with
a 1,024-byte limit, independently confirmed by a native preflight. The later record in the partition chosen for recovery is acknowledged. After
repairing the writer, only that partition's failed record is retried on the same
consumer. The other partition's later record is then acknowledged, ensuring a
commit candidate is supplied after recovery. This prevents a drained earlier
acknowledgement from masking accidental removal of the other partition's limit.
Disposal flushes the final offsets.

The recovered partition must commit its later record plus one. The other partition
must remain exactly at its failed record. A failure in one partition must not cap
progress in another, and recovery must not release another partition's protection.
Native selected body, key and wire identity must survive forwarding. A recreated
consumer must receive the unresolved partition's failed record and then its later
record. After rejection and acknowledgement respectively, both partitions must
commit their last record plus one. The invalid topic must contain exactly two
selected native records, in recovery order.

Cross-partition delivery order is deliberately unspecified: the first receive loop
matches source IDs and checks native coordinates. The test does not assume the
broker delivers partition zero first. Source bytes and metadata are checked after
failure, and settlement is checked using broker-owned partition/offset coordinates.
Requests and receive loops are bounded, a 120-second cancellation token is used,
and finally cleanup disposes consumers and deletes unique topics.

These cases reuse KafkaBrokerRejectedWireWriter unchanged, with public factory and
consumer APIs and the real local broker. No helper, dependency, handler, registration
or testing API is added. This is sequential partition isolation, not controlled
concurrent forwarding, rebalance, transaction or automatic pump recovery coverage.

## Validation after approval

Copy the test unchanged into MessagingGateway and run on both frameworks. Existing
per-partition tracking is expected to pass initially. Establish RED with the named
clear-all-partitions mutation below, retaining the test unchanged. Only implement a
fix if the approved behavior reveals a production defect.

| Mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Clear every partition's failed offsets after one successful retry | 4 | Equal remaining partition commit, line 193 |
| Apply the earliest outstanding failure globally to every partition's commit | 4 | Equal recovered partition commit, line 191 |
| Omit ordinary acknowledgement storage, retaining rejection acknowledgement | 4 | Equal recovered partition commit, line 191 |
| Report failed broker forwarding as successful rejection | 4 | False, line 149 |
| Omit failed native offset restoration | 4 | True, line 157 |
| Replace recovered native body with source bytes | 4 | Equal, line 189 |
| Replace recovered native key with source key | 4 | Equal, line 187 |
| Replace recovered native wire ID with source identity | 4 | Equal, line 188 |

Require 32 intended failures across eight mutations. The global-cap mutation keeps
normal highest-acknowledgement retention but caps the candidate at the minimum of
all unresolved offsets irrespective of partition. Native-output mutations apply
only to repaired records smaller than 1,024 bytes. Compile errors, unrelated
exceptions and missing deliveries/timeouts are not evidence.

Restore production bytes and optimized builds, rerun focused cases on both frameworks,
then both full Kafka suites (373 each, retaining all 369). Build every library/test
target. Verify unchanged approved test/helper, source/dependency scope, warning
diagnostics, HEAD/stash, local links and whitespace. Preserve logs/TRX and stop only
task-owned services. No commit or push. Kafka and global #4170 readiness remain open.


## Execution evidence

The approved test is byte-identical to the compiled test under
`tests/Paramore.Brighter.Kafka.Tests/MessagingGateway/`. The reused
`KafkaBrokerRejectedWireWriter` is unchanged. Initial and restored focused runs
pass all four cases on both frameworks. Every mutation fails all four cases at
its named assertion; compilation errors and timeouts are not counted as evidence.
Production files are restored byte-for-byte after the mutation checks.

Logs, TRX results, snapshots, manifests and the mutation report are retained under
`/private/tmp/brighter-4170-kafka-partition-forward-isolation/`.

These cases establish sequential isolation between two partitions, partial
recovery and redelivery after consumer recreation. They do not establish
concurrent commit safety, rebalance lifecycle handling or automatic pump recovery.
Kafka and global feature readiness remain open. No commit or push.

Final verification confirms all 1,169 production C# files and 268 dependency/build
files match the pre-batch manifests. Library targets `netstandard2.0`, `net8.0`,
`net9.0` and `net10.0`, and both test targets, build with no new warning diagnostics.
Both full suites retain every previous outcome by exact test name. HEAD and stash
are unchanged. `verification.json` records the checks and all eight mutation results.
