# Kafka failed-forward source-offset barrier

Status: approved, implemented and validated. All four cases reproduce the unsafe
source commit at True, line 174, on both .NET 9 and .NET 10. All four pass after the
fix and after mutation restoration. Eight mutations produce 32 intended failures.
Both full Kafka suites pass 361 cases with no failures/skips, retaining all 357
prior outcomes. Only KafkaMessageConsumer changes. No commit or push.

The [test](When_a_later_kafka_message_is_acknowledged_should_not_commit_past_a_failed_forward.cs)
combines sync/async consumers with commit batch sizes 1 and 10. Two native records
are published in order to one source partition and both are read before rejection,
representing deliveries already buffered by a consumer. Rejection of the first record
fails because the selected writer produces an oversized invalid record; native
preflight confirms the topic's broker size limit. The same consumer then acknowledges
the second source record. This does not require continued receiving after rejection
fails, so a safe implementation may also pause or rewind future consumption. Disposal flushes all buffered source offsets.
A native observer must see no committed offset beyond the failed first record.
Both an unset commit and a commit at/before that record are safe and accepted.

After repairing the selected writer, a fresh consumer in the same group must receive
the failed original record and the later record again, then forward the first
successfully. Acknowledging that later record and disposing must commit its native offset
plus one. The invalid topic must contain exactly one record with selected native
body/key/wire identity. The failed source body/identity/topic/key/count/tenant and
actual acknowledgement offset must survive. Redelivery of the later record is
expected: it was processed earlier but could not safely be committed across the gap.

The case reuses [KafkaBrokerRejectedWireWriter](../../../../tests/Paramore.Brighter.Kafka.Tests/TestDoubles/KafkaBrokerRejectedWireWriter.cs) unchanged. No new helper, handler,
registration, mock or dependency is added. Topics/group are unique, native calls and
receive loops are bounded, a 90-second token covers both consumer lifetimes and
finally cleanup runs on assertion failure. The approved broker and writer recovery
cases remain in the regression suite.

Before this change, failed rejection restored the message's offset but recorded
no commit barrier, while commits reduced acknowledgements to the highest offset
per partition. All four initial cases fail at True, line 174, on both frameworks. A later acknowledgement must not make the failed source irrecoverable.
This is a gateway settlement prerequisite for pump safety. The pumps still ignore
false rejection results for unacceptable messages; these tests do not prove
same-consumer retry, automatic pump recovery or multi-partition/rebalance behavior.

## Validation after approval

Copy the test unchanged into MessagingGateway. Preserve behavioral RED on both
frameworks before implementing only the failed-forward offset barrier. Reuse native
source coordinates; do not infer settlement from mapped wire identity. Retain safe
progress after recovery, and preserve existing behavior when no failed forward is
pending. Do not add a public testing API.

| Mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Permit a later acknowledgement to commit past the failed source | 4 | True safe committed offset, line 174 |
| Report failed broker forwarding as successful rejection | 4 | False, line 149 |
| Omit failed source offset restoration | 4 | True, line 160 |
| Clear the failed source binary body | 4 | Equal, line 156 |
| Replace recovered native body with source bytes | 4 | Equal, line 185 |
| Replace recovered native key with source key | 4 | Equal, line 183 |
| Replace recovered native ID with source identity | 4 | Equal, line 184 |
| Omit ordinary source acknowledgement storage | 4 | Equal final committed later offset, line 186 |

Require 32 intended failures across eight mutations. Native committed-offset
assertions run after disposal flushes buffered acknowledgements. Recovered-output
mutations apply only to the smaller repaired record, preserving the initial broker
rejection. Omitting ordinary acknowledgements must retain rejection acknowledgement:
after recovery the commit remains at the first record plus one, failing the final
assertion for the later record plus one. Compilation failures, preflight errors,
missing deliveries and unrelated timeouts are not proof.

Restore production bytes and optimized builds, rerun focused cases on both frameworks
and both full Kafka suites (361 cases each, retaining all 357). Build every library
and test target; verify approved test/helper identity, scope, dependencies, warnings,
HEAD/stash, documentation links and whitespace. Preserve logs/TRX and stop task-owned
services. No commit or push. Global #4170 production readiness remains open.


## Implementation and evidence

Both rejection failure paths record the earliest failed native source offset per
partition. The shared offset reduction caps commits at that offset, so batch,
disposal and revocation commit paths use the same limit. The approved tests exercise
batch sizes 1/10 and disposal, not a live revocation. No public API is added;
public rejection remarks describe the consumer-lifetime limit. No producer or
publisher implementation changes in this batch.

At this checkpoint, the conservative limit remained for the consumer lifetime.
Recreation after repair was verified. Retrying successfully on the same consumer
did not release it, and limited commits drained later acknowledgements from the
existing bag. The subsequent same-consumer recovery batch below addresses release
and retention of those acknowledgements. Thread-safe storage does not establish atomicity with
an already-running commit. Concurrent forwarding/commits, multiple outstanding
failures, partition reassignment and automatic pump retry remain unverified.
Pumps still ignore false rejection results. This fix prevents the sequential
later-acknowledgement loss demonstrated here; it is not complete pump recovery.

Evidence is under `/private/tmp/brighter-4170-kafka-failed-forward-offset-barrier/`:
initial RED, green/restored/full TRX and logs; eight mutation TRX/logs;
mutations.json; original/implementation snapshots; source/dependency manifests;
all-target build logs; and verification.json. The four approved tests and reused
helper are unchanged. All 357 previous outcomes are retained by exact name on both
frameworks. All four library targets and both test targets build with no new warning
diagnostics. Only KafkaMessageConsumer changes among 1,282 source files; the other
1,281 and all 268 dependency entries are unchanged. No source file is added. All
mutation bytes are restored. HEAD/stash are unchanged and task-owned Kafka/schema
services are stopped after verification.

The subsequent [four same-consumer recovery cases](../kafka-failed-forward-same-consumer-recovery/README.md)
now release the matching limit after retry and retain later acknowledgements. Eight
mutations produce 32 intended failures; both full suites pass 365 cases. This
supersedes the consumer-lifetime limit and drained-acknowledgement behavior described
above. The subsequent [multiple-failure batch](../kafka-multiple-failed-forwards/README.md)
retains all outstanding failures in a partition and verifies partial recovery;
both full suites pass 369 cases. Partition isolation, concurrent commits/rebalance
and automatic pump recovery remain open. Kafka and global #4170 are not production ready.
