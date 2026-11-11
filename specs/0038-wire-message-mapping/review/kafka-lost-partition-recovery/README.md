# Kafka failed-forward recovery after partition loss

Status: completed with the approved
[assignment-observation amendment](../kafka-lost-partition-assignment-observation/README.md).
The amended four cases reproduce and fix stale-offset regression on both
frameworks; eight mutations produce 32 intended failures, and both full suites
pass 377 cases with all 373 previous outcomes retained. This directory retains
the original unamended test and review plan as historical evidence; the compiled
test is byte-identical to the approved amendment. Only KafkaMessageConsumer changes.
This follows the validated [partition-isolation batch](../kafka-partition-forward-isolation/README.md).

The [test](When_a_lost_partition_returns_after_another_consumer_recovers_should_resume_commits.cs)
covers sync/async consumers with RoundRobin and CooperativeSticky assignment.
It uses the real broker, public consumer factories and the existing
`KafkaBrokerRejectedWireWriter`, without new helpers or dependencies.

## Behavior

The original consumer reads two records from one partition. Forwarding the first
fails because its selected writer emits a 4,096-byte record to an invalid topic
limited to 1,024 bytes. A native preflight verifies the limit. Acknowledging the
second record must leave the broker commit exactly at the failed record.

The original consumer then stops polling. Its configured 15-second maximum poll
interval expires while a replacement consumer polls the same group. The test
does not use a fixed sleep to assume ownership changed: the replacement must
actually receive the failed source record at its native offset. The replacement
successfully forwards it with the repaired writer, acknowledges the later record,
and closes. Native observations verify the resulting source commit and exactly
one invalid record with the selected body, key and wire identity.

A third source record is published. The original consumer instance resumes
polling and must receive that record, acknowledge it and commit its offset plus
one. A failed-forward limit belonging to its earlier assignment must not push the
group commit back to the already-recovered first record.

Both existing lost/revoked callbacks remove assignment entries but retain failed
forward state. The expected initial behavioral failure is the final commit
equality at line 166, not a timeout or inability to rejoin. This remains a source
finding until the approved test runs. If setup cannot reach that assertion,
record the actual failure and correct the fixture without claiming it proves
the settlement defect.

The resumed consumer permits only the expected `Local_MaxPollExceeded` consume
notification. Other consumer exceptions fail the test. Reads have 45-second
bounds, requests use a three-minute cancellation token where supported, and
finally blocks dispose both consumers and delete the unique topics.

## Validation after review

Copy the test unchanged to the Kafka project's `MessagingGateway` directory.
Run all four cases on .NET 9 and .NET 10. Establish behavioral RED at the final
commit assertion before making the smallest correction required by that behavior.
If initially green, use the named retain-stale-state mutation to establish RED.
Do not introduce reflection, test-only exports or broker mocks.

| Mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Retain failed-forward limits across partition loss/reassignment | 4 | Final source commit, line 166 |
| Remove failed-forward commit limiting | 4 | Initial protected source commit, line 121 |
| Report a failed forward as successful rejection | 4 | False, line 115 |
| Omit source-topic restoration after rejection | 4 | Source topic, line 217 |
| Omit ordinary acknowledgement storage | 4 | Initial protected source commit, line 121 |
| Replace the repaired writer's body with source bytes | 4 | Native body, line 151 |
| Replace the repaired writer's key with the source key | 4 | Native key, line 149 |
| Replace the repaired writer's wire ID with the source ID | 4 | Native wire identity, line 150 |

The retain-stale-state mutation removes the lifecycle cleanup introduced by the
fix, preserving all ordinary failed-forward protection. Native-output mutations
apply only when the mapped record is smaller than 1,024 bytes, retaining the
initial broker failure. Require 32 intended assertion failures across the eight
mutations; compilation errors, missing deliveries and unrelated exceptions do not
count. Restore production bytes and optimized builds afterwards.

Run restored focused checks and both full Kafka suites, expecting 377 cases per
framework with all 373 preceding outcomes retained. Build all four library and
both test targets, compare warnings, verify reviewed test/helper identity,
dependencies, source scope, HEAD/stash, links and whitespace. Retain logs/TRX,
snapshots and manifests. Stop only task-owned services. No commit or push.

This batch addresses lost ownership followed by another consumer's recovery and
reacquisition by the original instance. It does not establish graceful rebalance
cleanup for only a subset of partitions, fencing of stale in-flight
acknowledgements, concurrent failure registration versus native commits, highest
acknowledgement state across assignments, consumer-protocol groups, or automatic
pump recovery. Those remain separate readiness gates.
