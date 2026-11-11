# Broker-confirmed assignment before testing lost-partition recovery

Status: approved, implemented and validated. All four amended cases reproduce
stale-offset regression on .NET 9 and .NET 10 and pass after the correction.
Eight mutations produce 32 intended assertion failures. Both complete Kafka
suites pass 377 cases without failures/skips, retaining all 373 preceding
outcomes. The approved amended test and reused helper remain unchanged.
Only `KafkaMessageConsumer.cs` changes in production; no commit or push.

## Why the fixture needs correction

The original four cases fail at the final commit assertion on both frameworks.
RoundRobin commits offset 0 instead of 3. CooperativeSticky leaves offset 2.
A candidate correction clears per-partition forwarding state from the revoked
and lost callbacks. Both RoundRobin cases then pass on .NET 9, but both
CooperativeSticky cases still fail.

Temporary diagnostics show that the CooperativeSticky original consumer returns
the third record before broker-recognized membership is restored. Its commit is
rejected with `Broker: Unknown member`. A returned record therefore does not prove
that the ownership transition required by this test's scenario has completed.
The CooperativeSticky result cannot yet isolate stale-offset cleanup after
reassignment. The delivery/commit behavior before group rejoin remains a separate
finding, not a defect fixed by this amendment.

Evidence is preserved in
`/private/tmp/brighter-4170-kafka-lost-partition-recovery/`: initial logs/TRX for
both frameworks, the first candidate's `green-net9.0` failure, diagnostic logs/TRX,
the candidate snapshot and the original source snapshot. Temporary diagnostic
changes and the incomplete candidate correction have been restored to the
pre-batch production bytes. The original approved compiled test remains unchanged.
Mutation validation and full regressions have not run for this batch.

## Approved amendment

The [amended test](When_a_lost_partition_returns_after_another_consumer_recovers_should_resume_commits.cs)
retains all four combinations and all settlement, payload, identity and writer
assertions. Each consumer gets a distinct client ID through the existing public
configuration hook. Before publishing the third record, the original consumer
polls to process coordinator events and the native admin client verifies:

- The group is stable and has exactly one member.
- That member has the original consumer's client ID.
- The broker assigns the source partition to that member.

The observation is bounded to 45 seconds, with bounded admin requests. During
this wait no application records are expected because the replacement already
settled both source records and the third has not been published. Unexpected
redelivery fails the fixture. Only the deliberate maximum-poll notification is
tolerated. No fixed delay substitutes for observing the assignment, and no
reflection or new production API is used.

After review, replace the compiled test with this amended copy. Establish RED
again on both frameworks against the original production source. Then reapply
and evaluate the candidate correction, making only changes supported by the
observed behavior. Preserve the original evidence; do not count the earlier
CooperativeSticky failures as proof of stale-state cleanup after reassignment.

The eight mutations from the original review remain required, with line numbers
updated to this amended file. Require 32 intended assertion failures, restored
focused passes and both full suites with 377 passes each, retaining the previous
373 outcomes. Build all supported targets and verify source/dependency scope,
approved test/helper identity, warnings, links and whitespace. No commit or push.


## Amended reproduction evidence

With broker-confirmed assignment, all four cases fail at line 167 on both
frameworks: expected final offset 3, actual offset 0. Unlike the unamended
CooperativeSticky run, these failures occur after the broker confirms the original
consumer owns the partition again. Logs/TRX and source manifests are retained in
`/private/tmp/brighter-4170-kafka-lost-partition-assignment-observation/`.

The candidate clears the failed-forward set and its retained highest acknowledged
offset for each partition listed by revoked/lost callbacks. Revocation first
attempts its existing pending-offset commit. Cleanup uses the existing metadata
lock and does not touch other partitions. No public signatures or dependencies
change. This does not establish fencing of late acknowledgements or concurrent
commit/lifecycle safety; those remain separate gates.


## Final validation

The correction clears forwarding state for partitions named by the revoked and
lost callbacks. Revocation performs its existing pending-offset commit first.
The cleanup removes both the failed-forward set and the retained highest
acknowledgement for those partitions under the existing metadata lock. State for
other partitions is retained. No public declarations or dependencies change.

| Framework | Initial intended failures | Green focused | Restored focused | Full suite |
| --- | ---: | ---: | ---: | ---: |
| .NET 9 | 4 | 4 passed | 4 passed | 377 passed, 0 failed/skipped |
| .NET 10 | 4 | 4 passed | 4 passed | 377 passed, 0 failed/skipped |

All eight mutations fail four cases each at their intended assertions. The
retain-stale-state mutation fails at line 167; removal of commit limiting and
ordinary acknowledgement storage fail at line 121; false rejection success
fails at line 115; omitted source-topic restoration fails at line 247; native
body/key/wire-ID substitutions fail at lines 151/149/150. Compile failures,
timeouts and unrelated exceptions are not counted.

Library targets `netstandard2.0`, `net8.0`, `net9.0`, `net10.0` and both test targets
build with no new warning diagnostics. Verification preserves all 373 preceding
outcomes by exact test name on each framework, checks the reviewed test/helper,
all mutation restorations, public declarations, source/dependency manifests,
HEAD/stash, local links and whitespace. Of 1,169 production C# files, only the
consumer changes; all 268 dependency/build files remain unchanged.

Evidence is under
`/private/tmp/brighter-4170-kafka-lost-partition-assignment-observation/`, including
logs/TRX, snapshots, manifests, mutation results and `verification.json`. Final
framework suites run concurrently on isolated Kafka 4.0.2/schema-registry 8.0.6
pairs, with no shared broker state. The second environment's compose file is
retained there. Earlier unamended failures and diagnostics remain in the original
evidence directory; they are not substituted for this amended reproduction.

This closes the tested ownership-loss/recovery/reacquisition scenario. Graceful
partial revocation, late acknowledgement fencing, concurrent commits and failure
registration, and automatic pump recovery remain separate production-readiness
gates. The previously observed delivery before broker-recognized rejoin is not
fixed or accepted as safe by this scoped correction.
