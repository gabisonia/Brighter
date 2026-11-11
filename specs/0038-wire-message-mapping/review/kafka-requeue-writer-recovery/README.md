# Kafka immediate retry writer-failure recovery — validated batch

Status: approved, implemented and validated. Both full Kafka suites pass 395 cases. The original reviewed gap and test plan are retained below; final results follow.

## Reviewed gap and required behavior

The broker-rejection batch handles negative persistence confirmation. An immediate
retry writer can instead throw while mapping or return null before Kafka receives
anything. The current immediate-retry catch is limited to `ChannelFailureException`;
these other failures can escape after source-offset metadata has been removed.
This source finding is not yet runtime evidence.

Four approved cases cover sync/async × throwing/null writer results. They reuse
`KafkaRecoverableWireWriter` unchanged, with source ID `bad-id` selecting its
failure path. Two ordinary source records are published and consumed through the
real broker and public consumer factory.

The desired contract is:

- A mapping failure returns false without escaping an exception. The test records
  the exception explicitly so an unexpected exception fails an assertion.
- Source ID, topic, key, handled count, body and delivery offset remain intact.
  No retry record is appended while mapping fails.
- Acknowledging the second source record commits offset 0, preserving the failed
  first delivery.
- Repairing the same writer and retrying the same delivery succeeds. Commit 2
  retains the later acknowledgement while leaving the new retry unacknowledged.
- Exactly one mapped retry exists at offset 2 with the expected native key, wire
  ID and binary body. Successful recovery need not retain source-offset metadata.

The native Kafka client observes records and committed offsets independently of
the writer. There is no transport mock, helper edit or proposed public API change.

## Approval scope

After approval, copy the test unchanged into the Kafka test project and run both
frameworks. Expected behavioral RED is the no-escaped-exception assertion receiving
a mapping exception (or the native client's null-message validation exception).
Investigate a different failure before changing production code.

Implement only the immediate retry failure/result and source-preservation behavior
needed by these cases. Preserve the confirmed broker-failure behavior and existing
delayed dispatch. Cancellation and missing-confirmation semantics require separate
coverage; do not claim that these cases establish them.

Run named mutations independently and restore each one:

1. Restrict handling to negative broker confirmation — escaped-exception assertion.
2. Report success after writer failure — false-result assertion.
3. Omit failed-source offset restoration — source delivery-offset check.
4. Remove the unresolved retry barrier — commit after the later acknowledgement.
5. Retain the barrier after successful recovery — final commit 2.
6. Discard the stored later acknowledgement — final commit 2.
7. Replace mapped native key, wire ID or body with source values, separately —
   native retry content assertions.

Build all library targets and run both full Kafka suites, retaining every prior
case from the broker-recovery batch. Keep approved test/helper bytes unchanged,
record exact failures and any surviving mutations, and do not commit or push.

## Remaining limits

Automatic pump recovery, caller cancellation, missing confirmations, concurrent
retry/commit races, rebalance during mapping, duplicate retries of one delivery,
consumer recreation and scheduler-backed retries remain outside this batch.

The repository's `.agent_instructions/testing.md` requires “STOP after writing the
test and ASK FOR APPROVAL” and “DO NOT run tests without approval.”


## Kafka immediate retry writer-failure recovery validated

The approved four writer-recovery cases
cover sync/async retry and writers that throw InvalidOperationException or return
null. Initial runs on both frameworks produce two passes and two intended failures:
the producer already wraps the tested writer exception in ChannelFailureException,
so its recovery path works, while a null native message escapes as
NullReferenceException. The initial runner expected four failures and stopped after
.NET 9; its expectation was corrected after inspecting the producer's wrapping
behavior, then .NET 10 confirmed the same result. No test assertions changed.

The immediate retry catch in each sending mode now handles non-cancellation
exceptions, so null-result failures reach the existing source-offset restoration,
false result and commit-barrier recovery path. OperationCanceledException remains
excluded, preserving the existing propagation policy; its source-restoration and
recovery behavior are not established by these cases. Delayed dispatch is unchanged.

All four cases pass on both frameworks before and after mutation restoration.
Nine mutations produce 36 intended failures: restricting recovery to negative
broker confirmation, reporting success after failure, omitting source-offset
restoration or the barrier, retaining the barrier after recovery, discarding the
later acknowledgement, and replacing the mapped native key, wire ID or body.
The first mutation also establishes sensitivity of the throwing cases that were
green on arrival. Native observations verify commit 0 after the later acknowledgement,
commit 2 after recovery, and exactly one correctly mapped retry at offset 2.

Both complete Kafka suites pass **395 cases, zero failures and zero skips**,
preserving all 391 preceding outcomes. All four library targets and both test
targets build with no new warning diagnostics. Only KafkaMessageConsumer changes
relative to this batch's production baseline: two exception filters and their XML
remarks. External public declarations, all 268 dependency/build files, the approved
test and reused helper are unchanged. All mutations are restored. No commit or
push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-writer-recovery/`, including
initial mixed results, green/restored/full TRX files, mutation logs, source
snapshots, build logs and verification.json. The evidence covers the tested writer
exception and null result, not every possible application exception type.
Cancellation recovery, missing confirmations, concurrent forwarding/rebalance,
scheduler-backed retries and automatic pump recovery remain unverified or unfinished.
Full-feature production readiness remains open.

The next [cancelled retry recovery case](../kafka-requeue-cancellation-recovery/README.md)
is now validated (396 passing cases per full suite). It permits a
copy from the cancelled attempt because cancellation does not establish whether
Kafka persisted it; it requires source recovery and leaves all retry copies unacknowledged.
