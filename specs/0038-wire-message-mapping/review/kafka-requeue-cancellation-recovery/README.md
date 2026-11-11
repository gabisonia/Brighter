# Kafka cancelled immediate retry recovery — validated batch

Status: approved, implemented and validated. Both full Kafka suites pass 396 cases. The reviewed plan is retained below; actual results follow.

## Gap and proposed contract

The writer-failure correction excludes `OperationCanceledException` from the retry
failure catch, preserving cancellation propagation. Immediate requeue removes the
source-offset bag entry before calling the writer, so cancellation can leave the
caller without the delivery metadata needed to retry. That source finding still
needs a runtime test.

The proposed asynchronous case uses the existing `GatedKafkaRejectionWireWriter`
unchanged. Its first mapping call is gated after entry and before native publication.
Two source records are consumed. The test starts retry of offset 0 on a task,
waits for writer entry, cancels the caller token, releases the gate, then checks:

- Cancellation propagates as `OperationCanceledException`; it is not converted
  into an ordinary false result. The test's separate overall timeout has not fired.
- Source ID, topic, key, handled count, body and offset remain available for retry.
- Acknowledging the second source record commits offset 0 while the retry is unresolved.
- Retrying the same delivery with a live token succeeds on the same consumer.
  Commit advances to 2, retaining the later acknowledgement and leaving all retry
  copies unacknowledged.
- Native retry copies have the configured key, wire ID and mapped binary body.

Cancellation does not guarantee that Kafka did not persist the first attempt.
The test deliberately permits one or two retry copies after successful recovery,
corresponding to three or four total records. It does not claim exactly-once
delivery or assert that cancellation rolls back a broker write. Consumer disposal
precedes final native observation. The gate is always released, the task joined
with a bound, and the consumer/topics cleaned up.

## Approval scope and validation plan

After approval, copy the test unchanged into the Kafka test project and run both
frameworks. Expected behavioral RED is the explicit source-offset-presence
assertion after cancellation. Investigate a different failure before implementing
the minimum async immediate-retry source-restoration change. Preserve cancellation
propagation and the unresolved commit barrier.

Check these named mutations independently, restoring every mutation:

1. Omit cancellation source-offset restoration — source-offset-presence assertion.
2. Swallow cancellation and return false — cancellation-type assertion.
3. Release the barrier on cancellation — commit after the later acknowledgement.
4. Retain the barrier after recovery — final commit 2.
5. Discard the later acknowledgement — final commit 2.
6. Substitute source key, ID or body for mapped values, separately — native retry
   content assertions.

Build all library targets and run both full Kafka suites, preserving the preceding
writer-recovery batch's results. Keep the approved test and reused helper unchanged,
record actual results and any surviving mutations, and do not commit or push.

## Limits

This does not cover cancellation before entry, missing confirmations or timeout
policy, synchronous writer-thrown cancellation, rebalance during mapping,
concurrent duplicate retries, process restart, scheduler-backed retries or automatic
pump recovery. It demonstrates explicit caller recovery after cancellation during
an async retry's mapping step.

The repository's `.agent_instructions/testing.md` requires “STOP after writing the
test and ASK FOR APPROVAL” and “DO NOT run tests without approval.”


## Kafka cancelled immediate retry recovery validated

The approved cancellation-recovery case
reproduces a missing source offset on both frameworks after caller cancellation
during the retry writer's gated mapping call. Cancellation already propagates,
but the source-offset-presence assertion fails. The initial .NET 9 runner expected
xUnit's default assertion label; xUnit emits the test's custom message instead.
The runner check was corrected and .NET 10 confirmed the same assertion failure.
No test or helper was changed.

Async immediate requeue now restores the extracted source offset before rethrowing
OperationCanceledException. It retains the unresolved barrier. A later source
acknowledgement commits offset 0, and explicit retry with a live token on the same
consumer succeeds and advances the commit to 2. Native observations allow one or
two mapped retry copies: cancellation does not prove the first attempt was absent
from Kafka. This is not an exactly-once claim. Synchronous and delayed paths are
unchanged relative to this batch's baseline.

The case passes before and after mutation restoration on both frameworks. Eight
mutations fail at their intended assertions: lost offset restoration, swallowed
cancellation, premature barrier release, a retained barrier after success, a lost
later acknowledgement, and substituted native key, wire ID or body. All mutations
are restored. Both complete Kafka suites pass **396 cases, zero failures and zero
skips**, retaining all 395 preceding outcomes. All four library targets and both
test targets build with no new warning diagnostics. Only KafkaMessageConsumer
changes relative to the production baseline; public declarations, all 268
dependency/build files, the approved test and existing helper remain unchanged.
No commit or push is performed.

The first .NET 9 full run stalled in the existing Fragile=CI
KafkaMessageConsumerCommitRevokeConcurrency test during native assignment inside
consumer disposal. A non-terminating createdump stack report identified the
blocked call; the stalled test container was then stopped and the full suite rerun
with five-minute inactivity hang diagnostics. The final .NET 9 pass is that rerun,
not a clean first-run result. This intermittent rebalance/disposal stall remains
an unresolved production-readiness concern; no speculative fix or test weakening
was made. The stack report and stalled-run console log are retained alongside the
successful results.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-cancellation-recovery/`, including
initial/green/restored/full TRX files, mutation logs, source snapshots, build logs
and verification.json. This establishes explicit async caller recovery after
cancellation during mapping. Cancellation before entry, internal confirmation
timeouts, synchronous writer-thrown cancellation, general forwarding/rebalance
concurrency, scheduler-backed retries and automatic pump recovery remain open.
The full feature is not yet production-ready.

Availability correction, 2026-11-10: the temporary evidence directory named above
is no longer present locally. Its raw failed stack and stalled-run log have not
been recovered. The earlier results and interpretation are retained as historical
records, not claims of currently accessible raw evidence. See the
[live-capture follow-up](../kafka-live-capture/README.md) for preserved later healthy
traces and the validated collection method; the shutdown finding remains open.

The next [confirmation-timeout cases](../kafka-requeue-confirmation-timeout-recovery/README.md)
are now validated (398 passing cases per full suite). They
separate internal timeout failure from caller cancellation and permit late retry
copies without claiming cancellation of broker writes.
