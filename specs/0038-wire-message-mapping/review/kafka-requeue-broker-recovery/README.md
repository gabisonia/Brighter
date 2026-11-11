# Kafka immediate retry broker-failure recovery — validated batch

Status: approved, implemented and validated. Both full Kafka suites pass 391 cases. The original reviewed source finding and test plan are retained below, followed by final results.

## Source finding before this batch

Before this batch, immediate `Requeue`/`RequeueAsync` sent, flushed, then acknowledged the source.
The publisher reports negative delivery outcomes through callbacks; flushing is
not itself proof of successful persistence. The source offset is also removed from
the message bag before forwarding. The selected-writer success tests do not prove
safe source settlement or recovery when Kafka rejects the retry.

## Two approved broker-backed cases

Sync and async cases use a fresh one-partition topic with a 2,048-byte record limit.
Two small source records are published and consumed. An independent native
4,096-byte publication verifies `MsgSizeTooLarge` before testing the consumer.
The existing `KafkaBrokerRejectedWireWriter` initially emits an oversized retry.

The required behavior is:

1. Requeue returns false when Kafka rejects persistence. The source ID, topic,
   key, body, handled count and original offset remain available for recovery.
2. The topic still contains only the two original records. Acknowledging the
   second source record commits offset 0, preserving the failed first record.
3. Repairing the writer and retrying the same first delivery succeeds on the same
   consumer. The group commits offset 2, retaining the earlier acknowledgement
   of the second source record while leaving the new retry unacknowledged.
4. Exactly one retry exists at offset 2 with the configured native key, wire ID
   and mapped binary body. Successful recovery need not retain the source-offset
   bag entry.

The native Kafka observer supplies all record-count and committed-offset evidence.
There is no transport mock or new test helper. Timeouts and disposal/topic cleanup
are bounded in the same manner as the preceding approved broker tests.

## Approval scope and validation plan

After approval, copy the test unchanged into the Kafka test project and run both
frameworks. Expected behavioral RED is `Assert.False(failedRetry)` observing true.
Investigate a different failure before changing production code. Then implement
only the immediate retry confirmation, failed-source preservation and commit
barrier/recovery behavior needed by this test, retaining existing scheduled retry
behavior.

Run these named mutations independently and restore production after each:

- Ignore negative persistence confirmation — false-result assertion.
- Omit restoring the source offset after failure — failed-delivery offset check.
- Omit or release the failed retry's commit barrier — later-acknowledgement commit 0.
- Retain the barrier after successful retry — final commit 2.
- Discard the later acknowledgement — final commit 2.
- Replace the mapped key, wire ID or body with source values, separately — native
  retry content assertions.

Build all library targets and run both complete Kafka suites, preserving all
results from the preceding selection batch. Keep approved test/helper bytes
unchanged, report any surviving mutations or unavailable evidence, and do not
commit or push.

## Remaining limits

This does not prove writer exception/null handling, cancellation, missing delivery
confirmation, concurrent forwarding/commit races, recovery after consumer
recreation, scheduler-backed retry mapping or automatic pump recovery. It tests
an explicit caller retry on the same consumer after a confirmed broker rejection.

The repository's `.agent_instructions/testing.md` requires “STOP after writing the
test and ASK FOR APPROVAL” and “DO NOT run tests without approval.”


## Kafka immediate retry broker-rejection recovery validated

The approved two broker-recovery cases
reproduce false success after Kafka rejects an oversized retry on both .NET 9 and
.NET 10. Both sync and async requeue previously returned true. An independent
native publication verifies the broker's message-size rejection before the tested
operation, and native observers verify source commits and retry contents.

Immediate requeue now registers the unresolved source offset and waits for
per-send persistence confirmation. A negative confirmation returns false and
restores source-offset metadata; later acknowledgements remain capped at that
record. Successful retry releases its barrier while retaining later acknowledgements.
The existing confirmation helpers are shared with rejection under names that
reflect forwarding rather than a particular destination. Their five-second bound
remains unchanged. Delayed dispatch calls remain unchanged. The immediate path
handles ChannelFailureException here; other mapping exceptions, cancellation and
missing confirmations remain separate coverage gaps.

Both cases pass on both frameworks before and after mutation restoration. Eight
mutations produce 16 intended failures: ignored negative confirmation, omitted
source-offset restoration, omitted barrier, retained barrier after recovery,
discarded later acknowledgement and replacement of mapped key, wire ID or body.
The restoration mutation fails at the expected missing source-offset lookup; all
other failures are the intended result, commit or native-content assertions.

Both complete Kafka suites pass **391 cases, zero failures and zero skips**,
preserving all 389 preceding outcomes. All four library targets and both test
targets build with no new warning diagnostics. Only KafkaMessageConsumer changes
relative to this batch's production baseline. External public declarations, all
268 dependency/build files, the approved test and reused helper are unchanged.
All mutations are restored; no commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-broker-recovery/`, including
RED/green/restored/full TRX files, mutation logs, source snapshots, build logs and
verification.json. This establishes an explicit caller retry on the same consumer
after confirmed broker rejection. Automatic pump recovery, writer exception/null
handling, cancellation, missing confirmations, general concurrency/rebalance safety
and scheduler-backed retry mapping remain unverified or unfinished. Full-feature
production readiness remains open.

The next [writer-failure recovery batch](../kafka-requeue-writer-recovery/README.md)
is drafted outside compiled projects and is unrun pending approval.
