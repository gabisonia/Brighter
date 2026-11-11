# Kafka immediate retry confirmation timeout — validated batch

Status: approved, implemented and validated. Both full Kafka suites pass 398 cases. The reviewed contract and plan are retained below; actual results follow.

## Gap and proposed contract

Immediate retry uses a five-second internal confirmation deadline. Its expiration
currently cancels a task, just as caller cancellation does. Caller cancellation
must propagate; an internal confirmation timeout should instead return false,
preserve the source offset and keep later acknowledgements behind the unresolved
retry. The approved cancellation test remains a regression guard for that distinction.

Two cases cover synchronous and asynchronous immediate retry through the public
consumer factory. They reuse `GatedKafkaRejectionWireWriter` unchanged. After
writer entry, the test holds mapping for seven seconds, past the current internal
deadline, while the caller token remains live. It then releases mapping and checks:

- No exception escapes; retry returns false and source metadata is retained.
- Acknowledging source offset 1 commits offset 0 while its retry is unresolved.
- Retrying the same offset 0 with the same consumer succeeds and advances commit
  to 2, preserving the later acknowledgement.
- One or two native retry copies retain the configured key, wire ID and body.

The expired attempt may still publish. The test permits that duplicate, and does
not claim a timeout cancels a broker write. It does not require return within five
seconds: a synchronous application writer cannot be interrupted by this deadline.
The seven-second wait is confined to these timeout tests; the overall timeout is
90 seconds. The writer's gate is released in cleanup and the task is joined.

## Approval scope and validation plan

After approval, copy the two cases unchanged into the Kafka test project. Expect
both to fail at the no-exception assertion because internal timeout currently
escapes as cancellation. Inspect any different result before implementation.
Implement only the timeout/caller-cancellation distinction and source recovery
needed by these cases; preserve the already-tested caller-cancellation behavior.

Check these named mutations independently, restoring each afterward:

1. Propagate internal deadline cancellation instead of reporting failed retry.
2. Report success after the timeout.
3. Omit source-offset restoration on timeout.
4. Release the unresolved barrier after timeout.
5. Retain the barrier after successful recovery.
6. Discard the later acknowledgement.
7. Substitute source key, wire ID or body for configured native values, separately.

Run the existing caller-cancellation case as well, build every Kafka library
target, and run both full Kafka suites. Keep the reviewed tests/helper unchanged
and retain all preceding results. Do not commit or push.

## Limits

This exercises deadline expiry during a gated mapping call. It does not establish
behavior for every missing callback, broker outage, process restart, synchronous
writer-thrown cancellation, rebalance, concurrent retry, scheduler or automatic pump.

The repository's `.agent_instructions/testing.md` requires “STOP after writing the
test and ASK FOR APPROVAL” and “DO NOT run tests without approval.”


## Kafka immediate retry confirmation-timeout recovery validated

The two approved timeout cases
initially fail at the no-exception assertion on both frameworks: the internal
five-second deadline escapes as TaskCanceledException even though the caller token
remains live. The synchronous and asynchronous confirmation helpers now convert
internal deadline cancellation into ChannelFailureException. Async conversion
explicitly excludes caller cancellation. Existing immediate-retry failure handling
returns false, restores source metadata and retains the unresolved commit barrier.
These helpers are shared with rejection forwarding, whose existing failure handling
continues to catch the exception; the new deadline scenario specifically tests retry.

Both timeout cases pass before and after mutation restoration on both frameworks.
The existing caller-cancellation recovery case also passes on both frameworks.
Native observations establish commit 0 after a later source acknowledgement and
commit 2 after retry with the same consumer; one or two correctly mapped retry
copies are permitted because deadline expiry cannot roll back a broker write.
The confirmation deadline cannot interrupt a blocked synchronous application writer.

Nine mutation variants produce 18 intended assertion failures: propagating timeout,
reporting success after timeout, losing the source offset, prematurely releasing
the barrier, retaining it after successful retry, losing the later acknowledgement,
and substituting the native key, wire ID or body. The first mutation attempt did
not compile because its exception variable became unused under warnings-as-errors;
that temporary mutation was corrected and rerun. The compile failure is retained
as runner evidence and is not counted as a behavioral mutation failure. No test
assertions changed, and all mutations are restored.

Both full Kafka suites pass **398 cases, zero failures and zero skips**, preserving
all 396 preceding outcomes. Both full runs include five-minute inactivity hang
diagnostics. All four library targets and both test targets build without new
warning diagnostics. Only KafkaMessageConsumer changes relative to this batch's
production baseline. Public declarations, delayed-dispatch calls, all 268
dependency/build files, the approved test and reused helper remain unchanged.
No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-confirmation-timeout-recovery/`,
including initial/green/restored/cancellation/full TRX results, mutation logs,
source snapshots, build logs and verification.json. The prior .NET 9 native
assignment/disposal stall remains unresolved even when this batch passes. These
cases do not establish every missing-callback, broker-outage, restart, cancellation,
rebalance, concurrency, scheduler or automatic-pump behavior. Full-feature
production readiness remains open.
