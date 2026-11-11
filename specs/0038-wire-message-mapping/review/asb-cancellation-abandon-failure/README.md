# ASB cancellation cleanup with a lost lock — native failure precondition not met

Status: approved test executed on both frameworks, but its native failure
precondition is not met. All four cases return OperationCanceledException rather
than the required ServiceBusException(MessageLockLost). No cleanup-failure defect
is established; no production correction or mutations were applied. The unchanged
approved test/helper are preserved here and outside the compiled test project.

## Approved contract (not established)

Two queue/topic cases send three native deliveries. The reader maps the first
delivery and uses a callback to the same consumer's public NackAsync to abandon it
with its actual native lock token. It then maps the second delivery and cancels the
caller token. The third delivery must remain unvisited by the reader.

Cancellation cleanup attempts to abandon the batch, including the already-abandoned
first lock. The broker must reject that stale lock with a native ServiceBusException
whose Reason is MessageLockLost. The caller must receive that native failure, rather
than successful partial results or cancellation hiding the cleanup failure. Cleanup
must nevertheless attempt the other batch deliveries before resetting the receiver.

The same consumer, with an active token, must recover all three original IDs exactly
once within the bounded recovery window and before the earliest recorded original
lock deadline. Recovery must preserve native bodies/sequences, custom identity,
body and headers, and attach correct current native lock/sequence metadata. The two
previously mapped messages must receive fresh lock tokens. Every recovered delivery
must have delivery count two; the originally unvisited ID must reach the reader
exactly once. There are two mapper calls before failure and five overall. Native
acknowledgement must remove all three messages, confirmed by bounded peek.

## Native failure mechanism and limits

This is deliberately induced stale-lock settlement, not simulated network failure.
The callback uses the public consumer API and real broker I/O; no receiver mocks,
private reflection, forged native messages or production test hooks are added.
It is test fault injection through the configurable reader, not a recommendation
that application readers settle messages while mapping. The synchronous reader
waits for its public NackAsync callback to finish before returning its first mapping.
The callback is configured before receiving. ASBTestCommand only configures the
subscription; no new handlers or closed-generic logging registrations are required.

The first NackAsync must finish before cancellation. Because it logs rather than
propagates MessageLockLost, its callback count alone does not prove native release;
the subsequent recovery assertions also require delivery count two and a fresh lock.
The repeated abandonment must actually yield MessageLockLost. If the emulator
accepts repeated abandonment, returns a different failure, or cannot establish the
three-message batch, preserve that evidence and review an amendment. Do not label
such a result proof of defective cleanup, silently retry, or weaken the contract.

The SDK does not guarantee a minimum receive batch size. Visibility peeks prove
publication, not acquisition. Delivery count two for the previously unvisited ID
proves acquisition by the cancelled receive in this isolated fixture. A count of
one is a batch setup limitation. The unvisited original lock is not directly observed;
the timing bound uses the two recorded locks and the fixture's one-minute lock
duration. Recovery polls for 20 seconds with two-second receives, so the last receive
may extend beyond that window.

## Named production mutations

1. Stop cleanup at the first abandonment exception: the recovery ID set is incomplete.
2. Continue after the first error but omit release of the unvisited third delivery:
   the recovery ID set is incomplete.
3. Swallow the native cleanup failure and propagate only caller cancellation:
   the expected ServiceBusException assertion fails.
4. Clear the configured reader on the cleanup failure/reset path: the explicit
   recovered mapped-ID Assert.Contains fails.
5. Replace recovered custom body with native bytes: the mapped-body assertion fails.
6. Omit native completion during acknowledgement: final native peek is nonempty.

If initially green, use these named mutations to establish RED. Only failures at
the intended assertions count. Preserve native setup/compilation errors separately.
Restore all mutations; adopt the approved test/helper unchanged. Build all library
targets, run focused cases on both frameworks and both full available suites,
retaining all 610 prior outcomes. Compare production files against this batch's
baseline to preserve unrelated workspace changes. No commits or pushes.

## Approval scope and outstanding work

The adjacent test/helper were approved and executed unchanged. Any amended failure
mechanism requires a concrete test/helper review under the repository test-first
approval gate before adoption or execution.
This proposes preservation of the native cleanup error while attempting remaining
releases; it does not claim successful recovery when every settlement fails, a
network outage contract, or handling of multiple distinct cleanup failures.
Broker-wait/settlement cancellation, sessions, unresolved session-lock instability,
eight DLQ-blocked cases, 52 unavailable hosted classes and broader transport/release
gates remain open. No release-readiness claim is made.

## Observed native result

Each queue/topic case on .NET 9 and .NET 10 reaches the intended two mapper calls,
active timeout, caller cancellation and completed first Nack callback. It fails at
line 96: expected ServiceBusException, actual OperationCanceledException. The
production release path reaches its cancellation check without an abandonment
exception. This identifies failure of the chosen native fault-injection mechanism;
it does not distinguish SDK behavior from broker behavior and is not counted as RED
for the desired cleanup contract. Recovery assertions were not reached.

No speculative exception handling was added. The two adopted files were moved to
`/private/tmp/brighter-4170-asb-abandon-failure/adopted-*.cs`, verified byte-identical
to the approved review copies. They are not left as failing tests in the compiled
suite and were not weakened, skipped or converted to mocks. Initial TRXs, logs and
isolated result summaries remain in the same evidence directory. The six proposed
mutations were not executed because their native failure precondition is absent.

This release gate remains open. A replacement test must first establish a real
native abandonment error while other buffered locks remain live, or use a suitable
hosted environment. Repeating the same green cancellation path cannot supply that
evidence. Multiple cleanup failures and network failures remain separate contracts.

Both restored full available suites pass 610 cases, zero failures/skips, with exact
retention of all 610 prior outcomes. All library targets build and all 1,171
non-generated production C# files remain unchanged. Nothing committed or pushed.

The isolated emulator containers and network were removed after verification.
