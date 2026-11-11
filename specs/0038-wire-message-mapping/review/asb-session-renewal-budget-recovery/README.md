# Native session renewal-budget expiry and recovery — validated

Status: approved, adopted and validated. Both full available suites pass 628 cases,
retaining all 626 prior outcomes. Five mutations produce ten intended assertion
failures. No production changes, commits or pushes.

## Contract

Two queue/topic cases receive one native session delivery using a five-second lock
and six-second automatic renewal budget. Preserve all custom/native mapping checks.
The observing SDK client captures the real receiver's initial SessionLockedUntil.
Wait thirteen seconds without settling or closing the receiver. Require the native
expiry to have advanced beyond its original value (renewal occurred) and then to be
in the past (the bounded budget stopped renewal and the lock has expired).

Receive again on the same consumer. The existing gateway can report one
ChannelFailureException while replacing a failed receiver; permit exactly that
boundary and one subsequent receive, or immediate successful recovery. No other
exception, empty successful receive, second failure or unlimited retry is accepted.
Require a different real session receiver with the same SessionId, native delivery
count two, a fresh lock, original sequence, custom mapping and reader count two.
Acknowledge the recovered delivery and verify native removal by bounded peek.
Do not acknowledge the expired original lock or inject a fabricated SDK exception.

This contract follows the gateway's existing explicit channel-failure/replacement
boundary; it does not demand transparent recovery in one receive. A passing result
by itself does not distinguish these two permitted recovery paths.

## Native fixture

Reuse AsbObservingSessionClient, AsbObservingSessionClientProvider and
AsbExternalWireReader unchanged; all I/O remains native SDK I/O. No receiver doubles,
private reflection, production hooks, handlers or logging registrations.

Merge the adjacent PT5S session entities with all baseline entities. New names:
wire-budget-session-queue and wire-budget-session-topic/budget-session-observer.
Overrides use BrighterTestsASBBudgetRecoveryQueue/Topic/Subscription. Fresh isolated
fixture per focused case; include entities in full available regressions.

Thirteen seconds exceeds the six-second renewal budget plus a five-second renewed
lock, with two seconds of margin. The assertions use actual native expiry, not elapsed
time alone. If broker timing does not satisfy the precondition, preserve the failure
and review an amendment instead of silently increasing delays or weakening assertions.
This is native emulator coverage, not a guarantee under arbitrary host scheduling.

## Named production mutations

1. Ignore the configured renewal budget: use TimeSpan.FromMinutes(5) for the session
   ServiceBusLock in TrackLock. The native-expiry-in-the-past assertion must fail.
2. Disable session renewal entirely: pass TimeSpan.Zero instead. The initial
   expiry-advance assertion must fail.
3. Clear Subscription.WireMessageReader in ResetReceiverAsync: after native recovery,
   reader count or mapped identity must fail. An unrelated native exception does not
   count as valid RED; preserve unsupported recovery-path evidence for review.
4. Replace custom mapped body with native bytes: binary-body equality fails.
5. Omit native completion: final native peek remains nonempty.

Run mutations against both queue/topic cases on .NET 9; accept only intended assertion
failures. Restore each mutation. No test rewrites during execution without review.

## Work after approval

Adopt the test unchanged; build all library targets and tests. Run initial/fixed and
restored focused cases on .NET 9/.NET 10, the five mutations, and both full available
suites including this class. If initially green, mutations establish RED. Only an
observed product defect justifies minimal correction; preserve public contracts.
Retain all 626 prior outcomes plus two new cases if validated. Verify approved source,
fixture and unchanged helpers, and production restoration. Clean up this batch's
fixture. No commits or pushes.

## Remaining scope

This covers budget-driven native session expiry and bounded same-consumer recovery.
It does not validate arbitrary renewal failures, network partitions, contention,
concurrent calls, abandonment failure recovery, DLQ or hosted Service Bus. The earlier
intermittent in-memory session-lock failure, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader transport/release gates remain open.

## Native session renewal-budget expiry and recovery — validated

The [approved queue/topic batch](review/asb-session-renewal-budget-recovery/README.md)
observes one native session delivery with a five-second lock and six-second renewal
budget. After thirteen seconds, native SessionLockedUntil has advanced beyond its
original value and is in the past: renewal occurred and then stopped. The same
consumer reacquires the session through a different native receiver and obtains the
same message with delivery count two, fresh lock, original sequence and preserved
native/custom mapping. Reader count remains two. Acknowledgement and native peek
confirm removal.

The approved recovery contract permits immediate recovery or one ChannelFailureException
followed by one successful receive, preserving the gateway's existing replacement
boundary. A green result alone does not distinguish those permitted paths; no claim
of transparent single-call recovery is made.

All four initial cases pass without production changes. Five named mutations on
.NET 9 produce ten intended assertion failures: ignoring the budget fails native
expiry (line 134); disabling renewal fails initial expiry advancement (line 132);
clearing the reader during receiver reset fails recovery reader count (line 153);
replacing custom body fails equality (line 114); omitting completion fails final
native peek (line 184). Both cases detect every defect. All mutations are restored;
restored focused cases pass on both frameworks. No test or harness amendments were
needed during execution.

All library targets and tests build. Both full available suites pass **628 cases,
zero failures/skips**, retaining all 626 prior case/outcome pairs and adding exactly
the two new cases. All 1,171 non-generated production C# files match pre-batch hashes.
The adopted test matches the approved draft, all three reused helpers are unchanged,
and the approved native entity fragment is present. No production correction,
public API change or dependency change was required.

Evidence: `/private/tmp/brighter-4170-asb-budget-recovery/`, including initial/restored
focused runs, ten mutation failures, both expanded regressions, exact baseline
comparisons and source verification. Nothing committed or pushed.

This closes the narrow native emulator budget-expiry and bounded same-consumer
recovery gate. It does not validate arbitrary renewal failures, network partitions,
contention, concurrent calls, abandonment failure recovery, DLQ or hosted Service Bus.
The intermittent in-memory session-lock finding, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader transport/release gates remain open. Overall production
readiness is not established.

The isolated emulator containers and network were removed after validation.
