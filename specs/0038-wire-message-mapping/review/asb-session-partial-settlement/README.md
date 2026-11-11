# Native session renewal after partial settlement — validated

Status: approved, adopted and validated. Both full available suites pass 624 cases,
retaining all 622 prior outcomes. Five mutations produce ten intended assertion
failures. No production changes, commits or pushes.

## Contract

Two queue/topic cases use five-second native session locks and a thirty-second
renewal budget. Receive one uniquely identified native message, then send and receive
a second message with the same SessionId while the first remains unsettled. Each
ReceiveAsync requests one delivery, so this does not rely on the broker filling a
multi-message batch. Require the same real session receiver for both deliveries.
Verify custom mapping and native body, ID, delivery count, session, distinct lock
and sequence metadata for each delivery.

Acknowledge the first mapped message. Require no settlement exception and an open
native session receiver. Record its current SessionLockedUntil. Wait eight seconds,
require that recorded expiry to have elapsed and the current native expiry to have
advanced, then acknowledge the second mapped message successfully. Reader count must
remain two; bounded native peek from the first sequence must be empty after both
settlements. The interval starts after first settlement, so renewal observed solely
before settlement cannot satisfy the expiry-advance check.

Reuse AsbObservingSessionClient, AsbObservingSessionClientProvider and
AsbExternalWireReader unchanged. These use real SDK I/O; no mocked receiver, private
reflection or production test hook. No new handlers or logging registrations.

## Fixture

Merge the adjacent PT5S session entity fragment alongside all baseline entities,
including earlier renewal/session-acquisition fixtures. New queue/topic names are
wire-partial-session-queue and wire-partial-session-topic/partial-session-observer.
Environment overrides use BrighterTestsASBPartialSettlementQueue/Topic/Subscription.
Use a fresh fixture for each focused case. Keep entities for both full regressions.
The initial native lock must be live, and the post-settlement recorded expiry must
be exceeded by the wait; incompatible broker timing is a failed precondition, not
usable characterisation evidence.

## Named production mutations

1. Stop session renewal on every settlement: in ForgetAsync make stopRenewal true
   even when another tracked delivery remains. Both cases must fail the explicit
   expiry-advance assertion after first settlement.
2. Close the native session after every acknowledgement regardless of remaining
   deliveries: the immediate open-receiver assertion after first settlement fails.
3. Bypass the configured reader: reader count or mapped identity fails.
4. Replace custom mapped body with native bytes: binary-body equality fails.
5. Omit native completion: final native peek remains nonempty.

Only the intended assertion failures count. Restore every mutation; preserve any
setup failures or unexpected exceptions separately. Do not weaken the approved test.
If initial cases pass, the mutations establish RED for characterisation. If a real
production defect appears, make only its minimal correction, preserving public APIs.

After approval, adopt the test byte-for-byte. Build all library targets/tests; run
initial/fixed and restored cases on .NET 9 and .NET 10, mutations on .NET 9 for both
cases, and both full available suites including this class. Retain all 622 prior
outcomes plus the two new cases if validated. Verify approved source/entity equality,
unchanged reused helpers, and restored production hashes. Clean up this batch's
isolated fixture. No commits or pushes.

## Limits

This covers two overlapping deliveries received sequentially on one session and
partial acknowledgement. It does not cover concurrent calls, native multi-message
batch sizing, abandonment/rejection, competing consumers, renewal failure recovery,
or hosted Service Bus. The intermittent in-memory session-lock finding, eight
DLQ-blocked cases, 52 unavailable hosted classes and broader release gates remain
open. This batch alone cannot establish overall production readiness.

## Native session renewal after partial settlement — validated

The [approved queue/topic batch](review/asb-session-partial-settlement/README.md)
receives two native deliveries sequentially on the same session receiver, with both
initially unsettled. After acknowledging the first, the native session remains open.
The test records SessionLockedUntil after that settlement, waits eight seconds with
a configured five-second lock interval, verifies the recorded expiry has elapsed
and the current native expiry has advanced, then completes the second delivery.
Both messages preserve custom/native mapping and distinct lock/sequence metadata;
reader count remains two and final native peek confirms removal.

All four initial cases pass without production changes. Five named mutations on
.NET 9 produce ten intended assertion failures: stopping renewal after the first
settlement fails expiry advancement (line 178); closing the session after the first
acknowledgement fails the immediate open-receiver assertion (line 168); bypassing the
reader fails reader count (line 110); replacing custom body fails equality (line 115);
omitting completion fails final native peek (line 197). Both queue/topic cases detect
each defect. All mutations are restored, and restored focused cases pass on both
frameworks without test changes or harness corrections.

All library targets and tests build. Both full available suites pass **624 cases,
zero failures/skips**, retaining all 622 prior case/outcome pairs and adding exactly
the two new cases. All 1,171 non-generated production C# files match pre-batch hashes.
The adopted test matches the approved draft; all three reused helpers are unchanged,
and the approved PT5S session entity fragment is present in the isolated configuration.
No production correction, public API change or dependency change was required.

Evidence: `/private/tmp/brighter-4170-asb-partial-settlement/`, including initial and
restored focused results, ten mutation failures, both expanded regressions, exact
baseline comparisons and source verification. Nothing committed or pushed.

This closes the narrow native emulator partial-acknowledgement renewal gate for two
deliveries received sequentially on one session. It does not cover concurrent calls,
native multi-message batch sizing, abandonment/rejection, competing consumers, renewal
failure recovery or hosted Service Bus. The intermittent in-memory session-lock
finding, eight DLQ-blocked cases, 52 unavailable hosted classes and broader transport
and release gates remain open. Overall production readiness is not established.

The isolated emulator containers and network were removed after validation.
