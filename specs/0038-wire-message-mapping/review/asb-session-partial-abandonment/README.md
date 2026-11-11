# Native session renewal after partial abandonment — validated

Status: approved, adopted and validated. Both full available suites pass 626 cases,
retaining all 624 prior outcomes. Six mutations produce twelve intended assertion
failures. No production changes, commits or pushes.

## Contract

Two native queue/topic cases receive two messages sequentially on the same session
receiver, leaving both unsettled. Each has a unique native identity/body and distinct
lock/sequence metadata, with custom mapping checked before settlement. Nack the first
mapped delivery and require the native session to remain open while the second stays
unsettled. Record SessionLockedUntil after abandonment, wait eight seconds against a
five-second native lock, and require expiry advancement beyond that recorded value.

Only after this renewal check, receive again on the same consumer and native session.
Require exactly the first message to return with native delivery count two, a fresh
lock token, the original sequence and preserved native/custom content. Reader count
must be three. Acknowledge the still-held second message, require the session remains
open, then acknowledge the redelivered first message. Bounded native peek from the
original sequence must be empty.

Observing renewal before redelivery is deliberate: if abandonment incorrectly stops
the remaining delivery's renewal, another receive must not restart it and conceal
the defect. The redelivery check is also essential because NackAsync handles some
native errors without throwing; successful return alone does not prove abandonment.
No cancellation, lock-loss injection or DLQ operation is part of this batch.

## Fixture and reuse

Reuse AsbObservingSessionClient, AsbObservingSessionClientProvider and
AsbExternalWireReader unchanged. All I/O is native SDK I/O. No receiver substitutes,
private reflection, production hooks, new handlers or logging registrations.

Merge the adjacent PT5S session entities alongside all baseline entities. New names:
wire-abandon-session-queue and wire-abandon-session-topic/abandon-session-observer.
Overrides use BrighterTestsASBPartialAbandonmentQueue/Topic/Subscription. Use fresh
isolated fixtures per focused case and retain the entities for the full suites.
Receive requests one message at a time, avoiding an assumed minimum native batch size.

## Named production mutations

1. Stop renewal on every settlement in ForgetAsync: the explicit expiry-advance
   assertion after abandonment fails before any redelivery receive occurs.
2. Close the native session after NackAsync despite other tracked deliveries: the
   immediate open-receiver assertion after abandonment fails.
3. Omit native abandonment in NackAsync: the first message stays locked while renewal
   continues; the subsequent redelivery Assert.Single fails on an empty receive.
4. Bypass the configured reader: reader count or custom identity fails.
5. Replace custom mapped body with native bytes: binary-body equality fails.
6. Omit native completion: final native peek remains nonempty.

Require intended assertion failures in both queue/topic cases, not setup exceptions,
unrelated failures or outer timeouts. Preserve unexpected outcomes and review any
amendment instead of weakening the contract. Restore every production mutation.

## Approved work requested

After review, adopt the test byte-for-byte. Build all library targets/tests. Run
initial/fixed and restored focused cases on .NET 9 and .NET 10, six mutations on
.NET 9 for both cases, and both full available suites including this class. If initial
cases are green, mutations establish RED for characterisation. Only a demonstrated
product defect justifies a minimal correction; preserve public contracts.

Retain all 624 prior outcomes plus two new cases if validated. Verify approved source
and entity equality, unchanged reused helpers, and restored production hashes.
Clean up only this batch's fixture. No commits or pushes.

## Limits

This validates successful native partial abandonment, continuing renewal and mapped
redelivery. It does not validate abandonment failure recovery, rejection/DLQ,
concurrent calls, competing consumers, native multi-message batch sizing, or hosted
Service Bus. The intermittent in-memory session-lock finding, eight DLQ-blocked cases,
52 unavailable hosted classes and broader transport/release gates remain open.

## Native session renewal after partial abandonment — validated

The [approved queue/topic batch](review/asb-session-partial-abandonment/README.md)
receives two native deliveries sequentially on the same session receiver. After
abandoning the first, it verifies that the session stays open and its native expiry
advances past the value recorded after abandonment while the second remains unsettled.
Only then does it receive the first message again, requiring native delivery count
two, a fresh lock token, original sequence and preserved native/custom mapping.
Both deliveries are acknowledged, reader count remains three, and final native peek
confirms removal. Successful NackAsync return alone cannot satisfy the test.

All four initial cases pass without production changes. Six named mutations on
.NET 9 produce twelve intended assertion failures, two for each defect: stopping
renewal fails expiry advancement (line 178); closing after abandonment fails the
immediate open-receiver assertion (line 168); omitting native abandonment fails
redelivery Assert.Single (line 183); bypassing the reader fails reader count (line
110); replacing custom body fails equality (line 115); omitting completion fails
final native peek (line 226). All mutations are restored. Restored focused cases
pass on both frameworks; no test changes or harness corrections were needed.

All library targets and tests build. Both full available suites pass **626 cases,
zero failures/skips**, retaining all 624 prior case/outcome pairs and adding exactly
the two new cases. All 1,171 non-generated production C# files match pre-batch hashes.
The adopted test matches the approved draft, all three reused helpers are unchanged,
and the approved PT5S session entity fragment is present in the isolated configuration.
No production correction, public API change or dependency change was required.

Evidence: `/private/tmp/brighter-4170-asb-partial-abandonment/`, including initial and
restored focused results, twelve mutation failures, both expanded regressions,
exact baseline comparisons and source verification. Nothing committed or pushed.

This closes the narrow native emulator partial-abandonment/renewal/redelivery gate.
It does not validate abandonment failure recovery, rejection/DLQ, concurrent calls,
competing consumers, native multi-message batch sizing or hosted Service Bus. The
intermittent in-memory session-lock finding, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader transport/release gates remain open. Overall production
readiness is not established.

The isolated emulator containers and network were removed after validation.
