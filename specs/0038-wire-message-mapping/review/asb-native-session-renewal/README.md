# Native ASB session renewal with custom mapping — expiry control unresolved

Status: approved batch executed; native expiry prerequisite remains unresolved.
Initial cases pass, but disabling renewal produces a closed-receiver exception rather
than the required lock-loss evidence. Mutation restored; unchanged test preserved
outside the compiled project. Both restored baselines retain 620 passing cases.

## Contract

Two queue/topic cases send a unique native session message to isolated entities with
five-second session locks. Bounded native peek establishes visibility. A consumer
with RequireSession=true, buffer size one, a thirty-second automatic renewal budget
and the existing external wire reader receives it. Verify custom identity, binary
body, headers, native SessionId, first delivery count, lock token and sequence.

After receipt, wait eight seconds, then acknowledge the same mapped message through
the same consumer. Record the settlement exception and require Assert.Null, so a
native lock-loss error is an intended assertion failure rather than an unrelated
uncaught exception. The reader must still have run exactly once; bounded native peek
from the original sequence must show the delivery removed.

Reuse AsbExternalWireReader and AsbBoundedSessionClientProvider unchanged. The latter
configures a real SDK client, ten-second native timeout and zero retries. No mocked
receiver, production test hook, private reflection or new logging registrations.
ASBTestCommand is configuration only. Follow the existing ASB wire mapping collection.

## Fixture and validity boundary

Merge the adjacent entity fragment into the isolated emulator namespace, keeping all
baseline entities, including the earlier session-acquisition entities. New names:
wire-renewal-queue and wire-renewal-topic/renewal-observer. Queue and subscription
LockDuration must be PT5S and RequiresSession true. Tests accept the corresponding
BrighterTestsASBRenewalQueue/Topic/Subscription overrides. Use a fresh fixture per
focused case and retain these entities for full available regressions.

A delay beyond configured lock duration alone does not establish native renewal.
The renewal-disabled mutation below must produce lock loss at the settlement
assertion in both cases. If it remains green (for example because the emulator does
not enforce the configured expiry), preserve that evidence and mark this mechanism
unsupported; do not claim renewal coverage, weaken the test or retry until green.
Setup failures, unsupported entity settings and outer timeouts are not usable RED.
The initial passing execution is characterisation, not evidence by itself.

## Named mutations

1. Disable session renewal: pass TimeSpan.Zero instead of the configured budget when
   TrackLock creates the session ServiceBusLock. After eight seconds, native settlement
   must fail and Assert.Null(settlementFailure) must detect it. This is the prerequisite
   control for the five-second native lock enforcement claim.
2. Bypass the configured reader: use the default mapper instead. Custom identity or
   reader-count assertions must fail.
3. Replace the custom mapped body with native bytes: custom binary-body equality fails.
4. Omit native completion: settlement returns but final native peek remains nonempty.

Run initial/fixed and restored focused cases on .NET 9 and .NET 10; run named mutations
on .NET 9 for both queue/topic cases. Restore each mutation. If a real defect is
established, make only the minimal correction without changing public contracts.
Verify approved source and reused-helper equality. Build all library targets and run
both full available suites, retaining all 620 prior outcomes plus the two new cases
if the native enforcement prerequisite is established. Preserve raw diagnostics and
clean up only this batch's fixture. No commits or pushes.

## Remaining scope

This tests native emulator renewal of one mapped session delivery through successful
completion. It does not explain the intermittent in-memory session-lock failure or
close that finding, establish hosted Service Bus behavior, session contention,
renewal failure recovery, cancellation/arrival races, multi-delivery renewal lifetime,
or in-flight settlement cancellation. Eight DLQ-blocked cases, 52 unavailable hosted
classes and broader release gates remain open. No production-readiness claim.

## Native session renewal control — expiry cause not established

The [approved native queue/topic batch](review/asb-native-session-renewal/README.md)
initially passes on both .NET 9 and .NET 10 without production changes. Each case
receives a custom-mapped session delivery from an entity configured with a five-second
lock, waits eight seconds, acknowledges it, and confirms removal by native peek.

Disabling only the session renewal budget produces two intended Assert.Null failures
at the settlement assertion (line 131), but the recorded exception is
ObjectDisposedException: the native ServiceBusReceiver is already closed. Neither
case reports ServiceBusException with a lock-loss reason. Thus the control detects
the absence of renewal, but does not directly establish the approved native
lock-expiry prerequisite or explain why the SDK receiver closed. The runner records
validated=false and stops. No test or expected exception was weakened to accept this
result, and the other three mutations were not run. These are assertion failures,
not setup failures, but are insufficient for the stronger planned expiry claim.

The mutation is restored. All 1,171 non-generated production C# files match pre-batch
hashes; reused reader and native-client helper are unchanged. The approved test is
preserved byte-for-byte in its review directory and temporary evidence, outside the
compiled project pending resolution of the prerequisite. No production fix or new
passing baseline case is claimed.

Restored library and test builds pass. Both full available suites pass **620 cases,
zero failures/skips**, retaining the exact prior case/outcome multisets. Evidence:
`/private/tmp/brighter-4170-asb-native-session-renewal/`, including four initial passing
cases, the two control failures, mutation/source verification and both regression TRXs.
Nothing committed or pushed.

A follow-up needs direct evidence linking native session expiry to receiver closure,
or a separately reviewed control that observes native session-lock state and release.
Do not reclassify ObjectDisposedException as a native lock-loss reason without that
evidence. The intermittent in-memory session-lock finding, eight DLQ-blocked cases,
52 unavailable hosted classes and broader release gates remain open.

The isolated emulator containers and network were removed after validation.

## Follow-up

The [approved expiry-observation amendment](../asb-native-session-expiry-observation/README.md)
now validates native renewal directly: disabling renewal fails the native-expiry
assertion in both cases, and both full suites pass 622 cases. This supersedes the
original exception-based prerequisite for the narrow renewal gate while retaining
its original evidence and leaving the exact prior link-closure cause unproven.
