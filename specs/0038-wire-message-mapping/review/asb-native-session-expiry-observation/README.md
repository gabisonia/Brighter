# Native session-expiry observation — validated

Status: approved, adopted and validated. Both full available suites pass 622 cases,
retaining all 620 prior outcomes. Four mutations produce eight intended assertion
failures. No production changes, commits or pushes.

## Reason for the amendment

The previous renewal-disabled mutation correctly failed the settlement assertion,
but the SDK surfaced ObjectDisposedException rather than an explicit lock-loss reason.
The [SDK trace](../../asb-native-session-sdk-trace.md) explains how a closed session
link can trigger SDK transport disposal before a subsequent settlement checks the
stored lock-loss error. It does not prove why the emulator closed that link.

This amendment strengthens the observable contract instead of relaxing the exception
expectation: measure native SessionLockedUntil directly, then require it to advance
through automatic renewal before successful completion.

## Concrete changes

The adjacent test replaces only the client provider and adds observations:

- The observing client invokes base.AcceptNextSessionAsync on the real SDK client,
  saves the native receiver and its initial SessionLockedUntil before returning it
  to Brighter, and forwards the same caller token. It does not wrap or override the
  receiver, issue renewals, alter expiry, or substitute any I/O.
- At receive, require the native session ID to match and the initial expiry to be
  in the future. After the existing eight-second wait, require wall time to exceed
  that original broker-reported expiry, then require native SessionLockedUntil to
  be greater than the original value and the receiver to remain open.
- Preserve every existing custom/native mapping check, reader count, settlement
  Assert.Null and final native peek. The five-second entity configuration is unchanged.

The SDK initializes SessionLockedUntil from the accepted AMQP link and updates it
from a successful renewal response. The helper merely observes this public property.
The test therefore detects renewal without relying on the later exception type.
Both initial-expiry assertions deliberately reject an incompatible lock duration or
clock setup rather than silently accepting a longer native lock. A new helper per
file follows the existing native-client provider extension point. No new message
handler or closed-generic logging registration is needed.

## Named production mutations and execution

1. Disable session renewal in TrackLock by supplying TimeSpan.Zero: the explicit
   native-expiry-advance Assert.True must fail for both queue/topic cases. A different
   assertion or exception is not accepted as the intended RED. No exception-type
   requirement is applied to the later settlement, which this mutation cannot reach.
2. Bypass the configured reader: reader count or custom identity fails.
3. Replace the custom mapped body with native bytes: custom-body equality fails.
4. Omit native completion: the final native peek remains nonempty.

After approval, adopt the amended test and both helpers without changing their text.
Use fresh fixtures for focused cases, with the adjacent PT5S session entity fragment
merged alongside all baseline entities. Run initial/fixed and restored focused cases
on .NET 9 and .NET 10; run all four mutations on .NET 9, both cases. Restore every
mutation. If the explicit expiry mutation does not fail as specified, stop and preserve
the evidence without weakening the test. Only a demonstrated product defect justifies
a minimal production correction; preserve public interfaces and subclass hooks.

Build all library targets and tests; run both full available suites retaining all
620 prior outcomes plus two new cases if validated. Verify approved sources, unchanged
external reader and restored production hashes. Clean up only the isolated batch
fixture. No commits or pushes.

## Scope limits

This establishes native emulator renewal through the SDK expiry property and later
successful settlement. It does not prove the exact cause of the earlier link closure,
validate hosted Service Bus, or resolve the intermittent in-memory renewal failure.
Session contention, renewal failure recovery, other session paths, eight DLQ-blocked
cases, 52 unavailable hosted classes and broader release gates remain open.

## Native ASB session-expiry observation — validated

The [approved amendment](review/asb-native-session-expiry-observation/README.md)
validates native emulator session renewal with a custom wire reader for queue and
topic deliveries. A real SDK client records the accepted receiver and original
broker-reported SessionLockedUntil before returning it to Brighter. After eight
seconds, the original five-second lock has expired, the native expiry has advanced,
and the receiver remains open. The same mapped message is acknowledged successfully
and native peek confirms removal. Custom identity/body/headers, native session/lock
metadata and exactly one reader invocation are preserved.

All four initial cases pass without production changes. Disabling the session renewal
budget makes both cases fail the explicit native-expiry-advance assertion at line 135.
This establishes the renewal dependency directly, independently of the prior
ObjectDisposedException result. Bypassing the reader, replacing the mapped body and
omitting native completion each produce the intended assertion failure in both cases.
Four named mutations therefore yield eight intended assertion failures on .NET 9;
all mutations are restored. Restored focused cases pass on both frameworks.

All library targets and tests build. Both expanded full available suites pass
**622 cases, zero failures/skips**, retaining all 620 prior case/outcome pairs and
adding exactly the two new cases. All 1,171 non-generated production C# files match
pre-batch hashes. Adopted test and both observing helpers match the approved drafts;
the reused external reader and approved fixture fragment are unchanged. No production
correction, public contract change or dependency change was needed.

Two harness corrections are preserved in the evidence: the reader-bypass validator
initially expected line 116 instead of the actual reader-count assertion at line 111;
raw failures were validated without changing or rerunning the test. The first .NET 9
regression filter omitted the new class and passed only the 620 baseline cases.
The exact-count check caught this omission; the filter was expanded and the complete
622-case suite passed. Neither correction changed test expectations or masked a failure.

Evidence: `/private/tmp/brighter-4170-asb-session-expiry/`, including raw TRXs, initial
and restored focused results, mutations.json, the original validator report,
baseline-only-net9.0 artifacts, expanded regressions and source verification.
Nothing committed or pushed.

This closes the narrow native emulator renewal/mapping/completion gate. It does not
prove the causal link event behind the earlier disposed-receiver exceptions, resolve
the intermittent in-memory session-lock failure, or establish hosted Service Bus
behavior. Session contention, renewal failure recovery, other session/cancellation
paths, eight DLQ-blocked cases, 52 unavailable hosted classes and broader transport
and release gates remain open. Overall production readiness is not established.

The isolated emulator containers and network were removed after validation.
