# ASB rejection cancelled before entry — validated

Status: approved, implemented and validated. Both cases pass on .NET 9 and .NET 10.
Three mutations produce six intended assertion failures. Both full available suites
pass 618 cases, zero failures/skips, retaining all 616 prior outcomes.

## Approved contract

Two native queue/topic cases publish and receive one distinct binary delivery through
the configured AsbExternalWireReader. Bounded peek establishes source visibility;
mapping, native bytes, delivery count one, lock and sequence are checked.

RejectAsync with an already-cancelled token and an explicit rejection reason must
directly throw OperationCanceledException without dead-lettering or completing the
delivery. Peek from the original sequence must still find the original ID/body.
The mapped identity/body and lock/sequence metadata must remain intact, with only
one reader invocation.

Before the original lock expires, acknowledge the same mapped Message on the same
consumer with an active token. Completion must succeed without redelivery or another
mapping, and bounded native peek must confirm removal. This is recovery through
acknowledgement, not a successful rejection retry or proof of DLQ behavior.

## Known native limitation

The emulator has an unresolved dead-letter defect recorded by earlier reader-failure
validation. Correct pre-entry cancellation should never reach that path, but the
unfixed implementation and a removed-guard mutation may reach it. Preserve native
logs and classify any broker abort, timeout or unrelated SDK failure separately.
Do not count an emulator failure or hang as intended mutation evidence, weaken the
test, or claim the existing eight DLQ-blocked cases are resolved. If the limitation
prevents usable RED/mutation evidence, report this gate as incomplete and review an
alternative rather than silently changing the contract.

A returned successful rejection fails the expected cancellation assertion directly.
Unexpected native exceptions require inspection of their cause; they are not
automatically accepted as cancellation-regression evidence. Initial setup/visibility
failures and naturally expired locks also do not establish this behavior. Each
focused case uses a fresh fixture to isolate any defective dead-letter attempt.

## Named production mutations

1. Remove the RejectAsync entry guard: the expected cancellation exception must
   fail with usable native evidence, subject to the emulator limitation above.
2. Return successful rejection for an already-cancelled token without settlement:
   the expected cancellation exception fails.
3. Omit native completion during the subsequent active-token acknowledgement:
   the final native peek remains nonempty.

If initially green, use the named mutations to establish RED. Only intended assertion
failures count. Preserve setup/build/broker failures separately and restore every
mutation. Adopt the approved test unchanged and verify AsbExternalWireReader remains
unchanged. Build all library targets, run both focused cases on .NET 9 and .NET 10
and both full available suites, comparing all 616 prior outcomes. Preserve unrelated
workspace edits and public receiver-wrapper compatibility. No commits or pushes.

## Scope and remaining gates

The adjacent test was approved and validated unchanged. No new helper,
receiver substitute, reflection or production test hook is added. ASBTestCommand
only configures the subscription; no handler or logging registration is introduced.

In-flight rejection/settlement cancellation, successful DLQ settlement, receive-arrival
races, provisioning, session acquisition, abandonment-failure recovery, session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. This batch does not establish overall release readiness.

## Implementation and validation evidence

All four initial executions fail at line 115 because rejection returns successfully
without throwing cancellation. The anticipated emulator limitation does not prevent
usable RED for this narrow case. RejectAsync now checks the caller token at entry,
before channel or receiver access, with XML documentation for the exception. All four
corrected and all four restored executions pass, preserving the delivery for later
acknowledgement on the same consumer without another receive or mapping.

Three named mutations fail both cases on .NET 9 at their intended xUnit assertions:
removed entry guard and successful early return at line 115 (no cancellation
exception), and omitted subsequent native completion at line 145. All six failures
are intended assertions; no emulator exception or hang is counted. All mutations
are restored. This does not test DLQ reading or resolve the eight earlier blocked
cases, even though this path supplies usable cancellation evidence.

All library targets build. Both full available suites pass 618 cases on their first
runs, zero failures/skips, retaining exactly all 616 prior case/outcome pairs.
Only AzureServiceBusConsumer changes among 1,171 non-generated production C# files:
the RejectAsync entry guard and XML exception documentation. The approved test and
reused reader remain unchanged. No public signatures, receiver-wrapper contracts or
dependencies change. Evidence: `/private/tmp/brighter-4170-asb-reject-cancellation/`
contains original RED, corrected/restored TRXs, mutations.json, regression TRXs,
exact baseline comparisons and source verification. Nothing committed or pushed.

The isolated emulator containers and network were removed after validation.
