# ASB buffered mapping cancellation — validated

Status: approved, implemented and validated. Both cases pass on .NET 9 and .NET 10.
Both full available suites pass 608 cases with zero failures/skips, retaining all
606 previous case/outcome pairs.

## Approved contract

Two native queue/topic cases send two distinct binary deliveries and establish
visibility using bounded peeks. A buffer of two receives both. The configured reader
maps each delivery, cancelling the caller token only after its second mapping.
ReceiveAsync must throw OperationCanceledException without returning the first
already-mapped message. Both withheld deliveries must recover on the same consumer
with an active token before their original one-minute locks expire.

Recovery allows partial batches and either delivery order. A 20-second polling
window uses two-second receives (the final receive can extend past the window).
Each original native ID must recover exactly once with delivery count two, a fresh
lock token, unchanged native body/sequence, the custom reader's identity/body/headers,
and correct gateway lock/sequence metadata. Both acknowledgements must remove the
messages, verified with bounded native peek. Mapping occurs four times in total.

The helper delegates to the existing AsbExternalWireReader unchanged and records
deliveries sequentially. No native broker I/O is substituted. ASBTestCommand is only
subscription configuration; no new handler or closed-generic logging setup is needed.

## Native batch precondition and limits

SDK maximum batch size does not guarantee a minimum batch size. Initial visibility
does not prove both messages were received. The explicit assertion of two mapper
calls in the first ReceiveAsync establishes that precondition before examining the
cancellation result. If it fails, classify the run as a batch/setup limitation,
not evidence of a cancellation defect. Do not silently retry until green or weaken
this assertion; preserve evidence and review an amendment if required.

This batch covers earlier mapped deliveries and the delivery that triggers
cancellation. It does not cover unvisited buffered deliveries, cancellation during
broker wait/settlement, sessions, or throwing readers. Those remain separate work.

## Named production mutations

1. Release only the cancellation-triggering delivery, leaving earlier mapped
   deliveries locked: recovery set is incomplete before the original expiry.
2. Release only earlier mapped deliveries, leaving the triggering delivery locked:
   recovery set is incomplete before the original expiry.
3. Suppress cancellation and return accumulated mapped messages: expected
   OperationCanceledException assertion fails after the two-call precondition.
4. Clear the configured reader during cancellation reset: recovered mapped identity
   or mapper invocation assertions fail.
5. Replace recovered custom body with native bytes: mapped body assertion fails.
6. Omit native completion during acknowledgement: final native peek is nonempty.

Setup exceptions, compilation failures and failures of the batch precondition do
not count as intended mutation failures. Restore all mutations and confirm the
approved draft matches adopted sources. Run both full available suites and compare
all 606 baseline outcomes. Preserve unrelated workspace edits; no commits or pushes.

## Validation evidence

All four initial executions fail at the incomplete recovery set assertion (line 127),
after proving both deliveries were mapped in one receive. Only the triggering
message recovers. The minimal correction abandons the received batch before the
existing cancellation throw/reset path. All four green and all four restored
executions pass with unchanged approved test/helper sources.

Six named mutations produce 12 targeted failures: ten xUnit assertion failures and
two exact mapped-identity lookup failures. The lost-reader mutation returns default
IDs and fails the deliberate `Single` lookup at line 108 with “Sequence contains no
matching element”; it is not counted as an xUnit assertion failure. The release-only
mutations fail line 127, suppressed cancellation line 94, native body line 120, and
omitted completion line 136. All mutations are restored. The earlier-only release
mutation initially failed to compile because its temporary LINQ call lacked a
qualification. That log is preserved, excluded from mutation evidence, and the
qualified mutation then fails both intended recovery assertions.

All library targets build. Both full available suites pass 608 cases on their first
runs, retaining exactly all 606 prior outcomes. Only AzureServiceBusConsumer changes
among 1,171 non-generated production C# files. No public signature or dependency
changes. Evidence: `/private/tmp/brighter-4170-asb-buffered-cancellation/`, including
initial/green/restored TRXs, mutations.json, regression TRXs, exact baseline
comparisons and source-verification.json. No commits or pushes.

The native two-delivery precondition holds in every executed case. This does not
turn the SDK maximum into a minimum guarantee for future runs. Unvisited buffered
deliveries, abandonment failures, broker-wait/settlement cancellation, sessions,
session-lock instability, eight DLQ-blocked cases, 52 hosted classes and broader
transport/release gates remain open. This batch does not establish release readiness.

This batch's isolated emulator containers and network were removed after validation.
