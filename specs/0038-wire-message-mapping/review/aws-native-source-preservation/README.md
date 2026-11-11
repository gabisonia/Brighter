# Native SNS scheduler source preservation

Status: approved, implemented and validated. Exact approved tests are in both
Scheduler test projects. Initial RED, all six named mutations, restored focused
checks, all supported target builds and all eight full regressions are complete.

## Contract

Native SNS scheduling must preserve the supplied message. When its correlation
ID is absent, retain the existing nonempty generated wire correlation without
assigning that fallback back to the source header. When an ID is supplied, use
that exact value on the wire and leave the source intact.

Before this correction, both SDKs wrote a generated correlation ID back to
`message.Header.CorrelationId`. The preceding destination-selection tests supply
an ID and therefore do not cover this case. The new cases reproduce this at the source-correlation equality assertion in
both SDKs and frameworks; the correction keeps the fallback local to serialization.

- [V3 test](V3/When_scheduling_an_sns_message_should_preserve_source_and_wire_correlation.cs)
- [V4 test](V4/When_scheduling_an_sns_message_should_preserve_source_and_wire_correlation.cs)

Four cases per SDK, eight total, cover sync/async scheduling with absent/supplied
correlation. Each uses the public factory and native SNS, IAM and Scheduler SDKs
to create a topic, unique group and role, then read the stored native schedule.
The schedule is an hour in the future and deleted during bounded cleanup; it does
not execute. The test checks SNS destination, original payload and message ID,
presence/type/nonempty wire correlation, preservation of a supplied correlation,
the unchanged source correlation, and exact serialized source equality including
tenant metadata and deposit telemetry. No internal helper or mock is driven.

## Work requested after approval

Copy the two exact drafts into the respective AWS Scheduler test projects. Run
all eight cases on both frameworks. Missing-correlation cases are expected to fail
at source correlation equality (two per SDK/framework), while supplied-correlation
controls should pass. Compiler/setup failures are not behavioral RED.

Correct only the reproduced source mutation while retaining generated native
wire correlation and supplied IDs. No new public API, callback format, default
gateway writer, outbox schema or provider dependency change is proposed. The
expected implementation keeps the fallback in local serialization state.

Run these independent named mutations on .NET 9 in each SDK:

| Mutation | Expected failures per SDK | Assertion |
| --- | ---: | --- |
| Write generated correlation back to the source | 2 missing-ID cases | Source correlation remains absent |
| Generate a new wire correlation even when supplied | 2 supplied-ID cases | Native correlation equals the supplied value |
| Omit the correlation attribute | All 4 cases | Native correlation attribute is present |

Six mutations should produce sixteen intended assertion failures. Keep the test
bytes unchanged; restore exact production bytes after each mutation and confirm
focused GREEN on both frameworks. Build both schedulers on all supported targets.
Run all four complete scheduler suites (74 cases expected per suite) and all four
complete AWS gateway suites (611 cases each), retaining their prior outcomes. Scheduler runs use
separate Floci 2.3.0 containers/default accounts to avoid the known emulator SNS
account-scope limitation. No real AWS or remote CI pass is claimed.

This closes the identified native SNS source-mutation gap only. It does not
establish custom writers inside native AWS targets, callback redelivery after
writer failure, SNS/RabbitMQ pump telemetry, other schedulers, or other transport
integrations. Preserve failures and return semantic test corrections for review.
All changes remain local, uncommitted and unpushed.


## Results

Initial runs fail exactly the two missing-ID source assertions per SDK/framework;
the supplied-ID controls pass. The fix keeps the generated correlation local to
native serialization. All focused cases then pass on both frameworks. Six named
mutations produce sixteen intended assertion failures with unchanged test bytes.
All four full Scheduler suites pass 74 cases each, retaining the prior 70; all
four full gateway suites retain exactly 611 passing cases. No skips or failures.
Both schedulers build on all four supported targets.

[Validation details](../../validation.md#native-sns-scheduler-source-and-correlation-preservation)
record exact assertions, the verification-script line correction, build warnings,
run times, artifacts and emulator limits. Only the two Scheduler source files
changed in this batch. All changes remain uncommitted and unpushed.

The next [SNS pump telemetry batch](../sns-pump-telemetry/README.md) is prepared for
review outside compiled projects. Full #4170 completion remains open.
