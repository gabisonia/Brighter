# AWS scheduler destination selection

Status: implemented and validated locally. Destination tests, mutations and all
eight full scheduler/gateway regressions pass after correcting native timestamp
loss exposed by existing tests. Production snapshots preserve the prior WIP.

## Proposed behavior

Both SDKs must honor `UseMessageTopicAsTarget` when the business and scheduler
endpoints are different. Native mode keeps targeting the business endpoint with
the application payload. Callback mode targets `SchedulerTopicOrQueue` with a
`FireAwsScheduler` envelope that retains the original business topic, identity,
correlation, payload, tenant metadata and deposit telemetry. The callback retains
the sync/async dispatch choice. Scheduling must leave the source message intact.

There are eight cases per SDK, sixteen total: sync/async × SQS/SNS × native/callback.
Each runs on .NET 9 and .NET 10. Initial runs reproduced callback destination
failures in both SDKs. V4 additionally failed when its SDK topic lookup encountered
an empty topic collection before the SQS fallback.

- [V3 test](V3/When_scheduling_with_distinct_destinations_should_honor_the_selected_target.cs)
- [V4 test](V4/When_scheduling_with_distinct_destinations_should_honor_the_selected_target.cs)

Each test creates two distinct real native queues or topics, an isolated scheduler
group and an IAM role trusted by Scheduler. It calls the public factory and
scheduler, then independently reads the stored schedule using the native SDK.
The schedule is an hour in the future and is deleted in teardown; there is no
sleep until delivery, mocked scheduler, or manufactured callback execution.
The role needs no delivery policy because this batch never fires the target.
Cleanup removes the schedule, group, endpoints and role, using a bounded token.

This verifies persisted target selection and callback payload preservation. It
does not prove native scheduling execution, callback-to-producer mapping,
dispatch-time trace propagation, IAM delivery permissions or scheduler retry
recovery. Those remain required subsequent work for #4170. There is no proposed
writer execution inside the AWS service or change to the default native mode.

## Local integration setup included for review

The scheduler test helpers currently ignore `AWS_SERVICE_URL` and prefer the
local default credential profile. Four helper drafts enable the same explicit
local endpoint and per-process test account behavior already used by the gateway
suites. Without that environment variable, existing profile/environment behavior
is retained.

- [V3 endpoint helper](V3/GatewayFactory.cs), [V3 credentials helper](V3/CredentialsChain.cs)
- [V4 endpoint helper](V4/GatewayFactory.cs), [V4 credentials helper](V4/CredentialsChain.cs)

Only these existing helpers and the two new test files are proposed. There are no
new request/handler types, generated pumps, shared result counters or logger
registrations. No package or public API addition is proposed. Run locally with
explicit emulator URL and test credentials; do not execute against a real AWS
account. If the emulator lacks a required operation, record that limitation;
a setup error is not behavioral RED. The current scheduler CI uses real AWS;
these tests use genuine native resources and do not depend on fabricated ARNs.

## Work requested after approval

After approval, copy the exact six drafts. Establish the predicted callback
failures and correct only the destination lookup they reproduce in both SDKs.
Preserve native mode and the original message. Run the complete test, then verify
these independent named mutations on .NET 9 in both SDKs:

| Mutation | Expected cases per SDK | Assertion |
| --- | ---: | --- |
| Restore callback destination resolution from the original business message | 4 | Stored target destination |
| Disable the native-target branch | 4 | Native mode still selects the business destination |
| Omit the contained message in the callback envelope | 4 | Callback message is present |

Six mutations should produce 24 intended assertion failures. Compilation/setup
errors do not count. Restore exact source bytes after every mutation. Run restored
focused checks on both frameworks, build both schedulers on all supported targets,
and run all four full AWS Scheduler suites and all four full AWS gateway suites.
Compare existing outcomes and report unsupported emulator features separately;
do not exclude failing tests to claim full validation. Existing long-running
scheduler cases are included, even though remote CI filters its `Fragile=CI` trait.

If test semantics need correction, preserve the failure and submit the concrete
correction for review. All work remains local, uncommitted and unpushed. This batch
does not complete the remaining callback, telemetry, retry or transport work.

## Observed results and corrections

The initial V3 run had four passing native cases and four failing callback cases
per framework. V4 had two passing native SNS cases, two callback SNS destination
failures and four SQS cases failing inside `FindTopicAsync` on a null collection.
Both callback branches now resolve the destination from the callback message.
V4 uses a null-safe paginated topic lookup. All eight cases per SDK pass on both
frameworks before and after the six named mutations. Those mutations produce
24 intended assertion failures; exact source restoration is verified.

The copied test needed a fully qualified public IAM factory extension because
the test helper exposes an ambiguous extension with the same name. Assertions
are unchanged. The initial compilation failure is retained. One temporary native
branch mutation initially produced unreachable-code diagnostics; its corrected
runtime condition produced the required assertion failures. The compilation
failure is retained and is not counted as behavioral RED.

Full regression exposed setup defects in existing fixtures: twelve publications
lacked their required SQS channel name, twenty files passed an invalid 60-second
SQS polling timeout, and eight native payload fixtures omitted their UTF-8 charset.
These corrections touch 22 distinct fixture files. Their assertions, scheduling
delays and overall polling deadlines remain unchanged.

With delivery working, all four existing native-message cases per suite exposed
timestamp truncation: `Convert.ToString` discarded fractional seconds. Both SNS
and SQS scheduler payloads now use invariant round-trip formatting. The existing
full-header equivalence assertions provide RED without new or weakened tests.
This changes the native timestamp text from a culture-dependent value to an ISO
round-trip value; Brighter's existing parser accepts it. Header names, payloads,
target selection and callback envelope structure retain their contracts.

Floci 1.5.19 can store these schedules but cannot correctly invoke their universal
SNS/SQS targets. Floci 2.3.0 supports those targets; its SNS scheduler dispatcher
does not preserve random account namespaces. Final scheduler runs therefore use
four separate 2.3.0 containers with the default test account. The optional
`AWS_TEST_ACCOUNT_ID` override applies only when `AWS_SERVICE_URL` is configured.
Without an override the local helper retains per-process account isolation;
real-AWS credential selection is unchanged. Interrupted setup runs and native
timestamp failures remain in the artifact directory and are not passing suites.

All supported scheduler targets build with zero warnings/errors. With the final
binaries, all four complete scheduler suites pass 70 cases each, and all four
complete gateway suites pass 607 cases each, with zero failures or skips.
All prior gateway names/outcomes are retained. Both frameworks run the same
scheduler cases, including all eight new cases per SDK. Final validation uses
Floci 2.3.0; repository emulator pins remain unchanged.

Evidence: `/private/tmp/brighter-4170-scheduler-destinations/`, including TRX files,
mutation logs, original snapshots, correction lists and `verification.json`.
The next [native callback mapping batch](../aws-native-callback-mapping/README.md)
is prepared outside compiled projects and remains unapproved and unrun.
