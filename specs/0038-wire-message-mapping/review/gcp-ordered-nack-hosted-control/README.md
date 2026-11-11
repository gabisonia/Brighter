# Hosted Pub/Sub ordered-nack control

Status: approved and executed. All 24 hosted SDK observations recovered both messages; all owned resources are deleted. This does not establish Brighter production readiness.

The native SDK control reproduces missing redelivery without Brighter. The subsequent
raw protocol control isolates an emulator stall: adding a stream and acknowledging
the second message before nacking the first prevents redelivery within thirty seconds;
repeating the second ack releases it. Both frameworks exhibit this pattern. This does
not establish hosted-service behaviour or justify changing Brighter defaults/tests.

Review [Program.cs](Program.cs) and [HostedControl.csproj](HostedControl.csproj).
This is the earlier native SDK control with explicit hosted targeting. The receive,
nack, immediate subsequent receive/ack, passive observation, bounds and result reporting
are unchanged. It references no Brighter production assembly and uses the pinned SDK.
The emulator-specific raw protocol experiment is not transplanted to the hosted service.

## Target and bounded effects

Read-only local configuration inspection found the active gcloud project's name is
`brighter-pubsub-validation` and an Application Default Credentials file exists.
Before execution this established only local configuration, not credential validity
or project permissions. The approved execution below subsequently exercised ADC and
the required Pub/Sub operations successfully; credentials were not printed.

Execution requires the exact argument `--project brighter-pubsub-validation`, rejects
PUBSUB_EMULATOR_HOST, and uses ProductionOnly detection on every native client builder.
No alternative project is selected from an environment variable or credential quota
project. If this is not the intended validation project, change the reviewed target
before execution.

Run four fixed rounds × SDK-default/one/two subscriber clients on each framework:
twelve observations per framework, twenty-four total. Each observation creates one
uniquely named temporary topic and ordered subscription prefixed `hosted-ordered-nack-`,
publishes two small synthetic messages and deletes its own resources in finally.
Successful full execution therefore creates twenty-four temporary topic/subscription
pairs and publishes forty-eight messages. No existing topic/subscription is modified
or deleted. Normal cloud usage charges may apply.

Use ADC through the SDK; do not copy credentials into source, logs or evidence. Local
host execution avoids mounting credential files into containers. Record actual runtime
and processor counts; these may differ from the Linux emulator runs. Do not change
client counts, repeat until green, increase bounds, or upgrade dependencies to obtain
a desired result. Stop on authentication, permission, quota, setup or cleanup errors;
report the affected resource names if cleanup fails. Do not repair IAM or enable APIs
implicitly.

Each observation retains its ninety-second operation budget, fifteen-second initial
receive, thirty-second redelivery loop and bounded cleanup. Enforce a twenty-minute
process limit per framework. Results and native traces must distinguish completed nack,
raw redelivery and application receipt. Exit zero alone does not mean all scenarios
recovered. No credentials or real application payloads belong in the report.

Passing hosted observations would provide a bounded comparison with the emulator,
not prove absence of intermittent failures, validate the Brighter hosted integration,
clear unrelated lifecycle findings or complete issue #4170. Preserve failing emulator
and Brighter evidence. Do not skip or weaken those tests based on this control.

## Approval boundary

[Repository testing instructions](../../../../.agent_instructions/testing.md) require
"STOP after writing the test and ASK FOR APPROVAL". This is a new hosted experiment
with explicit temporary cloud resources beyond the approved emulator matrix.
Approval covers this exact project, fixed observations, owned-resource cleanup and
local evidence/report updates. It excludes IAM/API changes, other projects, deployment,
commits and publication. The user subsequently approved and this exact batch was executed. That approval
does not extend to the separately drafted Brighter hosted adapter below.


## Execution outcome

All twelve observations on each framework recovered both application IDs and completed
correlated native nacks. All first native messages were observed twice with unchanged
identity/body/order and a new ack ID. Both builds have zero warnings/errors; both
processes exit zero. Read-only hosted listings independently confirm all 24 topics
and 24 subscriptions were deleted after publishing 48 synthetic messages.

The local host is Darwin 25.6.0 arm64, eleven processors, .NET runtimes 9.0.20 and
10.0.11. Pub/Sub 3.38.0 and GAX gRPC 4.13.1 remain pinned. There is no Brighter
production dependency. This differs from the Linux emulator environment; passing
observations do not establish a hosted failure rate or resolve Brighter regressions.
Eleven/net9 and twelve/net10 lease-extension calls were client-cancelled after result
recording during shutdown; these are preserved alongside the successful nack traces.

All 6,484 src/tests C# files and the approved Program.cs/project are unchanged.
Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-hosted-control/`, including
`final-verification.json`, native logs, per-framework results and `cleanup.json`.
See the [validation record](../../validation.md) for the comparison and its limits.

The next [Brighter hosted comparison](../gcp-ordered-nack-hosted-brighter/README.md)
is a concrete unbuilt/unrun review draft. No production change, commit or publication.
