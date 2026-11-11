# Hosted ordered-nack comparison through Brighter

Status: approved and executed. All twelve hosted Brighter observations passed; all owned resources are deleted. Broader production readiness remains open.

The preceding [hosted SDK control](../gcp-ordered-nack-hosted-control/README.md)
contains no Brighter production dependency. This comparison exercises Brighter's
public producer/channel factories and sync/async send, receive, nack and acknowledge
operations against the same explicitly approved validation project.

Review [the observation](When_nacking_an_ordered_gcp_delivery_should_send_a_native_nack_and_recover_both_messages.cs),
[the execution entry point](Program.cs), [the project](HostedBrighterControl.csproj)
and [the adapter diff](hosted-adapter.patch).

## Reused behavior

The observation retains the existing emulator diagnostic's operation sequence,
assertions and bounds. It changes only hosted targeting, unique resource prefix,
execution wrapper, request type and resource-name tracing. The diff makes this
reviewable. Existing production/test sources and their emulator selection remain
unchanged. No new production behavior is proposed.

Each case creates an ordered native subscription and sends two binary messages with
one ordering key through Brighter. It checks application/native identity, body,
ordering key and pending receipt, nacks the actual first delivery, then immediately
receives and acknowledges subsequent deliveries. It does not wait for the native
nack RPC before receiving. The final assertions require a successfully completed
deadline-zero native request correlated to that first delivery, its receipt's Nack
reply and both application IDs. Native stream observations remain passive.

The existing assertion already has eighteen intended mutation failures recorded in
the [emulator diagnostic](../gcp-ordered-nack-native-observation/README.md). This is
an environment comparison of that behavior, not a new characterization claim.
No mutations or production changes are included. A passing hosted comparison does
not erase the failed emulator restoration, establish that every historical failure
has the isolated emulator cause, or prove general production readiness.

## Fixed execution and effects

Run six sync/async × SDK-default/one/two client cases once on .NET 9, then once on
.NET 10: at most twelve unique topic/subscription pairs and twenty-four synthetic
messages. Zero leaves the SDK client count unset. Each resource is prefixed
`hosted-brighter-nack-`. Run locally with ADC, never copying or printing credentials.
The executable requires exactly `--project brighter-pubsub-validation`, rejects
PUBSUB_EMULATOR_HOST and configures every native client builder ProductionOnly.

Preserve the ninety-second operation budget, fifteen-second first receive,
thirty-second subsequent receive loop, ten-second channel disposal and fifteen-second
native delete bounds. Enforce a twenty-minute process limit per framework, including
synchronous and producer-disposal paths. Preserve every trace and exit. Stop the
matrix at its first failure, including assertion, setup, auth, quota or cleanup;
do not continue to the other framework or retry until green. Retain the failed
resource names and verify cleanup of only this batch's resources.

Build artifacts and source snapshots belong in a separate evidence directory with
per-project intermediate paths. Preserve the exact production baseline and compare
the hosted adapter with its reviewed source. Parse case identities and results,
not merely exit codes. Run no unrelated hosted tests. No IAM changes, API enablement,
SDK upgrades, client-count changes, timeout increases or test weakening are included.

If the unchanged Brighter path fails, report where native and application evidence
diverge before proposing a correction. If it passes, record a bounded hosted comparison;
broader lifecycle, generated-pump, mapping, regression and release work remain open.

## Approval boundary

The repository's [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Following the repository
test-first workflow, this concrete hosted adapter was prepared for review before the user approved
its execution. The approved batch is now complete. Approval covers the twelve fixed observations in the
named project, owned-resource cleanup and local evidence/report updates. It excludes
implementation changes, other projects, deployment, commits and publication.


## Execution outcome

All six sync/async × default/one/two-client cases pass on each framework. The native
trace independently confirms all twelve correlated nacks, redelivery of each first
message with preserved identity/body/order and a new acknowledgement ID, and recovery
of both application IDs. Both processes exit zero. Twenty-four synthetic messages
were published; all twelve topics and twelve subscriptions are independently verified
absent after cleanup.

Both frameworks build with zero errors and four total CS0618 warnings from the
unchanged diagnostic's MessageHeader.MessageType checks/traces. All 6,484 existing
src/tests C# files and the approved adapter code/project remain unchanged. Native
logs retain six client-cancelled RPC observations per framework alongside the successful
required nacks. No production correction, mutation, dependency upgrade, test weakening,
commit or publication is included.

Execution used Darwin 25.6.0 arm64, eleven processors, runtimes 9.0.20/10.0.11 and
Pub/Sub 3.38.0/GAX gRPC 4.13.1. Evidence is under
`/private/tmp/brighter-4170-gcp-ordered-nack-hosted-brighter/`, including full traces,
case results, source/adapter snapshots, `final-verification.json` and `cleanup.json`.
See [validation](../../validation.md) for scope and limits. The earlier emulator
restoration/regression failures remain recorded and unresolved; this is a bounded
hosted comparison, not broad lifecycle or release sign-off.

The next [factory shutdown/replacement comparison](../gcp-hosted-factory-stream-shutdown/README.md)
is a concrete unbuilt/unrun review draft targeting the separate lifecycle finding.
