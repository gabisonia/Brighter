# Hosted factory stream shutdown and replacement comparison

Status: approved and executed. All sixteen hosted factory shutdown/replacement cases passed; all owned resources are deleted. Broader production readiness remains open.

The [hosted ordered-nack comparison](../gcp-ordered-nack-hosted-brighter/README.md)
exercises active-consumer settlement. It does not exercise disposing a consumer with
an unsettled delivery and recovering that delivery through a replacement. The earlier
emulator regression intermittently missed the replacement receive, including the
sync/two-client factory case. This comparison targets that separate release finding.

Review [the observation](When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client.cs),
[the entry point](Program.cs), [the project](HostedFactoryShutdownControl.csproj)
and [the adapter diff](hosted-adapter.patch).

## Reused behavior and limits

This ports the existing approved factory shutdown diagnostic to hosted targeting.
The public factory/channel path, native shutdown observation, assertions and bounds
remain unchanged. Only targeting, unique resource prefix, request type, executable
wrapper and resource-name tracing change. SDK-owned connections remain in use.
The linked interceptor forwards real calls; no broker response or fault is injected.

Each case publishes one binary message with distinct application/native identity,
receives it through a factory-created Brighter channel and holds its receipt unsettled.
Disposal must finish within ten seconds, send a successful native deadline-zero
request during shutdown, avoid native acknowledgement, and complete or cancel the
pending callback. The message must then recover either through native Pull or through
a newly built channel from the same factory, preserving identity and bytes. The new
channel must own a distinct receipt and acknowledge it successfully.

The existing shutdown assertion observes a nonempty ack-ID set for the exact
subscription and SDK client generation after disposal starts; it is not a new raw
StreamingPull correlation assertion. Preserve that distinction. It is already guarded
by the emulator diagnostic's four mutations and 24 intended failures recorded in
the [native shutdown amendment](../gcp-factory-native-shutdown-observation/README.md).
This environment comparison introduces no production change or new mutation claim.

A passing bounded comparison cannot erase intermittent emulator failures, validate
buffered-after-requeue shutdown, or establish general lifecycle/concurrency safety.
The buffered-disposal finding and other release requirements remain separate.

## Fixed execution and effects

Run eight sync/async × one/two clients × native/replacement recovery cases once on
.NET 9, then once on .NET 10. At most sixteen unique topic/subscription pairs and
sixteen synthetic messages are created, with prefix `hosted-factory-shutdown-`.
No existing resource is modified. The executable requires the exact argument
`--project brighter-pubsub-validation`, rejects PUBSUB_EMULATOR_HOST, and configures
all native client builders ProductionOnly. Use local ADC without copying or printing
credentials. Normal cloud usage charges may apply.

Retain the ninety-second operation budget, twenty-second receive/recovery bounds,
ten-second primary disposal, three-second cleanup disposal and fifteen-second delete
bounds. Enforce a twenty-minute process limit per framework. Run sequentially and
stop at the first assertion, setup, authentication, quota or cleanup failure; do not
continue the other framework or repeat until green. Record all case identities,
native traces, exits, runtime/SDK versions and affected resource names. Verify deletion
of only this batch's owned resources. Do not repair IAM, enable APIs, select another
project, upgrade dependencies, lengthen bounds or change the client counts.

Build into isolated per-project artifact paths and preserve approved adapter/source
snapshots. Existing production/test sources, emulator selection and historical results
stay intact. No unrelated hosted suite or full-regression rerun is included in this
environment-only comparison. A failure must be diagnosed from retained native and
application traces before proposing a production correction.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". This adapter was prepared
under the test-first review workflow and subsequently approved. Its fixed batch is now complete. Approval covers the sixteen
fixed observations in the named project, owned-resource cleanup and local evidence/
report updates. It excludes production changes, other projects, deployment, commits
and publication. The previous ordered-nack approval does not execute this batch.


## Execution outcome

Eight cases pass on each framework. All sixteen cases satisfy the native shutdown
nack, forbidden-ack, callback completion/cancellation, identity/body recovery and
applicable replacement-receipt assertions. Trace verification confirms one stream
build for native recovery and two for replacement recovery. Disposal takes
0.135493–0.175947 seconds on .NET 9 and 0.128783–0.185121 seconds on .NET 10,
within the unchanged ten-second bound.

Both builds succeed with zero errors and two total obsolete-header warnings from
the unchanged diagnostic trace. Both processes exit zero. Sixteen synthetic messages
were published; all sixteen topic/subscription pairs are independently verified
absent after deletion. The approved adapter and all 6,484 existing src/tests C# files
are unchanged. No production correction, mutation, repeated-until-green run, SDK
upgrade, commit or publication is included.

The host is Darwin 25.6.0 arm64, eleven processors, runtimes 9.0.20/10.0.11, with
Pub/Sub 3.38.0/GAX gRPC 4.13.1. Evidence lives in
`/private/tmp/brighter-4170-gcp-hosted-factory-stream-shutdown/`, including source
snapshots, complete logs, fixed-case results, `final-verification.json` and `cleanup.json`.
See [validation](../../validation.md) for limits. This supplies bounded hosted evidence;
it does not erase historical emulator failures or establish general lifecycle safety.

The next [buffered-redelivery comparison](../gcp-hosted-buffered-stream-shutdown/README.md)
is a concrete unbuilt/unrun review draft, with SDK ownership changes explicitly shown.
