# Hosted buffered-redelivery shutdown comparison

Status: approved and executed. All eight hosted buffered-redelivery cases passed; all owned resources are deleted. Broader production readiness remains open.

The [factory shutdown comparison](../gcp-hosted-factory-stream-shutdown/README.md)
holds a delivery already returned to its caller. The remaining buffered-redelivery
finding concerns a requeued message delivered into the local buffer but never read
by the application before shutdown. These are different callback states.

Review [the observation](When_stopping_a_gcp_stream_should_release_the_observed_native_delivery.cs),
[the entry point](Program.cs), [the project](HostedBufferedShutdownControl.csproj)
and [the adaptation diff](hosted-adapter.patch).

## Behavior and production collaborators

Reuse the existing diagnostic's four buffered sync/async × native/replacement recovery
cases. Its behavioral sequence and assertions stay unchanged: publish a binary native
message, receive through the public gateway consumer, requeue through that consumer,
then await the public GcpStreamConsumer.Reader and TryPeek the unread redelivery.
The readiness assertions establish its native identity, body, distinct pending receipt
and incomplete callback before disposal. No fixed sleep substitutes for readiness.

Disposal must finish within ten seconds, avoid native acknowledgement and complete a
successful deadline-zero request after shutdown begins. The buffered callback must
cancel or complete with Nack. Native Pull or a replacement gateway consumer must recover
the same application/native identity and body; replacement acknowledgement must complete.
The native shutdown assertion observes a nonempty ack-ID set; it is not a newly added
raw StreamingPull/ack-ID correlation assertion.

The prior component diagnostic constructed SubscriberClientImpl with a shared native
API client and a no-op disposal callback. This adapter instead constructs a real
SubscriberClient through its public builder, with SDK-owned connections and passive
ApiSettings.Interceptor observation. It uses one client and flow-control limit one,
matching the earlier diagnostic's single native-client array. GcpStreamConsumer and
GcpPubSubStreamMessageConsumer are composed through their public constructors with
the same real collaborator types used by the production factory. Their existing
shutdown owns StopAsync and DisposeAsync; the old extra client-stop cleanup loop is
removed. Native admin/Pull calls use a separate ProductionOnly API client.

These ownership changes are explicit in the diff. They are necessary to exercise the
real shutdown behavior. This component diagnostic can observe the existing public
buffer; it does not use reflection, expose a new API, mock the SDK or claim to drive
the public channel factory/generated pump. The separate factory comparison covers
that entry point. A passing result cannot establish the cause of every historical
failure or full lifecycle safety.

## Fixed execution and limits

Run four buffered cases on .NET 9, then four on .NET 10: at most eight unique topic/
subscription pairs and eight synthetic messages, prefixed `hosted-buffered-shutdown-`.
The executable requires exactly `--project brighter-pubsub-validation`, rejects
PUBSUB_EMULATOR_HOST and targets production explicitly on native builders. Use local
ADC without copying or printing credentials. No existing resource is modified.

Retain the ninety-second operation budget, twenty-second first receive/buffer-readiness/
recovery bounds, ten-second primary disposal, three-second receipt/cleanup bounds and
fifteen-second deletes. Enforce a twenty-minute process limit per framework. Stop at
the first assertion, setup, auth, quota or cleanup failure; do not run later cases or
the other framework, repeat until green, increase bounds, change client count, upgrade
dependencies, repair IAM or enable APIs. Record the exact resource names and verify
owned-resource deletion with read-only listings.

Build into isolated per-project artifact paths. Preserve all source/adapter snapshots,
case identities, native/application traces, runtime versions, failures and exits.
Existing production/tests and historical emulator failures stay intact. The reused
assertions have the earlier diagnostic's mutation evidence; no new mutation or full
characterization claim is made for this hosted environment/ownership comparison.
No full regression rerun or unrelated hosted suite is included. A failure requires
evidence-based diagnosis before proposing any implementation change.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". This concrete adapter was
prepared through the test-first review workflow and subsequently approved. Its fixed
batch is now complete. Approval covers
the eight fixed hosted observations, owned-resource cleanup and local evidence/report
updates. It excludes production changes, other projects, deployment, commits and
publication. Previous shutdown/factory approval does not execute this new adapter.


## Execution outcome

All four cases pass on each framework with actual unread buffered readiness before
shutdown. Native shutdown nack, forbidden-ack, callback completion/cancellation,
identity/body recovery and replacement acknowledgement assertions all pass unchanged.
Trace verification records disposal at 0.121941–0.237365 seconds on .NET 9 and
0.121745–0.164711 seconds on .NET 10, within the original ten-second bound.

Both builds have zero warnings/errors; both processes exit zero. All eight temporary
topic/subscription pairs are independently verified absent after publishing eight
synthetic messages and deleting them. The approved SDK-ownership adaptation and all
6,484 existing src/tests C# files remain byte-identical. No implementation correction,
mutation, SDK upgrade, timeout increase, commit or publication is included.

Execution uses Darwin 25.6.0 arm64, eleven processors, runtimes 9.0.20/10.0.11 and
Pub/Sub 3.38.0/GAX gRPC 4.13.1. Evidence is under
`/private/tmp/brighter-4170-gcp-hosted-buffered-stream-shutdown/`, including complete
traces, fixed results, source snapshots, `final-verification.json` and `cleanup.json`.
See [validation](../../validation.md) for scope and limits. Historical emulator
failures remain recorded; this bounded comparison is not whole-feature sign-off.

The next [publication writer-isolation batch](../gcp-publication-writer-isolation/README.md)
is a concrete unbuilt/unrun review draft for local emulator execution, returning to
remaining mapping/isolation coverage.
