# Azure Service Bus native wire writer — proposed batch

Status: approved, including the correction from internal constants to identical
literal wire-header names. Implemented and validated locally. The test and helper
in the compiled project match the approved review copies byte for byte.

Validation: twelve native cases pass on .NET 9 and .NET 10. Ten approved mutations
produce 52 intended assertion failures on .NET 9; all changes are restored. Each
framework passes 539 available regression cases (536 in the main run plus three
existing session mapping cases omitted by the initial discovery filter). These
include the twelve native cases. No failures or skips. All four library targets
build with zero warnings/errors in the final build. Fifty-two hosted test classes
remain unexecuted because the isolated fixture does not provide their broader
infrastructure; this is not a complete hosted suite result.

Evidence: `/private/tmp/brighter-4170-asb-native-wire-writer/` contains build logs,
TRX results, mutation outcomes, the regression scope manifest, image IDs and source
verification. No commits or pushes were performed.

## Reviewed contract

Add nullable `AzureServiceBusPublication.WireMessageWriter` with type
`IAmAWireMessageWriter<ServiceBusMessage>` and expose
`DefaultAzureServiceBusWireMessageWriter` for composition. A missing writer retains
the existing mapping; explicitly selecting the default must produce the same
observed fields. Keep the existing static publisher API compatible.

The configured writer owns the native body and application metadata. The gateway
owns delivery: the publication destination selects the sender, and configured
publication TTL overrides a writer's TTL. A custom writer's metadata must not be
overwritten or supplemented with the default CloudEvents mapping. Writers must not
mutate their source message. This follows the shared writer contract already used
by the implemented transports.

## Twelve native cases

The test matrix covers queue/topic, sync/async send, and three writer selections:
implicit default, explicit default, and a complete custom representation. Sends
use the public producer registry factory. Receives use the native Service Bus SDK,
not the Brighter reader, so matching writer/reader mistakes cannot cancel out.

The custom writer supplies a binary prefix containing invalid UTF-8 bytes, distinct
native message ID, subject, correlation ID and content type. It places traceparent,
tracestate and baggage in external application properties and emits no CloudEvents
properties. Its one-minute TTL deliberately conflicts with the publication's
five-minute TTL, making delivery-control ownership observable. Default cases retain
the existing native fields, CloudEvents metadata and source bag entry. Every case
checks that the source body, identity, destination, metadata and bag remain unchanged.

## Native environment

Use a dedicated environment with the supplied `emulator-config.json`, derived from
the repository's Service Bus emulator sample. It pre-provisions `wire-mapping-queue`,
`wire-mapping-topic`, and subscription `wire-observer`. Their default TTL is one hour;
sessions and duplicate detection are disabled. The test uses `OnMissingChannel.Assume`
and makes no administrative calls. Do not point it at shared application entities.

The existing `ASBCreds` provider reads `BrighterTestsASBConnectionString` or
`BrighterTestsASBNameSpace`; neither is configured in the current shell. After test
approval, provision an isolated local emulator and configure the test process without
printing credentials. The entity names can be overridden with
`BrighterTestsASBWireQueue`, `BrighterTestsASBWireTopic`, and
`BrighterTestsASBWireSubscription`. Run frameworks against isolated environments or
sequentially, since their receivers must not compete for these entities.

Each case completes the native message it observed and disposes its SDK client.
There is no queue purge or deletion of externally owned entities. The dedicated
fixture must start empty, and the xUnit collection serializes these twelve cases.
The runner needs an outer hang timeout because the synchronous send interface has
no cancellation argument. Fixture provisioning/cleanup is a runner responsibility.

## Approval and validation plan

After approval, copy both C# files unchanged into `MessagingGateway` and
`TestDoubles` respectively. Missing-public-API compilation failures are expected
initially, but do not count as behavioral regression evidence. Implement the
minimum contract above, then prove these named mutations against the unchanged tests:

1. Ignore the configured writer — custom native mapping assertions fail.
2. Overwrite mapped message ID, subject, correlation ID, content type or body with
   source values, independently — the corresponding native assertions fail.
3. Ignore publication TTL — native TTL assertion fails for default and custom cases.
4. Add default CloudEvents metadata after custom mapping — absence assertion fails.
5. Remove the default CloudEvents ID — default compatibility check fails.
6. Mutate the source subject during custom mapping — source-preservation assertion fails.

Restore every mutation, build all library targets, run both focused frameworks,
and run the applicable complete Azure Service Bus suites. Report any hosted/native
suite portion whose administrative or broker capabilities are unavailable; emulator
success is scoped evidence, not a substitute for that release gate. Do not mark the
whole Azure transport complete or commit/push as part of this batch.

## Remaining scope

Reader selection and settlement, batch sends, scheduling, retry/rejection mapping,
writer failure/null results, sessions/partitioning, concurrent reuse, endpoint
isolation, handler telemetry and hosted compatibility remain separate requirements.
The earlier Kafka native-close stall also remains unresolved; twenty fresh passing
executions of its existing regression test did not establish a fix.

The user approved this batch and the four literal-header assertion corrections
before implementation. The review gate for this batch is satisfied.
