# Azure Service Bus native reader and settlement — proposed batch

Status: implemented and locally validated after approval of the subscription setup
correction. Twelve native cases pass on each framework; ten mutations yield 64 clean
intended failures. Each framework passes 551 available regression cases with no
failures/skips. All library targets build. Fifty-two hosted test classes remain
unexecuted; the broader transport is not complete.

The original draft omitted required subscription configuration. The approved
correction uses `AzureServiceBusSubscription<ASBTestCommand>` and explicit
Reactor/Proactor selection. Assertions and helper semantics were preserved. Both
compiled files match their review copies exactly. The legacy MessageType assertion
emits one obsolete-API warning per target; existing test warnings also remain.

The initial nack mutation had expired-lock interference. A five-minute-lock repeat
produced twelve failures precisely at the second receive; the one-minute fixture was
restored for final regressions. Omitted-lock and stale-message mutations fail at the
receive assertion, before later metadata assertions, because dispatch rejects the
invalid lock. See validation.md for the complete evidence and limitations.

Evidence: `/private/tmp/brighter-4170-asb-native-wire-reader/`. No commits or pushes.

## Contract for review

Add nullable `AzureServiceBusSubscription.WireMessageReader` typed as
`IAmAWireMessageReader<ServiceBusReceivedMessage>`, and expose
`DefaultAzureServiceBusWireMessageReader(AzureServiceBusSubscription)` for composition.
Implicit and explicit defaults preserve the existing mapping and public creator API.
The configured reader receives the actual native SDK delivery, not a reconstruction.
The consumer attaches the actual lock token and sequence number after mapping,
overwriting any reader-supplied values for those gateway-owned fields. It does not
overwrite application identity, subject, correlation or body from a custom reader.
Keep the existing wrapper APIs compatible; no public native accessor is needed merely
to test internal plumbing. Custom selection on legacy third-party wrappers without
native delivery access is outside this batch and must not be claimed validated.

## Twelve native cases

The matrix combines queue/topic, sync/async consumer factory creation and operations,
and implicit default/explicit default/custom readers. Send directly through the native
SDK so writer/reader errors cannot cancel out. Use invalid UTF-8 body bytes, distinct
native identity, subject and correlation, plus an application property.

The custom strategy returns distinct identity, subject, correlation and body, omits
the original application property, adds its own property, and deliberately supplies
an empty lock token and negative sequence number. Assertions require the consumer to
replace those values with native delivery metadata before settlement.

Every case receives, nacks, receives again, checks unchanged sequence and fresh lock,
then acknowledges. Custom cases also check native delivery count two and preserved
native body and fields. A native SDK peek starting at the exact sequence number must
observe removal after acknowledgement. The peek polls for up to two seconds and uses
an explicit sequence each time so its cursor cannot hide an unacknowledged message.
A retained locked message is visible to peek; merely failing to receive while locked
would not establish successful acknowledgement.

## Named mutations after initial green

1. Ignore configured reader selection: custom mapping assertions fail.
2. Replace custom mapped identity with native identity: custom identity assertion fails.
3. Replace custom mapped subject with native subject: custom subject assertion fails.
4. Replace custom mapped body with native body: custom binary body assertion fails.
5. Do not replace a reader-supplied lock token: custom metadata assertion fails.
6. Do not replace a reader-supplied sequence number: custom metadata assertion fails.
7. Omit native abandon during nack: second receive assertion fails.
8. Omit native completion during acknowledgement: bounded native peek assertion fails.
9. Reuse the first mapped message on redelivery: fresh-instance/lock/call-count assertions fail.
10. Change native subject before custom reading: native-input preservation assertion fails.

Mutation implementation changes production only, never approved assertions/helpers.
Restore every mutation and confirm green. Compiler errors do not establish behavioral
regression protection. Preserve the exact assertions under review.

## Fixture and verification

Reuse the previous batch's isolated emulator entity configuration at
`../asb-native-wire-writer/emulator-config.json`, with no hosted credentials or shared
application entities. Start with empty entities. Run frameworks sequentially and
recreate the dedicated fixture after failed runs/mutations so locked or abandoned
messages cannot contaminate later cases. Cleanup is a runner responsibility; tests
must not delete or purge externally owned entities. A runner hang timeout is required
because sync consumer operations do not expose cancellation.

After approval, copy the test and helper unchanged into MessagingGateway and
TestDoubles. Establish expected missing-API RED, implement the minimum contract,
run native cases on both frameworks, all named mutations, all available ASB
regressions (including the previous writer cases), and all library target builds.
Retain an explicit manifest of hosted tests that cannot run in this fixture. Do not
claim complete hosted compatibility. No commits or pushes are part of this batch.

## Remaining scope

Throwing/null reader recovery, mixed batches, sessions, retry count mapping,
forwarding, reader composition failure semantics, concurrent reuse, endpoint
isolation, custom telemetry and handler propagation remain separate requirements.
The broader scheduler/transport gates and Kafka native disposal stall remain open.

The repository's `.agent_instructions/testing.md` requires “STOP after writing the
test and ASK FOR APPROVAL” and “DO NOT run tests without approval.” The original batch and the subscription-construction correction were both approved
before their execution. This batch's review gate is satisfied.
