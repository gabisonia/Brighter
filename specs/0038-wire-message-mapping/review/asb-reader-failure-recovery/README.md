# Azure Service Bus reader failure recovery — validation blocked

## Native x64 isolated Docker follow-up — 2026-11-11

The [local Docker acceptance record](../x64-acceptance/asb-recovery-isolated/README.md)
runs each original case on unused entities, one process per theory row. All eight
historical test identities are retained on both targets. .NET 9 passes six and
fails the synchronous throwing-reader topic and queue cases at the native DLQ
receive assertion, with duplicate `S.SP-SequenceNumber:1` faults on both entities.
.NET 10 passes all eight cases. Fresh entities eliminate inherited wrong-ID
observations without reliably eliminating the underlying fault. All four owned
containers are stopped. Source/assertions are unchanged; the ten mutations and
remaining acceptance gates stay open. This is not a production-readiness sign-off.

## Docker retry — 2026-11-10

A bounded retry of the unchanged eight cases used fresh, isolated Docker fixtures
for each framework. The published emulator `latest` digest matches the cached
image; this is not an upgrade result. The emulator runs natively on ARM64 with a
separate SQL Server 2022 backend running under amd64 emulation.

.NET 9 passes all eight cases. .NET 10 passes all four queue cases and fails all
four topic cases. Its first topic case receives no DLQ delivery. Three later topic
cases receive an earlier case's ID. At the first failure, the broker logs a duplicate
`S.SP-SequenceNumber:1` insertion and faults the topic subscription. This reproduces
the earlier emulator fault; it does not establish a .NET 10 or Brighter defect.
The successful .NET 9 run does not resolve it.

Both runs have continuous wall/monotonic clocks. The 691 relevant source inputs
match the latest validated build state; the test and helper match the reviewed
copies. No production code, tests or assertions changed. Both SQL backends remained
running until cleanup, but these short trials do not establish long-term SQL stability
or supported native-x64 SQL Server acceptance.

The initial setup attempt exhausted Docker's automatic address pools before any
test ran. A checked explicit subnet permitted the isolated fixtures to start without
removing existing networks. All four owned containers are stopped and retained.
No Azure login, cloud resource, commit or push was required.

[Verification and case order](../asb-docker-retry/verification.json),
[.NET 9 TRX](../asb-docker-retry/net9.0.trx),
[.NET 10 TRX](../asb-docker-retry/net10.0.trx) and
[the failing broker log](../asb-docker-retry/brighter-4170-asb-docker-retry-broker-net10-0.log)
are retained. Fixture configuration, runner and source hashes are alongside them.
The runner records this one-shot trial and refuses to overwrite its retained network.
Local fixture passwords are redacted from retained logs. Raw SQL logs remain under
`/private/tmp/brighter-4170-asb-docker-retry/`.

The batch remains incomplete. A healthy broker and the outstanding mutation/full
regression checks are still required; another unchanged passing retry cannot supply
that evidence.

## Earlier validation

Status: the original batch and subsequent ReceiveAndDelete observer correction are
approved, applied and executed. The compiled test/helper match the review copies.
The fallback implementation builds for all library targets. This batch remains
incomplete; it is not waiting for another approval.

Fresh isolated runs after the correction pass seven of eight cases on each framework;
the first topic case receives no native DLQ message. The same fault persists with
the native ARM64 emulator and after an additional 45-second readiness delay. A
queue-only mutation run also reproduces the DLQ failure, disproving the earlier
assumption that queue coverage could be validated independently. Emulator logs show
an internal duplicate SequenceNumber:1 insertion followed by entity abort. This is
evidence of a broker fault, not a complete diagnosis of every recovery path.

Final existing regressions pass 551 cases on each framework with zero failures/skips,
retaining exactly the preceding case/outcome multiset. These exclude the eight new
recovery cases. No assertions were weakened. Mutation validation is incomplete and
all production mutations are restored. Fifty-two hosted test classes remain unrun.
Completing this batch requires a healthy broker, followed by the full recovery and
mutation checks; hosted test credentials are not currently configured.

Evidence and runners: `/private/tmp/brighter-4170-asb-reader-failure-recovery/`,
including `corrected-baseline-verification.json`, `queue-mutations.json`,
`queue-only-emulator-logs/` and `corrected-emulator-logs/`. The two dedicated test
containers are stopped after validation. No changes are committed or pushed.

## Contract for review

A configured reader throwing a mapping exception or returning null must produce an
MT_UNACCEPTABLE Brighter message retaining the native message ID, exact binary body,
subscription destination, lock token and sequence number. This lets the ordinary
consumer rejection path settle the original broker delivery. Do not acknowledge,
dead-letter or otherwise settle inside the mapping failure handler itself.

This batch exercises InvalidOperationException and null return values. It does not
establish cancellation policy, handling of every exception type, default-parser
failures, or behavior of a reader that has already mutated its native input.

Rejecting the fallback with reason Unacceptable must move the original delivery to
the native dead-letter subqueue, preserving its payload, identity and application
properties. A subsequent healthy message through the same consumer must still use
the configured reader and support acknowledgement. No new public API is required.

## Eight native cases

Queue/topic × sync/async × exception/null reader result. The public consumer factory
creates consumers with the already approved subscription reader selection. A generic
ASBTestCommand subscription supplies the required request context, and pump mode is
explicitly Reactor or Proactor. No handlers or request mappers run; the existing
command type is configuration context only.

The native SDK sends binary messages containing invalid UTF-8. A strategy helper
records the actual delivery, throws or returns null when marked by an application
property, and maps healthy messages to distinct identity/subject/body. Tests assert
the failure control signal, original bytes and gateway-owned delivery metadata,
reject using the public consumer API, then independently receive from the native
DLQ. They check native fields, original application properties, reason and description
using receive-and-delete for the independent DLQ observation. A healthy delivery follows on the same consumer;
its custom mapping and successful acknowledgement are checked with an explicit-
sequence native peek, bounded to two seconds.

## Named mutations

1. Let the mapping exception escape again: throwing cases fail to receive a fallback.
2. Do not turn a null reader result into a fallback: null cases fail to receive it.
3. Emit an ordinary event instead of MT_UNACCEPTABLE: control-signal assertion fails.
4. Discard the failed delivery body: exact binary body assertion fails.
5. Replace the original ID with an empty ID: fallback identity assertion fails.
6. Replace the fallback lock token with an empty GUID: lock-token assertion fails
   (or dispatch rejects the invalid lock and the receive assertion fails first).
7. Replace the fallback sequence number with -1: sequence assertion fails.
8. Skip native dead-lettering but return success: native DLQ receive assertion fails.
9. Stop invoking the configured reader after its first failure: healthy custom
   identity/body or reader call-count assertion fails.
10. Skip native completion of the healthy delivery: native peek assertion fails.

The initial exception/null failures establish RED directly. Mutation failures must
be attributable to the intended contract, not compiler errors or contamination from
an earlier case. Do not alter approved assertions or helper behavior to pass checks.

## Fixture and validation plan

Reuse the isolated emulator entity configuration from the writer batch. Start with
empty normal and dead-letter entities. Never use shared application entities or
hosted credentials for these tests. Runner setup/cleanup owns the dedicated fixture;
recreate it after failed runs and between mutations. For deliberate settlement
omissions, use a five-minute fixture lock or isolate cases individually so locks from
earlier failures cannot expire into later cases. Restore the normal one-minute lock
for final regressions. Pin cached image IDs, disable image pulls, and retain outer
hang detection because synchronous consumer operations lack cancellation arguments.

After approval, copy the test and helper unchanged into MessagingGateway and
TestDoubles, establish RED, implement the smallest failure fallback, run the native
matrix on .NET 9 and .NET 10, verify the named mutations, build every library target,
and run all available ASB regressions including the previous writer/reader cases.
Preserve the manifest of 52 hosted classes that the static fixture cannot cover.
Record any additional failure or environment limitation explicitly. No commits or
pushes are included.

## Remaining scope

Mixed-batch isolation is separate: the SDK is allowed to return fewer messages than
requested, so two queued messages alone cannot prove that mapping occurred in one
native receive batch. Sessions, failed settlement recovery, cancellation, reader
input mutation, concurrent reuse, endpoint isolation, retries/forwarding and handler
telemetry remain open. The Kafka disposal stall and broader release gates remain open.

The repository's `.agent_instructions/testing.md` requires “STOP after writing the
test and ASK FOR APPROVAL” and “DO NOT run tests without approval.” The original tests were approved. The DLQ observation/cleanup correction above
requires review before execution.
