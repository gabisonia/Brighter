# GCP pull reader failure recovery

Status: approved tests adopted unchanged; pull recovery implemented and validated.
Full affected regressions still reproduce a known streaming lifecycle failure.

## Remaining production gap

Before this correction, GcpPullMessageConsumer.ReadWireMessage directly invoked
the configured reader and dereferenced its result. Both receive paths map the native
response with Select(ReadWireMessage).ToArray outside their receive error handlers.
A throwing reader therefore propagated its exception, and a null result was dereferenced
before the native receipt is attached. This differed from the approved streaming-reader
recovery behavior and [ADR 0087](../../../../docs/adr/0087-configurable-wire-message-readers.md)'s failure contract. Runtime RED was observed on both frameworks before the minimum correction;
see the execution record below.

## Eight proposed cases

Review [the test](When_a_gcp_pull_reader_fails_should_preserve_the_delivery_and_recover.cs)
and [its configured reader](GcpRecoverablePullWireReader.cs). Cases cross sync/async
receive, throw/null reader failure and presence/absence of the native Topic attribute.

The public consumer factory selects the reader through GcpPubSubSubscription.
The subscription supplies a request type and uses native pull with buffer size one.
Each case creates a unique topic/subscription on an explicit emulator. Real native
publishing supplies invalid UTF-8 bytes, an invalid MIME attribute, handled count 7
and an external marker, with no application-ID or other CloudEvents requirements.
The invalid MIME value prevents the default parser from being a safe fallback.
The reader is a configured strategy; no native I/O is mocked or synthesized.

The first receive must return one MT_UNACCEPTABLE message without throwing. It must
retain native ID, raw bytes, handled count and the actual native ack ID. Topic is
preserved when present and empty when absent. The original native delivery must be
unchanged, and no successful acknowledgement or release RPC may have occurred.

The caller then enables the reader, requeues the unacceptable message, and observes
a successful real ModifyAckDeadline RPC with zero seconds and the correct ack ID.
Redelivery must use that same configured reader and produce its custom identity,
topic, binary body and marker. The consumer must replace its deliberately spoofed
receipt and handled count with native delivery controls. Explicit acknowledgement
must complete a real Acknowledge RPC with that delivery's ack ID. A new marker
message must then be mapped and acknowledged through the same consumer.

The existing GcpPurgeFaultInterceptor is only a passive observer of successful native
unary RPCs. Its ledger checks method, deadline, ack IDs and call count; it substitutes
no response. A completed RPC is transport evidence, not an exactly-once guarantee.
Only caller-driven Requeue is used: existing Pull Nack intentionally does nothing
and is outside this correction. Native/admin clients and cleanup use real I/O.
The receive loop retains the previous recovery tests' three-second calls and bounded
window; cleanup deletes only owned resources. Execution also needs a process timeout.

## RED and implementation after approval

Run all eight cases on net9.0/net10.0 before implementation. The expected RED is the
initial Assert.Null(error): the reader's exception or the null-result dereference
escapes. A setup error is not accepted as this RED. If all cases already pass, inspect
and validate the named mutations below before treating them as characterization.

Once the failing assertion is established, add the minimum per-delivery recovery in
the pull consumer. Preserve native payload/ID/topic without reparsing invalid MIME,
attach real receipt/count after mapping and leave settlement to the caller. Keep
using the selected reader after recovery. No timeout, Nack policy, SDK dependency,
factory lifetime or outbox behavior change is part of this batch.

## Named mutation checks after green

Each mutation changes production only, is restored before the next and is followed
by a green focused run. All eight cases must fail at the indicated behavior.

| Mutation | Required failure |
| --- | --- |
| Restore the original unprotected ReadWireMessage body | Initial receive error assertion |
| Use the default parser for the failure fallback | Initial receive error from invalid MIME |
| Bypass the configured reader with the default reader | Missing configured-reader delivery record |
| Replace the real receipt with a fabricated ack ID | Native receipt equality |
| Automatically acknowledge an unacceptable result before returning it | Non-empty settlement ledger before caller release |
| Make native ReleaseByHandle/ReleaseByHandleAsync no-ops | Missing successful release RPC in the ledger |
| Make native AckByHandle/AckByHandleAsync no-ops | Missing successful acknowledgement RPC in the ledger |
| Switch away from the configured reader after its first failure | Missing second configured-reader delivery record |

Unexpected assertion outcomes need investigation; do not weaken the checks to count
them as intended RED. After restoration, run both focused suites, all affected Pub/Sub
regressions and every GCP library target. The existing emulator CI category selects
these tests and hosted CI excludes Requires=PubSubEmulator.

## Scope and approval

This batch covers single-delivery configured reader recovery. Mixed native batches,
other malformed metadata, cancellation, native delivery-attempt policies, concurrent
receivers, ordering and the intermittent streaming disposal findings remain separate.
The successful factory shutdown diagnostics do not close those existing findings.

The repository's [.agent_instructions/testing.md](../../../../.agent_instructions/testing.md)
requires: "STOP after writing the test and ASK FOR APPROVAL". Approval covers adopting
these two files, execution, the minimum production correction supported by the RED,
eight named mutation checks and full regressions. It does not authorize commits or
publication. The factory-shutdown validation is complete: 388 passes and 55 skips per framework,
with no production change. Its earlier intermittent findings remain open.

## GCP pull reader recovery completed

The approved eight pull-reader recovery cases and configured reader were adopted
unchanged. Before implementation, all eight fail at the initial Assert.Null(error)
on both frameworks: four escaping reader exceptions and four null dereferences.
The pull consumer now catches thrown/null reader results, logs the mapping failure,
constructs an unacceptable message from native ID/topic/raw bytes without reparsing
invalid MIME, and attaches the native receipt and delivery count. The caller retains
settlement ownership and the configured reader remains selected after recovery.
Public Receive/ReceiveAsync remarks describe that behavior. No Nack, timeout, SDK,
factory, streaming or scheduler policy changes are included.

All eight cases pass initially after correction and in final restored runs on both
frameworks. Eight named production mutations produce **64 intended assertion
failures**, covering the unprotected reader, invalid-parser fallback, reader bypass,
fabricated receipt, premature acknowledgement, missing release, missing acknowledgement
and loss of the configured reader after failure. Each is restored exactly and followed
by eight passing net10.0 cases. The first auto-ack mutant did not compile because
MessageHeader.MessageType is obsolete and production warnings are errors. It was
replaced with a local failure flag in the temporary mutant; tests and intended behavior
were unchanged. That compile error is not counted as RED, and its log is retained.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 395 | 1 | 55 | 451 |
| net10.0 | 395 | 1 | 55 | 451 |

Every one of the 443 prior case identities is retained, with exactly eight new passing
cases per framework. Observed full-run failures:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

The ordered nack failures are the previously recorded missing nacked-message IDs
at the first final Assert.Contains (generated Proactor line 158 / Reactor line 152).
These failures predate this pull-only correction; the async failure was also observed
in the preserved pre-stream-recovery control. Their causes remain unresolved.
These remain release blockers; a passing focus run is not substituted for the
full result. The baseline comparison records changed outcomes explicitly rather
than claiming every previous pass is retained.

All four GCP library targets build with zero warnings/errors. Of 6,473 pre-existing
src/tests C# files, 6,472 remain byte-identical; only GcpPullMessageConsumer changes.
The two added compiled files exactly match the approved review copies. Existing CI
traits include native emulator execution and exclude hosted runs. No hosted,
Firestore, Spanner or remote-CI validation is claimed.

Evidence: `/private/tmp/brighter-4170-gcp-pull-failures/`, including initial RED,
green, mutation/restoration and full TRXs/JSON, logs, before/after source copies,
source hashes, all-target build and `final-verification.json`. The first full runner
launch encountered a Docker name collision before any tests ran; its logs are kept
separately and the runner names were corrected. All three owned emulators are
removed after validation; unrelated containers are untouched. No commit is made.

The subsequent [writer recovery batch](../gcp-writer-failure-recovery/README.md)
has now been approved and validated; its execution record is available there. It covers sync/async/single-
message bulk throw/null behavior, no publication on mapping failure, source preservation,
real publication after explicit retry and continued custom mapping. Mixed batches,
concurrency, rejection/scheduler selection, historical lifecycle failures and the
remaining gateway integrations still prevent feature-wide production readiness.
