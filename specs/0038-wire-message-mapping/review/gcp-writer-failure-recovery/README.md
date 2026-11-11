# GCP writer failure recovery

Status: approved tests adopted unchanged; writer null-result recovery implemented and validated.

## Behavior and source finding

The public producer factory already selects the configured publication writer.
Before correction, GcpMessageProducer.CreateWireMessage dereferenced a null writer
result when applying OrderingKey. Both frameworks reproduced NullReferenceException
before the explicit null guard was added; see the execution record below.
Existing thrown mapping exceptions should retain their identity and propagate.
Neither failure should publish a default fallback or prevent explicit retry on the
same producer once the configured writer recovers.

Review [the six cases](When_a_gcp_wire_writer_fails_should_reject_the_send_and_recover.cs)
and [the configured writer](GcpRecoverableWireWriter.cs). They cross synchronous,
asynchronous and single-message bulk sending with throwing/null results. The
proposed null contract is InvalidOperationException with the message
"The wire message writer returned null."; the throwing branch requires the exact
exception instance supplied by the writer. This adds a useful null-result error
without changing the existing thrown-exception contract.

Each case uses the public publication/factory, a unique native emulator topic and
ordered subscription, real SDK publishing and independent native pulling. The
existing GcpPurgeFaultInterceptor passively records attempted and successful Publish
requests through the native CallInvoker, without replacing any I/O. Both ledgers
must be empty immediately after mapping failure. Successful recovery and a fresh
message must each add exactly one native publication, use the same selected writer,
produce custom binary payload and attributes, and receive a non-empty distinct
native identity. Gateway-owned ordering overrides the writer's spoofed key. The
source identity, bytes, topic, partition and application bag remain intact.

A successful Publish RPC followed by native pull is positive delivery evidence;
empty ledgers describe this observed client path, not a global exactly-once claim.
The bulk case has one message deliberately: mixed-batch partial progress and retries
need a separate contract. Admin resources and the transport are owned by the test;
cleanup is bounded to 15 seconds per resource, operations use a 90-second token,
and execution must retain the external process/hang timeout. No new request type,
pump, logging registration or I/O substitute is introduced.

## Approved work to execute after review

Run all six cases on net9.0/net10.0. Expected RED: the three null cases fail the
InvalidOperationException type assertion; thrown cases should characterize existing
behavior. Setup/compile errors do not count as RED. Add only an explicit null guard
in the shared CreateWireMessage path, plus appropriate public XML documentation.
Do not change scheduler, cancellation, batch atomicity, SDK or settlement behavior.

After green, validate these named production mutations, restoring the exact fixed
source and confirming focused green after each:

| Mutation | Intended assertion failure |
| --- | --- |
| Remove the explicit null-result guard | Three null cases fail the exception type assertion |
| Bypass the configured writer with the default writer | All six fail the expected mapping exception assertion |
| Wrap a thrown mapping exception in a different InvalidOperationException, retaining the null guard | Three throwing cases fail exception-instance identity |
| Publish a default fallback after mapping failure, then rethrow the original error | All six fail the empty attempted-publication ledger |
| Omit gateway ordering enforcement | All six fail the independently observed native ordering key |
| Omit the native PublishAsync call in individual and bulk paths while still invoking the writer | All six fail the publication-count assertion after recovery |

The expected total is 30 intended failures. A build error or a different assertion
is not accepted as the named behavior. Restore all production mutations, run both
focused suites, all affected Pub/Sub regressions and all four library targets.
The existing GcpNativeWireWriter/Requires traits already select emulator execution
and exclude these cases from hosted CI.

## Review boundary

The repository's [.agent_instructions/testing.md](../../../../.agent_instructions/testing.md)
requires "STOP after writing the test and ASK FOR APPROVAL". Approval covers adopting
these two files, initial execution, the minimal supported null-result correction,
the six mutations and the full available regressions. No commit or publication is
included. This scope does not close reader mixed-batch isolation, writer concurrency,
partial bulk publication, scheduler/rejection mapping, historical streaming lifecycle
findings, unavailable hosted coverage or other transport rollout gates.

## GCP writer failure recovery completed

The approved six cases and writer were adopted unchanged. Initial runs on both
frameworks reproduce three null-result failures at the InvalidOperationException
type assertion: the actual exception is NullReferenceException. The three throwing
cases already pass. An explicit null guard in GcpMessageProducer.CreateWireMessage
now produces the reviewed error before native publication. Existing thrown exceptions
retain their identity. Public send XML remarks describe mapping failure and the
possibility that earlier bulk messages have already been published.

All six corrected cases pass on both frameworks. Six approved production mutations
produce **30 intended assertion failures**: three removed null guards, six configured
writer bypasses, three changed exception identities, six fallback publications,
six lost ordering keys and six omitted native publications. Each mutation is restored
exactly and followed by six passing net10.0 cases; both final focused suites pass six.
The tests prove native publication absence on mapping failure, explicit same-producer
retry, fresh-message delivery, source preservation and gateway ordering control.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 402 | 0 | 55 | 457 |
| net10.0 | 401 | 1 | 55 | 457 |

All 451 prior case identities remain, with exactly six new passing cases per framework.
The comparison records existing outcome changes explicitly. Full-run findings:

- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

The first full launch raced emulator startup, producing connection-refused setup
failures. Both invalid runs were stopped and their logs/runner exit records archived;
they do not establish behavioral RED or a valid full matrix. Fresh emulators passed
an explicit TCP-listener readiness probe before the complete runs above. Source and
tests were unchanged for the rerun.

All four library targets build with zero warnings/errors. Of 6,475 pre-existing
src/tests C# files, 6,474 remain byte-identical; only GcpMessageProducer changes.
The two new compiled sources exactly match the approved review copies. Existing CI
traits include emulator execution and exclude hosted runs. No hosted, Firestore,
Spanner or remote-CI coverage is claimed. All batch-owned emulators are removed;
unrelated containers remain untouched, and no commit is made.

Evidence: `/private/tmp/brighter-4170-gcp-writer-failures/` contains RED/green,
mutation/restoration and full TRXs/JSON, archived startup failures, readiness evidence,
emulator logs, source snapshots, all-target builds and `final-verification.json`.

At the writer-recovery checkpoint, the [send-cancellation draft](../gcp-send-cancellation/README.md)
was pending review. That batch has since been approved and validated; see its
execution record for the correction and remaining lifecycle failures. It covers cancellation before
entry or during mapping for immediate async and single-message bulk sends. Native
in-flight cancellation, partial batches, reader isolation/concurrency, scheduler and
rejection mapping, historical lifecycle failures, hosted checks and the remaining
five gateway integrations still prevent feature-wide production readiness.
