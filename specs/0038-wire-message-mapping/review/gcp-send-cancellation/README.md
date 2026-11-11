# GCP cancellation before native publication

Status: approved tests adopted unchanged; pre-submission cancellation implemented and validated. Full regressions retain unresolved streaming lifecycle findings.

## Source finding and proposed contract

Before correction, the asynchronous individual and bulk producer paths accepted
CancellationToken without checking it before custom mapping or native submission.
The pinned SDK's PublishAsync(PubsubMessage) overload has no cancellation parameter.
All four approved cases reproduced ignored cancellation on both frameworks before
the pre-mapping/pre-submission checks were added; see the execution record below.

Review [the four cases](When_a_gcp_send_is_cancelled_before_publication_should_preserve_the_source_and_recover.cs)
and [the configured writer](GcpCancellationWireWriter.cs). They cross individual
async/single-message bulk sends with cancellation before entry/during mapping.
Cancellation before entry must skip the writer and native publication. Cancellation
triggered synchronously by the writer must be observed before passing its result
to the native publisher. Both failures must be OperationCanceledException carrying
the caller's exact token, without attempted or completed Publish RPCs.

The caller then retries the same source with an active token through the same
producer and configured writer. Independent native pull verifies custom binary
payload, application identity/marker and gateway-controlled ordering, followed by
native acknowledgement. A fresh message must also publish successfully with a new
native identity. Source ID/body/topic/partition/application bag remain intact.

This uses the public factory and real SDK/emulator transport. The existing interceptor
passively records attempted/successful Publish requests and replaces no response.
The strategy cancels its supplied token source without failing native I/O. Bulk batch
creation uses a separate active token so the test targets SendAsync cancellation,
not CreateBatchesAsync. Each bulk contains one message; partial batch progress and
cancellation between successive messages remain separate. The fixture owns its native
resources, bounds cleanup and operations, and requires the external process/hang timeout.
No new request type, pump or logging registration is involved.

## Execution after approval

Run all four cases on net9.0/net10.0 before changing production. Expected RED is the
initial OperationCanceledException assertion: the current send completes instead.
A setup failure is not RED. Once established, add only entry and post-mapping
cancellation checks before native submission in the two affected send paths, with
public XML documentation that states the boundary accurately. Preserve existing
writer failure handling, native publication-await behavior and scheduler policy.

Once PublishAsync has been invoked, this scope cannot promise cancellation of a
queued or in-flight native publish, nor recall a message accepted by the broker.
It does not introduce cancellation races against the confirmation task or claim
that checking a token makes cancellation/publication atomic.

Validate these named production mutations, restoring the fixed source and confirming
focused green after each:

| Mutation | Intended failure |
| --- | --- |
| Remove entry checks while retaining post-mapping checks | Two before-entry cases fail the zero writer-call count |
| Remove post-mapping checks while retaining entry checks | Two during-mapping cases fail the cancellation exception assertion |
| Throw cancellation with a default token instead of the caller token | All four fail cancellation-token equality |
| Move all cancellation checks after awaited native publication | All four fail the empty attempted-publication ledger |
| Omit native publication while retaining mapping and cancellation checks | All four fail the publication-count assertion after active-token retry |

Expected total: 16 intended assertion failures. Preserve the approved tests when a
mutation fails elsewhere; investigate instead of counting setup or unrelated failures.
Then run both restored focused suites, the full available Pub/Sub regressions and
all four library targets. The existing GcpNativeWireWriter/Requires traits select
the emulator and exclude hosted runs; no CI change is expected.

## Review boundary

The repository's [.agent_instructions/testing.md](../../../../.agent_instructions/testing.md)
requires "STOP after writing the test and ASK FOR APPROVAL". Approval covers adopting
these two files, initial execution, the minimum supported correction, the five named
mutations and full available regressions. No commit or publication is included.
Mixed batches, native in-flight cancellation, scheduler paths, reader concurrency,
rejection mapping, historical streaming lifecycle failures, unavailable hosted checks
and the remaining gateway integrations remain independent release gates.

## GCP send cancellation before native submission completed

The approved four cases and configured writer were adopted unchanged. All four
initially fail on both frameworks at the OperationCanceledException assertion:
the send completes without cancellation. Immediate individual sends now check the
caller token before mapping and immediately before native submission. Bulk sends
apply those checks per message. Cancellation retains the caller's token and prevents
submission at those boundaries; the active-token retry uses the same producer and
selected writer. Once submitted, the existing native publication await remains.
Public XML remarks and exception tags state that cancellation cannot recall an
already submitted message. Scheduler and batch-creation behavior remain unchanged.

All four corrected cases pass on both frameworks. Five named production mutations
produce **16 intended assertion failures**: two removed entry checks, two removed
post-mapping checks, four lost caller tokens, four publications before cancellation
and four omitted recovery publications. Each mutation is restored exactly and followed
by four passing net10.0 cases; both final focused suites pass four. Tests observe no
native publication for cancellation before entry or during mapping, then real retry
and fresh-message publication with preserved source data and native ordering.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 405 | 1 | 55 | 461 |
| net10.0 | 404 | 2 | 55 | 461 |

All 457 prior case identities remain, with exactly four new passing cases per framework.
Existing outcome changes are recorded explicitly. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.Stream.GcpStreamDisposeWithUnsettledMessageAsyncTests.When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_async_should_return_promptly — Assert.Equal() Failure: Values differ
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpFactoryStreamShutdownRecoveryTests.When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(useAsync: False, clientCount: 2, nativeRecovery: False) — Assert.Equal() Failure: Values differ
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

The factory shutdown failure is newly observed in this batch, not established as a
pre-existing failure of that case. Its net10.0 sync/two-native-client/replacement-channel
trace records successful disposal, a cancelled callback and a second factory build,
then MT_NONE after the 20-second receive window (test line 155). It uses native
publication and the unchanged streaming path, outside the producer method changed
here. Three focused runs of the saved pre-change binaries and one current run each
pass all eight factory cases. This non-reproduction does not resolve the full-suite
finding or establish its cause. No lifecycle fix or wider bound is introduced.
The 322 saved baseline binary artifacts retain their hashes, and the baseline producer
source matches the pre-batch snapshot exactly. The async ordered-nack failure remains
the previously recorded missing nacked-message ID at generated Proactor line 158.
The net9.0 async unsettled-disposal case also reproduces its previously recorded
empty replacement receive at line 83; its cause remains open.

All four library targets build with zero warnings/errors. Of 6,477 pre-existing
src/tests C# files, 6,476 remain byte-identical; only GcpMessageProducer changes.
The two added compiled sources exactly match their approved review copies. Both full
runs start only after their fresh emulator listener is ready. Existing CI traits
include emulator execution and exclude hosted runs; no hosted, Firestore, Spanner
or remote-CI coverage is claimed. All three batch-owned emulators are removed;
unrelated containers are untouched, and no commit is made.

Evidence: `/private/tmp/brighter-4170-gcp-send-cancellation/` contains RED/green,
mutation/restoration and full TRXs/JSON, native lifecycle traces and focused controls,
baseline artifact hashes, readiness evidence, emulator logs, source snapshots,
all-target build, cleanup record and `final-verification.json`.

The next [partial-bulk cancellation draft](../gcp-partial-bulk-cancellation/README.md)
is outside compiled projects, unbuilt/unrun pending review. It covers a confirmed
first publication followed by cancellation before or during the second mapping,
then explicit caller retry of only the remainder. This adds no general progress API
or duplicate-free whole-batch retry guarantee. Other partial batches, in-flight
cancellation, reader concurrency, scheduler/rejection mapping, hosted checks,
historical lifecycle findings and five remaining gateway integrations remain open.
