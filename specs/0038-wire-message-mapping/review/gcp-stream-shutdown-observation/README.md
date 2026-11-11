# GCP native shutdown observation

Status: original approved diagnostic batch. The subsequent
[assertion-order amendment](../gcp-stream-shutdown-ack-observation/README.md) is now
approved and validated; its review copy is the current compiled test. This page
retains the original execution results below. No production correction is made.

## Why this batch is needed

The streaming mapping-recovery batch passes its eight focused cases but its full
matrix reports ordered nack/redelivery and unsettled-disposal failures. Focused
pre-change repetitions and current rechecks pass, so they do not explain the failures.

A subsequent full run of the preserved pre-change binaries uses the same combined
Pub/Sub filter on isolated emulators. The net9.0 result is 363 passes, one failure
and 55 skips; the ordered async nack/redelivery case fails again at its nacked-ID
assertion. The net10.0 result is 364 passes, no failures and 55 skips. This establishes
that the ordered net9.0 failure predates the mapping-recovery correction. It does
not reproduce or explain the unsettled-disposal failures, and neither full control
closes the earlier buffered-redelivery finding.

Evidence: `/private/tmp/brighter-4170-gcp-lifecycle/control-full-net9.0.trx` and
`control-full-net10.0.trx`, with logs and parsed results alongside them.
All 651 preserved build artifacts match their saved hashes. Both control emulators
are removed after execution. No production or compiled-test changes were needed.

The existing buffered tests wait two seconds without observing whether the callback
arrived. The async disposal tests check Task.WhenAny without awaiting the completed
disposal task. The sync tests use Task.Wait, which does propagate a fault. None
observes the native shutdown nack request or distinguishes missing broker redelivery
from replacement-stream delivery failure.

## Eight proposed cases

Review [the test](When_stopping_a_gcp_stream_should_release_the_observed_native_delivery.cs).
The theory crosses sync/async consumer operations, held/buffered deliveries, and
recovery through a replacement Brighter stream or an independent native pull.

Every case creates a unique topic/subscription on an explicitly selected emulator.
The real Google SubscriberClientImpl receives one real SubscriberServiceApiClient,
with native flow control limited to one message. Public GcpStreamConsumer and
GcpPubSubStreamMessageConsumer constructors expose the channel reader for observation.
This deliberately bypasses production factory construction to isolate shutdown;
it does not claim factory/cache replacement coverage.

The existing GcpPurgeFaultInterceptor is reused only as a passive unary-call observer.
No fault is injected, response synthesized or I/O substituted. Its BeforeCall and
AfterCall hooks record request start and successful completion. Native subscription
management, lease extension, nack and acknowledgement still reach the emulator.
There is only one business message and one active receiver before shutdown.

For a buffered case, the test requeues the first delivery, observes the callback's
Nack result, waits for the public channel reader to become readable, then peeks
without consuming. It requires a different pending callback envelope with the
original native ID and bytes. A sleep is not the arrival condition.

Disposal must finish successfully within the existing ten-second bound. Afterward,
a real ModifyAckDeadline request started during shutdown must have completed with
deadline zero and at least one acknowledgement ID. No native acknowledgement may
have succeeded. The original callback must be terminal and canceled or Nack; Ack
is rejected. RPC completion is transport evidence, not a promise of immediate
server redelivery or durable settlement.

Recovery retains the existing twenty-second receive bound. A replacement stream
must preserve application ID, native ID and bytes and produce a distinct callback;
acknowledgement must complete that callback with Ack. The independent native pull
variant must preserve the same native ID, application-ID attribute and bytes before
native acknowledgement. Comparing these paths helps locate a failure but cannot
alone prove its cause.

The test logs native RPC starts/successes, buffered arrival, disposal task status,
redelivery and unfinished RPCs. Cleanup is bounded, deletes only owned entities,
and reports client cleanup failures. A successful behavior cannot pass with a
recorded cleanup failure; cleanup does not replace an earlier assertion failure.

## Execution and controls after review

1. Adopt the test unchanged and run all eight cases on net9.0/net10.0 against isolated
   emulators. Use a process timeout in addition to the operation-level bounds.
2. If a case fails, use its stage and native trace to identify the missing observation
   before changing production. Keep existing full-suite tests unchanged.
3. If cases pass initially, use these named production mutations to establish RED:

| Mutation | Required assertion failure |
| --- | --- |
| Omit both native StopAsync and DisposeAsync in GcpStreamConsumer.StopAsync | No successful shutdown nack, eight cases |
| Remove the handler's streamMessage.Cancel cancellation registration | Disposal timeout is observed as an error, eight cases |
| Return native Ack immediately after writing the callback to the channel | Unwanted native acknowledgement or missing buffered redelivery, eight cases |
| Omit Accept in GcpPubSubStreamMessageConsumer.Acknowledge | Replacement stream callback does not complete with Ack, four cases; native-pull cases remain passing |

Restore each mutation, run both focused suites again, then run the full affected
Pub/Sub matrix and all library target builds. A nonreproduction or a focused pass
does not close the original full-suite findings. Unexpected mutation outcomes need
review; do not weaken assertions or increase redelivery bounds to obtain green.

The new GcpNativeStreamLifecycle category must be included in emulator CI and excluded
from hosted CI if the test is adopted. It uses no generated pump or handler and
needs no generated-pump collection or additional logging generic registration.

## SDK source check

The pinned [Google Pub/Sub 3.38.0 shutdown implementation](https://github.com/googleapis/google-cloud-dotnet/blob/Google.Cloud.PubSub.V1-3.38.0/apis/Google.Cloud.PubSub.V1/Google.Cloud.PubSub.V1/SubscriberClientImpl.cs)
selects a cancellation source for NackImmediately and returns the subscriber's
completion task. Its [single-channel implementation](https://github.com/googleapis/google-cloud-dotnet/blob/Google.Cloud.PubSub.V1-3.38.0/apis/Google.Cloud.PubSub.V1/Google.Cloud.PubSub.V1/SubscriberClientImpl.SingleChannel.cs)
queues outstanding leases for nack and sends them through ModifyAckDeadlineAsync
with zero seconds. Those native calls are the diagnostic boundary above. The source
review does not establish that an SDK defect caused any observed failure.

## Remaining scope

These are shutdown diagnostics, not ordered-nack tests, custom reader tests or
hosted-service evidence. Factory cache replacement, competing consumers, multiple
in-flight messages and broad concurrency remain separate. No dependency update,
timeout relaxation or speculative shutdown correction is proposed.

## Original execution results (2026-10-29)

The user approved the eight cases and four mutations. The adopted test initially
needed only explicit `Grpc.Net.Client.GrpcChannelOptions` qualification for CS0104;
its operations, bounds and assertions are unchanged. All eight cases pass initially
on net9.0 and net10.0. Together, the native traces record real shutdown nack completion,
four buffered arrivals and four recoveries through each recovery path per framework.
No cleanup failure is recorded in either initial green run.

| Native mutation on net10.0 | Result | Observation |
| --- | --- | --- |
| Omit both shutdown calls | 8 fail | Missing successful shutdown nack, line 157 |
| Remove callback cancellation | 8 fail | Disposal timeout reaches Assert.Null, line 156 |
| Return Ack immediately | 8 fail | Four buffered-arrival timeouts, line 136; four held cases fail the earlier missing-nack assertion, line 157 |
| Omit consumer Accept | 4 fail, 4 pass | Replacement callback acknowledgement times out, line 192; native pull cases pass |

Every mutation is restored byte for byte and followed by eight passing net10.0 cases.
The final restored net9.0 focus also passes eight. All four library targets build
with zero warnings/errors. The emulator CI filter includes GcpNativeStreamLifecycle;
the existing Requires!=PubSubEmulator hosted filter excludes these cases.

The premature-Ack mutation's four held cases do not reach the approved forbidden-Ack
assertion, although their native traces show successful unwanted acknowledgements.
This is an unexpected assertion outcome, not a surviving mutation. The
[assertion-order amendment](../gcp-stream-shutdown-ack-observation/README.md) was pending
review at the end of this original batch. It is now approved and validated: all 28
mutation failures reach their intended assertions. No assertion is weakened or removed.

Full affected Pub/Sub results: net9.0 has **380 passes, no failures and 55 skips**;
net10.0 has **379 passes, one failure and 55 skips**. Both contain all 427 prior case
identities plus exactly eight passing diagnostics, for 435 cases each. The net10.0
failure is `GcpStreamDisposeWithUnsettledMessageAsyncTests`, at the replacement
message ID assertion (line 83): the receive returns an empty message within its
existing twenty-second bound. It previously failed on net9.0 in the reader-recovery
batch. No cause is established, and passing ordered/buffered cases do not close
the earlier intermittent findings. No new production correction is made.

All 6,472 adopted source hashes are checked: only the qualified test type differs;
all existing source files and both mutated production files are byte-for-byte
unchanged. At the end of this original batch, the compiled test matched this review copy.
The current compiled ordering is in the approved amendment. All three emulators created
for this batch are removed; pre-existing containers are untouched.
Evidence: `/private/tmp/brighter-4170-gcp-shutdown-observation/` contains builds,
TRXs, native traces, mutation summaries, source snapshots and restoration checks.
