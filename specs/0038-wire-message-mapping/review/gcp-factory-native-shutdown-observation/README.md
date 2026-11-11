# Observe native shutdown RPCs through factory-owned GCP clients

Status: approved amendment adopted unchanged and characterized; no production correction. Historical lifecycle findings remain open.

## Finding to investigate

The existing eight factory shutdown cases establish disposal completion, terminal
callback state and recovery, but do not record native nack RPCs. The send-cancellation
full run observed an empty replacement receive in the sync/two-client case despite
successful disposal, a canceled callback and a second factory build. Focused controls
and the following full matrix did not reproduce it. Its cause remains unknown.

The separate native shutdown observer sees RPCs through an externally supplied
CallInvoker. This amendment uses the pinned SDK's public
SubscriberClientBuilder.ApiSettings.Interceptor hook instead. Factory-created SDK
clients retain their own connections and ordinary shutdown callbacks. No transport,
SubscriberClient or native response is replaced. Pub/Sub 3.38.0 and resolved GAX
4.13.1 expose this hook. The execution record below validates its use in the eight
approved scenarios.

## Concrete amendment

Review [the amended existing test](When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client.cs).
On approval it replaces the compiled file of the same name, preserving its eight
case identities: sync/async × one/two native clients × native/replacement recovery.
GcpPurgeFaultInterceptor is reused unchanged. No new helper, request type, pump,
logging registration or CI filter is needed.

Each factory generation gets a passive interceptor that records unary request start
and successful completion, including subscription, acknowledgement IDs and deadline.
Before disposal the test records a monotonic timestamp. After successful disposal
and before any recovery, it requires:

- No completed Acknowledge RPC from the first factory generation.
- A successful ModifyAckDeadline RPC from that generation, started after shutdown
  began, for the exact subscription, with deadline zero and nonempty acknowledgement IDs.

The existing callback, message identity/content, second-build, receipt and recovery
assertions remain. Disposal stays bounded to ten seconds and recovery to twenty;
the ninety-second operation budget, bounded cleanup and process hang limit remain.
Every scenario publishes exactly one message to its unique subscription. Request
traces provide native acknowledgement IDs; the public receipt still exposes only
the message ID, so this does not claim an independently captured streaming
ack-ID-to-message-ID association. Successful RPC completion does not itself prove
redelivery: the unchanged recovery assertion remains necessary.

## Approved execution and mutations

Preserve the original compiled file and run all eight amended cases on net9.0 and
net10.0. Expected characterization requires these four production-only mutations:

| Mutation | Required RED |
| --- | --- |
| Omit native StopAsync and DisposeAsync in GcpStreamConsumer.StopAsync | Eight missing successful shutdown nack assertions; no callback-state assertion substitutes for the RPC assertion |
| Remove the streamMessage.Cancel cancellation registration | Eight disposal-error assertions with timeout evidence |
| Replace stopped-consumer eviction with one consumer.TryStart call, ignoring its result | Four replacement cases fail second-build count; native recovery cases pass |
| Omit Accept from the consumer's Acknowledge method | Four replacement cases fail receipt acknowledgement with timeout evidence; native recovery cases pass |

Expected total: 24 intended assertion failures. Restore exact production sources
after each mutation and require eight focused passes. Finish with both restored
focused suites, full available Pub/Sub matrices and all four library targets.
Retain prior case identities; this amendment adds no cases to the matrix.

If an initial behavior fails, preserve its complete trace and identify whether the
observer was reached, disposal failed, a native nack was absent, or recovery failed
after a successful nack. A setup/observer failure is not a product defect. A minimal
production correction is authorized only when behavioral RED and native evidence
establish its cause. Do not guess at SDK workarounds, change dependencies, extend
timeouts or treat passing focused runs as resolution of historical failures.

## Review boundary

The repository's [.agent_instructions/testing.md](../../../../.agent_instructions/testing.md)
requires "STOP after writing the test and ASK FOR APPROVAL". Approval covers this
amendment, its four mutations, full regressions and an evidence-supported minimum
correction. It includes no commit or publication. Ordered-nack failures, buffered
disposal, default client count, broader concurrency and the remaining wire-mapping
rollout are separate open requirements; this amendment cannot close them by itself.

## GCP factory native shutdown observation characterized

The [approved amendment](README.md)
replaces the existing factory test unchanged from review, retaining all eight case
identities. All eight pass initially and after restoration on net9.0/net10.0 without
a production correction. The public SDK ApiSettings.Interceptor hook captures native
unary RPCs while the factory-created SDK clients retain their own connections.

Each case observes a successful deadline-zero ModifyAckDeadline request for the
exact subscription and first factory generation, started after disposal began and
containing acknowledgement IDs. No completed native acknowledgement precedes recovery.
The original disposal, callback, recovery, identity/content, replacement-build and
receipt-acknowledgement assertions remain. SDK versions and all time bounds remain
unchanged. Native acknowledgement IDs are recorded, but this amendment does not capture
streaming delivery responses to independently associate each acknowledgement ID with
a native message ID. Every scenario publishes only one message to its unique subscription.

Four production-only mutations produce **24 intended assertion failures**: omitting
native shutdown fails all eight native-nack assertions; removing callback cancellation
causes eight disposal timeouts; omitting stopped-client eviction causes four missing
replacement builds; omitting Accept causes four replacement-acknowledgement timeouts.
Each exact restoration returns eight net10.0 passes; final focused suites pass eight
on both frameworks. No setup/build failure is counted as behavioral RED.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 408 | 2 | 55 | 465 |
| net10.0 | 410 | 0 | 55 | 465 |

All 465 prior case identities remain unchanged, including eight passing amended cases
on each framework. No new cases are added. Outcome changes are recorded in
final-verification.json. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.Stream.GcpStreamDisposeWithUnsettledMessageAsyncTests.When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_async_should_return_promptly — Assert.Equal() Failure: Values differ
- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set

The net9.0 failures are the missing nacked-message ID in generated Reactor line 152
and the empty replacement receive in async unsettled disposal at line 83. Both have
been observed previously. The net10.0 matrix is green in this run; it does not close
those intermittent findings.

The historical ordered-nack, unsettled/buffered disposal and sync/two-client factory
replacement findings remain open. Successful focused observation and later passing
recovery do not establish their cause or a fix. Native RPC completion remains distinct
from independently observed redelivery.

Of 6,481 pre-existing src/tests C# files, 6,480 remain byte-identical; only the approved
factory test changes. All production sources and reused helpers are unchanged, and
no compiled source is added. All four library targets build without warnings/errors.
Test-project builds retain warnings. Full runs wait for fresh emulator readiness.
Existing CI traits select emulator execution and exclude hosted execution; no hosted,
Firestore, Spanner or remote-CI result is claimed. All three batch-owned emulators are
removed; unrelated containers remain untouched. Nothing is committed or published.

Evidence: `/private/tmp/brighter-4170-gcp-factory-native-shutdown/` contains
initial/restored, mutation and full TRXs/JSON; successful native nack observations;
original test and source snapshots; readiness/emulator logs; library build;
cleanup.json and final-verification.json.

The [ordered-nack observation draft](../gcp-ordered-nack-native-observation/README.md)
is outside compiled projects, unbuilt/unrun pending review. Six sync/async × default/
one/two-client cases correlate native streaming delivery acknowledgement IDs with
nack RPCs while preserving immediate receive/ack sequencing. Three named mutations
are specified. Other lifecycle, concurrency, scheduler/rejection, hosted and gateway
rollout requirements remain open; full-feature production readiness is not established.
