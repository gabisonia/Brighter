# GCP factory stream shutdown and replacement

Status: approved, adopted and validated. All eight cases pass on both frameworks;
all four mutations reach their intended assertions. Full affected Pub/Sub suites
pass 388 tests with 55 skips per framework. No production correction was needed.

## Why this is the next diagnostic boundary

The native shutdown observation cases pass using one directly constructed
SubscriberClientImpl and an externally owned transport. They establish the callback
and native nack behavior of that setup. They bypass the channel/consumer factories,
the stopped-client cache, SDK-created connections and multiple subscriber clients.
The existing full-suite unsettled-disposal cases still intermittently return an
empty replacement receive. Async versions also do not await the completed disposal
task, so a successful Task.WhenAny check does not establish successful disposal.

The pinned [SDK builder source](https://github.com/googleapis/google-cloud-dotnet/blob/Google.Cloud.PubSub.V1-3.38.0/apis/Google.Cloud.PubSub.V1/Google.Cloud.PubSub.V1/SubscriberClientBuilder.cs#L107-L127)
creates ClientCount native clients (processor count when unset), lets each own its
connection and supplies their shutdown callbacks to SubscriberClientImpl.
The factory leaves ClientCount unset unless the application configures it. This is
an identified coverage difference, not evidence that multiple clients caused the
historical failures. The pinned source is saved alongside this amendment's evidence
in `/private/tmp/brighter-4170-gcp-shutdown-ack-observation/SubscriberClientBuilder.cs`.

The amended diagnostic full regressions retain all 435 cases but report one ordered
nack failure per framework (379 passes/one failure/55 skips each). The disposal cases
pass that run; prior intermittent disposal findings remain unresolved. This draft
does not cover the ordering failure.

## Eight approved cases

Review [the test](When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client.cs).
The matrix crosses sync/async channel operations, one/two native subscriber clients,
and recovery through native pull or a replacement factory-created channel.

Every case requires an explicit emulator, creates a unique native topic/subscription,
and sends one binary message through the real native publisher. Administration and
native pull use their own explicit transport. The stream factory uses EmulatorOnly
and ClientCount through its public configuration hook; it receives no custom
CallInvoker, SubscriberClient, transport, reader or writer. The SDK owns its real
stream connections and shuts them down through its ordinary builder path.

The first receive must preserve application/native IDs and bytes and expose a pending
native callback receipt. Channel disposal must finish successfully within ten seconds;
the test awaits the actual task result. The callback must be terminal and canceled
or Nack, never Ack. Recovery remains bounded to twenty seconds and must preserve
both IDs and bytes. Native recovery awaits the real acknowledgement RPC; stream
recovery requires acknowledgement to complete the new receipt with Ack.

The replacement path deliberately reuses the same subscription object and factory.
The public stream-configuration callback must run again, showing that the stopped
cached stream was replaced; the native-pull path must not build another stream.
This extends the existing fresh-subscription disposal tests. It does not replace
those tests or change their bounds. There is no sleeping to infer callback arrival.

The trace records each factory stream build and configured client count, initial
receive, actual disposal outcome, callback status, recovery and native pull outcomes.
It does not observe streaming native RPC payloads or prove that an empty receive was
caused by a missing nack. Cleanup deletes only owned entities, bounds disposal/delete,
and rejects cleanup errors after a successful behavior. A process hang bound is
required because factory-owned clients are intentionally not exposed for cleanup.

## Named mutations if initially green

Only production code changes temporarily; the test and assertions stay fixed.

| Mutation | Required RED |
| --- | --- |
| Omit both native StopAsync and DisposeAsync in GcpStreamConsumer.StopAsync | The original callback is still pending: eight IsCompleted assertions fail |
| Remove the streamMessage.Cancel cancellation registration | Eight disposal errors at Assert.Null with timeout evidence |
| Replace the stopped-consumer eviction loop in GcpPubSubConsumerFactory with one call to consumer.TryStart(), ignoring its result | Four replacement paths fail the expected second stream-build count; four native-pull cases pass |
| Omit Accept in GcpPubSubStreamMessageConsumer.Acknowledge | Four replacement receipt acknowledgements time out; four native-pull cases pass |

Restore each mutation and confirm green. An unexpected assertion, setup failure or
surviving mutation is not accepted as the specified RED. If the initial cases fail,
use their stage/trace to establish a cause before any correction; do not introduce
a guessed SDK workaround, change a dependency or increase a timeout.

After adoption, execute all eight cases on net9.0/net10.0 against isolated emulators,
validate the named mutations, run full affected Pub/Sub regressions and build all
GCP library targets. The existing emulator category includes these tests and hosted
CI excludes their Requires=PubSubEmulator trait. A passing focused run does not close
intermittent findings in the unchanged full-suite cases.

## Approval boundary

The repository's [.agent_instructions/testing.md](../../../../.agent_instructions/testing.md)
requires: "STOP after writing the test and ASK FOR APPROVAL". Approval covers adoption,
execution, the four named mutations and a minimal production correction only if the
test and native evidence establish its cause. It does not authorize committing or
publishing changes. This draft is separate from the already approved assertion-order
amendment, whose validation proceeds independently.

## Execution results (2026-10-29)

The first net9.0 execution fails all eight cases during setup: the subscription
constructor requires a request type. This is not behavioral RED. The fixture adds
`requestType: typeof(TestDoubles.MyCommand)` using the existing command type. No
assertion, bound or operation sequence changes. The original approved file is saved
as `approved-original.cs` in the evidence directory, and verification confirms this
required configuration is its only difference from the compiled/review copy.

With that correction, all eight cases pass on net9.0 and net10.0. The approved
mutations then establish the following RED on net10.0:

| Mutation | Result | Intended observation |
| --- | --- | --- |
| Omit native StopAsync and DisposeAsync | 8 fail | Original callback remains pending |
| Remove callback cancellation | 8 fail | Awaited disposal times out |
| Omit stopped-consumer eviction | 4 fail, 4 pass | Replacement configuration is not invoked a second time; native pull paths pass |
| Omit consumer Accept | 4 fail, 4 pass | Replacement acknowledgement times out; native pull paths pass |

All 24 failures reach their intended assertions. Each mutation is byte-restored and
followed by eight passing net10.0 cases. Final focused runs pass eight on each framework.
All four library targets build with zero warnings/errors. The new test's diagnostic
MessageType log uses the existing obsolete header property and emits CS0618 in test
builds; this is not a library build warning or an execution failure.

Full affected Pub/Sub results are **388 passes, zero failures and 55 skips** on both
frameworks. All 435 previous case identities are retained, with exactly eight new
passing cases: 443 total per framework. Earlier ordered-nack and disposal failures
are not reproduced in these runs; their cause remains unestablished. No production
change was made, so these green runs do not establish a correction for them.

All 6,472 existing src/tests C# files match their pre-batch hashes. The only new
compiled source is this test, which matches the review copy after the required
request-type addition. All three batch-owned emulators are removed. No SDK update,
timeout relaxation, commit or publication is made. Existing emulator/hosted CI trait
filters already include/exclude these tests appropriately.

Evidence: `/private/tmp/brighter-4170-gcp-factory-shutdown/` contains initial setup
failures, configured green runs, mutation/restoration pairs, full TRXs and emulator
logs, builds, source snapshots and `final-verification.json`.

The next [pull-reader recovery draft](../gcp-pull-reader-failures/README.md) remains
outside compiled projects, unbuilt and unrun pending approval. This completes the
scoped factory characterization, not GCP or feature-wide production readiness.
