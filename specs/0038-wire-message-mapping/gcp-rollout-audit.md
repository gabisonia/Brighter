# GCP Pub/Sub wire-mapping rollout audit

Status: the initial source audit is preserved below. Publication writer integration
has since been implemented; see [native writer evidence](review/gcp-native-wire-writer/README.md).
Pull reader selection is also implemented; see [native pull reader evidence](review/gcp-native-pull-reader/README.md).
Streaming reader selection is implemented; see [native stream reader evidence](review/gcp-native-stream-reader/README.md).
Expanded streaming regressions expose existing redelivery/lifecycle failures, including
a final net10.0 async buffered-redelivery failure. Those and the remaining failure gates are open.
The latest validated ASB baseline remains 628 per framework.

## Initial implementation gaps

GcpPublication and GcpPubSubSubscription do not expose wire-writer or wire-reader
selection. The shared Publication and Subscription base types also have no such
selection. A search of the GcpPubSub source finds no wire-mapping contract usage.

GcpMessageProducer.SendWithDelayAsync calls Parser.ToPubSubMessage directly for
immediate sends. Sync send delegates into that path. The bulk SendAsync overload
independently calls Parser.ToPubSubMessage for each message; it does not delegate to
the individual-send path. Delayed sends pass through the configured scheduler, requiring a separate
return-path check when mapping selection is added.

Both sync and async GcpPullMessageConsumer receives map native ReceivedMessage values
through Parser.ToBrighterMessage. GcpPubSubStreamMessageConsumer.ReceiveAsync maps
GcpStreamMessage through a different parser overload. Both paths therefore need
explicit reader integration; validating pull alone cannot complete this transport.

## Native ownership that mapping must preserve

Pull acknowledgement/nack/requeue requires ReceiptHandle to be the native AckId
string. Streaming operations require ReceiptHandle to be the GcpStreamMessage object,
which controls completion of the SDK callback. These are different delivery controls
and must be attached by the gateway after a custom reader returns, rather than
trusted from arbitrary mapped header contents.

The parser also applies DeliveryCount.Resolve using broker delivery information and
routed-rejection metadata. Custom mapping must not accidentally discard that policy.
Pub/Sub ordering-key behavior is controlled by publication/client configuration;
a writer integration must preserve the enabled/disabled ordering contract.

GcpRejectionRouter constructs destination producers and stamps rejection metadata,
including removing ReceiptHandle before publishing. Its mapping choice needs explicit
destination configuration. A custom reader cannot be assumed to provide its inverse.
Default behavior and failure-to-forward settlement must remain covered independently.

## Implementation and verification order

1. Add publication-level writer selection and a default adapter that preserves the
   current parser behavior. Exercise public producer-factory configuration with native
   observation of custom bytes/attributes and ordering, for sync, async and bulk sends.
2. Add subscription-level reader selection for pull. Prove custom mapping plus native
   acknowledgement/redelivery using a deliberately incorrect reader-supplied receipt
   handle; gateway-owned AckId must win. Preserve default parsing and delivery counts.
3. Integrate the streaming reader and retain callback settlement ownership. Validate
   native acknowledgement, nack/redelivery, shutdown and failed mapping behavior.
4. Validate thrown/null mappings, endpoint isolation, destination-specific rejection
   writers, scheduler return paths and ordering. Complete both-framework regression
   matrices and report unavailable native/hosted capabilities explicitly.

These are separate behavioral review batches under the repository test-first gate.
The first writer batch now has separate implementation and execution evidence;
the source audit alone does not validate any remaining behavior.

## Existing infrastructure and regression boundary

The repository provides docker-compose-gcp.yaml for a Pub/Sub emulator on port 8085,
with PUBSUB_EMULATOR_HOST and GOOGLE_CLOUD_PROJECT configuration. Image availability,
startup and behavior were not checked in this audit; use an isolated fixture for new
native tests rather than taking over an existing emulator.

The Gcp test project combines Pub/Sub, Firestore and Spanner tests. Its generated
Pub/Sub configurations identify Pull, PullOrdering, Stream and StreamOrdering
categories. A Pub/Sub-only pass is not a whole-project pass; report unavailable database
and hosted suites separately. Handwritten generated-pump tests must join the matching
configuration collection, per repository instructions.

## Source references

- src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpPublication.cs
- src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpPubSubSubscription.cs
- src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpMessageProducer.cs
- src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpPubSubMessageProducerFactory.cs
- src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpPullMessageConsumer.cs
- src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpPubSubStreamMessageConsumer.cs
- src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpPubSubConsumerFactory.cs
- src/Paramore.Brighter.MessagingGateway.GcpPubSub/Parser.cs
- src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpRejectionRouter.cs
- tests/Paramore.Brighter.Gcp.Tests/test-configuration.json

The feature remains not production-ready: GCP reader/writer selection and
[stream reader failure recovery](review/gcp-stream-reader-failures/README.md) are
implemented. Eight recovery cases pass on both frameworks; thirteen mutations
produce 96 intended failures. Full affected Pub/Sub runs have 370 passes/two
failures on net9.0 and 371 passes/one failure on net10.0, with 55 skips each.
The lifecycle failures do not reproduce in focused pre-change/current controls
and remain unresolved. Pull mapping recovery and other lifecycle checks stay open.
MQTT, Redis, RocketMQ, SQL queue transports, remaining scheduler paths and global
release checks are also still open in tasks.md.

## Latest pull-recovery checkpoint

Pull and streaming reader failure recovery are now validated. See the [pull execution
record](review/gcp-pull-reader-failures/README.md): eight unchanged cases per framework,
64 intended mutation failures and all four library targets built. Latest full results:

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 395 | 1 | 55 | 451 |
| net10.0 | 395 | 1 | 55 | 451 |

All 443 prior identities remain, with eight new passing cases. The known ordered
nack failures recur; lifecycle findings remain open. Only the pull consumer
changes in this batch. The [writer recovery draft](review/gcp-writer-failure-recovery/README.md)
is unbuilt/unrun pending review. Mixed batches, concurrency, scheduler/rejection paths,
hosted checks and other gateway integrations still prevent production sign-off.

## Latest writer-recovery checkpoint

The [writer recovery batch](review/gcp-writer-failure-recovery/README.md) now validates
throw/null failures, no native publication on failure and explicit same-producer retry.
Six unchanged cases pass per framework; six mutations produce 30 intended failures.
Only the producer null guard and public XML remarks change among existing C# files.
All four library targets build. Complete ready-fixture results:

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 402 | 0 | 55 | 457 |
| net10.0 | 401 | 1 | 55 | 457 |

All 451 prior identities remain, with six new passing cases. Startup-race evidence is
archived separately. Historical lifecycle findings remain open. The [send-cancellation
draft](review/gcp-send-cancellation/README.md) is unbuilt/unrun pending review; mixed
batches, concurrency, scheduler/rejection paths and other gateway integrations remain.

## Latest send-cancellation checkpoint

The [send-cancellation batch](review/gcp-send-cancellation/README.md) now validates
caller-token cancellation before mapping and native submission, with explicit same-
producer retry. Four unchanged cases pass per framework; five mutations produce 16
intended failures. Only producer checks/XML change among existing sources. All four
library targets build. Full ready-fixture results:

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 405 | 1 | 55 | 461 |
| net10.0 | 404 | 2 | 55 | 461 |

All 457 prior identities remain, with four new passing cases. The newly observed
two-client factory replacement failure and known ordered-nack failure remain open.
Factory focused pre-change/current controls do not reproduce the failure or establish
a fix. The [partial-bulk draft](review/gcp-partial-bulk-cancellation/README.md) was pending review at this checkpoint; its approved results follow. Other lifecycle, concurrency, scheduler/rejection, hosted
and gateway rollout requirements remain open.

## GCP cancellation after partial bulk progress characterized

The [approved two cases](review/gcp-partial-bulk-cancellation/README.md) are adopted
unchanged. Both pass initially and after exact mutation restoration on net9.0 and
net10.0 without a production change. A real first Publish response triggers either
immediate cancellation or cancellation during the second mapping. The tests verify
the caller token, exactly one native publication, the expected writer inputs and
unchanged sources. Independent native pull/ack observes the confirmed prefix and
both explicitly selected remainder messages retried through the same producer.

Four named production mutations produce **six intended assertion failures**: moving
the entry check outside the loop visits an unwanted second mapping (one failure);
removing the post-mapping check publishes an unwanted second message (one failure);
default-token cancellation loses token identity (two failures); omitting gateway
ordering enforcement changes independently observed native ordering (two failures).
Each mutation is restored exactly and followed by two passing net10.0 cases. Both
final focused suites pass two cases. No setup or compile failure is counted as RED.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 407 | 1 | 55 | 463 |
| net10.0 | 406 | 2 | 55 | 463 |

All 461 prior case identities remain, with exactly two new passing cases per framework.
Outcome changes are recorded in final-verification.json. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

These ordered-nack failures are the missing nacked-message ID at the first final
assertion: Reactor line 152 and Proactor line 158. The previously failing async
unsettled-disposal and sync/two-client factory replacement cases pass in these runs;
this does not establish a fix.

The prior ordered-nack, unsettled/buffered disposal and sync/two-client factory
replacement findings remain open. This characterization changes no streaming code
and establishes no lifecycle fix. The known remainder belongs to this deterministic
fixture: no progress API or duplicate-free whole-batch retry guarantee is introduced.

All 6,479 pre-existing src/tests C# files remain byte-identical. The one added compiled
test matches its approved review copy; both reused helpers are unchanged. All four
library targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero warnings/errors.
Test-project builds retain existing warnings. Both full runs wait for fresh native
emulator readiness. Existing CI traits include these tests in emulator execution
and exclude hosted runs. No hosted, Firestore, Spanner or remote-CI result is claimed.
All three batch-owned emulators are removed; unrelated containers are untouched.
Nothing is committed or published.

Evidence: `/private/tmp/brighter-4170-gcp-partial-bulk-cancellation/` contains
initial/restored, mutation and full TRXs/JSON; emulator readiness and logs;
source hashes and exact snapshots; all-target build; cleanup.json and
final-verification.json.

At the partial-bulk cancellation checkpoint, the [writer-failure draft](review/gcp-partial-bulk-writer-failures/README.md)
was pending review. It has since been approved and characterized below. It covers configured
writer throw/null after confirmed progress. Other partial-batch and concurrency
paths, in-flight cancellation, scheduler/rejection mapping, historical lifecycle
findings, hosted checks and five remaining gateway integrations still prevent
feature-wide production readiness.

## Read-only lifecycle diagnostic follow-up

The pinned Pub/Sub 3.38.0 builder exposes SubscriberClientBuilder.ApiSettings;
its effective settings clone caller settings before adding routing metadata.
The resolved Google.Api.Gax.Grpc 4.13.1 documents ServiceSettingsBase.Interceptor as the public
hook for observing gRPC calls. GcpPubSubConsumerFactory invokes the connection's
StreamConfiguration before building its SubscriberClient. This identifies a
candidate observation path through ApiSettings.Interceptor that leaves native
connection construction and one/two-client selection with the SDK, unlike supplying
one shared CallInvoker. Runtime behavior was unvalidated at this source-audit
checkpoint; the approved factory observation results below now establish it for
the eight one/two-client scenarios.

A future reviewed lifecycle diagnostic can correlate attempted/completed
ModifyAckDeadline requests (including deadline zero), native delivery/ack IDs and
replacement receives. Such observation should preserve the existing receive bounds,
source/SDK versions and failure assertions; it must not replace native I/O or claim
that callback cancellation alone proves broker release. No lifecycle implementation
or test change is made by this source inspection.

## GCP writer failure after partial bulk progress characterized

The [approved two cases](review/gcp-partial-bulk-writer-failures/README.md) are adopted
unchanged. Both pass initially and after mutation restoration on net9.0/net10.0
without a production change. After a confirmed first Publish, the second mapping
either throws the writer's original exception or returns null. The bulk preserves
the original exception instance or documented null-result error, visits only the
first two source objects and publishes only the first. Independent native pull/ack
verifies the prefix, then the explicitly selected two-message remainder retried
through the same producer. Source IDs, bytes, topics, partitions and bags stay intact.

Four production-only mutations produce **six intended assertion failures**: removing
the null guard produces NullReferenceException (one); replacing the writer exception
loses instance identity (one); deferring mapping failure until after the loop visits
the third source (two); omitting gateway ordering enforcement changes independently
observed native ordering (two). Each mutation is restored exactly and followed by
two passing net10.0 cases. Both final focused suites pass two. No build/setup failure
is counted as behavioral RED.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 409 | 1 | 55 | 465 |
| net10.0 | 409 | 1 | 55 | 465 |

All 463 prior case identities remain, with exactly two new passing cases per framework.
Outcome changes are recorded in final-verification.json. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.Stream.GcpStreamDisposeWithUnsettledMessageTests.When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_should_return_promptly — Assert.Equal() Failure: Values differ
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set

The net9.0 failure is the previously observed empty replacement receive in the
synchronous unsettled-disposal case at line 82. The net10.0 failure is the missing
nacked-message ID at the first final assertion in generated Reactor line 152.
The factory diagnostic cases pass in these runs; that does not establish a fix.

Historical ordered-nack, unsettled/buffered disposal and sync/two-client factory
replacement findings remain open. No streaming code changes in this characterization,
and it establishes no lifecycle fix. The known remainder is specific to the fixture;
there is no general progress API or duplicate-free whole-batch retry guarantee.

All 6,480 pre-existing src/tests C# files remain byte-identical. The sole added compiled
test matches its approved review copy; both reused helpers remain unchanged. All four
library targets (netstandard2.0/net8.0/net9.0/net10.0) build without warnings or errors.
Test-project builds retain warnings. Both full runs wait for fresh native emulator
readiness. Existing CI traits select emulator execution and exclude hosted execution;
no hosted, Firestore, Spanner or remote-CI result is claimed. All three batch-owned
emulators are removed; unrelated containers remain untouched. Nothing is committed
or published.

Evidence: `/private/tmp/brighter-4170-gcp-partial-bulk-writer-failures/` contains
initial/restored, mutation and full TRXs/JSON, emulator readiness/logs, source hashes
and exact snapshots, all-target build, cleanup.json and final-verification.json.

At the partial-bulk writer-failure checkpoint, the [factory native shutdown observation amendment](review/gcp-factory-native-shutdown-observation/README.md)
was pending review. It has since been approved and characterized below. It adds native nack/ack
RPC evidence to the existing eight factory cases through the public SDK settings
hook, retaining native connection ownership, case identities, assertions and bounds.
Other batch/concurrency paths, scheduler/rejection mapping, hosted checks, historical
lifecycle findings and five remaining gateway integrations still block feature-wide
production readiness.

## GCP factory native shutdown observation characterized

The [approved amendment](review/gcp-factory-native-shutdown-observation/README.md)
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

At the factory-observation checkpoint, the [ordered-nack observation draft](review/gcp-ordered-nack-native-observation/README.md)
was outside compiled projects pending review. Its approved execution and unresolved failure are recorded below. Six sync/async × default/
one/two-client cases correlate native streaming delivery acknowledgement IDs with
nack RPCs while preserving immediate receive/ack sequencing. Three named mutations
are specified. Other lifecycle, concurrency, scheduler/rejection, hosted and gateway
rollout requirements remain open; full-feature production readiness is not established.


## GCP ordered-nack observation: native failure reproduced

The [approved six cases](review/gcp-ordered-nack-native-observation/README.md) and two
passive helpers are adopted unchanged. Initial runs pass six cases on both frameworks.
All three approved production mutations produce six intended missing-native-nack
assertions each: **18 intended failures**. Exact production restoration is verified.
The first two restoration runs pass six cases each. The third has **five passes and
one failure**: async/default SDK clients successfully nack the observed first native
ack ID, but that message is not redelivered within thirty seconds. The original
receipt completes with Nack and the other message is received/acknowledged.

Raw StreamingPull observation contains no second native delivery of the missing
message. The full net9.0 matrix independently reproduces the same diagnostic failure.
Both failing traces include an overlapping lease extension and second-message ack;
completion timestamps do not establish the server's internal processing order.
Final focused runs pass six on each framework, but do not clear either captured failure.
**The restoration gate remains unresolved; characterization is not marked complete.**
No production correction, timeout increase, client-count reduction or SDK upgrade
is made.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 413 | 3 | 55 | 471 |
| net10.0 | 415 | 1 | 55 | 471 |

All 465 prior case identities remain, with six diagnostics added. Full failures:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpOrderedNackRpcRecoveryTests.When_nacking_an_ordered_gcp_delivery_should_send_a_native_nack_and_recover_both_messages(useAsync: True, clientCount: 0)
- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async
- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async

Read-only inspection of the pinned emulator 0.8.36 jar identifies a possible
interaction between deferred ordered acknowledgements and stream ownership changes.
This remains a hypothesis: current traces lack server stream ownership and processing
order. It does not establish a Brighter defect or a hosted Pub/Sub defect.
The [native SDK control](review/gcp-ordered-nack-native-control/README.md) is prepared
outside compiled tests, unbuilt/unrun pending review, to investigate without Brighter
in the delivery path. Its bounded outcomes will not replace the Brighter regressions.

All 6,481 pre-existing src/tests C# files are byte-identical to the pre-batch snapshot;
only the three approved compiled files are added. All four library targets build with
zero warnings/errors. Existing test-build warnings remain. All three owned emulators
are removed after saving logs; no unrelated container is changed. Nothing is committed
or published. Hosted/Firestore/Spanner/remote-CI validation is not claimed. Historical
disposal, other GCP mapping/concurrency/scheduler gaps and five remaining gateway
integrations continue to block full-feature production readiness.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-observation/` contains all
initial/mutation/restored/final/full TRXs and JSON, native trace summaries, exact source
snapshots/hashes, emulator bytecode inspection, all-target build, readiness/cleanup and
`final-verification.json`. The failed restoration is retained as
`restored-disconnect-receipt.trx` and `restored-failure-native-trace.txt`.


## GCP ordered nack reproduced without Brighter

The [approved native SDK control](review/gcp-ordered-nack-native-control/README.md)
builds without warnings/errors and executes unchanged. The dependency manifests contain
Google.Cloud.PubSub.V1 3.38.0 and Google.Api.Gax.Grpc 4.13.1, with no Brighter production
assembly. Both frameworks run the fixed four rounds × default/one/two clients.

| Native control | Observations | Both IDs recovered | Missing first ID within 30s | Correlated native nacks completed |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 12 | 11 | 1 | 12 |
| net10.0 | 12 | 11 | 1 | 12 |

Both missing-redelivery observations occur in round 3 (the fourth round), with client
count unset and Environment.ProcessorCount 11. Each records only one raw delivery of
the first native message, a successful nack for its exact observed ack ID, and the
second message's successful acknowledgement. No Brighter receipt, mapper, channel or
factory participates. All one/two-client observations recover both IDs; this is not a
recommendation to change the default count or evidence that a lower count is always safe.
The concurrent framework runs and small fixed sample do not establish a failure rate.

This demonstrates that Brighter is not necessary to trigger the observed failure.
It does not yet distinguish an SDK defect from emulator behaviour, prove permanent
message loss, establish hosted behaviour, or clear the existing Brighter restoration
gate. The same overlapping ack/nack/lease-extension pattern remains visible. No
production correction is justified by these observations alone.

The [prepared protocol control](review/gcp-ordered-nack-protocol-control/README.md)
removes high-level subscriber handling and automatic lease management. Its four native
RPC/stream-membership combinations and separately recorded recovery intervention are
outside compiled tests, unbuilt/unrun pending review. See also the local
[upstream reproduction report](gcp-ordered-nack-upstream-reproduction.md), not published.

All 6,484 pre-existing src/tests C# files remain byte-identical. No production or compiled
test source changes, no new test assertions/mutations, and no dependency upgrades occur.
The two owned emulators are removed after saving logs. The earlier full Brighter matrix
remains the regression checkpoint: net9.0 413 passed/3 failed/55 skipped; net10.0 415
passed/1 failed/55 skipped, with all four library targets built. Those suites are not
rerun because this control changes no production/test source. Other release gaps remain
open; nothing is committed or published.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-native-control/` contains approved
source snapshots, build/dependency artifacts, all 24 RESULT lines and full native
traces, both isolated failure traces, per-framework JSON, readiness/emulator logs,
source hashes, cleanup.json and final-verification.json.


## GCP ordered-nack emulator protocol stall isolated

The [approved protocol experiment](review/gcp-ordered-nack-protocol-control/README.md)
uses generated gRPC service clients directly: no Brighter, high-level subscriber
handler, automatic lease management, substituted response or emulator-internal call.
After correcting readiness setup, both frameworks complete the same four observations:

| Add a second stream | Complete second ack before first nack | net9.0 redelivery within 30s | net10.0 redelivery within 30s |
| --- | --- | --- | --- |
| No | No | Yes | Yes |
| No | Yes | Yes | Yes |
| Yes | No | Yes | Yes |
| Yes | Yes | No | No |

For the stalled combination, a successful correlated nack is followed by thirty seconds
without the first native message. Repeating only the second message's original ack
then releases the first message onto stream 1 with the same native ID/body and a new
ack ID: 37.620 ms after the intervention on net9.0 and 46.755 ms on net10.0. The initial
failure remains recorded; subsequent recovery is not counted as passing that bound.
The new stream's readiness is independently observed through a separate marker before
settling either target message.

This isolates a failure mode in the pinned emulator's ordered delivery protocol that
does not require Brighter or high-level SDK handling. Read-only bytecode inspection is
consistent with a deferred acknowledged entry preventing ownership transfer after the
earlier delivery is nacked. The live server's internal state was not instrumented.
This does not prove every historical failure has the same cause or establish hosted
Pub/Sub behaviour. Original Brighter tests, defaults and bounds remain intact; their
restoration gate and unrelated lifecycle findings remain open.

The first run exposed a readiness setup race on both frameworks: a marker reached the
original stream before the second was registered. Two single-stream observations per
framework completed, then setup aborted. These logs and nonzero exits are preserved
under initial-setup-error; they are not behavioural RED. The sole executable correction
acknowledges such an early marker and waits for a subsequent marker on stream 1 within
the same fifteen-second setup bound. Exact comparison confirms all settlement,
observation and recovery code remains unchanged. Initial failed runners were stopped
after reporting their unhandled setup error to avoid waiting for crash-dump writing.
A fresh pair of emulators is used for the corrected matrix.

Both initial and corrected project builds have zero warnings/errors. All 6,484 existing
src/tests C# files remain byte-identical. Both initial and both corrected owned emulators
are removed, and native logs are saved. The prior full Brighter regression/library
checkpoint remains unchanged; it is not rerun for this standalone diagnostic. No
production fix, dependency upgrade, test skip, commit or publication is included.

The local [upstream reproduction report](gcp-ordered-nack-upstream-reproduction.md)
now includes the discriminating protocol matrix. The next [hosted control draft](review/gcp-ordered-nack-hosted-control/README.md)
explicitly targets the locally configured brighter-pubsub-validation project, whose
ADC file exists but validity/permissions are unverified. It is unbuilt/unrun pending
review; no hosted API call or credential disclosure occurs. Its results cannot replace
Brighter hosted integration or broader release validation.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-protocol-control/` contains
approved/executed source snapshots, readiness-only.patch, initial setup failures,
all eight final observations, isolated failure/intervention traces, dependency/build
artifacts, source hashes, emulator logs, cleanup.json and final-verification.json.

## Hosted native SDK comparison (2026-10-29)

The approved [hosted control](review/gcp-ordered-nack-hosted-control/README.md)
ran its unchanged four-round × default/one/two-client matrix against
`brighter-pubsub-validation`, sequentially on both frameworks. Every observation
recovered both application IDs within the original thirty-second loop and recorded
successful native nack completion correlated to the first delivery. Native traces
show two copies of the first message, preserving its native ID, application ID,
binary body and ordering key, with a new acknowledgement ID on redelivery.

| Framework | Observations | Correlated native nacks | Both IDs recovered | Missing first ID |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 12 | 12 | 12 | 0 |
| net10.0 | 12 | 12 | 12 | 0 |

Execution used local macOS/Darwin 25.6.0 arm64, eleven reported processors,
Microsoft.NETCore.App 9.0.20/10.0.11 and unchanged Pub/Sub 3.38.0/GAX gRPC 4.13.1.
The emulator comparison ran in Linux containers, so this is not a controlled
single-variable comparison of server implementations. The raw protocol experiment
independently isolates the emulator handoff stall; these hosted observations did
not reproduce missing redelivery. Neither result establishes absence of intermittent
hosted failure or validates Brighter's hosted integration.

Both builds have zero warnings/errors; both processes exit zero, completing every
owned-resource delete. Read-only project listings independently verify that all
24 owned topics and 24 owned subscriptions are absent. Exactly 48 synthetic messages
were published. ADC was used without copying or printing credentials. No auth,
permission, quota, setup or cleanup error occurred. Traces retain eleven/net9 and
twelve/net10 client-cancelled lease-extension RPCs after the RESULT records during
subscriber shutdown; these do not replace the successfully completed nacks.

All 6,484 pre-existing src/tests C# files and the approved executable/project remain
byte-identical. No production change, mutation, SDK upgrade, timeout increase,
client-count reduction, skipped assertion, repeated-until-green run, commit or
publication is included. This standalone control does not rerun or supersede the
Brighter regression matrix or clear its failed restoration gate.

Evidence is in `/private/tmp/brighter-4170-gcp-ordered-nack-hosted-control/`:
source and review snapshots, build log, lock/dependency artifacts, runtime/environment
records, both full native logs and twelve-row results, `final-verification.json`,
read-only post-delete listings and `cleanup.json`.

The next [hosted Brighter adapter](review/gcp-ordered-nack-hosted-brighter/README.md)
is prepared through the test-first review workflow, unbuilt and unrun. It reuses the
existing six sync/async × client-count diagnostic assertions through public Brighter
factories, with hosted targeting shown in a mechanical diff. Its proposed twelve
observations across both frameworks use at most twelve resource pairs and twenty-four
synthetic messages in the same explicitly named project. Earlier SDK-only approval
does not execute this new adapter. Other GCP lifecycle, mapping, generated-pump and
broader release requirements remain open.

## Hosted Brighter ordered-nack comparison (2026-10-30)

The approved [hosted Brighter adapter](review/gcp-ordered-nack-hosted-brighter/README.md)
passes all twelve fixed observations against `brighter-pubsub-validation`. The matrix
runs sync/async operations with SDK-default, one and two subscriber clients once per
framework. Unlike the preceding SDK-only control, these observations use Brighter's
public producer/channel factories and actual send/receive/nack/acknowledge paths.

| Framework | Fixed cases | Passed | Failed | Correlated native nacks | Both application IDs recovered |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 6 | 6 | 0 | 6 | 6 |
| net10.0 | 6 | 6 | 0 | 6 | 6 |

The existing diagnostic assertions execute unchanged: binary body, application/native
identity, native ordering key, initially pending receipt, successful native deadline-zero
request correlated to the first delivery, receipt Nack completion and both recovered
application IDs. The original immediate receive/ack sequence and all observation bounds
remain intact. Independent trace verification finds two raw copies of every first
message with the same identity/body/order and a new acknowledgement ID on redelivery.

Both frameworks build with zero errors and four total CS0618 warnings: two uses of
the existing obsolete MessageHeader.MessageType per framework in the copied diagnostic.
Those checks and traces were retained, not suppressed or rewritten. The linked gateway
and core are built from the current source baseline. Execution uses local Darwin 25.6.0
arm64, eleven processors, .NET runtimes 9.0.20/10.0.11, Pub/Sub 3.38.0 and GAX gRPC
4.13.1. Six client-cancelled native RPC observations per framework remain in the full
traces; all required nacks complete successfully and no setup/cleanup failure occurs.

Both processes exit zero. The twelve unique topic/subscription pairs were deleted,
and subsequent read-only project listings independently confirm all twelve topics and
twelve subscriptions are absent. Exactly twenty-four synthetic messages were published.
ADC was used locally; no credential was copied or printed. No repeat-until-green,
mutation, default-client reduction, timeout increase, dependency upgrade or production
correction is included. All 6,484 existing src/tests C# files and the approved adapter
code/project remain byte-identical.

This supplies bounded hosted evidence for the ordered-nack path, closing that specific
missing comparison. It does not demonstrate absence of intermittent failures, reproduce
every provisioning/pump detail of the generated tests, or clear the retained failing
emulator restoration/regression evidence. No full regression suite was rerun for this
unchanged-source environment comparison. Buffered/unsettled disposal, factory replacement,
other mapping/lifecycle/concurrency coverage and overall production readiness remain open.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-hosted-brighter/` contains approved
snapshots, source hashes, build/artifact/dependency records, runtime/environment records,
both full logs and six-row result files, `final-verification.json`, post-delete listings
and `cleanup.json`. No commit, deployment or publication occurred.

The next [hosted factory shutdown adapter](review/gcp-hosted-factory-stream-shutdown/README.md)
is prepared under the test-first review workflow, unbuilt and unrun. It reuses the
existing eight sync/async × one/two-client × native/replacement recovery assertions,
with the hosted adaptation shown in a mechanical diff. The proposed sixteen cases
across both frameworks create at most sixteen resource pairs and sixteen synthetic
messages in the same explicitly named project. It targets the separate shutdown/
replacement finding; it does not cover buffered-after-requeue disposal. Previous
ordered-nack approval does not execute this new batch.

## Hosted factory stream shutdown comparison (2026-10-30)

The approved [hosted factory adapter](review/gcp-hosted-factory-stream-shutdown/README.md)
passes all sixteen fixed cases against `brighter-pubsub-validation`. Each framework
runs sync/async × one/two subscriber clients × native Pull/replacement-channel recovery.
The public factory/channel path and existing assertions execute unchanged.

| Framework | Fixed cases | Passed | Failed | Observed disposal duration |
| --- | ---: | ---: | ---: | --- |
| net9.0 | 8 | 8 | 0 | 0.135493–0.175947 seconds |
| net10.0 | 8 | 8 | 0 | 0.128783–0.185121 seconds |

Every case disposes within the original ten-second bound, observes a successful
native deadline-zero request during shutdown for the exact subscription and first
SDK client generation, observes no native acknowledgement from that generation,
and satisfies the pending callback completion/cancellation assertions. Native Pull
or a replacement channel recovers the same native/application identity and binary
body. Replacement cases build generation two, obtain a distinct receipt and complete
its Ack reply. Native-Pull cases retain one SDK stream generation. The shutdown nack
assertion observes a nonempty ack-ID set; it does not newly correlate that set to a
raw StreamingPull response. The trace verifier preserves that limit.

Both frameworks build successfully with zero errors and two total CS0618 warnings
from the existing MessageHeader.MessageType trace, one per framework. Execution uses
Darwin 25.6.0 arm64, eleven processors, .NET runtimes 9.0.20/10.0.11, Pub/Sub 3.38.0
and GAX gRPC 4.13.1. All 6,484 existing src/tests C# files and the approved adapter
code/project remain byte-identical. No mutation, implementation change, SDK upgrade,
client-count change, timeout increase or repeat-until-green is included.

Both processes exit zero, including cleanup. Sixteen synthetic messages were published
across sixteen unique topic/subscription pairs. Subsequent read-only project listings
independently confirm all sixteen owned topics and sixteen owned subscriptions are
absent. ADC remained local; no credentials were copied or printed. No assertion,
setup, auth, quota or cleanup failure occurred. No commit, deployment or publication.

This closes the missing bounded hosted factory shutdown/replacement comparison.
It does not establish a universal cause for the historical empty replacement receives,
erase the retained emulator failures, exercise buffered-after-requeue disposal, or
complete broader lifecycle/concurrency/mapping validation. No full regression suite
was rerun for this unchanged-source environment comparison. Production readiness
remains open.

Evidence: `/private/tmp/brighter-4170-gcp-hosted-factory-stream-shutdown/` contains
approved/source snapshots, isolated build/dependency artifacts, environment/runtime
records, complete traces, per-framework eight-row results, disposal timings and
`final-verification.json`, read-only post-delete listings and `cleanup.json`.

The next [buffered-redelivery hosted adapter](review/gcp-hosted-buffered-stream-shutdown/README.md)
is prepared through the test-first review workflow, unbuilt and unrun. It reuses four
existing buffered cases per framework, with actual public buffer readiness/peek
assertions before disposal. Its explicit adaptation replaces the old manually
constructed SDK client/no-op disposal callback with an SDK-owned client and lets the
production consumer own shutdown/cleanup. It composes the public gateway components
with real SDK collaborators; it does not claim to drive the channel factory or pump.
The proposed eight cases create at most eight resource pairs and eight synthetic
messages in the same named project. Earlier factory approval does not execute it.

## Hosted buffered-redelivery shutdown comparison (2026-10-30)

The approved [buffered-shutdown adapter](review/gcp-hosted-buffered-stream-shutdown/README.md)
passes all eight fixed cases against `brighter-pubsub-validation`. Each framework
runs sync/async × native Pull/replacement-consumer recovery with one SDK-owned
subscriber client. Every case establishes unread buffered redelivery through the
public Reader.WaitToReadAsync/TryPeek assertions before disposal; no readiness sleep
or assumption replaces that precondition.

| Framework | Fixed cases | Passed | Failed | Observed disposal duration |
| --- | ---: | ---: | ---: | --- |
| net9.0 | 4 | 4 | 0 | 0.121941–0.237365 seconds |
| net10.0 | 4 | 4 | 0 | 0.121745–0.164711 seconds |

The buffered delivery has the original native ID/body, a distinct pending receipt
and an incomplete callback before shutdown. All cases complete disposal inside the
original ten-second bound, observe a successful deadline-zero native request started
after shutdown begins, prohibit premature native acknowledgement and satisfy callback
cancellation/Nack assertions. Native Pull or a replacement consumer then recovers the
same application/native identity and bytes. Replacement acknowledgement completes.
Trace verification independently confirms BUFFERED precedes DISPOSE, the completed
shutdown nack and the same native ID on recovery. The assertion observes a nonempty
ack-ID set, not a new raw StreamingPull/ack-ID correlation check.

The approved ownership adaptation executes unchanged: SubscriberClientBuilder owns
connections; GcpStreamConsumer owns native stop/disposal. The old no-op disposal
callback/shared SDK implementation is not used. Public gateway components are composed
with real SDK collaborators; this comparison does not claim to drive the factory or
generated pump. The preceding sixteen-case hosted comparison covers the factory path.

Both builds succeed with zero warnings/errors. Both processes exit zero. Eight
synthetic messages were published across eight unique topic/subscription pairs;
read-only project listings independently confirm all eight topics and eight subscriptions
are absent after deletion. There are no assertion, setup, auth, quota or cleanup errors.
ADC remained local and credentials were not copied or printed.

Execution uses Darwin 25.6.0 arm64, eleven processors, .NET runtimes 9.0.20/10.0.11,
Pub/Sub 3.38.0 and GAX gRPC 4.13.1. All 6,484 existing src/tests C# files and the
approved adapter code/project remain byte-identical. No production correction,
mutation, SDK upgrade, timeout/client-count change, retry-until-green, commit,
deployment or publication is included. No full regression matrix is rerun for this
unchanged-source environment comparison.

This closes the missing bounded hosted buffered-redelivery comparison. It does not
establish the cause of every historical emulator failure, erase the retained failed
restoration/regression evidence, or validate general concurrency, mapping or pump
behavior. Overall issue #4170 production readiness remains open.

Evidence: `/private/tmp/brighter-4170-gcp-hosted-buffered-stream-shutdown/` contains
approved/source snapshots, isolated build/dependency artifacts, runtime/environment
records, full traces, four-row per-framework results, disposal timing verification,
`final-verification.json`, read-only post-delete listings and `cleanup.json`.

The next [publication writer-isolation review](review/gcp-publication-writer-isolation/README.md)
is prepared through the test-first workflow, unbuilt/unrun and not adopted into the
compiled suite. Six cases cross sync/async/bulk sends with publication creation order,
using custom A/default/custom B endpoints in one public producer factory. Native body/
attribute isolation, source preservation and writer selection are checked. Two named
production-only mutations, exact restorations, library builds and full available
Pub/Sub regression matrices are proposed. This next batch uses isolated emulators;
no hosted calls are proposed.

## GCP publication writer isolation (2026-10-30)

The approved [six-case isolation batch](review/gcp-publication-writer-isolation/README.md)
is adopted and validated against isolated native Pub/Sub emulators. Sync/async/bulk
sends preserve custom A/default/custom B mapping when the same public factory creates
all three producers in either publication order. Native binary bodies, endpoint/source
attributes, canonical default identity, writer selection/call counts and source
preservation are checked. This establishes sequential endpoint isolation, not concurrent
operations or scheduler/rejection mapping.

All six cases pass initially on both frameworks. The initial build identifies xUnit2031
in the new test; an equivalent Assert.Single(collection, predicate) overload removes
that warning without changing its predicate, assertion order, data or bounds. The
approved original and two-line style diff are retained. Both focused suites pass again.

Two named production-only mutations on .NET 10 produce twelve intended native-body
assertion failures. Ignoring configured writers removes custom A's binary prefix in
all six cases. Reusing the first publication's writer adds A's prefix to the default
endpoint in forward creation order, and applies B's prefix to A in reverse order.
Every mutation is restored byte-for-byte and followed by six passes. Final focused
runs pass six cases on each framework. No implementation correction is needed.

| Framework | Passed | Failed | Skipped | Total | New isolation cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 421 | 1 | 55 | 477 | 6 |
| net10.0 | 422 | 0 | 55 | 477 | 6 |

Both full matrices preserve all 471 prior case identities and add exactly six. The
net9.0 failure is the previously recorded generated async ordered-nack case:
`StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async`,
at its missing-redelivery Assert.Contains on line 158. The prior sync ordered-nack
and async/default-client native diagnostic failures pass in this run; the previous
net10.0 async ordered-nack failure also passes. Those outcome changes do not establish
a fix or erase the retained intermittent failures. No retry-until-green or test
exclusion is used. Full-feature production readiness remains open.

All four GCP library targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero
warnings/errors. The larger test dependency graph builds with repository warnings;
the final build has no warning attributed to either new file. All 6,484 pre-existing
src/tests C# files remain unchanged. Exactly the approved test/helper are added; the
helper is byte-identical to approval and the test differs only by the equivalent
assertion style cleanup. Both production mutation targets are restored exactly.

All three batch-owned emulators are removed and their logs retained. There are no
hosted calls, credential use, dependency upgrades, unrelated container changes,
commits, deployments or publication. This Pub/Sub selection does not execute the
unavailable Firestore/Spanner/hosted suites or constitute whole-project verification.

Evidence: `/private/tmp/brighter-4170-gcp-publication-writer-isolation/` contains approved
snapshots, all source hashes, `assertion-style-only.patch`, initial/style/mutation/
restored/final focused TRX/log/JSON records, both full 477-case matrices and outcome
comparisons, all-target library build evidence, emulator logs, `final-verification.json`
and `cleanup.json`.

The next [concurrent writer batch](review/gcp-concurrent-publication-writer/README.md)
is a concrete unbuilt/unrun review draft. Three cases overlap two mappings through
the same factory-created producer in sync/async/bulk modes using a thread-safe writer
and explicit barrier. One named native-result-cache mutation, exact restoration,
library builds and full available regressions are proposed. This next batch remains
local to isolated emulators and does not include hosted execution.

## GCP concurrent publication writer (2026-10-30)

The approved [three-case concurrent writer batch](review/gcp-concurrent-publication-writer/README.md)
is adopted unchanged. Two operations overlap through the same public-factory-created
producer in sync, async and bulk modes. A two-party reader-independent writer barrier
requires overlapping mappings. Independent native observation checks both distinct
binary bodies before per-message identity, custom attributes and source preservation;
the gateway must still clear a writer-supplied ordering key for unordered publication.
Each bulk operation owns one source message; this does not prove concurrent batch
internals, arbitrary user-writer thread safety, scheduling or all concurrency paths.

All three cases pass initially on both frameworks. The named production-only native
result cache mutation then makes all three .NET 10 cases fail at the intended payload
multiset assertion: expected ["VwAA/wGA", "VwD+AAJ/"] versus actual
["VwD+AAJ/", "VwD+AAJ/"]. Both writer calls and the overlap barrier still complete;
compilation/setup/timeout errors are not counted as mutation evidence. The producer
is restored byte-for-byte, then all three cases pass again on both frameworks.
No implementation correction is needed.

| Framework | Passed | Failed | Skipped | Total | New concurrency cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 425 | 0 | 55 | 480 | 3 |
| net10.0 | 424 | 1 | 55 | 480 | 3 |

Both full matrices retain all 477 preceding case identities and add exactly three,
all passing. Each full matrix runs once against its own fresh, readiness-checked
pinned emulator. No retries, exclusions or altered time bounds are used to turn a
behavioral failure green. Full-run failures are retained:

- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpFactoryStreamShutdownRecoveryTests.When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(useAsync: False, clientCount: 2, nativeRecovery: False)` — Assert.Equal() Failure: Values differ


The sole failure is the previously recorded synchronous/two-client factory replacement
case at line 185: shutdown's deadline-zero ModifyAckDeadline completes successfully,
disposal completes and a second native client generation is built, but replacement
Receive returns MT_NONE after twenty seconds. This is retained as a redelivery finding,
not attributed to the new writer characterization or claimed fixed by earlier hosted
passes. The preceding net9.0 async ordered-nack failure passes in this run; the net10.0
factory case passed in the preceding run. Neither outcome change clears the historical
intermittent lifecycle gates. All other prior case outcomes are unchanged.

All 6,486 pre-existing src/tests C# files remain byte-identical. Exactly the approved
test and helper are added; their compiled and review copies match their approved
snapshots. All four gateway library targets build with zero warnings/errors. Test dependency builds retain
repository warnings; none is attributed to the new files. No production, dependency,
or CI selection change is needed for this characterization.

The first sandboxed emulator launch was blocked, and an early runner invocation
found no built assembly and executed zero tests. These infrastructure attempts are
retained separately as launcher-before-build-net9.0.log/json; they are not behavioral
runs. After the build completed, both initial focused suites ran once and passed.

All three owned emulators are removed and their logs retained; unrelated containers
are untouched. No hosted API, credentials, Firestore/Spanner suite, remote CI,
commit, deployment or publication is involved. The selected Pub/Sub regressions do
not establish whole-project or full-feature production readiness. Historical emulator
lifecycle failures, remaining gateway integrations and other release gates remain open.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-publication-writer/` contains
approved snapshots, all source hashes, native-cache mutant, initial/mutation/restored
TRX/log/JSON records, both full 480-case matrices with prior-identity/outcome comparison,
all-target library build, emulator logs, final-verification.json and cleanup.json.

The next [concurrent pull reader batch](review/gcp-concurrent-pull-reader/README.md)
is prepared as an unbuilt/unrun review draft. Two consumers share a safe reader and
must preserve overlapping mapping, per-delivery receipts/counts and subsequent native
acknowledgement. Four named mutations, exact restoration, affected regressions and
owned cleanup are proposed; no hosted execution is included.

## GCP concurrent pull reader (2026-10-30)

The approved [two-case shared pull-reader batch](review/gcp-concurrent-pull-reader/README.md)
is adopted unchanged. Separate public-factory-created consumers share a safe reader;
sync/async receives overlap through a two-party mapping barrier. Both successful
barrier crossings must be observed before comparing mapped bodies as a multiset.
Each consumer retains its own mapped identity, native content and custom marker,
while the gateway replaces fabricated receipt handles and handled counts with that
delivery's native values. Native inputs remain unchanged and mapped objects stay
separate. Delivery lookup uses source markers rather than cross-topic ID uniqueness.

Each consumer acknowledges its own message. After twelve seconds, beyond the native
ten-second ack deadline, a six-second native observation on each subscription sees
its fresh marker and no original message. This is bounded settlement evidence;
it does not establish unlimited absence of duplicates or streaming/concurrent settlement.

Both baseline cases pass on net9.0 and net10.0 without a production change. Four
named production-only mutations on net10.0 yield eight intended failures:

- Cached mapped body: both cases expect ["UgAA/wGA", "UgD+AAJ/"] but observe
  ["UgAA/wGA", "UgAA/wGA"] at the payload multiset assertion.
- Omitted receipt override: both cases observe reader-invalid-receipt instead of the
  corresponding native acknowledgement handle.
- Omitted handled-count override: both cases expect native count 3 but observe the
  configured reader's 777.
- Omitted native acknowledgement: the observer sees the original delivery alongside
  the fresh marker, failing Assert.DoesNotContain in both cases.

Every mutation is restored byte-for-byte and followed by two net10.0 passes. The final
net9.0 run also passes both cases. The last net10.0 restoration is its final focused
run. No compile/setup failure is accepted as mutation evidence, no assertions/bounds
are altered, and no behavioral retry-until-green is used.

| Framework | Passed | Failed | Skipped | Total | New reader cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 426 | 1 | 55 | 482 | 2 |
| net10.0 | 426 | 1 | 55 | 482 | 2 |

Both full available Pub/Sub matrices run once on separate fresh, readiness-checked
pinned emulators. All 480 preceding case identities remain, plus exactly two new
passing cases. Full-run failures are retained:

- net9.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Assert.Contains() Failure: Item not found in set
- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Assert.Contains() Failure: Item not found in set

Changes from the immediately preceding full matrices:

- net9.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Passed → Failed.
- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Passed → Failed.
- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpFactoryStreamShutdownRecoveryTests.When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(useAsync: False, clientCount: 2, nativeRecovery: False)` — Failed → Passed.


Both current failures are the generated synchronous ordered-nack case at Reactor
line 152: the expected nacked-message ID is missing from the final redelivery set.
This known emulator finding predates this pull-reader batch. The preceding net10.0
synchronous/two-client factory replacement failure passes here. All other prior
case outcomes remain unchanged. No streaming source changed in this increment.

A passing historical failure does not establish its cause or a fix. The emulator
lifecycle gates, broader mapping/scheduler paths and remaining gateway integrations
remain open. These results do not establish full-feature production readiness.

All 6,488 pre-existing src/tests C# files remain byte-identical. Exactly the approved
test/helper are added, and their compiled/review copies match the approved snapshots.
All four library targets build with zero warnings/errors. The test dependency graph
retains repository warnings, including CS0618 for the new test's deliberate legacy
MessageHeader.MessageType compatibility assertion. The approved assertion is retained;
no test change is needed and the helper introduces no warning. Existing CI categories
already select these cases; there is no workflow or dependency change.

All three owned emulators are removed and their logs retained; unrelated containers
are untouched. There are no hosted calls, credential use, commits, deployment or
publication. The available Pub/Sub selection excludes unavailable Firestore/Spanner
and hosted suites, and is not whole-project or remote-CI verification.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-pull-reader/` contains approved
snapshots, 6,488 source hashes, four saved production mutants, initial/mutation/restored/
final TRX/log/JSON records, both full 482-case matrices with case/outcome comparisons,
all-target library build, emulator logs, final-verification.json and cleanup.json.

The next [concurrent stream-reader batch](review/gcp-concurrent-stream-reader/README.md)
is a concrete review-only draft, unbuilt/unrun and outside the compiled suite.
Two factory-created streaming consumers share a safe reader; mapping, receipt ownership,
callback completion and subscription-specific native acknowledgement must remain
independent. Five named mutations, exact restorations, library builds and affected
regressions are proposed, using isolated emulators only.

## GCP concurrent stream reader (2026-10-30)

The approved [shared stream-reader batch](review/gcp-concurrent-stream-reader/README.md)
is adopted unchanged. Two sync/async cases establish overlapping mapping across
separate public-factory consumers sharing a safe reader, with one SDK client per
subscription. Exact mapped body/identity/marker isolation, unchanged native inputs,
native wrapper/count ownership and independent callback completion are verified.
Each consumer's acknowledgement also completes a native Acknowledge RPC for its
own subscription with one nonempty acknowledgement ID. This is bounded native
settlement evidence; it does not decode streaming delivery ack IDs independently
or establish unlimited absence of redelivery or concurrent settlement.

Both initial cases pass on .NET 9 and .NET 10 without production corrections.
Five independent production mutations on .NET 10 produce ten intended failures:
cached bodies fail payload multiset equality; omitted receipt override fails exact
wrapper identity; omitted count override exposes 777 instead of native 3; omitted
Accept leaves the callback incomplete; replacing its completed Ack with native Nack
leaves the native acknowledgement incomplete after the original callback succeeds.
All mutations fail at their reviewed assertion, not setup or compilation. Both
production targets are restored byte-for-byte after each mutation, followed by two
passes. Final focused runs pass both cases on each framework.

| Framework | Passed | Failed | Skipped | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 429 | 0 | 55 | 484 | 2 |
| net10.0 | 428 | 1 | 55 | 484 | 2 |

Each full available Pub/Sub matrix runs once on a fresh readiness-checked pinned
emulator. All 482 preceding case identities remain, plus exactly two new passing
cases. The .NET 10 failure is the known generated synchronous ordered-nack case:
StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.
When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second,
at the final Assert.Contains redelivery assertion. The same case changes from
failed to passed on .NET 9; all other preceding outcomes are unchanged. Passing in
one run establishes neither a root cause nor a fix. Earlier lifecycle findings and
broader release gates remain open. There is no retry-until-green or bound change.

All 6,490 pre-existing src/tests C# files are byte-identical. Exactly the approved
test/helper are added and match their review and approved snapshots. All four GCP
library targets build with zero warnings/errors. The test dependency graph retains
3,320 repository warnings; neither new file introduces a warning. Existing CI
categories select the new cases; no workflow/dependency change is needed.

All three owned emulators are removed, logs retained and unrelated containers left
untouched. No hosted calls, credential use, commits, deployment or publication occur.
The selection excludes unavailable Firestore/Spanner and hosted coverage and is not
whole-project or remote-CI verification. Whole-feature production readiness remains
open, including the five remaining gateway integrations.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-stream-reader/` contains source
hashes, approved snapshots, five saved mutants, initial/mutation/restored/final
TRX/log/JSON results, both full matrices and outcome comparisons, the all-target
library build, final-verification.json, emulator logs and cleanup.json.

The next [MQTT publication writer batch](review/mqtt-publication-writer/README.md)
is a concrete review-only draft, unbuilt/unrun and outside the compiled suite.
Six sync/async cases cover legacy, custom and composed default payloads with native
broker observation, gateway-owned routing/QoS and source preservation. Four named
mutations propose eighteen assertion failures. Implementation, native regression
baselines and execution await the repository's test-review approval.
