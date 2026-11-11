# Concurrent GCP stream reader

Status: approved, adopted unchanged and validated. Scoped characterization complete; broader lifecycle and release gates remain open.

The pull-reader batch covers unary receives. This test-first draft checks two separate
public-factory-created streaming consumers sharing a safe reader. Review
[the two cases](When_gcp_stream_consumers_share_a_reader_should_keep_overlapping_deliveries_isolated.cs)
and [the helper](GcpConcurrentStreamWireReader.cs).

## Behavior

Sync and async cases each create two isolated topic/subscription pairs and publish one
distinct binary message per pair through the native SDK. Each factory-created stream
uses one SDK client, with SDK-owned connections and emulator-only configuration.
Dedicated workers receive concurrently. A twenty-second two-party barrier after each
mapping and two observed successful crossings establish actual overlapping reader calls.
The helper composes the existing GcpNativeProbeStreamReader without replacing I/O.

The test first compares the two mapped bodies as a multiset, then verifies each
consumer's mapped identity, body and marker. Delivery lookup uses the distinct source
marker, not assumed cross-topic native-ID uniqueness. Native payloads/attributes must
remain unchanged. Fabricated reader receipts and handled counts must be replaced with
the exact GcpStreamMessage wrapper and native counts. Mapped messages/headers and
receipt objects must remain distinct. Both callbacks must initially be unsettled.

Acknowledge the first mapped message through its own consumer, require its callback to
complete with Ack, and observe a successful native Acknowledge RPC for its subscription
with one nonempty acknowledgement ID. The second callback and native acknowledgement
must still be pending. Then repeat for the second consumer. Native completion is
observed through the existing GcpPurgeFaultInterceptor on the SDK ApiSettings public
hook; the interceptor does not replace the transport. Each subscription carries only
one message. This does not independently decode streaming wire delivery acknowledgement
IDs or establish unlimited absence of redelivery.

This covers overlapping mapping across separate consumers and subsequent individual
settlement. It does not cover multiple SDK clients per subscription, concurrent
settlement, reader failures, nack/requeue, ordered-stream handoff, shutdown recovery,
or repeated concurrent receives on a single consumer.

## Approved execution

Adopt the two files unchanged into MessagingGateway/TestDoubles. Run both cases on
.NET 9 and .NET 10 against isolated, readiness-checked pinned Pub/Sub emulators.
Reuse the existing probe reader and native interceptor. No new handlers/logging
registrations, dependency upgrade, hosted API or ADC is needed. The existing
GcpNativeStreamReader CI category selects these cases.

Retain the ninety-second operation budget, twenty-second barrier, twelve-second
receive polling, three-second callback completion, ten-second native acknowledgement
and disposal, fifteen-second resource-delete and external process/hang bounds. Await
both receives before cleanup; attempt every owned cleanup despite earlier errors.
No implementation change is proposed unless initial evidence establishes a defect.

## Named production mutations

Run each independently on .NET 10, never altering tests to obtain RED:

1. In GcpPubSubStreamMessageConsumer.ReadWireMessage, cache the first completed mapped
   MessageBody across instances using a static field and Interlocked.CompareExchange,
   after the configured reader returns. Keep separate headers and native receipts.
   Both cases must pass the overlap precondition and fail the payload multiset equality
   with duplicate mapped bodies.
2. Omit the native ReceiptHandle override. Both cases must fail the exact wrapper
   `Assert.Same(delivery, mapped[index].Header.Bag["ReceiptHandle"])` assertion.
3. Omit the native HandledCount override. Both cases must fail native count equality
   (3 or 7 versus the configured reader's 777).
4. Make GcpPubSubStreamMessageConsumer.Accept a no-op. Both cases must fail
   `Assert.Null(replyError)` after the original callback remains incomplete for three
   seconds; setup, overlap and mapping assertions must have passed.
5. In BrighterStreamHandler.HandleMessage (GcpStreamConsumer.cs), still await each
   GcpStreamMessage completion but return SubscriberClient.Reply.Nack instead of its
   result. Both cases must first observe the original wrapper complete with Ack, then
   fail `Assert.Null(nativeAckError)` because no native acknowledgement completes for
   the subscription within ten seconds. A missing callback, mapping error or cleanup
   failure does not count as this native-acknowledgement evidence.

Restore every affected production file byte-for-byte after each mutation and require
both cases green. Finish with both frameworks green, all four gateway library targets
built, and both full available regression matrices run once, retaining the preceding
482 identities plus two (484 expected with the same selection). Retain historical
failures/skips; do not retry until green or change time bounds. If initial behavior is
RED, diagnose it before an evidence-supported minimum correction. Record unavailable
Firestore/Spanner/hosted coverage explicitly.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers adoption,
scoped native testing, the five named mutations and exact restorations, an evidenced
minimum correction if needed, affected regressions/library builds, owned cleanup and
local reports. Hosted calls, unrelated changes, commits, deployment and publication
are excluded. The approved execution is complete; no production correction was needed.

## Results

Both initial and final focused cases pass on .NET 9 and .NET 10. All five named
mutations produce ten intended assertion failures; exact production restoration
is followed by two passes each time. Full matrices retain all 482 preceding case
identities plus two new passes: .NET 9 has 429 passes/55 skips; .NET 10 has 428
passes/one known synchronous ordered-nack failure/55 skips. The same historical
failure passes on .NET 9 without establishing a fix. No other prior outcome changes.

All 6,490 preceding C# files and approved test/helper bytes remain unchanged. All
four library targets build cleanly; neither new file adds a warning. Three owned
emulators are removed and logs retained. See [validation](../../validation.md)
and evidence in `/private/tmp/brighter-4170-gcp-concurrent-stream-reader/`.
The next [MQTT publication writer draft](../mqtt-publication-writer/README.md)
remains outside the compiled suite, unbuilt/unrun pending review approval.
