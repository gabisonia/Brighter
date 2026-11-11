# GCP scheduled publication selection

Review-after #4170 characterisation. Four native cases cross synchronous/asynchronous
submission and default/composed selected writing. Drive public producer factory,
real InMemoryScheduler timer, CommandProcessor callback, typed registry/outbox and
native ordered Pub/Sub receive/ack. Verify no early mapping, selected body, fresh
traceparent/tracestate/baggage and preserved caller/type/destination.

Named RED mutation: replace the selected writer invocation in GcpMessageProducer
with DefaultGcpPubSubWireMessageWriter. Both custom cases must fail native body
comparison; default controls must pass. Restore source bytes exactly. Run both full
available Pub/Sub matrices, preserve known failures/skips and compare identities.
No hosted-service or generic durable-provider claim. No commits or pushes.

Initial four native cases reached a source-trace immutability assertion after all
selected-mapping/telemetry assertions passed. That assertion overreached the existing
producer contract: InMemoryScheduler retains the submitted message reference and
producer telemetry writes its dispatch context. Remove only that assertion and its
snapshot; keep source payload/type/topic ownership and all wire tracing assertions.
No production change or defect claim is made for that fixture correction. Original
test and four failure logs are retained.

## Results — 2026-11-09

Four focused cases pass per framework. The named writer bypass produces exactly
two custom-body failures and two passing default controls. Source is restored exactly;
restored focused runs pass. All 508 previous full-matrix identities are retained,
with four new passing cases (512 total): .NET 9 has 457 passes/55 skips; .NET 10 has
456 passes/one historical generated ordered-nack failure/55 skips. The prior .NET 9
failure passes in this run; that is not a root-cause disposition or a correction.
No product implementation change is needed. The new test is integrated locally.
Evidence: `/private/tmp/brighter-4170-gcp-scheduled-publication/`.

The integrated main-workspace project rebuilds successfully. All four new callback cases pass again on both .NET 9 and .NET 10 against the pinned native emulator. Its owned fixture is stopped with logs retained.
