# RabbitMQ scheduled retry selection

Review-after for #4170; this scoped package is validated and integrated locally.
No commits or pushes. This addresses the scheduler branch identified by the source audit.

Add `RmqSubscription.ScheduledRequeuePublicationTopic` to select a registered producer
when a delayed retry uses a scheduler. Capture it when creating the consumer. Add
`RmqPublication.DestinationQueue` for an explicit default-exchange queue destination,
separate from its local publication selector and the message's logical topic. Null
settings preserve existing behavior. Direct/native-delay retry keeps its existing
subscription writer selection.

The consumer schedules a private message/header copy carrying the reserved producer
selector. It must retain payload, event type, metadata and persistence without changing
the received message. The registered producer invokes its writer at dispatch, after
trace context is available. Native routing remains owned by the gateway. Scheduler
acceptance and eventual confirmed publication are separate observations.

Four native cases cover sync/async factory/operations with default/custom writing.
A source queue and sibling share the ordinary binding; the selected retry must reach
only the source queue even when the logical topic differs. Mutating the subscription
after consumer creation must not change its captured selection. The test observes no
early publication, an unchanged source, a later native body/identity/metadata, the
dispatch trace, successful confirmation and an empty outbox.

Named production mutations will omit the private selector, ignore the destination
queue, and schedule the original message while setting its selector. Expected failures
are respectively missing selected delivery, missing queue-specific delivery, and source
serialization equality. Restore exact bytes after each. Full Async regressions and all
library targets are required; the analogous Sync implementation remains a separate
affected-project validation boundary.

## Current evidence

The initial test build fails on the two missing public settings. Adding configuration
stubs alone produces four missing-dispatch failures on each framework. The six-file
selection/copy/routing implementation then reaches native delivery. Its first focused
run exposes missing OpenTelemetry SDK initialization in the test setup; using the same
initialization as the existing Quartz integration tests yields four passes on each
framework. No assertion was removed. Missing dispatch is now asserted explicitly at
the bounded confirmation observation.

The original baseline fixture lacked the mutual-TLS certificates/listener and the
native-delay port expected by existing tests. Its diagnostic log is preserved; the
incomplete process was stopped rather than waiting through repeated connection
timeouts. The owned fixture now uses the repository's generated test certificates,
TLS configuration, native-delay plugin and listeners. Complete unchanged baselines
are running before mutations and final regressions. No existing test is excluded.

Evidence and isolated sources:
`/private/tmp/brighter-4170-rmq-scheduled-selection/`. Integration must compare the six
main-checkout files with their saved pre-change hashes before applying the validated
bytes. RocketMQ's concurrent source-integrity check must finish first.

## Completed validation — 2026-11-07

Both complete baselines pass 556 cases with two existing skips. The final native
suites pass 560 cases with those same skips on each framework, preserving every
prior identity/outcome and adding four cases. All twelve intended mutation failures
are verified, with exact restoration and four passes after each restoration.
All four library targets build. One restored run exposed an ActivityStopped timing
race in the new assertion; the test now explicitly waits for producer-span completion
within the original timeout. All mutations and regressions were repeated with that
correction; the prior failure evidence is retained.

The six production files were integrated only after RocketMQ's source-integrity
check finished and their original main-checkout hashes matched. Main and isolated
project test sources are identical. The integration manifest is
`/private/tmp/brighter-4170-validated-integration/integration.json`.
Positive-delay sends on explicitly queue-targeted publications still need separate
acceptance; this package establishes scheduler-backed retry and zero-delay dispatch.
