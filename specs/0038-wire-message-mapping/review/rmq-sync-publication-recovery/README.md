# RabbitMQ Sync publication mapping failure and outbox recovery

Status: validated without production changes. All six cases pass initially and
after mutation restoration on .NET 9 and .NET 10. Five named mutations produce
26 intended failures. Both complete Sync suites pass 257 cases with one existing
skip, retaining every prior outcome. All 1,280 production files and dependencies
remain unchanged. The subsequent [eighteen immediate-retry cases](../rmq-sync-requeue-writer/README.md)
are now implemented and validated; see that batch for its later results.

## Six cases and contract

The [test](When_rmq_sync_publication_mapping_fails_should_preserve_pending_outbox_messages_and_recover_confirmations.cs)
covers three publication failures through both `Send`/`ClearOutbox` and the existing
`SendAsync`/`ClearOutboxAsync` compatibility APIs: writer throw, writer null result,
and native-properties callback throw. Factory construction uses the supported
synchronous `RmqMessageProducerFactory.Create`; no async factory or consumer API is
introduced. The Sync async producer methods remain wrappers over blocking I/O.

Each case first calls the public producer and requires the mapping error to surface
without a native delivery or confirmation. The same message is then dispatched
through a real `OutboxProducerMediator<Message, CommittableTransaction>` using an
`InMemoryOutbox`, default resilience policies and the same real broker producer.
Exhausted mapping retries must leave the original ID, topic, body and tenant data
pending with no native delivery or confirmation.

A different healthy message is dispatched while that writer still fails the first
message. Its real broker confirmation must identify the healthy message, and only
that outbox entry becomes dispatched. This exposes stale pending-confirmation
entries left by the failed send; the test does not synthesize a broker ack. The
native delivery must contain the healthy ID/body/profile and persistence.

Repairing the strategy and clearing the original pending message must publish its
retained content, confirm its original ID/topic and mark it dispatched. Closing the
producer registry drains confirmation callbacks before asserting exactly two
successful confirmations and no extra native delivery. The original application's
ID/topic/body/tenant values remain intact. Confirmation and outbox observation use
bounded waits; this is not an exactly-once guarantee under broker/network failure.

[SyncRecoverablePublicationWireWriter](SyncRecoverablePublicationWireWriter.cs)
is a strategy that composes the default writer, selectively fails one known ID,
and adds a native profile on successful writes. It allows the healthy message
through while the failed message remains unpublishable. No mocks, reflection,
internal APIs, simulated acks, handler registrations or new request types are used.
The Sync test project has no logging initializer; no logger factory or handler
closed generic is added. The standard mediator closed generic uses the existing
logging configuration, matching other transport integration tests.

Both files have the contributor's MIT license. Tests join the `RMQ` collection,
use a unique exchange and an exclusive broker-owned queue, and retain the package's
RabbitMQ 4.2 fixture and existing dependencies.

## Validation after approval

Copy the two files unchanged into `MessagingGateway` and `TestDoubles`. Build and
run all six cases on .NET 9 and .NET 10. If they pass initially, use the following
named production mutations for behavioural RED. If a real failure occurs, preserve
it and make only the correction required by these expectations, then prove the
same mutations. Never change the tests to obtain a favorable result.

| Mutation | Cases | Intended failure |
| --- | ---: | --- |
| Ignore the publication writer when constructing the producer's publisher | 6 | Mapping exception present, line 110 |
| Swallow native-properties callback exceptions and continue publishing | 2, failureMode 3 | Mapping exception present, line 110 |
| Omit removal of a failed send's pending confirmation in the producer's finally block | 6 | Healthy confirmation ID, line 152 |
| Publish an empty wire body after mapping | 6 | Healthy native body, line 165 |
| Mark the outbox entry dispatched after the failed send attempt, despite confirmation support | 6 | Original pending entry, line 129 |

The last mutation applies the same defect to both sync and non-bulk async dispatch
paths in `OutboxProducerMediator`: mark dispatched after the resilience-wrapped send
regardless of its result. Use native producer callbacks in every run. Require
**26 intended assertion failures** across five mutations; compilation errors,
timeouts, wrong case selections and unrelated failures do not count. Check actual
case names and assertion locations in TRX. Restore every mutated production byte.

Rebuild with normal optimization, rerun the six cases and run both full Sync suites,
expecting **257 passes and one existing skip each**, retaining all 251 preceding
passes by exact test name. Build the Sync library's four targets and compare source,
dependency, approved-test and Git-state manifests. If a required correction changes
Core or another production project, also run that project's complete suites on both
frameworks. Record all failures/rechecks, update usage/validation and stop owned
brokers. No commit, push, publication or review-gear change is authorized.

This batch covers publication failure/confirmation/outbox recovery. Custom retry
mapping, concurrent selection, scheduler-backed retry and the remaining #4170
transport/scheduler scope still require their own completed implementation and
validation.
