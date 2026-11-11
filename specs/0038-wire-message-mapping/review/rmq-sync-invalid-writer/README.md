# RabbitMQ Sync invalid-message writer selection

Status: implemented and validated with the approved
[native-tag assertion correction](../rmq-sync-invalid-writer-native-tag/README.md).
All fourteen cases pass initially after amendment and after mutation restoration
on both frameworks. Nine mutations produce 22 intended failures. Both complete
Sync suites pass 251 cases/one existing skip, retaining every prior outcome.
Only three production files change; the other 1,277 files and dependencies remain
unchanged. Initial missing-setting errors and both incorrect-assertion runs remain
in the evidence. The subsequent [six publication recovery cases](../rmq-sync-publication-recovery/README.md)
are now validated without further production changes; see that batch for results.

## Contract

Add nullable `RmqSubscription.InvalidMessageWireMessageWriter`, typed
`IAmAWireMessageWriter<RmqWireMessage>`, using the existing Sync body and native
properties callback contract. The consumer factory captures the selection for each
consumer. Null preserves the default writer. Existing public constructors and
package dependencies remain unchanged.

For an unacceptable message with an invalid route, invoke the selected writer
after rejection metadata and the invalid topic are applied. The writer may replace
the wire body/properties or compose `DefaultRmqWireMessageWriter`. Routing,
persistence, reserved-header cleanup, mandatory publication, confirms and source
acknowledgement remain gateway responsibilities. Restore the original message topic
after success or failure. Writer throw/null, native-properties callback failure and
an unbound invalid route must leave the original delivery available for recovery.

A delivery-error rejection, or an unacceptable rejection with no invalid route,
continues to use native dead-lettering. It must bypass the selected invalid writer
and preserve original wire content. Writer selection is independent for consumers
created by the same factory and independent of the reader/publication writer.

This batch does not add custom requeue selection or an asynchronous Sync consumer.
Publication and acknowledgement remain separate broker operations; a failure after
publication may still duplicate the delivery. The source observations are bounded,
not a proof of atomicity or absence forever.

## Fourteen cases

| File | Cases | Observable behavior |
| --- | ---: | --- |
| [Replacement/composition and recovery](When_rmq_sync_selects_an_invalid_writer_should_preserve_mapping_and_recover_from_failures.cs) | 10 | Each writer form succeeds, throws, returns null, throws from its native-properties callback, or encounters an unbound invalid route. Failures retain original native bytes/identity/count and restore topic. Repairing the writer/binding permits successful forwarding through a new consumer. The forwarded delivery proves rejection metadata, handled count, selected mapping and gateway controls. |
| [Consumer isolation](When_rmq_sync_subscriptions_select_invalid_writers_should_keep_each_consumers_mapping_isolated.cs) | 2 | Create two consumers from one factory before receiving, with custom/default selections in both orders. Each forwards its own bytes, identity, original topic and count with only its selected profile. |
| [Native dead-letter compatibility](When_rmq_sync_rejection_uses_native_dead_lettering_should_preserve_original_wire_content.cs) | 2 | Delivery-error with an invalid route, and unacceptable with no invalid route, bypass a throwing custom writer. Native DLX output preserves bytes, ID, MIME type, persistence, native headers/count and includes broker death metadata. |

[SyncRejectionProfileWireWriter](SyncRejectionProfileWireWriter.cs) is a strategy
implementation, not a mock. Its replacement serializes a proof payload; composition
adds a proof header to default output. Both intentionally supply conflicting
persistence, delay and delivery-tag values so native inspection checks the gateway's
ownership. No handler/request type or logging closed generic is added.

Tests use public Sync factory/consumer APIs, independent RabbitMQ.Client 6 native
publishing/inspection, unique owned queues and the isolated RabbitMQ 4.2 fixture.
After disposal, native source recovery/absence is observed for up to two seconds;
native dead-letter arrival is polled for up to five seconds. All three tests join
the existing `RMQ` collection. The approved files are now also in the test project.

## RED and regression plan after approval

1. Copy all four approved files unchanged into the Sync test project (helper under
   `TestDoubles`, tests under `MessagingGateway`). Build both frameworks and retain
   the expected missing-setting compile failure. A compile failure is not
   behavioural RED.
2. Implement the minimum subscription/factory/consumer selection and XML docs.
   Run all fourteen cases on .NET 9 and .NET 10, preserving approved test bytes.
3. Prove the following named production mutations on .NET 9. Use diagnostic
   compilation/JIT settings for reliable source locations. Select exact theory
   inputs with `DisplayName` filters and verify actual cases by TRX name. A build
   error, timeout or unrelated exception is not evidence.

| Mutation | Selected cases | Required failures and assertion |
| --- | --- | --- |
| Ignore the selected invalid writer | Recovery, failureMode 0 | 2: profile presence, line 186 |
| Acknowledge before invoking the invalid writer | Recovery, failureMode 1, 2 or 3 | 6: recovered original present, line 147 |
| Omit original-topic restoration | Recovery, failureMode 0 | 2: original topic, line 178 |
| Publish invalid forwards without mandatory routing | Recovery, failureMode 4 | 2: forwarding exception, line 127 |
| Cache the first writer selection for all factory consumers, including a first null | Isolation | 2: selection-specific profile, line 118 |
| Omit native BasicReject | Native dead-letter | 2: dead-letter arrival, line 126 |
| Force transient delivery mode after the writer callback | Recovery, failureMode 0 | 2: native persistence, line 190 |
| Swallow the native-properties callback exception and continue publishing | Recovery, failureMode 3 | 2: callback exception, line 127 |
| Skip publisher cleanup of delay and delivery-tag headers | Recovery, failureMode 0 | 2: no writer-owned delay, line 191 |

Require **22 intended failures** across nine mutations. Restore all production bytes
before normal builds and rerun fourteen focused cases on each framework. Then run
both full Sync suites, expecting **251 passes and one existing skip each**, retaining
all preceding outcomes by exact test name. Build the Sync library for
netstandard2.0/net8.0/net9.0/net10.0. Verify approved bytes, change scope, dependency
files, HEAD and saved stash; update usage/validation and stop task-owned brokers.

Full Sync parity and #4170 remain open beyond this batch: publication writer/callback
and outbox recovery, custom retry selection/controls, concurrency, scheduler-backed
retry and the remaining transport/scheduler work in [tasks](../../tasks.md).
No commit, push, publication or review-gear change is part of this approval.
