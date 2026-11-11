# RabbitMQ Sync endpoint wire mapping

Status: original proposal superseded by the approved
[native-properties callback amendment](../rmq-sync-native-properties/README.md).
The amended six cases pass on both frameworks; eight mutations produce 26 intended
failures; both complete Sync suites pass 219 cases with one existing skip.

The original test copies reached missing-API RED. The first implementation then
failed because RabbitMQ Client 6's concrete BasicProperties class is internal.
That attempt was saved and restored before implementing the amended contract.
The original four C# drafts below remain unchanged as review history; the compiled
writer helper uses the approved callback. Existing consumer constructors remain
unchanged, and full Sync parity is still open.

This proposal established the first endpoint integration using public factories
and an independent native client. The original contracts below document the
superseded construction assumption; use the amendment for the implemented API.

## Original public contracts (superseded)

Use the existing shared `IAmAWireMessageWriter<T>` and `IAmAWireMessageReader<T>`
interfaces. Add transport-specific types to the RMQ Sync package:

- `RmqWireMessage` carries `ReadOnlyMemory<byte> Body` and native
  `IBasicProperties Properties`, initialized to fresh RabbitMQ V6 properties.
- `RmqPublication.WireMessageWriter` optionally selects a writer of that type.
- `RmqSubscription.WireMessageReader` optionally selects a reader of
  `BasicDeliverEventArgs`.
- Public `DefaultRmqWireMessageWriter` and `DefaultRmqWireMessageReader` preserve
  existing formats and support composition. Null configuration selects defaults.

Keep the existing consumer constructors source-compatible and retain unconfigured
behavior. Route configured mapping through the public factories. This does not add
async factory/consumer support: RMQ Sync already exposes a Task-based producer
send wrapper, which is tested through its synchronous factory. No RabbitMQ client
upgrade or new package dependency is proposed.

## Six native cases

The [four writer cases](When_rmq_sync_sends_with_a_configured_wire_writer_should_preserve_mapping_and_native_controls.cs)
cover synchronous and Task-based sending, each with replacement mapping or default
composition. The [writer](SyncEndpointWireWriter.cs) changes the native ID and body
for replacement mapping and adds application/trace headers in both modes. It also
supplies deliberately incorrect persistence, delivery-tag and delay values.

Native assertions verify the custom/default body and properties, tenant header,
dispatch-time traceparent, configured persistence, removal of fabricated delivery
controls, original confirmation ID/topic, unchanged application payload/bag and
no duplicate. Confirmation identity must remain the Brighter ID even when the
native ID differs. The source starts with a distinct old traceparent so late
propagation produces an assertion failure, rather than a null dereference.

The [two reader cases](When_rmq_sync_reads_with_a_configured_wire_reader_should_preserve_mapping_and_acknowledgement.cs)
cover a replacement reader and default composition. Native publication is
confirmed before the public consumer factory receives it. The
[reader](SyncEndpointWireReader.cs) deliberately returns incorrect delivery tag,
redelivery, persistence and handled count. The gateway must apply broker-owned
values while preserving the mapped application ID, topic, payload and tenant.
Acknowledgement and consumer disposal must leave no native delivery behind.

Both test classes use the existing RMQ collection and unique native resources.
The reader owns a durable queue; producer observation uses a server-named exclusive
queue. No request handler, new closed handler generic, mock, test-only public API
or generated fixture is introduced.

## Named mutations

Missing public APIs will initially prevent compilation. After implementation,
verify behavior through these independent mutations with unchanged test bytes.
Each mutation applies to the new Sync integration only, and is restored exactly.

| Mutation | Expected failures | Intended assertion |
| --- | ---: | --- |
| Ignore the publication writer and always use the default writer | 4 | Replacement native ID (2); composed tenant-header presence (2) |
| Ignore the subscription reader and always use the default reader | 2 | Replacement application ID (1); composed tenant-bag presence (1) |
| Omit gateway persistence override on writer output | 4 | Native persistent delivery mode |
| Retain the writer's fabricated delivery tag | 4 | Delivery-tag header absence |
| Retain the writer's fabricated delay on immediate send | 4 | Delay-header absence |
| Propagate producer context after writing/publishing | 4 | Native traceparent equals the active publish span |
| Omit gateway delivery-metadata normalization after configured reading | 2 | Returned delivery tag is in the broker range |
| Omit gateway handled-count normalization after configured reading | 2 | Native handled count equals three |

Expected total: 26 intended failures across eight mutations. Compilation failures,
timeouts and unrelated exceptions do not count. Diagnostic JIT settings may be
used to retain source locations; restored runs use normal runtime settings.

## Original validation plan

First capture the complete existing RMQ Sync baseline on .NET 9 and .NET 10,
both using isolated RabbitMQ **4.2** with TLS and native-delay support. Do not use
the Async suite's 4.3 broker or change Sync durability defaults to accommodate it.

Copy the two tests under `MessagingGateway` and two helpers under `TestDoubles`.
Observe the missing API failures, implement the minimal integration and preserve
existing serialization, constructor compatibility and delivery behavior. Run all
six cases on both frameworks, then the reviewed mutations. Rebuild the Sync library
on all supported targets and rerun the complete Sync suites, retaining each
baseline outcome plus six new passes. Shared Core changes are not expected; if
needed, include its complete regressions. Preserve all existing WIP and the stash;
do not commit or push.

This is the first Sync integration, not complete gateway parity. Writer/reader
failures, mixed batches, retries, rejection forwarding, native delay, isolation,
concurrency and scheduler-backed paths need separate coverage. AWS callback-pump
recovery, other schedulers/gateways and final configuration review remain open.
