# RabbitMQ Sync reader failures in mixed batches

Status: approved and implemented. Both reviewed files were copied unchanged into
compiled projects. All four cases reproduce the batch failure on both frameworks,
then pass after the consumer correction. Five mutations produce 20 intended
assertion failures. Both full Sync suites pass 223 cases with one existing skip,
retaining every preceding 219-pass outcome.

The endpoint integration previously let configured reader exceptions reach the
outer Receive catch, which throws ChannelFailureException. That prevented the
caller from receiving any delivery dequeued in the batch. The correction isolates
mapping failure per delivery and preserves its payload and native acknowledgement
information while continuing the batch.

The [four cases](When_rmq_sync_wire_reading_fails_should_preserve_the_delivery_and_continue_the_batch.cs)
combine a throwing/null-returning reader with failure first/last in a two-message
batch. An independent native client confirms both messages before consumption via
the public factory. The [selective reader](SyncSelectiveFailingWireReader.cs) fails
for one native ID and maps the other to a different application ID/topic/body.

Assert Receive completes without an exception and returns both deliveries in order; the failed delivery is unacceptable and
retains its native ID, routing key and exact binary body. The good delivery keeps
its custom mapping. Both retain distinct valid broker tags, persistence, native
handled count and redelivery state. Acknowledging both and disposing the consumer
must leave no native delivery behind. The native failure envelope is otherwise
valid for default reading, so silently falling back to the default is detectable.
No new public contract, request handler, mock or test-only export is needed.

## Named mutations

After the initial failure is fixed, run these independently with unchanged tests:

| Mutation | Expected failures | Intended assertion |
| --- | ---: | --- |
| Let configured reading failures escape to the batch-level catch | 4 | Receive completes without an exception |
| Replace failed custom reading with successful default mapping | 4 | Failed delivery is unacceptable |
| Omit the failed delivery's preserved native body | 4 | Exact failed binary body |
| Skip native metadata normalization for failed reading | 4 | Distinct broker delivery tags or valid delivery-tag range |
| Default-map the successful member of the mixed batch | 4 | Good delivery has the mapped application ID |

Expected: 20 intended failures across five mutations; compilation errors, timeouts
and unrelated exceptions do not count. Run the four new cases on .NET 9 and .NET
10, restore each mutation, rebuild all supported Sync library targets and run both
complete Sync regression suites on isolated RabbitMQ 4.2 with native-delay support.
Preserve prior outcomes plus four passes. Do not commit or push.

This batch covers configured throw/null behavior. Explicit unacceptable returns,
default-parser failure preservation, invalid-message forwarding, writer recovery,
retries, concurrency and scheduler paths remain separate work. It does not complete
RabbitMQ Sync parity or issue #4170.
