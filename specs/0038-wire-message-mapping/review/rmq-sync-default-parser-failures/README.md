# RabbitMQ Sync default-parser failures in mixed batches

Status: approved and implemented. The reviewed test was copied unchanged into the
compiled project. Four cases reproduce native body loss on both frameworks and
pass after the parser correction. Four mutations produce 12 intended assertion
failures. Final full suites pass 227 cases with one existing skip on each framework.
An earlier full .NET 9 run encountered a broker storage I/O failure during queue
binding; its evidence and passing rechecks are retained in [validation](../../validation.md).

The configured reader's throw/null path now preserves a failed native delivery.
The default parser catches its own errors and returns an unacceptable placeholder
before that catch can run. Its failure branch previously dropped native bytes,
delivery metadata and handled count. The correction retains all three. Direct
unconfigured consumers bypass configured-reader normalization, so both default
paths are covered.

The [four cases](When_rmq_sync_default_parsing_fails_should_preserve_the_delivery_and_continue_the_batch.cs)
combine an unset reader/explicit DefaultRmqWireMessageReader with malformed delivery
first/last in a two-message batch. The malformed native message contains an invalid
legacy message-type header and a distinctive binary body. The good delivery uses a
valid default format. An independent RabbitMQ Client 6 producer confirms both
publications before the public consumer factory receives them.

Receive must complete without an exception and return both messages in order.
The malformed delivery must be unacceptable and preserve native ID, topic, exact
binary body, a valid distinct broker tag, persistence, redelivery and handled count.
The good delivery retains its default ID/topic/body. Acknowledging both and closing
the consumer must leave no native delivery behind. No new public contract, helper,
handler generic, mock or dependency is needed.

## Named mutations

After the initial failure is fixed, run these independently with unchanged tests:

| Mutation | Expected failures | Intended assertion |
| --- | ---: | --- |
| Omit native body preservation on default-parser failure | 4 | Exact failed binary body |
| Skip default-parser delivery metadata normalization for failed parsing | 2, unset-reader cases | Valid delivery-tag range |
| Skip default-parser handled-count normalization for failed parsing | 2, unset-reader cases | Native handled count equals three |
| Classify a default-parser failure as an application event | 4 | Failed delivery is unacceptable |

Expected: 12 intended failures across four mutations. Explicit configured readers
retain the consumer-level metadata/count normalization, so the two default-parser
normalization mutations must only fail unset-reader cases. Diagnostic compilation
may be used for precise assertion locations; restored runs use normal optimization.
Compilation errors, timeouts and unrelated exceptions do not count.

After approval, copy the reviewed test into MessagingGateway, reproduce the failure
on .NET 9 and .NET 10, make the minimum parser correction and run all four cases on
both. Apply and restore the mutations, build all supported Sync library targets and
run both full Sync suites on isolated RabbitMQ 4.2 with TLS/native-delay support.
Retain every preceding outcome and add four passes. Do not commit or push.

This closes default-parser failure preservation, not full Sync parity. Explicit
custom unacceptable returns, native invalid-message forwarding, writer/callback
recovery, custom retry selection, concurrency and scheduler paths remain separate.
Full issue #4170 implementation remains open.
