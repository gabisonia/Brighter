# Kafka parser exception preservation

Status: approved, implemented and validated. All eight cases reproduce payload
loss on both frameworks and pass after content-type parsing is isolated. Nine named
mutations produce 68 intended assertion failures; the offset mutation retains four
explicit-reader passes. Restored focused runs pass on both frameworks. Both complete
Kafka suites pass 301 cases with no failures/skips, retaining all 293 prior outcomes.
The approved test is unchanged. Only KafkaMessageCreator changes in production.
No commit or push was made.

An invalid content-type value is now caught at MIME parsing, so the creator can
continue reading independent headers. The result remains unacceptable and retains
its binary body and readable metadata for acknowledgement or rejection. Missing
content type keeps the existing plain-text default; valid values keep existing
CloudEvents-first precedence. Other parser and custom-header exceptions remain open.

The [eight cases](When_kafka_header_parsing_throws_should_preserve_the_delivery_and_continue_receiving.cs)
combine sync/async consumption, unset/explicit default reader, and malformed delivery
first/last. A native producer sends two messages to a unique one-partition topic.
The failed message has `content-type=invalid/content/type`; all other fields
are valid. The content-type parser runs before handled count, trace and custom bag
mapping, so simply retaining fields parsed before the exception is insufficient.

Receive must return both deliveries without throwing. The failed delivery remains
unacceptable and retains its exact binary body, identity, topic, native key, handled
count, traceparent, tenant header and partition offset. The valid delivery retains
its ordinary mapped identity, topic, body and offset. Both deliveries are acknowledged;
a native observer verifies the actual committed final offset after consumer disposal.
No exact fallback content-type value is imposed for the malformed field.

This exercises public factory/consumer APIs against the real local Kafka broker.
No helper, mock, reflection, request handler, internal access or dependency is added.
Topics are cleaned up even after failure; consumption and broker observation are bounded.

## Validation after approval

Copy the file unchanged to MessagingGateway. On both frameworks, observe all eight
cases failing at the exact failed-body equality, preserving the original evidence.
Implement preservation without changing successful parsing, the public creator API,
or the protected virtual custom-header extension. Keep the failed result unacceptable;
ignoring malformed content type and treating the delivery as successful is not recovery.

Apply the following named production mutations independently on .NET 9, selecting
all eight cases and checking exact assertions and outcomes in TRX:

| Mutation | Expected failures | Required assertion |
| --- | ---: | --- |
| Clear the failed delivery body | 8 | Exact failed binary body equality |
| Clear the failed delivery identity | 8 | Failed identity equality |
| Clear the failed delivery partition key | 8 | Failed key equality |
| Clear the failed delivery handled count | 8 | Failed handled count equality |
| Clear the failed delivery traceparent | 8 | Failed traceparent equality |
| Remove failed-parser native offset metadata | 4 unset-reader cases | Offset bag entry exists (`Assert.True`) |
| Skip failed-parser custom-header preservation | 8 | Tenant bag entry exists (`Assert.True`) |
| Classify the failed delivery as an application event | 8 | Unacceptable classification equality |
| Omit acknowledgement offset storage | 8 | Actual committed final offset equality |

Require 68 intended failures across nine mutations. For the offset mutation, the
four explicit-default-reader cases must pass because the consumer owns offset
normalization. Keep that behavior intact. Compilation failures and unrelated
exceptions are not accepted as mutation evidence.

Restore production bytes and normal optimized builds. Rerun all eight focused cases
on both frameworks, then both complete Kafka suites, retaining all prior outcomes.
Check all supported build targets, warnings, source/dependency scope, approved file
identity, documentation links, whitespace, HEAD and stash. Preserve evidence and
stop task-owned infrastructure. Do not commit or push.

This batch covers a throwing content-type parser. Duplicate/binary custom headers,
other malformed fields, explicit custom unacceptable results, rejection forwarding,
writer recovery, factory isolation, retries, transaction/batch settings and concurrency
remain separate work. Kafka and the [global #4170 gates](../../tasks.md) stay open.


The subsequent [explicit-unacceptable batch](../kafka-explicit-unacceptable/README.md)
is validated without production changes: eight cases on both frameworks, ten mutations
with 80 intended failures, and 309 passes in both full suites. Evidence is under
`/private/tmp/brighter-4170-kafka-parser-exceptions/`.
