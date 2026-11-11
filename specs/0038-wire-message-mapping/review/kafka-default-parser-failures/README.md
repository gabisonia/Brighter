# Kafka default-parser failure preservation

Status: approved, implemented and validated. All eight cases reproduce payload
loss on both frameworks and pass after the parser correction. Eight named mutations
produce 60 intended assertion failures; the offset mutation also retains four
explicit-reader passes. Restored focused runs pass on both frameworks. Both full
Kafka suites pass 293 cases with no failures/skips, retaining all 285 prior outcomes.
The approved test is unchanged. Only KafkaMessageCreator changes in production.
No commit or push was made.

The default Kafka creator now maps the delivery before classifying failed required
headers as unacceptable. Both unset and explicit default readers preserve body,
identity, topic, key, handled count, trace, custom headers and native offset metadata.
The same mapping and metadata path serves successful and failed validation, retaining
the existing public creator and protected virtual custom-header extension.

The [eight cases](When_kafka_default_parsing_fails_should_preserve_the_delivery_and_continue_receiving.cs)
combine sync/async consumption, unset/explicit default reader, and malformed delivery
first/last. A native producer sends two messages to a unique one-partition topic.
One has an invalid legacy MessageType string; identity, native key, binary body,
handled count, traceparent and custom tenant header remain valid. The other message
uses the normal default format.

Receive must return both messages without throwing and preserve native order.
The malformed delivery must be unacceptable with its exact binary body, identity,
topic, key, handled count, traceparent, tenant value and native partition offset.
The successful delivery retains its default identity/topic/body and offset. Both
are acknowledged through the chosen interface. Closing the consumer drains the
partial commit batch; a separate native observer checks the actual committed offset.
The previous endpoint tests retain the live asynchronous commit check.

No helper, mock, reflection, request handler, internal access or package is added.
Topics are deleted after consumer disposal, including on failure. Reactor/Proactor
configuration matches the chosen path. Receive and broker calls have bounded waits.

## Validation after approval

Copy the test unchanged to MessagingGateway. Both frameworks should fail all eight
cases at the exact failed-body equality before production changes. Preserve those
failures, implement default-parser failure preservation, and run all eight cases
on .NET 9 and .NET 10. Keep successful parsing, public creator compatibility and its
virtual custom-header extension intact.

Apply each named production mutation independently on .NET 9. Select all eight
cases and verify exact outcomes and assertions in TRX.

| Mutation | Expected failures | Required assertion |
| --- | ---: | --- |
| Clear the failed default delivery's body | 8 | Exact failed binary body equality |
| Clear the failed default delivery's identity | 8 | Failed identity equality |
| Clear the failed default delivery's partition key | 8 | Failed key equality |
| Clear the failed default delivery's handled count | 8 | Failed handled count equality |
| Clear the failed default delivery's traceparent | 8 | Failed traceparent equality |
| Remove failed-parser native offset metadata | 4 unset-reader cases | Offset bag entry exists (`Assert.True`) |
| Skip failed-parser custom-header preservation | 8 | Tenant bag entry exists (`Assert.True`) |
| Classify the failed default delivery as an application event | 8 | Unacceptable classification equality |

Require 60 intended failures across eight mutations. For the offset mutation,
explicit default readers must retain four passes because the consumer normalizes
the selected reader's offset. Do not remove that gateway behavior to force eight
failures. Compilation errors and unrelated exceptions do not count as proof.

Restore production bytes, rebuild normally, and rerun all eight focused cases on
both frameworks. Run both full Kafka suites, preserving every prior outcome and
adding eight passing cases. Check supported targets, warnings, source/dependency
scope, approved file identity, documentation links, whitespace, HEAD and stash.
Preserve all evidence and stop task-owned services. Do not commit or push.

This batch targets a failed required header, not every malformed-input path.
Parser-thrown errors, duplicate/malformed custom headers, explicit custom
unacceptable results, rejection forwarding, writer recovery, factory isolation,
retries, transaction/batch settings and concurrency remain separate work. Kafka
and the [global #4170 gates](../../tasks.md) remain incomplete.


The subsequent [content-type parser batch](../kafka-parser-exceptions/README.md) is
implemented and validated with eight cases on both frameworks and 68 intended
mutation failures. Both full suites pass 301 cases. Other malformed headers remain open. Evidence for this completed
batch is under `/private/tmp/brighter-4170-kafka-default-parser-failures/`.
