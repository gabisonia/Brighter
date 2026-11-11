# Kafka explicit unacceptable reader results

Status: approved and validated as characterisation. All eight cases pass initially
and after mutation restoration on both frameworks without production changes.
Ten named mutations produce 80 intended assertion failures. Both complete Kafka
suites pass 309 cases with no failures/skips, retaining all 301 prior outcomes.
Both approved files, all 1,282 production files and dependencies remain unchanged.
No commit or push was made.

A configured reader may deliberately return an unacceptable message with its own
application mapping. That is a successful strategy invocation, unlike a thrown
exception or null result. The gateway should preserve the selected mapping and
classification while replacing its partition offset with the actual delivery offset.
It must continue mapping subsequent deliveries and acknowledge the native positions.

The [eight cases](When_a_kafka_reader_returns_unacceptable_should_preserve_its_mapping_and_acknowledge_the_native_offset.cs)
combine sync/async consumption, replacement/composition with the default reader,
and unacceptable delivery first/last. Both native messages have valid default headers.
The [reader strategy](KafkaExplicitUnacceptableWireReader.cs) deliberately returns
one unacceptable result and one event. It selects application body, topic, key,
handled count, traceparent and tenant value that differ from the native defaults.
Replacement mode also selects different identities. Both results include a deliberately
wrong native offset, which the consumer must replace.

Assert exactly two reader calls, both deliveries in native order, explicit unacceptable
classification, every selected failed-message field, the selected good-message mapping,
and actual offsets for both. Acknowledge through the selected interface, dispose the
consumer to flush the partial batch, and verify final committed offset using a separate
native client. Topics are unique and cleaned up on failure; waits are bounded.
No request handler, internal access, mock or package is added. The strategy implements
the existing public reader interface and composes the existing default adapter.

## Validation after approval

Copy the test and strategy unchanged into MessagingGateway and TestDoubles. These
are CHARACTERISE cases expected to pass without production changes. If they do,
prove RED with the named production mutations below; do not alter expectations to
manufacture a failure. If a real failure appears, preserve it and implement only the
reviewed behavior before applying the same mutation checks.

| Mutation | Expected failures | Required assertion |
| --- | ---: | --- |
| Ignore the configured reader | 8 | Exactly two strategy calls |
| Replace explicit unacceptable results with default parsing | 8 | Unacceptable classification equality |
| Clear the selected unacceptable body | 8 | Selected failed body equality |
| Replace the selected unacceptable topic with the native topic | 8 | Selected failed topic equality |
| Replace the selected unacceptable key with the native key | 8 | Selected failed key equality |
| Clear the selected unacceptable handled count | 8 | Selected failed handled count equality |
| Clear the selected unacceptable traceparent | 8 | Selected failed traceparent equality |
| Remove the selected unacceptable tenant header | 8 | Tenant bag entry exists (`Assert.True`) |
| Omit configured-reader offset normalization | 8 | Exact failed native offset equality |
| Omit acknowledgement offset storage | 8 | Actual committed final offset equality |

Require 80 intended failures across ten independent mutations on .NET 9. Except the
ignored-reader mutation, keep invocation behavior intact so the call-count assertion
does not mask the intended failure. No compile error or unrelated exception counts.
Restore production bytes, rebuild normally, and rerun focused cases on both frameworks.
Run both complete Kafka suites and retain every prior outcome. Check supported targets,
warnings, source/dependency scope, approved-file identity, links, whitespace, HEAD and
stash. Preserve evidence and stop task-owned infrastructure. Do not commit or push.

These cases do not establish rejection forwarding, duplicate/binary custom-header
handling, other malformed fields, writer recovery, factory isolation, retry selection,
transaction/batch settings or concurrency. Kafka and [global #4170 gates](../../tasks.md)
remain incomplete.


The subsequent [writer-recovery batch](../kafka-writer-recovery/README.md) is validated
without production changes: eight cases on both frameworks, nine mutations with
72 intended failures, and 317 passes in both full suites. Evidence is under
`/private/tmp/brighter-4170-kafka-explicit-unacceptable/`.
