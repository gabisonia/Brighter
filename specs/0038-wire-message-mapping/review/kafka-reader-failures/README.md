# Kafka configured reader failures

Status: approved, implemented and validated. All eight cases reproduce the receive
failure on .NET 9 and .NET 10, then pass after the consumer correction. Nine named
mutations produce 72 intended assertion failures. Restored focused cases pass on
both frameworks; both complete Kafka suites pass 285 cases with no failures/skips,
retaining all 277 preceding outcomes. The two approved files are unchanged. Only
KafkaMessageConsumer changes in production. No commit or push was made.

The consumer isolates configured reader exceptions and null results per delivery.
It returns an unacceptable message with native body, topic, key and offset, plus
metadata readable by the default parser. Applications can apply rejection policy
and acknowledge the actual offset. Later good deliveries still use the configured
reader. The recovered message remains unacceptable even when default parsing succeeds.

The [eight cases](When_a_kafka_wire_reader_fails_should_preserve_the_delivery_and_continue_receiving.cs)
combine sync/async consumption, throw/null failure, and failed delivery first/last.
A native producer publishes two messages to a unique one-partition topic. Both
messages have valid default headers, so an inappropriate successful fallback is
detectable. The [reader strategy](KafkaSelectiveFailingWireReader.cs) fails only for
one native identity and gives the other a different application identity/topic/body.

Receive must complete without an exception and return both messages in native order.
The failed delivery must be unacceptable and retain native identity, topic, key,
exact binary body, handled count, traceparent and partition offset. The good delivery retains the selected
mapping and its own native offset. Both are acknowledged through the selected
sync/async interface. Consumer disposal drains the partial commit batch; an
independent native consumer verifies the final committed offset. This checks
settlement after closing, while the preceding endpoint cases cover live commit
observation. Topic deletion follows consumer disposal in finally.

No mock, reflection, internal test access, request handler or package is added.
The helper is a real reader strategy. Receive and broker queries have bounded waits.
The tests explicitly select Reactor/Proactor to match their consumption path.

## Validation after approval

Copy the two files unchanged into MessagingGateway and TestDoubles. Observe all
eight cases fail at `Assert.Null(receiveError)` on both frameworks before making
production changes. Preserve the failures. Implement per-delivery custom mapping
failure handling without changing normal default parsing, native delivery operations
or the configured successful path. Run all eight cases on .NET 9 and .NET 10.

Apply each named production mutation independently on .NET 9, selecting all eight
cases. Compilation errors, timeouts and exceptions at unrelated assertions do not
count as mutation proof.

| Mutation | Expected failures | Required assertion |
| --- | ---: | --- |
| Let custom throw/null failure escape Receive | 8 | No receive exception (`Assert.Null`) |
| Replace failed custom reading with successful default parsing | 8 | Failed delivery is unacceptable |
| Clear the failed delivery's preserved identity | 8 | Failed native identity equality |
| Clear the failed delivery's preserved body | 8 | Exact failed binary body equality |
| Clear the failed delivery's handled count | 8 | Failed handled count equality |
| Clear the failed delivery's traceparent | 8 | Failed traceparent equality |
| Omit the failed delivery's native offset metadata | 8 | Offset bag entry exists (`Assert.True`) |
| Replace the successfully mapped result with default mapping after invoking the reader | 8 | Good application identity equality |
| Skip acknowledgement offset storage | 8 | Native committed offset equality after disposal |

Require 72 intended failures across nine mutations. Restore production bytes and
rerun all eight focused cases on both frameworks. Run both complete Kafka suites,
retaining all preceding outcomes by exact name and adding eight passing cases.
Build all supported library/test targets; check warnings, source/dependency scope,
approved files, documentation links, whitespace, HEAD and stash. Preserve original
failures and stop task-owned services. No commit or push is authorized.

Explicit unacceptable reader returns, malformed default-parser preservation,
invalid-message forwarding, writer failure recovery, factory isolation, retry
writers, transactional/batch configuration and concurrency remain separate work.
This batch does not complete Kafka or the [global #4170 gates](../../tasks.md).


The subsequent [required-header failure batch](../kafka-default-parser-failures/README.md)
is implemented and validated with eight cases on both frameworks and 60 intended
mutation failures. Both full suites pass 293 cases. Parser exceptions remain open. Evidence for this
completed batch is under `/private/tmp/brighter-4170-kafka-reader-failures/`.
