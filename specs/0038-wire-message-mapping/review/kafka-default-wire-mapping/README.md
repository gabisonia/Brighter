# Kafka default wire mapping adapters

Status: implemented and validated with the approved
[charset correction](../kafka-default-content-type/README.md). All four focused cases
pass initially and after restoration on .NET 9 and .NET 10. Ten independent mutations
produce ten intended assertion failures. Both complete Kafka suites pass 269 cases,
no failures/skips, retaining all 265 baseline outcomes. All supported targets build;
no warnings occur in new files. Existing source changes are limited to the publisher
and consumer; two default adapters are added. No commit or push was made.

## Public contract

Add `DefaultKafkaWireMessageWriter`, implementing
`IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>`. Its optional
`IKafkaMessageHeaderBuilder` constructor argument preserves the existing header
extension point; omission uses `KafkaDefaultMessageHeaderBuilder`. The writer maps
key, body and headers using the existing publisher rules. This native Kafka type
already exposes everything needed for composition, so no new carrier is necessary.

Add `DefaultKafkaWireMessageReader`, implementing
`IAmAWireMessageReader<ConsumeResult<string, byte[]>>`, delegating to the existing
`KafkaMessageCreator`. Keep that public creator and its virtual bag-entry extension
compatible. The new reader names the standard reader role and lets applications
compose the default mapping through the common wire-reader contract.

Route the existing publisher and consumer default mapping through these adapters,
so production and callers composing the defaults use the same path. Preserve
`KafkaPublication.MessageHeaderBuilder`, public constructors, topic selection,
confirmation bookkeeping, error behavior and native offset operations. This batch
adds the two reusable public adapters; configurable publication/subscription
strategies, custom failures and native delivery controls come in the next Kafka
increment. Do not leave parallel, unused copies of default mapping logic.

## Reviewed cases

| Test | Behavior |
| --- | --- |
| [Default writer compatibility](When_the_default_kafka_writer_maps_a_message_should_preserve_native_content_and_headers.cs) | Native key, exact binary body, ordered header bytes matching the existing builder, explicit identity/topic/count/delay/telemetry checks, local-header exclusion and unchanged serialized source. |
| [Legacy header builder](When_the_default_kafka_writer_uses_a_legacy_header_builder_should_preserve_its_selected_format.cs) | A genuine custom header strategy produces its exact two-header format, is invoked once, and retains native key/body and source values. No CloudEvents headers are imposed on its output. |
| [Writer output isolation](When_a_default_kafka_writer_is_reused_should_keep_each_native_message_and_header_collection_independent.cs) | Reusing a default writer creates distinct native messages and header collections. Earlier identity/key/body survive the later call; editing one output's headers leaves the other output and both sources intact. |
| [Reader compatibility](When_the_default_kafka_reader_maps_a_native_delivery_should_preserve_content_telemetry_and_offsets.cs) | Independently constructed native data retains binary content, telemetry, handled count, delay, correlation/reply metadata and native offset. Existing CloudEvents-ID/content-type and header-key precedence remain; topic comes from the native result. Repeated reads return independent messages/bags without changing native headers/key/body. |

The [legacy profile builder](KafkaLegacyProfileHeaderBuilder.cs) is an application
header strategy, not a broker mock. No new request types, handlers or logging
closed generics are introduced. The cases use public exports only and need no
broker for their focused execution. They belong to the existing Kafka collection
and category. Mutable body buffers are not promised to be deep-cloned; callers
must retain the existing read-only ownership convention. The isolation test is
sequential overlapping-output coverage, not a concurrent stress test.

## Approved execution plan after review

First record the complete existing Kafka suite baseline on .NET 9 and .NET 10,
with the repository's Kafka and schema-registry services. Preserve unavailable
external-service cases explicitly; do not call a filtered run the full suite.
Use task-owned services and avoid unrelated containers or existing data.

Copy the four tests unchanged to `MessagingGateway` and the helper to `TestDoubles`.
The initial expected RED is missing adapter types. Implement the smallest adapter
extraction and production delegation, then run all four cases on both frameworks.
A compilation failure is not behavioral proof: after GREEN, apply each named
production mutation independently and select only its corresponding case on .NET 9.
Do not modify tests to make a mutation fail.

| Mutation | Selected test | Required assertion failure |
| --- | --- | --- |
| Default writer emits an empty key | Default writer compatibility | Native key equality (`partition-key`) |
| Default writer emits an empty body | Default writer compatibility | Exact binary body equality |
| Default writer ignores the supplied legacy header builder | Legacy header builder | Builder invocation count (`1`) |
| Default writer caches its first complete native output across calls | Writer output isolation | Distinct native message instances |
| Default writer creates fresh native messages but reuses its first headers collection | Writer output isolation | Distinct header collections |
| Default reader caches its first mapped Message across calls | Reader compatibility | Distinct mapped Message instances |
| Default reader clears TraceParent after mapping | Reader compatibility | Exact supplied traceparent equality |
| Default reader replaces the native offset with topic `wrong`, partition 0, offset 0 | Reader compatibility | Native TopicPartitionOffset equality |
| Default reader replaces the mapped body with an empty MessageBody | Reader compatibility | Exact binary body equality |

Require nine intended assertion failures, with exact case and assertion verified
in TRX. Restore production bytes after each mutation, rebuild normally, rerun all
four focused cases on both frameworks, and run both complete affected Kafka suites.
Compare exact baseline outcomes, expecting four additional passing cases. Build
all library targets and both test targets, check newly introduced warnings, source
and dependency scope, approved files, documentation links, whitespace, HEAD/stash,
and stop task-owned services. Preserve any real failure before investigating it.
No new package or Core implementation change is planned.

## Remaining scope

These are the first Kafka adapter tests, not completion of Kafka or #4170. The
subsequent [endpoint batch](../kafka-endpoint-wire-mapping/README.md) validates selected
mapping, non-CloudEvents dispatch trace, native acknowledgement offsets and successful
confirmation identity. Reader/writer failures, recovery, remaining delivery controls,
custom retry/rejection writers, transactions, batches and concurrency remain open.
RabbitMQ scheduler-backed retry and the other [global completion gates](../../tasks.md)
also remain open; moving to Kafka does not mark those paths complete.
