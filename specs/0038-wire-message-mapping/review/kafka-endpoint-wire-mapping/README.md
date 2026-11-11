# Kafka endpoint wire mapping

Status: approved, implemented and validated. All eight cases pass on .NET 9 and
.NET 10 initially after setup correction and after mutation restoration. Eight
mutations produce 32 intended assertion failures. Both complete Kafka suites pass
277 cases with no failures/skips, retaining all 269 baseline outcomes. Six Kafka
production files change; dependencies and other production files are unchanged.
Original RED and setup failures are preserved. No commit or push was made.

## Contract

Add nullable `KafkaPublication.WireMessageWriter` of type
`IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>`, and nullable
`KafkaSubscription.WireMessageReader` of type
`IAmAWireMessageReader<ConsumeResult<string, byte[]>>`. Factories capture the configured
selection when creating endpoints. Omission preserves default mapping and the
existing publication header builder. An explicitly selected writer owns native
key/body/header mapping and may compose `DefaultKafkaWireMessageWriter` itself.
No inverse mapping is inferred from a reader.

The gateway owns the publication topic, publication confirmation identity and
native consumed offset used by acknowledgement. A custom writer can choose wire
identity independently from the source message. Both successful and unsuccessful
confirmations must identify the source send, even if its wire MessageId is changed
or absent. Existing behavior without a selected writer and existing legacy header
builder users require regression review; do not silently weaken their tests.
The first endpoint batch directly checks successful confirmation identity; failure
outcomes and outbox recovery remain follow-up coverage.

A selected reader may map identity, application topic, body and telemetry. The
gateway must replace any supplied TopicPartitionOffset with the real consumed
position before returning the message. Acknowledging it must commit the native
position plus one even when its mapped application topic differs.

## Cases

| Test | Cases | Evidence |
| --- | ---: | --- |
| [Configured publication writer](When_a_kafka_publication_selects_a_wire_writer_should_preserve_mapping_trace_and_confirmation_identity.cs) | 4 | Sync/async send × replacement/composition. Real Kafka receives the selected key, binary/custom body and headers on the configured topic. A fresh dispatch trace reaches a custom telemetry header; the stale source trace does not survive. Confirmation identifies the original message although the wire ID differs. Source identity/topic/key/body/bag survive. |
| [Configured subscription reader](When_a_kafka_subscription_selects_a_wire_reader_should_preserve_mapping_and_acknowledge_the_native_offset.cs) | 4 | Sync/async factory × replacement/composition. Native publication reaches the selected reader. Application identity/topic/body/telemetry remain mapped, while the deliberately wrong offset is replaced. A separate native consumer queries the committed group offset after acknowledgement. |

The [writer strategy](KafkaEndpointWireWriter.cs) replaces the native key and MessageId
in both modes. Composition retains default CloudEvents identity but moves traceparent
to an external header. Replacement creates exactly three headers. The
[reader strategy](KafkaEndpointWireReader.cs) supplies a wrong-topic, partition-7,
offset-999 position in both modes, so incorrect settlement cannot pass by coincidence.
Composition retains native identity; replacement selects an external identity.

Each case owns a unique one-partition topic and group, uses the public factories,
and deletes its topic in finally after disposing endpoints. Native admin/publisher/
consumer operations use the existing SDK. No mocks, reflection, handler types,
logging registrations or new packages are needed. Receive and confirmation waits
are bounded. Because acknowledgement schedules commit work in the background, the
native observer polls the committed offset for up to ten seconds (each broker query
has a two-second timeout), retaining exact offset equality as the final assertion.
The omit-acknowledgement mutation must fail that final assertion after the bounded
observation. The tests do not promise arbitrary custom strategy thread safety or
Kafka broker-outage atomicity.

## Validation after approval

Copy the two tests and two strategies unchanged into `MessagingGateway` and
`TestDoubles`. Preserve initial missing-property compile evidence, then implement
the minimum factory/publication/subscription/producer/consumer wiring and native
controls. Run all eight cases on .NET 9 and .NET 10. Do not accept compile failure
as the only RED proof. Apply the following named production mutations separately
on .NET 9, selecting the indicated test group.

| Mutation | Cases | Intended assertion |
| --- | ---: | --- |
| Ignore the publication writer | 4 writer | Writer invocation count |
| Run selected mapping before dispatch trace propagation | 4 writer | Native external-parent equals current span ID |
| Identify confirmation from mapped wire MessageId instead of source send | 4 writer | Confirmation MessageId equals source-id |
| Publish an empty mapped body | 4 writer | Native payload equality |
| Ignore the subscription reader in factory/consumer mapping | 4 reader | Reader invocation count |
| Omit gateway replacement of custom reader TopicPartitionOffset | 4 reader | Returned native offset equality |
| Clear mapped TraceParent after selected reading | 4 reader | Mapped external traceparent equality |
| Skip acknowledgement offset storage/commit | 4 reader | Native committed offset equals consumed offset plus one |

Require 32 intended failures across eight mutations, verified by exact case and
assertion in TRX. For the late-trace mutation, move mapping ahead of propagation
without introducing a second mapping call; compile errors or invocation-count
failures do not establish trace timing. Restore all production bytes, rebuild
normally, rerun eight focused cases on both frameworks, and run both complete
Kafka suites, retaining all prior outcomes by exact name and adding eight passes.
Validate all supported library/test targets, new warnings, dependency/source scope,
approved files, documentation, HEAD/stash and whitespace. Stop task-owned services.

Custom failure handling, factory isolation after configuration mutation, retries,
rejection paths, pending outbox recovery, transactional/batch settings and concurrency
remain separate. This is not a claim that Kafka or #4170 is complete. RabbitMQ
scheduler-backed retry and all other [completion gates](../../tasks.md) remain open.
Nothing is committed or pushed by this batch.


## Execution corrections and evidence

The first RED build also found a missing OpenTelemetry.Trace import and an ambiguous
Acks enum. The compiled tests add the import and qualify Confluent.Kafka.Acks.All;
assertions are unchanged. The next RED build fails only on the two missing mapping
properties on both test targets. After implementation, all four writer cases pass
on .NET 9, but the reader cases fail at subscription construction because the draft
omits the required pump type. The compiled reader fixture explicitly chooses Reactor
for sync and Proactor for async, matching the approved case parameter. All eight
cases then pass on both frameworks. Reviewed originals, pre-correction copies and
original failure evidence remain preserved.

Seven mutations run successfully before the acknowledgement mutation selector stops:
its source fragment matches three methods. No mutation is applied at that stop.
Narrowing the selector to Acknowledge completes the remaining four intended failures.
All eight mutations and production restoration are verified by exact test and
assertion. Full evidence is under
`/private/tmp/brighter-4170-kafka-endpoint-wire-mapping/`.

The subsequent [reader-failure batch](../kafka-reader-failures/README.md) implements
configured throw/null recovery, with eight cases on both frameworks and 72 intended
mutation failures. Both full suites pass 285 cases. Default-parser preservation and
the wider Kafka and #4170 scope remain open.
