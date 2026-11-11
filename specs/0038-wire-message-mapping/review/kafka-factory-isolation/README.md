# Kafka publication and subscription strategy isolation

Status: approved and validated as characterisation. After two mechanical subscription
setup corrections, all eight cases pass initially and after restoration on both
frameworks. Eight mutations produce 32 intended assertion failures. Both full Kafka
suites pass 331 cases with no failures/skips, retaining all 323 prior outcomes.
No production changes were needed. All 1,282 production files, dependencies and
both reused helpers remain unchanged. No commit or push was made.

The [four publication cases](When_kafka_publications_select_different_writers_should_keep_mapping_and_confirmations_isolated.cs)
create a default and a custom publication through the same factory, in both creation
orders and both synchronous/asynchronous factory/send modes. Both producers exist
before either sends. The custom strategy must run exactly once. Each topic must
contain exactly one record with its own expected body, key and wire identity.
Native observations distinguish default binary serialization from the custom encoded
body. Both confirmations must carry original source IDs and the correct topic; both
source messages remain unchanged. Registry disposal drains callbacks before assertions.

The [four subscription cases](When_kafka_subscriptions_select_different_readers_should_keep_mapping_and_acknowledgement_isolated.cs)
create default and custom consumers through one factory, in both creation orders and
both API modes. They use different consumer groups on one unique topic, so each sees
the same native delivery independently. Both consumers exist before either receives.
Only the custom consumer may invoke the selected reader. Its chosen ID, topic, body,
trace and tenant must remain distinct from the default mapping. Both receive the
actual native offset, even though the custom reader supplies a deliberately wrong one.
Acknowledgements are buffered below the commit threshold, then disposal flushes them.
A native observer verifies the committed offset separately for each group.

These cases reuse the existing
[KafkaRecoverableWireWriter](../../../../tests/Paramore.Brighter.Kafka.Tests/TestDoubles/KafkaRecoverableWireWriter.cs)
with failure disabled and
[KafkaEndpointWireReader](../../../../tests/Paramore.Brighter.Kafka.Tests/TestDoubles/KafkaEndpointWireReader.cs)
in replacement mode. No mock, internal access, helper, handler, logger registration
or dependency is added. Topics/groups are unique and cleanup runs on assertion failure.
Native operations and receive loops are bounded. Concurrent calls and changes to
configuration after endpoint construction are separate contracts.

## Validation after approval

Copy both files unchanged into MessagingGateway. Preserve initial results on both
frameworks. If initially green, CHARACTERISE with the named mutations below. If a
real failure appears, preserve it and implement only this contract, then prove the
same mutations. Do not change approved assertions to obtain a passing run.

| Mutation | Cases | Intended assertion |
| --- | ---: | --- |
| Cache the last selected writer across publisher instances | 4 publication | Selected writer call count, line 104 |
| Ignore the publication writer | 4 publication | Selected writer call count, line 104 |
| Replace selected native bodies with original bodies | 4 publication | Custom native body, line 125 |
| Confirm wire identity instead of source identity | 4 publication | Source confirmation ID set, line 106 |
| Cache the last selected reader across consumers, including null/default selection | 4 subscription | Selected reader call count, line 113 |
| Ignore the subscription reader | 4 subscription | Selected reader call count, line 113 |
| Omit custom-reader native-offset normalization | 4 subscription | Custom message's native offset, line 120 |
| Omit acknowledgement offset storage | 4 subscription | Native committed offset, line 140 |

Require 32 intended assertion failures across eight mutations. Shared-writer mutation
must affect the writer used at send time, not just a field copied into each publisher
before the next producer is created. Each consumer's shared-reader mutation must see
the most recently created consumer's choice, including default/null. Retain actual
native delivery and callback paths. For the remaining mutations, retain strategy
invocations so their count does not mask the intended assertion. Compilation errors,
missing deliveries, callback timeouts and unrelated exceptions are not mutation proof.

Restore production bytes and normal optimized builds. Rerun focused cases on both
frameworks and both complete Kafka suites, expecting 331 passes each and retention of
all 323 preceding outcomes. Build all four library targets. Check source/dependency
manifests, helper and approved-file identity, warnings, documentation links, whitespace,
HEAD and stash. Preserve evidence and stop task-owned services. No commit or push.

Broker failure/cancellation, rejection forwarding, other malformed headers, retry
mapping, transactions/batches, concurrency and the [global #4170 gates](../../tasks.md)
remain incomplete.


## Execution evidence

The original subscription draft needed native `Confluent.Kafka.Acks` qualification
and an explicit message pump type matching its existing sync/async case. The first
build failed with CS0104; the first .NET 9 run then passed four publication cases
while four subscription cases failed in construction. Both setup-only corrections
are preserved with original source/build/TRX evidence. No assertion, expected value,
case data or assertion line changed. These setup failures are not mutation proof.

All eight named mutations subsequently failed at their intended assertions, four
cases each. Production bytes were restored before optimized builds and full suites.
All four library targets and both test targets build with no new warning diagnostics.
Evidence is under `/private/tmp/brighter-4170-kafka-factory-isolation/`.
The subsequent [sixteen custom-header cases](../kafka-custom-header-failures/README.md)
are implemented and validated: ten mutations produce 144 intended failures and both
complete suites pass 347 cases, with only KafkaMessageCreator changed.
