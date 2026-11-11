# Kafka invalid-message writer selection

Status: approved, implemented and validated. All four cases reproduce ignored
writer selection on both frameworks after the configuration property alone is added.
All four pass after wiring, and again after restoration. Nine mutations produce
34 intended failures, with the two expected composition passes retained. Both full
Kafka suites pass 351 cases with no failures/skips, retaining all 347 prior outcomes.
Only KafkaSubscription, KafkaMessageConsumerFactory and KafkaMessageConsumer change.
Both approved files are unchanged. No commit or push was made.

The [test](When_rejecting_with_a_selected_kafka_writer_should_map_the_invalid_delivery_and_acknowledge_the_source.cs)
combines synchronous/asynchronous consumer factories and rejection with full
replacement/default-composed writers. It uses the nullable subscription property
`InvalidMessageWireMessageWriter` using the existing Kafka wire-message contract.
Omitted configuration must preserve the current default mapping and public constructors.
The factory must capture this selection per consumer. These cases cover an explicit
invalid-message route and `Unacceptable` reason; dead-letter routing/fallback,
configuration isolation, mapping failure/recovery and cancellation remain separate
required work.

The [helper](KafkaInvalidWireWriter.cs) creates fresh native messages and maps body,
key, wire identity, source identity, original topic, rejection reason and tenant.
Composition preserves the default binary body; replacement encodes it. Rejection
metadata must be available before the writer runs. Neither mode may mutate the
source identity/body/key/count. The gateway must restore the source topic after
forwarding. It owns the invalid destination and source acknowledgement offset.

A real single-partition source topic and invalid topic are unique per case. Native
Kafka observation verifies exactly one forwarded record and its selected content.
The source group buffers its acknowledgement below the batch threshold; disposal
flushes it. A native observer verifies the committed source offset independently of
the selected wire ID. Receive loops and native operations are bounded, with cleanup
on assertion failure. No mock, handler, logger registration or dependency is added.

## Validation after approval

Copy both files unchanged into the test project. The missing property is an expected
initial compile failure, not behavioral RED proof. Implement only the approved
contract, then require all four cases to pass on both frameworks and prove these
named production mutations without changing assertions:

| Mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Ignore the selected invalid-message writer | 4 | Equal call count, line 114 |
| Replace selected native body with source bytes, retaining writer invocation | 2 replacement; 2 composition pass | Equal body, line 126 |
| Replace selected native key with source key | 4 | Equal key, line 124 |
| Replace selected wire identity with source identity | 4 | Equal wire ID, line 125 |
| Clear original-topic metadata before mapping | 4 | Equal external original topic, line 128 |
| Clear rejection reason before mapping | 4 | Equal external reason, line 129 |
| Clear tenant before mapping | 4 | Equal external tenant, line 130 |
| Leave the source topic changed after forwarding | 4 | Equal source topic, line 131 |
| Omit rejection acknowledgement storage | 4 | Equal committed source offset, line 137 |

Require 34 intended assertion failures across nine mutations. Mutations must preserve
native delivery, selected-writer invocation where specified and bounded completion.
Compile errors, missing native records or unrelated timeouts do not count as proof.

Restore production bytes, normal optimized builds and all four focused cases on
both frameworks. Build all four library targets and both test targets; run both full
Kafka suites, expecting 351 passes each and retention of all 347 previous outcomes.
Verify approved-file identity, source/dependency scope, warnings, documentation links,
whitespace, HEAD and stash. Preserve evidence and stop task-owned services. No commit
or push. These cases do not establish Kafka or global #4170 production readiness.


## Implementation and evidence

The factory captures the writer through an internal init-only consumer property.
Only the lazy invalid-channel producer receives it; dead-letter and retry producers
retain their default writer selection. Public consumer constructors are unchanged.
Reject/RejectAsync enrich metadata before mapping, retain the original offset for
acknowledgement and restore the source topic in finally. The topic-restoration fix
also applies to default rejection; default wire serialization remains unchanged.

The expected missing-property compile error is preserved separately and is not
counted as behavioral RED. Adding only the property allowed all four cases to fail
at Equal call count, line 114, on both frameworks before the factory/consumer wiring.
No assertion or setup correction was needed. All nine mutations then reached their
intended assertions on .NET 9; source-body replacement leaves the two composition
cases passing because they intentionally retain the original bytes.

Evidence is under `/private/tmp/brighter-4170-kafka-invalid-writer/`. Optimized builds
cover all four library targets and both test targets with no new warnings. The
other 1,279 production files and 268 dependency entries are unchanged. All temporary
publisher/consumer/factory mutations are restored. The next
[four failure/recovery cases](../kafka-invalid-writer-recovery/README.md) are now
implemented and validated: forwarding exceptions return false and restore the source
offset without acknowledgement. Eight mutations produce 32 intended failures and
both full suites pass 355 cases. This selection batch itself validates successful
forwarding only. The [subsequent broker-failure batch](../kafka-invalid-broker-recovery/README.md) now verifies negative delivery confirmations and recovery; later-offset and automatic pump recovery remain open. Kafka and global production readiness remain open.
