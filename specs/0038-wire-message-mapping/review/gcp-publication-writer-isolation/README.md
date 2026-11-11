# GCP publication writer isolation

Status: approved, adopted and validated with the equivalent assertion-style cleanup recorded below. The scoped isolation cases pass; the full net9.0 matrix retains a known ordered-nack failure.

Existing writer tests validate one publication at a time. They do not establish that
several publications created by one factory retain different custom writers and the
default mapping, independent of creation order. This is a remaining endpoint-isolation
requirement in the wire-mapping rollout.

Review [the six cases](When_gcp_publications_select_different_writers_should_keep_native_mapping_isolated.cs)
and [the mapping helper](GcpEndpointIsolationWireWriter.cs).

## Behavior

The matrix crosses sync, async and bulk sending with forward/reverse publication
creation order. One public producer factory receives three publications: custom A,
default, custom B. All three producers are created before sending. Each endpoint sends
two distinct binary messages. Individual sends alternate between the three producers;
bulk sends exercise the batch path independently for each producer.

Native subscribers observe each topic separately. Custom writers prepend different
binary markers and emit endpoint/application/source attributes. The default endpoint
must retain its original binary body and canonical native application ID, with no
custom mapping markers. Native byte assertions precede writer call-count/configuration
assertions so the named defects report actual mapping contamination. Native message
IDs and application IDs stay distinct; duplicate native deliveries are acknowledged
and deduplicated by native message ID before the bounded count check.

Each writer must be invoked exactly twice, publication writer selections must stay
unchanged, and every source message must retain its ID, topic, bytes and source-marker
bag. All native messages remain unordered in this batch. Ordering controls already
have separate coverage; this test does not claim concurrency, scheduler or rejection
writer isolation. The helper implements the public wire-writer strategy, not a broker
mock. No command, mapper or handler type is added, so no logger registrations are needed.

## Approved execution proposed

After review, adopt the test into MessagingGateway and the helper into TestDoubles,
retaining these review copies. Run six cases on .NET 9 and .NET 10 against fresh,
readiness-checked, isolated Pub/Sub emulators. Every native builder is EmulatorOnly;
no hosted project or ADC is used. Each case owns three unique topic/subscription
pairs and publishes six synthetic messages. Keep the ninety-second operation budget,
ten-second producer disposal and fifteen-second resource deletion bounds. Set process
hang limits and preserve setup/cleanup errors separately from behavioral failures.

If the unchanged baseline passes, apply these named production-only mutations on
.NET 10, restoring exact production bytes and requiring six passes after each:

| Mutation | Required RED |
| --- | --- |
| In GcpMessageProducer, replace configured writer selection with a new default writer for every publication | All six cases fail the native binary-body assertion for custom A |
| In GcpPubSubMessageProducerFactory's publication loop, assign every publication the first publication's WireMessageWriter before creating its producer | All six cases fail the native binary-body assertion: default endpoint in forward creation order, custom A in reverse order |

Expected total: twelve intended mutation failures. Compilation, setup, timeout,
missing-ID or cleanup failures do not count as the required RED. The approved test
and helper must not be weakened to obtain the expected result. If initial behavior
is RED, retain native evidence and implement only an established selection/isolation
correction; do not assume a product defect from fixture failure.

Finish with restored focused runs on both frameworks, all four GCP library targets
and both available Pub/Sub regression matrices. Preserve the preceding 471 case
identities/outcomes and six new identities (477 expected total with the same selection).
Record every known lifecycle failure and skip rather than retrying until green or
excluding it. Hosted comparisons do not erase the emulator restoration/regression
gate. Unavailable Firestore/Spanner/hosted and other release coverage remains explicit.
Clean up only batch-owned resources and containers. No dependency upgrade is proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". This concrete test/helper
was prepared through the test-first workflow and subsequently approved. Its scoped
execution is complete, with the retained regression failure reported below. Approval covers
adoption, native emulator runs, the two named mutations and exact restorations, any
evidence-supported minimum selection correction, affected regressions/library builds,
owned-resource cleanup and local reports. It excludes hosted calls, unrelated product
changes, deployment, commits and publication.


## Execution outcome

Six cases pass initially and finally on both frameworks. Both named mutations produce
all six intended native-body failures on .NET 10 (twelve total), and each exact source
restoration is followed by six passes. No production correction is made.

An equivalent Assert.Single(collection, predicate) overload removes xUnit2031 from
the new test. The original approval snapshot and assertion-style-only.patch are retained;
no predicate, assertion order, bound or data changes. The helper remains unchanged.
All 6,484 prior src/tests C# files remain byte-identical; only the test/helper are added.

Full available matrices: net9.0 421 passed / 1 failed / 55 skipped; net10.0 422 passed /
0 failed / 55 skipped. Both retain all 471 preceding identities plus six new passes.
The net9.0 failure is the known async ordered-nack missing-redelivery assertion at
line 158. Passing previously failing cases does not establish a correction. All four
library targets build with zero warnings/errors. The test dependency graph retains
repository warnings; neither new file has a warning in the final build.

All three owned emulators are removed. Evidence is in
`/private/tmp/brighter-4170-gcp-publication-writer-isolation/`; see
[validation](../../validation.md) for complete outcomes and limits. No hosted calls,
commit, deployment or publication. Broader production readiness remains open.

The next [concurrent same-producer writer batch](../gcp-concurrent-publication-writer/README.md)
is prepared for review, unbuilt/unrun and not adopted into the compiled suite.
