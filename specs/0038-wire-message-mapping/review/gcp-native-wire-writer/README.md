# GCP native publication writer selection

Status: implemented and validated on net9.0 and net10.0 using a native Pub/Sub
emulator. This closes the immediate publication writer-selection batch, not GCP
or feature-wide production readiness.

## Contract and implementation

GcpPublication.WireMessageWriter selects an IAmAWireMessageWriter<PubsubMessage>.
A null selection uses DefaultGcpPubSubWireMessageWriter, which delegates to the
existing parser. Individual and bulk sends share the selected mapping path.
The gateway then sets OrderingKey from the source partition key when the publisher
client enables ordering, or clears it otherwise. Custom data and attributes remain
under the writer's control.

Six cases use the public producer factory: sync, async and bulk, each with ordering
enabled and disabled. Each sends two distinct binary messages to a unique topic
and a native subscription created before publication. The native observer checks
custom data, exact attributes and ordering without using a Brighter reader.
The tests also check one writer call per source and preservation of source fields.
The sync case uses factory.Create; the other cases use CreateAsync.

The test is adopted under MessagingGateway and GcpNativeProbeWireWriter under
TestDoubles in Paramore.Brighter.Gcp.Tests. No generated pump or logging registration
is needed. All native clients use EmulatorOnly and require PUBSUB_EMULATOR_HOST.

## Validation

| Check | net9.0 | net10.0 |
| --- | --- | --- |
| Ignored writer, before integration | 6 intended failures | 6 intended failures |
| Initial implementation | 6 passes | 6 passes |
| Restored implementation | 6 passes | 6 passes |
| Final emulator CI marker | 6 passes | 6 passes |
| Combined selected regressions | 228 passes, 30 existing skips | 228 passes, 30 existing skips |

The pre-change emulator CI selection passes 175 cases with 30 existing skips on
each framework. Another 47 factory/configuration cases pass on each framework.
The final comparison retains all 252 prior outcomes and adds exactly six passes.

The main regression runs pass 220 cases with 30 skips. An exact comparison catches
eight omitted streaming purge cases caused by the initial namespace exclusion.
Those eight cases pass in separate runs on each framework. The final totals above
combine the disjoint runs; they do not represent a single 228-pass invocation.
Both original and corrected filters and the initial comparison failure are retained.

All six named production mutations run on net10.0, producing 30 intended failures:

| Mutation | Failures | Intended assertion |
| --- | --- | --- |
| Bypass configured writer for individual sends | 4 | Writer call count |
| Bypass configured writer for bulk sends | 2 | Writer call count |
| Replace custom data with source data | 6 | Native binary body |
| Add an unwanted default attribute | 6 | Exact attribute count |
| Preserve the writer ordering key | 6 | Ordering key when enabled; send error when disabled |
| Invoke configured writer twice per source | 6 | Writer call count |

The unaffected cases in the two bypass runs pass. Case identities and assertion
locations were checked, and every mutation was restored byte for byte.

## Build and CI corrections

The first build lacked cached Google SDK assemblies. Restoring the existing package
graph resolved that issue without a dependency edit. The first adopted-test build
then found two references to MessageHeader.Id; both were corrected to the existing
MessageHeader.MessageId member. These compile failures do not count as RED.
With only the publication property added, all six cases then fail at the writer-call
assertion: expected two calls, actual zero.

The final test carries Requires=PubSubEmulator. The hosted GCP job already excludes
that trait, and the emulator job now includes GcpNativeWireWriter. Both frameworks
pass all six marked cases; the hosted exclusion selects none. The two member-name
corrections and CI trait are the only differences from the reviewed test/helper.
The workflow delta is two lines and preserves pre-existing workflow edits.

## Evidence and limits

Evidence: `/private/tmp/brighter-4170-gcp-writer/`, including TRX files, build logs,
named mutations, original drafts, correction diffs, filters, exact outcome comparisons
and source hashes. Final workspace builds pass for both frameworks. Only two existing
production files change, with one default adapter, one test and one helper added.
The other 6,458 pre-existing production/test C# files remain unchanged in this batch.

The isolated emulator uses image ID
`sha256:3797a874fae6b75aa25260b5bd40fa0397c2f1102f18ae3ac7dc887732fbe449`.
It publishes no host ports. Separate build outputs preserve baseline binaries during
execution. Each case cleans up its own topic/subscription; the isolated container is
removed after validation. Nothing is committed or pushed.

The selected regressions cover MessagingGateway cases except the Stream and
StreamOrdering conformance namespaces, while retaining all GcpPubSubPurge cases.
Cases marked Fragile=CI remain excluded. This validates some streaming purge behavior,
not full streaming conformance. Firestore, Spanner, Cloud Storage and hosted-only
suites were not executed; the GCP test project as a whole has not passed here.

Reader selection, thrown/null writers, endpoint isolation, concurrent mapping,
callback-enabled ordering, scheduler return paths and broader release gates remain
open. The ASB baseline remains 628 per framework and is separate from these GCP results.
