# ASB publication writer isolation — validated

Status: approved and validated. All six native cases pass on .NET 9 and .NET 10,
initially and after mutation restoration. Six mutations produce 36 intended
assertion failures. Both full available suites pass 585 cases with zero failures
or skips, retaining every one of the 579 previous case/outcome pairs.

## Approved contract

One registry holds a queue publication and a topic publication. One selects the
existing AsbExternalWireWriter; the other leaves its writer unset. Six cases cover
both placements of the custom writer and sync, async and bulk sends. Publications
have distinct five-minute and seven-minute TTLs.

Each case sends queue, topic, queue, topic, receiving and completing each message
through its native SDK receiver. This exercises returning to each producer after
using the other publication. Native identity, bytes, subject, correlation, content
type, metadata shape and TTL must follow that publication. The custom writer runs
exactly twice, only for its own messages. Bulk batches retain original source IDs.
Source IDs, routing, bodies and selected metadata remain unchanged.

Reuse the approved AsbExternalWireWriter unchanged. No new helper, handler or
logging registration is needed. Use isolated queue/topic destinations to prevent
unrelated traffic from contaminating native observations. Sync operations rely on
the external runner timeout because their public interface has no cancellation token.

This characterizes existing behavior; no production correction was required. It covers sequential cross-publication isolation, not concurrency or
two different custom writer implementations. Each bulk operation contains one source;
packing and splitting retain their separate existing tests.

## Named production mutations

1. Factory assigns the first publication's writer to both publications: one
   endpoint's custom invocation count or native mapping fails.
2. Factory assigns the last publication's writer to both publications: the
   corresponding mapping/invocation assertion fails in the opposite selection.
3. Factory assigns the first publication's TTL to both: topic native TTL fails.
4. Producer ignores the configured writer for both individual and batch creation:
   custom invocation count/native mapping fails.
5. Producer invokes the selected custom writer twice during individual or batch
   mapping: invocation count fails.
6. Producer applies the default writer's CloudEvents metadata to the custom native
   result during individual and batch mapping: metadata-shape assertion fails.

## Validation evidence

All six mutations fail all six cases at their intended assertions: writer invocation
counts, native TTL or CloudEvents metadata absence. All mutations are restored.
All 1,171 non-generated production C# files match the pre-batch hashes. The approved
test matches its compiled copy byte-for-byte, and the reused helper is unchanged.
All library targets build. Both final full suites pass 585 cases on their first runs,
zero failures/skips, with exact retention of all 579 previous outcomes.

Evidence: `/private/tmp/brighter-4170-asb-publication-isolation/` contains initial and
restored matrices, mutations.json, full regression TRXs, exact baseline comparisons
and source-verification.json. Dedicated fixture containers are stopped after validation.
Nothing committed or pushed.

The session-lock instability, eight DLQ-blocked cases, 52 unavailable hosted classes,
scheduled sends, cancellation, reader isolation and broader release gates remain
open. No commits or pushes are requested.
