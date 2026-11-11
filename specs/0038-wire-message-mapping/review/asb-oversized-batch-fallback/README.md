# ASB oversized batch fallback construction — validated

Status: approved and validated. All six cases pass on .NET 9 and .NET 10, initially
and after mutation restoration. Seven mutations produce 38 intended assertion failures.
Both available suites pass 565 cases, zero failures/skips, retaining the exact 559
previous case/outcome pairs. No production changes were required. The compiled test
matches the approved review copy byte for byte.

## Contract

Six cases: queue/topic × oversized-first/middle/last. Negotiate
batch capacity through a real native SDK sender. Bound it to 16 KiB–16 MiB, then use
one binary body 1 KiB larger than that capacity and two distinct 64-byte bodies. Use
the existing custom AsbExternalWireWriter through the public producer registry.

Creation must return one AzureServiceBusSingleMessageBatch holding the original
oversized source ID and its exact mapped native bytes, ID, metadata and publication
TTL. Exactly three writer calls establish no remapping during failed packing or
fallback construction. Every source ID must occur exactly once across returned batches;
source bodies and selected metadata remain unchanged. Empty SDK batches must not be
returned. The normal messages must still be sent and independently observed with their
custom mapping, TTL and distinct identities, then settled using the native SDK.

When oversize is first, both small messages fit in the still-empty ordinary batch;
when oversize is in the middle, ordinary batches exist on both sides of the fallback.
When oversize is last, the final empty SDK batch must be filtered out. The assertions
cover all three packing positions. Batch enumeration is not
asserted to preserve original global source order: fallback construction can leave an
empty ordinary batch that later messages fill. This batch does not define ordering.

## Deliberate evidence boundary

The oversized fallback Content is inspected through its existing public API. Only the
normal batches are sent. This is native batch-capacity and fallback-construction
coverage, not successful oversized-message delivery or rejection/recovery coverage.
Do not count that remaining delivery requirement as complete. No mocks or test-only
sender wrappers; no new helpers, request types or handlers.

## Named characterisation mutations

1. Use the default publisher when constructing fallback: native fallback ID/body or metadata fails.
2. Use the mapped native ID as the fallback bookkeeping ID: original-ID assertion fails.
3. Drop publication TTL from fallback content: fallback TTL assertion fails.
4. Drop the fallback from returned batches: fallback existence or source-ID coverage fails.
5. Remap when falling back: exact writer invocation count fails.
6. Discard normal batches surrounding an oversized message: normal-batch count or source-ID coverage fails.
7. Return empty SDK batches: nonempty normal-batch assertion or expected count fails.

All seven mutations are verified at the intended assertions on .NET 9. The first six
produce six failures each; returning empty SDK batches produces two failures, only for
oversized-last cases. The other four cases pass under that mutation as expected.
Every mutation uses a fresh fixture and each case has a distinct queue/topic. All
production mutations are restored; source hashes match the batch baseline.

Both initial and restored six-case matrices pass on each framework. Final complete
available ASB suites pass 565 cases each with zero failures/skips; exact comparison
retains every preceding result. All library targets build. Fifty-two hosted classes
remain unexecuted; the eight DLQ-blocked recovery cases remain excluded and are not
counted as passed. No oversized fallback is sent by these tests.

Evidence: `/private/tmp/brighter-4170-asb-oversized-batch-fallback/`, including initial
and restored matrices, mutations.json, full regression TRXs, exact baseline comparisons
and source-verification.json. Dedicated fixture containers are stopped after validation.
Nothing committed or pushed.
