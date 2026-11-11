# ASB native batch splitting — validated

Status: approved and validated. Both native cases pass on .NET 9 and .NET 10,
initially and after mutation restoration. Six mutations produce 12 intended assertion
failures. Both available suites pass 559 cases, zero failures/skips, retaining exactly
all 557 previous case/outcome pairs. Production is unchanged from the preceding batch;
the compiled test matches the approved review copy byte for byte.

## Contract and native setup

Two cases, queue and topic, use the existing custom AsbExternalWireWriter through the
public registry and IAmABulkMessageProducerAsync. A native sender negotiates batch
capacity without sending probe messages. Each of three distinct binary bodies uses
60% of that capacity, so every item fits alone but two cannot fit together. A bounded
16 KiB–16 MiB capacity precondition avoids unbounded allocations and reports an
unsupported environment explicitly rather than silently skipping the test.

Expect three nonempty native batches with one source ID each, correct destination,
and native size within the SDK limit. Exactly three writer calls prove that retrying
an item in a new batch reuses its mapped native message. Native receivers independently
verify unique mapped IDs, distinct binary bodies, custom metadata, publication TTL,
and absence of default CloudEvents metadata. Original source IDs, bodies, routes,
subjects, correlation IDs and content types remain unchanged.

Each case uses an isolated preconfigured destination. No mocks, new helpers, request
mappers or handlers. The existing approved writer is reused unchanged. The observer
settles normal queue/subscription deliveries and does not access a DLQ.

## Named characterisation mutations

The preceding batch-selection fix may already satisfy these cases. Prove that the
tests catch realistic production defects before marking the task complete:

1. Remap a source when retrying it in a new batch: writer count exceeds three.
2. Convert a retry with the default publisher: native identity/body or metadata fails.
3. Omit adding the full previous batch to the returned batch list: batch count/source IDs fail.
4. Lose the source ID when adding to a subsequent batch: ID sequence fails.
5. Reuse the first mapped message for all items: unique native identity assertion fails.
6. Drop publication TTL before adding to a new batch: native TTL assertion fails.

Each mutation must fail on its named behavior, then be restored. Do not edit the tests
to fit a mutation. If the cases fail because of broker size restrictions rather than
a production defect, retain that evidence and resolve the fixture/contract explicitly.

## Execution results

Initial and restored runs pass both cases on both frameworks. All library targets
build. Each of the six mutations fails both cases at the intended assertion, with
fresh broker fixtures between mutations. Exact final regression comparison retains
all 557 prior cases and adds only these two passing cases on each framework.

The first default-on-retry mutation expanded invalid binary bytes through default
text conversion and failed at batch type, before the intended native metadata assertion.
That run is retained but excluded from clean mutation evidence. The repeat preserves
native body bytes while applying default metadata, and both cases then fail at native
ID lookup. The first ID-loss mutation failed compilation due to an obsolete property;
its corrected temporary message construction produces the intended ID assertion
failures. Neither correction changed the approved tests. All production mutations are
restored and all production source hashes match the batch baseline.

The eight blocked reader-recovery cases and 52 unavailable hosted classes remain
excluded; none is counted as passed. Evidence:
`/private/tmp/brighter-4170-asb-batch-wire-splitting/`, including original/repeat mutation
artifacts, mutations.json, initial/restored TRXs, final regression TRXs, exact baseline
comparisons and source-verification.json. Dedicated fixture containers are stopped
after validation. No changes committed or pushed.

## Remaining boundary

This validates splitting of individually acceptable native messages only. It does not
prove oversized single-message fallback, sessions, cancellation, failure recovery,
concurrent reuse, or confirmation callbacks. In particular, do not infer that a message
rejected by a native batch can be sent successfully as an individual message in this
environment. That requires a separate broker-supported size setup and test batch.
No commit or push is included.
