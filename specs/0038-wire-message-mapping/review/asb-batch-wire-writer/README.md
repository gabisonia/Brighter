# ASB batch wire writer — validated small-message selection

Status: approved and implemented. All six native cases pass on .NET 9 and .NET 10,
initially and after mutation restoration. All seven named mutations are verified
with 26 intended assertion failures. Both available regression suites pass 557 cases,
zero failures/skips, retaining the exact previous 551 case/outcome multisets.
The approved test matches the compiled copy byte for byte.

## Contract

Batch creation through the public producer registry and IAmABulkMessageProducerAsync
must honor the publication writer exactly once per source message. Native SDK receivers
independently verify binary payload and metadata. Publication TTL overrides writer TTL.
Batch IDs retain original Brighter IDs even when the custom writer replaces native IDs;
source identity, route, payload and selected metadata remain unchanged.

Six cases: queue/topic × implicit default/explicit default/custom writer, with three
small distinct messages per case. Reuse the existing approved AsbExternalWireWriter;
no new helpers, request types or handlers. Use actual Service Bus, no mocks. The
observer uses the normal entity, so these cases do not depend on the failing DLQ path.
This does not guarantee the emulator will support them; report infrastructure failures
separately. Each case should run against an isolated destination.

## Original source finding

AzureServiceBusMessageProducer.CreateBatchesAsync delegates to
AzureServiceBusMessageBatches without the publication writer. Both
AzureServiceBusMessageBatch.TryAddMessage and AzureServiceBusSingleMessageBatch.CreateBatch
call the static default publisher. Individual SendWithDelayAsync already selects the
publication writer. Initial RED confirmed both custom cases failed at the expected
writer-count assertion (expected three, actual zero); all four default cases passed.

## Verified named mutations

1. Ignore the configured batch writer: custom invocation count/native mapping fail.
2. Replace explicit-default mapping with an empty native message: default contents fail.
3. Drop publication TTL override: native TTL fails (custom writer requests one minute).
4. Record mapped native IDs instead of source IDs: batch ID assertion fails.
5. Map every item from the first source: unique native identity/body assertions fail.
6. Invoke the custom writer twice per source: exact invocation count fails.
7. Mutate the original source body or subject during mapping: source assertions fail.

Mutation failure counts, in the order above: 2, 2, 6, 2, 6, 2, 6. Each run used
fresh fixtures with separate destinations for each case. The report script initially
rejected mutation 5 because Assert.True returned its custom message instead of an
Assert-prefixed message. Inspection confirmed all six original stacks point to the
intended uniqueness assertion at line 110. Only the report parser changed; the test
was not changed or rerun to manufacture a failure. All production mutations are restored.

Four production files now map once before packing and pass the native message through
the existing batch helpers. Publication TTL is applied after mapping. Public helper
signatures retain default mapping and batch bookkeeping retains original IDs. The
fallback reuses the same mapped native message; dedicated native oversize/splitting
coverage remains outside this batch. No dependencies or public configuration API changed.
All library targets build. Final build logs are incremental and do not erase previously
recorded test warnings.

Evidence: `/private/tmp/brighter-4170-asb-batch-wire-writer/`, including original RED,
initial/restored matrices, seven mutation results, both regression TRXs, exact baseline
verification and source-scope verification. Fifty-two hosted classes remain unexecuted;
the eight recovery cases blocked by the DLQ emulator fault are excluded explicitly.
The two dedicated batch containers are stopped after verification. Nothing committed
or pushed.

## Boundaries

Small-message batch selection only. Batch splitting, oversize single-message fallback,
partial failures, cancellation, confirmation callbacks and sessions need separate
coverage. Preserve the public batch helper defaults when implementing selection.
No commit or push is authorized by this review.
