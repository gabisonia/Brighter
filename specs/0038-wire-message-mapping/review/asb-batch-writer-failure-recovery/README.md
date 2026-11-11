# ASB batch writer failure recovery — validated

Status: approved and implemented. Four native cases pass on .NET 9 and .NET 10,
initially and after mutation restoration. All six mutations are detected with 20
assertion failures, qualified below. Both available regression suites pass 571 cases,
zero failures/skips, retaining all 567 prior case/outcome pairs. Approved test and
helper remain byte-identical to their compiled copies.

## Approved contract

Four cases: queue/topic × writer exception/null. The first source maps successfully;
the second throws or returns null; the third must not be visited. Batch creation must
report InvalidOperationException, preserve the original writer exception when thrown,
and publish nothing. A null writer result is an invalid mapping result and must produce
a deliberate InvalidOperationException rather than an accidental NullReferenceException.
This null-result exception type is the approved contract in this batch.

The same producer then creates and sends a batch containing the previously unvisited
healthy third source. The configured writer must still run, and a native SDK receiver
must observe its custom bytes, identity, metadata and publication TTL. Source IDs,
routes, bodies, subject, correlation, content type and failure marker remain unchanged.
Exactly two calls occur before failure and one more for the healthy retry.

The native peek after failed creation can detect accidental publication of already-packed
messages, but immediate visibility is not guaranteed; the observed limitation is recorded
below. It does not establish outbox transactions or broker send-failure atomicity.
Dedicated isolated destinations prevent unrelated traffic from contaminating this check.
No helper is used to replace broker I/O: AsbRecoverableBatchWireWriter is a strategy
that delegates healthy mapping to the existing AsbExternalWireWriter. It has no request
mapper/handler or closed-generic logging registration requirements.

## Reproduced defect and correction

Initial RED passes both writer-exception cases and fails both null cases with
NullReferenceException instead of InvalidOperationException. CreateBatchesAsync now
checks the writer result before applying TTL and throws a deliberate
InvalidOperationException for null. The original writer exception still propagates.
Only AzureServiceBusMessageProducer changes, adding this guard and XML exception
documentation. No public signatures or dependencies change.

## Named mutations

1. Remove the null-result guard: null cases fail on exception type.
2. Catch writer failure and return the partial batch as success: expected exception fails.
3. Wrap a writer-thrown exception in a new InvalidOperationException: exception identity fails.
4. Map the healthy recovery source using the default writer: invocation count/native mapping fails.
5. Clear the failed source body after mapping failure: source-preservation assertion fails.
6. Publish the already-packed first source while handling mapping failure: native empty-queue peek fails.

## Validation evidence and limitation

The six mutations produce 2, 4, 2, 4, 4 and 4 failures respectively. For accidental
partial publication, three cases fail at the immediate empty-queue peek. One topic
exception case initially sees an empty peek and then receives the already-packed first
message where the healthy third message is expected; its native-ID assertion catches
the defect. That is a valid partial-publication detection, but not evidence that the
immediate peek alone reliably detects publication. The original report rejected the
unexpected assertion location; review retained the raw result and documented the
causal downstream assertion. Tests were not changed or weakened, and no repeat was
used to hide this timing limitation.

All production mutations are restored. Initial/restored four-case matrices pass on
both frameworks. Final full available suites pass 571 cases each on their first runs,
with zero failures/skips and exact prior case/outcome retention. All library targets
build. The previous session-lock instability remains unresolved despite passing here.
Eight DLQ-blocked cases and 52 unavailable hosted classes remain excluded.

Evidence: `/private/tmp/brighter-4170-asb-batch-writer-failure-recovery/`, including
initial RED, green/restored matrices, mutations.json, the original assertion-location
report, partial-publication-assertion-review.json, full-regression TRXs, exact baseline
comparisons and source-verification.json. Dedicated fixture containers are stopped
after validation. Cancellation, single-send writer failures and batch send failure
recovery remain outside this batch. Nothing committed or pushed.
