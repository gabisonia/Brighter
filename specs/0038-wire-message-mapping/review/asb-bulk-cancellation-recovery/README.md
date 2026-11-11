# ASB bulk cancellation recovery — validated

Status: approved, implemented and validated. Initial runs on both frameworks reproduce
two creation failures while both submission cases pass. All four cases pass after the
minimal guard and after mutation restoration. Six mutations produce 16 intended
assertion failures. Both full available suites pass 602 cases, zero failures/skips,
retaining all 598 prior case/outcome pairs.

## Approved contract

Queue/topic × batch creation/submission cancellation. Reuse the approved
AsbCancellingWireWriter and its AsbExternalWireWriter dependency unchanged. Native
SDK receivers observe actual recovery deliveries; no broker I/O is mocked.

Creation cases provide three small sources. The first maps and enters a native batch;
the second maps and cancels the caller token; the third must remain unvisited. Creation
must propagate an OperationCanceledException-compatible exception, not return partial
success. Exactly two writer calls must have occurred. The same producer then creates
and sends a fresh batch from the third source with an active token, for three total
mapping calls. Initial RED confirms that the packing path misses this cancellation when the
second message fits the already-created batch: creation returns without an exception.
The producer now checks the token immediately after mapping, before applying TTL
and adding the native message. The third source remains unvisited.

Submission cases first create one batch containing two sources using an active token,
then cancel a separate caller token before SendAsync. Submission must propagate an
OperationCanceledException-compatible exception. The exact same batch and producer
must then succeed with the active token, without remapping: two total writer calls.
These cases cover cancellation before submission, not cancellation during broker I/O.

Both paths preserve original batch IDs, per-source custom native IDs/bytes/metadata,
publication TTL and source state. Native observation accepts either recovery delivery
order and requires distinct expected IDs. The creation recovery has one message;
submission recovery has two. It does not claim absence of further delayed duplicates.

Preserve the existing bulk exception contract: cancellation propagates directly,
unlike the individual-send ChannelFailureException wrapper. Accept SDK-derived
TaskCanceledException as an OperationCanceledException. The caller token must be
cancelled while the 90-second test timeout remains active.

An immediate empty peek after cancellation is best-effort only and is not proof of
publication atomicity. Isolated destinations prevent unrelated traffic. Each observed
native delivery is completed in a finally block. No new helper, handler or logging
registration is introduced.

## Named production mutations

1. Remove the cancellation check needed after mapping into an already-created batch:
   both creation cases fail the expected cancellation assertion.
2. Pass CancellationToken.None for native batch submission: both submission cases
   fail the expected cancellation assertion.
3. Catch cancellation and return partial batches/success for the corresponding
   creation/submission path: all four expected-exception assertions fail.
4. Wrap cancellation in ChannelFailureException in both bulk paths: all four direct
   cancellation-type assertions fail.
5. Clear the configured writer after cancelled batch creation: both creation cases
   fail recovery invocation count or custom native mapping.
6. Clear the second source body while reporting cancelled creation: both creation
   cases fail source preservation after successful healthy recovery.

## Validation evidence

Both frameworks initially pass the two submission cases and fail both creation cases
with no exception thrown. The minimum correction is ThrowIfCancellationRequested
immediately after mapping, plus XML cancellation documentation on CreateBatchesAsync.
No signatures or dependencies change; no stronger pre-entry or in-flight guarantee
is introduced.

The six mutations produce 2, 2, 4, 4, 2 and 2 intended failures respectively. All are
restored. The approved test matches its compiled copy byte-for-byte; both reused
helpers remain unchanged. Only the producer changes among 1,171 non-generated
production C# files. All library targets build. Both full available suites pass 602
cases on their first runs, zero failures/skips, retaining all 598 previous outcomes.
The best-effort peek limitation remains. Previous session-lock instability remains
unresolved despite passing here.

Evidence: `/private/tmp/brighter-4170-asb-bulk-cancellation/` contains initial RED,
green/restored matrices, mutations.json, full regression TRXs, exact baseline
comparisons and source verification. Dedicated fixture containers are stopped after
validation. Nothing committed or pushed.

Pre-entry cancellation, cancellation during broker I/O, oversized fallback batches,
consumer cancellation and scheduled-message cancellation remain separate. Session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader release
gates stay open. No commits or pushes requested.
