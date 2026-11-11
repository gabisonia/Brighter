# ASB send cancellation recovery — validated

Status: approved and validated. All four native cases pass on .NET 9 and .NET 10,
initially and after mutation restoration. Six mutations produce 24 intended assertion
failures. Both full available suites pass 598 cases, zero failures/skips, retaining
all 594 prior case/outcome pairs. No production correction was needed.

## Approved contract

Queue/topic × immediate/scheduled asynchronous sends use a configured writer that
first creates a valid native message, then cancels the caller's token for the marked
source before returning. This deterministically exercises cancellation between mapping
and native submission, without broker mocks, timing races or network interruption.
The helper delegates mapping to the approved AsbExternalWireWriter unchanged.

Preserve the producer's existing ChannelFailureException wrapper. Its InnerException
must be assignable to OperationCanceledException; either that type or an SDK-derived
TaskCanceledException is acceptable. The caller token must be cancelled while the
independent test timeout remains active. Exactly one mapping occurred before failure.
The test does not require skipping mapping when a token was cancelled before entry.

The same producer then sends an unmarked source through the same immediate/scheduled
path using the active timeout token. A native SDK receiver observes custom ID, bytes,
metadata and publication TTL. The total mapping count is two, and both original
sources retain their identities, routes, bodies, selected metadata and failure marker.
Scheduled recovery uses a five-second delay, whose detailed timing is covered by the
separate scheduled-writer tests.

An immediate empty peek after cancellation is best-effort evidence only: it cannot
prove absence of a delayed publication. The healthy message's native identity provides
an additional observation. Dedicated isolated destinations prevent unrelated traffic.
This is pre-submission cancellation, not a guarantee that cancelling an already
accepted broker operation retracts its message or resolves ambiguous send outcomes.

The helper is a mapping strategy with a caller-token cancellation callback; it does
not substitute for native I/O. No new handler or logging registration is introduced.
A 90-second test timeout bounds async work. The native delivery is completed in a
finally block. Sync sends have no caller-token parameter and are outside this batch.

## Named production mutations

1. Pass CancellationToken.None to both immediate and scheduled native calls:
   the expected cancellation exception is missing.
2. Swallow OperationCanceledException and report successful submission:
   expected outer exception fails.
3. Replace the cancellation inner exception with InvalidOperationException:
   inner cancellation-type assertion fails.
4. Clear the selected writer after cancellation: healthy recovery invocation count
   or custom native mapping fails.
5. Invoke the configured writer twice before native submission: the mapping count
   immediately after failure is two rather than one.
6. Clear the cancelled source body during error handling: source-preservation
   assertion fails after healthy recovery.

## Validation evidence

All six mutations produce four failures each at the intended assertions. Ignoring
or swallowing cancellation fails the required outer-exception assertion; replacing
its cause fails the inner cancellation-type assertion. Writer loss, duplicate mapping
and source-body mutation fail their respective count/preservation assertions.

All mutations are restored. All 1,171 non-generated production C# files match their
pre-batch hashes. Approved test/helper match their compiled copies byte-for-byte;
the reused external writer is unchanged. All library targets build. Both full
available suites pass 598 cases on their first runs, zero failures/skips, retaining
exactly all 594 prior outcomes. The best-effort peek limitation above remains; this
batch does not establish broker-side atomicity for in-flight cancellation.
Previous session-lock instability remains unresolved despite passing here.

Evidence: `/private/tmp/brighter-4170-asb-send-cancellation/` contains initial/restored
matrices, mutations.json, full regression TRXs, exact baseline comparisons and source
verification. Dedicated fixture containers are stopped after validation. Nothing
committed or pushed.

Cancellation during broker I/O, bulk operations, consumer receives/settlement and
scheduled-message cancellation remain separate. Session-lock instability, eight
DLQ-blocked cases, 52 unavailable hosted classes and broader release gates stay open.
No commits or pushes requested.
