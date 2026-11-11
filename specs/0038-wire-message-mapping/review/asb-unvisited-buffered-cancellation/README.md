# ASB cancellation with an unvisited buffered delivery — validated

Status: approved and validated characterisation. Both focused cases pass on .NET 9
and .NET 10 without production changes. Six mutations produce 12 intended assertion
failures. Both full available suites pass 610 cases, retaining all 608 prior outcomes.

## Approved contract

Two queue/topic cases send three distinct native binary deliveries. Bounded peeks
establish all three IDs and sequences. A consumer with buffer size three uses the
existing AsbBufferedCancellingWireReader unchanged: it maps the second delivery,
cancels the caller token, then returns its valid mapping. ReceiveAsync must report
OperationCanceledException after exactly two reader calls. The third ID must not
have reached the reader.

The same consumer then receives using an active token. All three original IDs must
recover exactly once within the bounded recovery window, preserving native bytes
and sequence, custom identity/body/headers, and current native lock/sequence
metadata. Both previously mapped messages must have fresh lock tokens. Recovery
must precede the earliest recorded original lock deadline. The dedicated fixture
uses one-minute entity locks; recovery polls for 20 seconds with two-second receives
(the last receive can extend beyond that window).

Every recovered delivery must have native delivery count two. In particular, the
previously unvisited ID must have exactly one reader invocation overall, with native
delivery count two. There must be five reader invocations total: two before
cancellation and three during recovery. Acknowledgement must remove all deliveries,
verified by bounded native peek from the earliest source sequence.

## Proof and limitations

An SDK maximum batch size is not a minimum guarantee. Initial peek proves visibility,
not acquisition. Delivery count two for the previously unvisited message, despite
only one mapping invocation, supplies evidence that the original cancelled receive
acquired it. The fixture is isolated: no competing receivers acquire messages and
the observer only peeks. A count of one is a batch-acquisition limitation, not proof
of defective cancellation. Preserve such a failure and review any amendment rather
than retrying until green or weakening the assertion.

The third message's original lock token/deadline is deliberately unavailable through
the reader because mapping it is the behavior being forbidden. No private reflection,
receiver substitution or native I/O mocking is introduced. The timing bound uses
the two observed original locks; it does not claim direct observation of the third
original lock. ASBTestCommand is subscription configuration only, with no new handler
or logging registration. The helper and AsbExternalWireReader are reused unchanged.

## Named production mutations

1. Release only the first two received messages when cancellation is observed:
   the recovered ID set must remain incomplete before original lock expiry.
2. Continue mapping the remaining received messages after cancellation, moving the
   cancellation throw after the mapping loop: the two-call assertion must fail.
3. Suppress cancellation and return the partial mapped result: the expected
   OperationCanceledException assertion must fail.
4. Clear the configured reader while handling cancellation: the explicit recovered
   mapped-identity Assert.Contains must fail before the identity lookup.
5. Replace recovered custom body with native bytes: the mapped-body assertion fails.
6. Omit native completion during acknowledgement: the final peek remains nonempty.

Compilation errors, setup failures and unproven batch acquisition do not count as
intended mutation failures. Preserve raw results and restore every mutation. If the
test is initially green, the named mutations establish RED without altering tests.
Run focused cases on .NET 9 and .NET 10 and both full available suites, comparing all
608 baseline outcomes. Confirm adopted test matches this draft byte-for-byte and
reused helpers remain unchanged. No commits or pushes.

## Validation evidence and remaining work

All four initial and all four restored executions pass. The native acquisition
precondition holds: the previously unvisited message has delivery count two when
mapped for the first and only time. There are exactly two mapping invocations before
cancellation and five overall. No production correction is needed; the previous
batch-release implementation already delivers this behavior.

All six named mutations fail both queue/topic cases on .NET 9 at their intended
xUnit assertions: incomplete recovery at line 131, extra mapping at line 91,
suppressed cancellation at line 94, lost reader at line 111, native-body replacement
at line 124, and missing completion at line 142. There are 12 intended assertion
failures, with no compilation/setup failures counted. All mutations are restored.

All library targets build. Both full available suites pass 610 cases on their first
runs, zero failures/skips, with exact retention of all 608 prior case/outcome pairs.
The approved test matches the compiled source byte-for-byte. All 1,171 non-generated
production C# files and both reused helpers are unchanged from this batch's baseline.
No public signatures or dependencies change. Evidence:
`/private/tmp/brighter-4170-asb-unvisited-cancellation/` contains initial/restored
TRXs, mutations.json, regression TRXs, exact baseline comparisons and source hashes.
Nothing committed or pushed.

Abandonment failures, broker-wait/settlement cancellation, sessions, unresolved
session-lock instability, eight DLQ-blocked cases, 52 unavailable hosted classes
and broader transport/release gates remain open. This batch establishes no overall
release-readiness claim. SDK batch-size variability remains a setup limitation;
passing runs do not imply a minimum batch-size guarantee.

This batch's isolated emulator containers and network were removed after validation.
