# ASB cancellation during mapping — validated

Status: approved, implemented and validated. Both cases pass on .NET 9 and .NET 10
after the correction and after mutation restoration. Six mutations produce 12 intended
assertion failures. Both full available suites pass 606 cases, zero failures/skips,
retaining all 604 previous case/outcome pairs.

## Approved contract

Publish one native delivery and confirm visibility using the approved bounded initial
peek pattern. Receive with an initially active caller token. The configured reader
maps the delivery, then cancels that token exactly once before returning its valid
mapped message. ReceiveAsync must propagate OperationCanceledException directly,
rather than dispatching the mapped message or returning successful emptiness.

The original delivery must remain on its native entity. The same consumer must recover
it with an active token, within a 20-second receive wait and before its original lock
expiry. The dedicated fixture uses one-minute entity locks. Recovery must have the
same native ID/sequence/body, a fresh lock token and delivery count two. The reader
runs once per receive, retains custom mapping, and the gateway attaches correct native
lock/sequence metadata. Acknowledgement must remove the delivery, verified by a bounded
native peek from the exact original sequence number.

The helper records first and latest deliveries, delegates mapping to the approved
AsbExternalWireReader unchanged, and cancels only on the first call. No native I/O is
mocked. It is used sequentially on one consumer; it proposes no shared-helper thread
safety guarantee. Existing ASBTestCommand provides subscription configuration only.
No new handlers or logging registration are introduced.

Initial RED on both frameworks confirms cancellation after mapping is ignored.
Adding only a post-mapping check reports cancellation but both queue/topic recovery
receives return empty on both frameworks. Receiver reset alone does not release the
delivery within the approved window. The final correction explicitly abandons the
current native delivery before throwing cancellation and entering the existing reset
path. Recovery then succeeds with a fresh lock and delivery count two. A native receive that succeeds only after natural lock
expiry does not satisfy this contract. This is deterministic cancellation at the
mapping boundary, not cancellation while waiting for the broker or during settlement.

## Named production mutations

1. Remove the cancellation check after mapping: expected cancellation fails.
2. Return a successful empty receive on mapping-boundary cancellation: expected
   exception fails.
3. Omit receiver reset/release when mapping-boundary cancellation propagates:
   the subsequent native recovery receive must fail to return its expected message
   before the original lock expires. A setup or unrelated SDK error does not count.
4. Clear the configured reader while handling cancellation: recovery invocation count
   or custom identity fails.
5. Replace recovered custom body with native bytes: custom body assertion fails.
6. Omit native completion during acknowledgement: final peek still sees the delivery.

## Validation evidence

All six named mutations fail both cases at their intended assertions. The release
mutation omits explicit abandonment while retaining reset, reproducing the empty
recovery receive. It demonstrates why the guard alone is insufficient. Both initial
ignored-cancellation and guard-only recovery failures are retained in the evidence.

All mutations are restored. The approved test/helper match compiled copies byte-for-byte;
the reused external reader is unchanged. Only AzureServiceBusConsumer changes among
1,171 non-generated production C# files. All library targets build. Both full available
suites pass 606 cases on their first runs, zero failures/skips, with exact retention
of all 604 prior outcomes. Session-lock instability remains unresolved despite passing.

Evidence: `/private/tmp/brighter-4170-asb-mapping-cancellation/` contains initial RED,
guard-only failures, green/restored matrices, mutations.json, full regression TRXs,
exact baseline comparisons and source verification. Docker initially exhausted its
address pools; removing the previous completed batch's stopped fixture/network freed
space. No application test ran during that infrastructure failure. This batch's fixture
containers and network are removed after validation. Nothing committed or pushed.

Buffered multi-message cancellation, broker-wait cancellation, settlement cancellation,
sessions, eight DLQ-blocked cases, 52 unavailable hosted classes, session-lock instability
and broader release gates remain open. No commits or pushes requested.
