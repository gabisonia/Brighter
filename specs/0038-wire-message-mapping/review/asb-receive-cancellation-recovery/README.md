# ASB receive cancellation recovery — validated

Status: implemented and validated with the approved
[initial visibility amendment](../asb-receive-cancellation-visibility/README.md).
Both amended native cases pass on .NET 9 and .NET 10, initially and after restoration.
Six mutations produce 12 intended assertion failures. Both full available suites pass
604 cases, zero failures/skips, retaining all 602 previous case/outcome pairs.

Original evidence remains: .NET 9 topic visibility null, queue ignored cancellation,
and .NET 10 ID mismatches caused by leftover messages from failed cases. Fresh fixtures
reproduced the topic setup race. The approved bounded wait resolved it without changing
cancellation/recovery assertions. Every isolated case/mutation uses a fresh fixture.
The compiled test matches the amendment byte-for-byte; the reused reader is unchanged.
Only the ReceiveAsync entry token check and XML exception documentation change in
production. All library targets build; all mutations are restored.

Evidence: `/private/tmp/brighter-4170-asb-receive-cancellation/` includes original RED,
amended/restored matrices, mutations.json, full-regression TRXs, baseline comparisons
and amended-source-verification.json. The initial mutation-harness precondition aborted
before source changes because three token checks existed; the harness was scoped to
the first (receive-entry) check before validation. No aborted harness run counts as RED.
Dedicated fixture containers are stopped. Nothing committed or pushed.

## Approved contract

Publish one message through the native SDK and observe its identity/sequence by peek.
Call the public async consumer with a token cancelled before entry. It must propagate
an OperationCanceledException-compatible exception directly, without invoking the
configured reader or consuming/settling the delivery. A native peek at the original
sequence must still observe the original identity and bytes.

The same consumer then receives with an active token. The approved AsbExternalWireReader
must run once and retain its custom identity, body, subject and metadata. The native
delivery count must still be one, guarding against an earlier receive followed by
abandon/reset. Native lock/sequence metadata must match the recovered delivery. Its
acknowledgement must remove the delivery, observed by polling native peek at that exact
sequence for up to two seconds. This is bounded settlement evidence, not an unlimited
absence guarantee. The 60-second timeout must remain active after cancellation.

Reuse the approved reader unchanged; no new helper, handler or logging registration.
Use isolated native destinations and explicit Proactor subscription configuration.
This tests pre-entry cancellation only. Initial queue RED and the remove-entry-guard mutation establish that ReceiveAsync
ignored a pre-cancelled token. The entry check now prevents receiving or mapping.

## Named production mutations

1. Remove the pre-entry cancellation check required by these cases: expected direct
   cancellation fails or reader count shows that mapping occurred.
2. Return an empty successful result for pre-entry cancellation: expected exception fails.
3. Wrap pre-entry cancellation in ChannelFailureException: direct cancellation-type
   assertion fails.
4. Clear the configured reader while reporting cancellation: recovery reader count or
   custom mapping fails.
5. Replace the custom recovered body with native bytes: custom body assertion fails.
6. Omit native completion during acknowledgement: post-acknowledgement peek still finds
   the previously observed delivery.

All six mutations fail both cases at the intended assertion. The two final full suites
pass 604 cases on their first runs, retaining exactly all 602 prior outcomes. Original
setup failures and contamination are recorded rather than replaced by passing reruns.
Previous session-lock instability remains unresolved despite passing here.

Cancellation while waiting on the broker, during mapping, for buffered deliveries,
or during settlement remains separate. Sync Receive has no caller-token parameter.
Session-lock instability, eight DLQ-blocked cases, 52 unavailable hosted classes and
broader release gates remain open. Nothing committed or pushed.
