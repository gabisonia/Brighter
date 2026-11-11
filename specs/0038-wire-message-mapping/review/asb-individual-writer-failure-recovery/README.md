# ASB individual writer failure recovery — validated

Status: approved, implemented and validated. Eight native cases pass on .NET 9
and .NET 10, initially and after mutation restoration. Six mutations produce 40
intended assertion failures. Both available suites pass 579 cases with zero
failures/skips, preserving all 571 prior case/outcome pairs.

## Approved contract

Eight cases: queue/topic × sync/async × writer exception/null. Public registry
producers retain the existing ChannelFailureException outer contract. A thrown
writer exception remains the exact InnerException. A null result produces a
deliberate InvalidOperationException as InnerException. Initial RED confirms
all four null cases produce NullReferenceException when publication TTL is applied;
the four writer-exception cases already pass. A null-result guard now deliberately
throws InvalidOperationException inside the existing ChannelFailureException wrapper.

After failure, the same producer sends a healthy source using the same public
sync/async path. A native Service Bus SDK receiver checks custom identity, body,
metadata and publication TTL. Exactly one mapping call occurs per attempted send;
source identity, routing, body and metadata remain unchanged.

Reuse the approved AsbRecoverableBatchWireWriter and AsbExternalWireWriter without
editing them. The existing failure marker and helper name remain unchanged to
avoid adding a duplicate strategy. Neither helper replaces broker I/O. No request
handler or closed-generic logging registration is introduced.

Use dedicated isolated destinations. The immediate empty peek is a best-effort
check, not proof of absence of delayed publication; healthy delivery identity
provides an additional observation. Sync sends have no cancellation token, so
their runtime bound relies on the external test runner timeout.

## Named production mutations

1. Remove the new null-result guard: null cases fail on inner exception type.
2. Swallow the mapping failure: expected outer exception assertion fails.
3. Replace the writer-thrown exception with a new InvalidOperationException:
   inner exception identity fails in throwing cases.
4. Clear the configured writer after failure: recovery invocation count/custom
   native mapping fails.
5. Invoke the configured writer twice for the healthy source: invocation count
   fails, without requiring duplicate broker delivery.
6. Clear the failed source body in failure handling: source preservation fails.

## Validation evidence

The mutations produce 4, 8, 4, 8, 8 and 8 failures respectively, each at its intended
assertion. All mutations are restored. The approved test is byte-identical to its
compiled copy; the reused approved helper is unchanged. Only the producer null guard
and XML exception documentation change in production. All library targets build.

Both full available suites pass 579 cases on their first runs, zero failures/skips,
retaining exactly all 571 prior outcomes. Previous session-lock instability remains
unresolved despite passing here. Eight DLQ-blocked cases and 52 hosted classes remain
excluded. The immediate-peek limitation above remains applicable.

Evidence: `/private/tmp/brighter-4170-asb-individual-writer-recovery/` contains initial
RED, initial/restored matrices, mutations.json, full regression TRXs, exact baseline
comparisons and source-verification.json. Dedicated fixture containers are stopped
after validation. Nothing committed or pushed.

This batch excludes scheduled sends, cancellation, outbox retry, broker failure
recovery, session-lock diagnosis and DLQ behavior. It does not establish full
production readiness. No commits or pushes are requested.
