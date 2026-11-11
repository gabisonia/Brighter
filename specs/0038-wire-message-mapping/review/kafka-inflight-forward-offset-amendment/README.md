# In-flight rejection: source-offset assertion amendment

Status: approved and validated. The compiled test and helper are byte-identical to this amendment. Both full Kafka suites pass 381 cases. The checkpoint below preserves the reason for amendment; final results follow.

## Historical evidence before amendment approval

All four original cases failed on both .NET 9 and .NET 10 at the intended broker assertion: expected commit 0, observed commit 2 while the rejection writer was gated. This confirms the in-flight commit defect.

A candidate correction moved registration of the unresolved source offset before forwarding, retaining it on failure and releasing it on success. All four cases on both frameworks then passed the gated broker assertion, but failed later because the test required source-offset bag metadata after successful forwarding. `CleanBagForResend` deliberately removes that metadata; the rejection failure path restores it for retry. Existing forwarding tests check retained offsets after failure, not after success.

At the pre-approval checkpoint, the candidate was saved in `/private/tmp/brighter-4170-kafka-inflight-forward/candidate-fix.cs`; production source was restored to the pre-batch bytes. The preceding lost-partition fix remains intact. No mutation or full-suite validation has been performed for this batch; the candidate is not production-ready evidence.

## Exact amendment

The test's delivery helper gains an optional `requireSourceOffset` parameter, true by default. Initial deliveries and the failed rejection still require the original offset. Successful rejection and successful retry check the same source ID, topic, partition key and binary body, but no longer require the removed delivery-control metadata.

All broker assertions are unchanged: commit 0 while the writer is blocked, commit 0 and no invalid record after failure, commit 2 after recovery, and exactly one correctly mapped invalid record. The four theory cases, gating, cleanup and writer are unchanged.

## Approved execution scope

Replace the compiled test with this amended version, rerun RED on both frameworks, apply the saved candidate, then complete the original mutation plan, all library builds and both full Kafka suites. Preserve all 377 preceding cases. Report the anticipated release-on-failure mutation coverage gap if that mutation survives; do not weaken or silently extend these tests. No commit or push is included.

The test-first workflow in `.agent_instructions/testing.md` says “STOP after writing the test and ASK FOR APPROVAL” and “DO NOT run tests without approval.” The amendment was staged outside compiled projects until approval, then copied unchanged for validation.

Raw logs and TRX evidence: `/private/tmp/brighter-4170-kafka-inflight-forward/initial-net9.0.trx`, `initial-net10.0.trx`, `green-net9.0.trx`, `green-net10.0.trx`. The `green` label denotes an attempted run, not a successful result.

## Final results


The approved offset-metadata amendment
retains the four synchronous/asynchronous and first-write success/failure cases.
All four reproduce a premature native commit on both .NET 9 and .NET 10: while
forwarding offset 0 is gated, acknowledging offset 1 commits 2 instead of 0.
Registering the unresolved source offset before forwarding fixes the defect;
failure retains the barrier and confirmed success releases it. Later acknowledgements
remain available for the final commit. Source metadata checks after failure and
native mapped key, wire ID and binary-body assertions remain intact.

All four pass on both frameworks before and after mutation restoration. Nine
mutations run against the new cases: eight yield 30 intended failures; releasing
the barrier on failure survives all four new cases because no subsequent commit
is attempted before retry. That same mutation is detected by all four existing
KafkaFailedForwardOffsetBarrierTests at the broker-commit assertion. This is 34
intended failures across ten mutation executions, including the explicitly recorded
survivor. No approved assertion was changed to improve those results.

Both complete Kafka suites pass **381 cases, zero failures and zero skips**,
retaining every one of the preceding 377 passing cases. The library builds for
netstandard2.0, net8.0, net9.0 and net10.0; tests build for net9.0 and net10.0.
Only KafkaMessageConsumer changes in production relative to this batch's baseline;
public declarations, dependency files and the approved amended test/helper remain
unchanged. All mutations are restored. No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-inflight-forward-amended/`, including
RED/green/restored/full TRX files, mutation results, supplementary survivor check,
source snapshots, build logs and verification.json. The original unamended test's
incorrect success-offset expectation remains documented in
`/private/tmp/brighter-4170-kafka-inflight-forward/`.

This proves the barrier for acknowledgements initiated after the writer is entered.
It does not establish safety for commits already reduced before registration,
rebalance during forwarding, late acknowledgements from a prior assignment,
concurrent retries of one record, missing confirmations, or automatic pump recovery.
The full-feature production-readiness gates remain open.
