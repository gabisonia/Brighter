# Redis scheduled retry: native send failure and cancellation

Status: focused validation and complete paced regressions passed on 2026-11-08. No commits or pushes.

This package implements completion-plan stage 1 using the native Redis broker,
real InMemoryScheduler, CommandProcessor and outbox. It adds no packages or mocks.

## Native send failure (two cases: sync and async)

Accept a selected scheduled retry, then corrupt the native counter before its callback.
The selected composed writer must run, but native INCR must fail. The outbox must
retain the revised body, original application identity and selected publication alias.
No native payload or queue entry may be written. Repair the counter and explicitly
clear the same outbox: only the selected source queue receives the composed wire
message, the local selector is absent on wire, and the outbox settles.

Named characterisation mutations, applied to production code and restored byte-for-byte:
1. Swallow native publish errors in both PublishMessage variants: the retained-outbox
   assertion must fail because the producer incorrectly reports success.
2. Ignore the selected wire writer in both SendWithDelay variants: writer-call or composed
   wire assertions must fail.

## Pre-cancelled submission (one async case)

A pre-cancelled positive-delay consumer retry must throw OperationCanceledException
carrying the token before accepting a job or releasing native in-flight identity.
The next receive must remain blocked. Immediate recovery must use the original native
ID; a subsequent uncancelled delayed retry must use the same fixed scheduler ID
without conflict, invoke the selected writer and settle normally.

Expected defect: the consumer currently trusts a scheduler which can ignore cancellation.
The scoped contract is rejection at the Redis consumer boundary before accepting the
scheduled retry. Cancellation after acceptance is not a rollback guarantee. No shared
scheduler cancellation semantics change is required for this boundary.

Named mutation after the fix: omit the cancellation check; the cancellation exception
assertion must fail. Full Redis regressions on both target frameworks will include
this package and the preceding scheduling-conflict package in one paced matrix.

## Focused evidence, 2026-11-07

The cancellation test first failed on both .NET 9 and .NET 10 because no exception
was raised. A token check before positive-delay submission fixes that boundary.
All three cases now pass on both frameworks. Five intended mutation failures and
five restored passes verify error propagation, selected writing and cancellation.
All four Redis library targets build. Only RedisMessageConsumer.cs changes existing
production behavior in this package; preceding files are preserved.

Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-boundaries`.
The combined regression plan covers all 207 identities per framework, preserving the
preceding 202 outcomes and adding these three cases plus two scheduling-conflict cases.
It runs four batches with real 3,605-second idle intervals and no license changes.
The inputs are a byte-verified frozen copy of the final focused source, so unrelated
integration work can proceed without changing the source under validation.

Each batch's command estimate uses preceding measured usage plus 150% of measured
new usage plus 100 commands. The largest estimate is 5,562, below a 5,600 planning
ceiling and the 6,000 free quota. This replaces the earlier 5,500 planning ceiling;
coverage and the real-time pacing remain unchanged. Measured new-package usage is
298 commands per framework; the conflict package uses 299.

## Complete paced regression evidence

The certified aggregate passes 203 cases with four existing skips on each framework.
All 207 identities appear exactly once in that aggregate, preserving the 202 prior
outcomes and adding the five boundary/conflict cases. Frozen source and assembly
hashes remain unchanged; current Core/Redis source and Redis test inventories match
the frozen inputs byte-for-byte. Owned batch containers are removed.

The first last-batch .NET 10 attempt was interrupted by host suspension: wall time
was 8,519.644 seconds versus 153.423 seconds of active monotonic execution. Two timing
failures and the complete evidence are retained, and that attempt is excluded from
the certified aggregate. The unchanged batch rerun passes after a real 31,407-second
idle interval. Earlier certified intervals exceed 3,605 seconds each. No license,
quota reset, clock manipulation, changed test filter or removed case was used.
This is complete batched coverage, not a single-process full-suite pass.

Independent reconciliation:
`/private/tmp/brighter-4170-redis-scheduled-retry-boundaries/paced/reconciled-verification.json`.
