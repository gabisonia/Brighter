# Redis scheduled retry writer recovery

Status: approved and complete (2026-11-04) under the user-approved paced regression plan.
Both frameworks pass 194 cases/four existing skips, retaining every preceding outcome.

This is a `/test-first` characterisation package for a failed scheduled Redis send
followed by explicit outbox recovery. It closes the writer-failure branch identified
in the [scheduler audit](../../scheduler-path-audit.md#redis-scheduled-retry-failure-audit-2026-11-02).
It does not establish missing-registration handling, automatic retry, native Redis
send-error recovery, cancellation or durable scheduler persistence.

## Files to review

- [Four test cases](When_a_scheduled_redis_writer_fails_should_retain_the_message_until_outbox_recovery.cs)
- [Persistent-failure writer](RedisScheduledRetryRecoverableWireWriter.cs)

After approval, copy the test unchanged into the Redis project's `MessagingGateway`
directory and the helper unchanged into `TestDoubles`. Existing helpers, project
references, dependencies, workflow files and production sources remain unchanged
unless the approved behavior reveals a Redis-local defect requiring a minimal fix.
Shared Core or scheduler fixes require a separate concrete review.

## Contract

Four cases cover sync/async scheduling and recovery, each with throwing/null writers.
The consumer receives a real native Redis message and schedules its revised payload
through the production `InMemoryScheduler`, command processor, scheduler handler,
producer registry and outbox mediator. Source storage topic, logical application topic
and registered retry publication topic differ. A sibling queue detects accidental
fan-out. Existing ordinary-publication and immediate-retry writers act as decoys.

The selected writer keeps failing until explicitly recovered. A fail-once helper
would allow a resilience retry to hide the retained-outbox behavior. This helper
implements the actual application wire-writer extension; it does not replace the
transport, scheduler or mediator. It has thread-safe failure/success counters.

Before the five-second due time, assert no writer invocation, outbox deposit or native
publication; the same consumer must receive and acknowledge the next source message.
Keep the caller's complete serialized representation unchanged.

Await the stopped producer publish activity, scoped to this test's tracer, then await
the real scheduler's asynchronous disposal to finish any callback still running.
This avoids treating writer entry as callback completion. The wait has the existing
45-second test cancellation bound. The activity is a completion signal, not a claim
that failed sends have correct telemetry. An absent signal is a test failure and
must be investigated rather than replaced with a guessed sleep.

After callback completion, assert at least one failed attempt and no successful
write. Exactly one outstanding message must retain its ID, logical topic, event type,
handled count, revised Unicode body, marker and local publication selector. Native
counter 41 and original payloads must remain unchanged; no retry payload, source-queue
delivery or sibling delivery may exist. The caller and decoy writers remain unchanged.

Recover the writer and explicitly call the existing sync/async `ClearOutbox` API.
Require one successful write, no further failed attempt, native counter 42 and exactly
one delivery to the selected queue. Decode the composed default representation and
check the same message fields, with the local selector absent from the wire. Check
TTL, unchanged source records/caller, no logical/publication alias storage and an empty
outbox. This is one delivery for this explicit recovery attempt, not an exactly-once
guarantee across arbitrary failures or duplicate outbox clearing.

## Execution and named mutations after approval

First capture initial focused outcomes on .NET 9 and .NET 10. Expected behavior is
already implemented; an initial green is characterisation, not proof that the new
test detects a defect. Run these independent temporary production mutations on .NET 10:

| Mutation | Required assertion failure | Cases |
| --- | --- | ---: |
| In `OutboxProducerMediator`, mark non-confirming sends dispatched even when sending returns false, in both sync and async branches | Exactly one outstanding message after the failed callback (actual zero) | 4 |
| In both ordinary Redis send paths, bypass the publication writer and use the default writer | Failed writer attempts must be positive (actual zero) | 4 |

Require eight intended assertion failures, exact production-byte restoration after
each mutation and four restored passes. A compile error, timeout, callback crash,
quota error or unrelated assertion does not count as mutation evidence. Temporary
Core mutation is permitted solely to demonstrate the retained-outbox assertion;
permanent shared Core changes are outside this package. Preserve all existing work
using file snapshots, never a checkout/reset against repository HEAD.

Finish with optimized focused checks on both frameworks and the four Redis library
targets. Require complete disjoint native Redis regression coverage: the preceding
194 identities and their outcomes must be preserved, plus these four new cases,
giving 194 passes and four existing skips across 198 identities per framework.
Do not label this a single-process full-suite pass.

Reuse the pinned native Redis image, native SDK runners, isolated Docker networking,
no host ports, existing process/hang bounds and owned-container cleanup. Reuse the
user-approved pacing: at least 3,605 real idle seconds after focused work and between
regression batches; preserve per-batch free-quota headroom and record actual gaps.
No paid license, dependency/test removal, timeout extension, clock manipulation or
quota-counter reset. Keep the laptop lid open during native runs. Retain any failed
attempt and stop on unexpected results rather than changing the reviewed contract.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require **“STOP after writing the test and ASK FOR APPROVAL”** and **“DO NOT run tests
without approval.”** This package follows the
[/test-first workflow](../../../../.claude/commands/tdd/test-first.md).

Approval covers adopting these four cases and helper, native focused execution,
the two named mutations and exact restorations, minimal Redis-only fixes if needed,
four-target builds, complete paced regressions, cleanup and local evidence updates.
No commit, push, hosted CI, deployment or new shared Core design is included.


## Redis scheduled retry writer recovery: focused validation (2026-11-04)

The user approved the four cases, persistent-failure helper and named mutation plan.
All four cases pass initially and in final optimized builds on .NET 9 and .NET 10.
The premature-settlement mutation produces four failures at the outstanding-message
assertion; bypassing the publication writer produces four failures at the failed-write
assertion. Exact restoration returns four passes after each mutation. No permanent
production change is needed. All four Redis library targets build with zero warnings
or errors; the test build retains 604 existing warnings.

The approved test/helper are unchanged, and independent verification preserves all
6,527 preceding C# files and 281 build/dependency/workflow inputs. The focused broker
is removed. Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-writer-recovery/focused-verification.json`
and `launch-verification.json` in the same directory.

Complete paced regressions are queued in four disjoint batches; their outcomes are
pending. Each framework is expected to cover 198 identities exactly once, with 194
passes and four existing skips, preserving all 194 preceding outcomes. The four new
cases measure 277 Redis commands per framework. An allowance of 516 commands puts
the smaller preceding batches at estimated totals of 5,174 and 5,202, under the 5,500
allocation threshold; no extra batch is required. No tests, dependencies, timeout
bounds or license settings are changed to achieve this allocation.

The first batch is eligible at `2026-11-04T16:16:16.683922+00:00`. Real 3,605-second
idle intervals remain required after focused work and between batches. The launcher
uses temporary idle-sleep prevention; keep the lid open during test execution.
Live state: `/private/tmp/brighter-4170-redis-scheduled-retry-writer-recovery/paced/status.json`.
The runner stops on unexpected results and retains evidence. Successful completion
independently verifies outcomes, exact identities, source/assembly hashes, waits and
cleanup before updating completion reports. This is batched coverage; no passing
single-process full suite or whole-feature production readiness is claimed.


## Redis scheduled retry writer recovery completion (2026-11-04)

Four approved sync/async × throwing/null cases pass through native Redis and the
production in-memory scheduler, command processor, handler and outbox mediator.
Failed callbacks leave exactly one outstanding message with its revised payload,
metadata and selected publication. Native counters, payloads and queues remain
unchanged. Explicit recovery sends one composed/default payload to the selected
queue and settles the outbox while preserving the caller and original native records.
No permanent production change is needed. This does not establish automatic retries
or exactly-once delivery across arbitrary failures.

Two independent production-only mutations produce eight intended assertion failures:
premature settlement loses the pending message, and bypassing the selected writer
fails the required failed-attempt assertion. Exact restoration returns four passes
after each mutation. Initial and final optimized focused checks pass four cases per
framework. Four Redis library targets build with zero warnings/errors. All 6,527
preceding C# files and 281 protected build/dependency/workflow inputs are unchanged;
only the approved test and writer helper are added.

Complete paced regressions pass **194 cases, zero failures and four existing skips
per framework**. All 198 identities run exactly once, preserving all 194 prior
outcomes. Real idle intervals are at least 3,605 seconds, no quota or host clock
errors are reported, and owned containers are removed. This establishes complete
batched coverage, not a single-process full-suite pass. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-writer-recovery/paced/final-verification.json`.

Invalid/missing registration, submission and native-send failures, cancellation,
durable persistence, concurrency and broader transport/release gates remain open.
Whole-feature production readiness, hosted CI, commits, pushes and deployment are
not claimed.

The subsequent [boundary package](../redis-scheduled-retry-boundaries/README.md)
completes the combined five-case conflict/failure/cancellation regression gate on
2026-11-08: 203 passes/four existing skips per framework, all 207 expected identities
retained in certified quota-paced batches. This supersedes the older matrix counts
for the current source state; earlier evidence remains historical.
