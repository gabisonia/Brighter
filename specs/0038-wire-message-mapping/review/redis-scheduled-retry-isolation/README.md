# Redis scheduled retry endpoint isolation

Status: approved and complete (2026-11-03) under the user-approved paced regression plan.
Both frameworks pass 190 cases/four existing skips, retaining every preceding outcome.

Review [the four cases](When_redis_subscriptions_select_scheduled_retries_should_isolate_destinations_and_repeated_payloads.cs).
Reuse `RedisPublicationWireWriter` and `RedisRecoverableRequeueWireWriter` unchanged.
No new API, dependency, helper, project reference or workflow is proposed.

## Contract and production path

Two subscriptions share one native Redis topic, consumer factory, scheduler and
producer registry, while selecting different retry publication aliases and queues.
One publication uses default serialization; the other composes that serialization
with the existing `C:` writer. Both use a non-default event type, and their logical
application topic differs from the shared native storage topic and both aliases.
The ordinary publication and immediate retry writer are decoys.

Four cases cover sync/async × default-first/composed-first creation and dispatch.
After both consumers are created, the subscriptions' selected aliases are swapped.
Each consumer must retain its captured selection. Two rounds alternate between the
endpoints, producing four scheduled deliveries per case. Each source has a distinct
application ID, body and marker. Repeated conversions must preserve the latest data
without reusing a prior serialized message.

The test uses the real InMemoryScheduler, CommandProcessor, FireSchedulerMessageHandler,
OutboxProducerMediator and native Docker Redis, through public factories and consumer
methods. There are no scheduler or broker test doubles. Before each five-second due
time, neither writer nor native counter/payload/queues may change. At dispatch, the
shared counter advances from 41 through 45. Only the selected queue receives each
native ID; the other endpoint and sibling queue remain unchanged.

The native wire prefix must match the endpoint. Decoding the default representation
(with the composed prefix removed where applicable) must preserve the current body,
application ID/topic/type, handled count and marker, and omit the local producer
selector. TTL, outbox settlement, unchanged original native payloads and all previous
callers' complete JSON snapshots are checked. Each consumer processes its next source
and finishes an empty receive after both rounds. The immediate and ordinary writers
remain unused. This is sequential reuse/isolation, not concurrent scheduling proof.

## Named characterization mutations

Existing behavior is expected to pass initially. Establish RED with independent,
production-only mutations on .NET 10; keep the reviewed test and helpers unchanged.

| Mutation | Exact intended assertion | Cases |
| --- | --- | ---: |
| Cache the first selected retry publication in RedisMessageConsumerFactory and supply it to every consumer from both Create paths | The second endpoint's queue lacks its expected new native ID after the shared counter advances | 4 |
| Reuse the first publication's writer selection, including null, for later publications in RedisMessageProducerFactory | The second endpoint's native wire prefix differs from its expected default/composed framing | 4 |
| Cache and return the first string in each DefaultRedisWireMessageWriter instance | Decoded body differs from the current revised body on repeated delivery | 4 |

For the factory selector mutation, cache the first non-null captured alias on the
factory instance; do not redirect the registry or scheduler. All retry publications
share the native topic, so the counter still advances and the test reaches the queue
assertion instead of waiting for an unobservable destination. For writer leakage,
keep each publication's alias, event type, destination and queue intact, changing
only its writer. For payload caching, keep valid original framing so decoding reaches
the current-body assertion. Compilation failures, callback crashes, unrelated setup
errors and timeouts do not count as RED.

Require twelve intended assertion failures. Restore production bytes exactly after
each mutation and require four passes. If the initial cases fail, stop and report
that evidence; do not silently broaden this characterization into a new behavior
change or rewrite the reviewed assertions.

## Execution after approval and preceding completion

1. Verify the preceding paced gate completed successfully and its independent report
   preserves all 190 case outcomes per framework. Snapshot current sources and
   protected inputs, then adopt the approved test unchanged. Respect a real
   3,605-second idle interval after the preceding native tests before this batch.
2. Run four initial cases on .NET 9 and .NET 10, the three named mutations and exact
   restorations, then four final ordinary optimized cases on both frameworks.
3. Build the four Redis library targets and verify unchanged production, helpers,
   dependencies/workflows and all preceding C# files. Only the approved test is added.
4. Run complete disjoint native regressions with the same free-quota pacing policy:
   3,605 real seconds after focused tests and between batches. Preserve all 190 prior
   outcomes and add four passes, expecting 190 passes/four existing skips across
   194 identities exactly once per framework. Choose batch allocation from the
   preceding actual command counts to retain headroom; do not bypass license limits.
5. Verify source/assembly hashes, exact identities/outcomes, real idle gaps and owned
   cleanup, then update local reports. Stop on unexpected evidence without automatic
   retries. Do not claim a single-process full-suite pass.

Keep the pinned native Redis image, SDK 9/10 runners, no host ports, 45-second async
cancellation, one-second receives and external process/hang bounds. Poll dispatch at
100 ms to limit observer traffic; the five-second due time remains real. Stop and
dispose the scheduler before deleting owned native keys. Use temporary idle-sleep
prevention for long runs; keep the laptop lid open.

## Scope and approval

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require **"STOP after writing the test and ASK FOR APPROVAL"** and **"DO NOT run tests
without approval"**. This is a new concrete package under the repository's
[/test-first workflow](../../../../.claude/commands/tdd/test-first.md); approval of the
preceding four selection tests does not approve these additional isolation cases.
Approval covers the unchanged four cases, twelve named mutation failures/restorations,
initial/final focused runs, four-target builds, paced regression schedule and local
reports/owned cleanup after the preceding gate. No commit, push, hosted call or
deployment is included.

Invalid/missing configuration, submission/callback failures, cancellation, durable
scheduler persistence, concurrent changes and broader feature release checks remain
separate work. This draft does not establish those guarantees or whole-issue readiness.


## Approval recorded and execution queued

The user approved this exact package with "I approve". Its four cases, reused helpers
and preceding source/protected inputs are pinned by hash in
`/private/tmp/brighter-4170-redis-scheduled-retry-isolation/approval.json` and companion
manifests. No additional review approval is required for this unchanged scope.

The queue waits for independent success of the preceding 190-case regression gate,
then at least 3,605 real seconds after its final native batch. It adopts the approved
test unchanged, runs initial characterization, twelve named mutation failures with
exact restoration, final optimized checks and all four library builds, then starts
complete paced regressions. An unexpected result stops the queue and retains evidence.
The active preceding source/assembly inputs remain unchanged.

Live execution status: `/private/tmp/brighter-4170-redis-scheduled-retry-isolation/status.json`.
No execution result, complete regression pass or whole-issue readiness is claimed
before that evidence exists. Keep the laptop lid open for the unattended schedule.


## Approved isolation batch: focused validation complete; regressions pending

Four cases pass initially and finally on .NET 9/.NET 10. All twelve intended
mutation failures and exact restorations are verified. Four Redis library targets
build without warnings/errors; all preceding production/test sources and protected
inputs remain unchanged. The approved test alone is added and the focused broker
is removed. Complete paced regressions are queued; no full regression pass is claimed.
Live evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced/status.json`.

First regression eligibility: 2026-11-02T23:45:51.175528+00:00.


## Isolation regression sleep interruption and unchanged rerun (2026-11-03)

The first paced regression attempt stopped during net9.0-batch-1: 91 passed,
four failed and two existing skips. Two new isolation cases saw an empty receive;
two existing reader/immediate-retry cases saw expired native payloads. The runner
reported 24,597 wall seconds versus 176 monotonic seconds and rejected the clock
discontinuity. macOS power logs confirm lid-closed sleep at 00:14 local time and
repeated maintenance sleep during the batch. Redis recorded 51 expired keys and
no quota error. Owned containers were removed. This is not a passing regression
result, and the original evidence remains under
`/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced/`.

On the user's continuation, the complete unchanged suite was restarted after the
required real idle interval had already elapsed. Source inventory, all pinned source,
dependency and assembly hashes, and approved tests/helpers match. No timeout, TTL,
dependency, assertion, license or clock setting was changed. The new run retains
3,605-second gaps between all batches and temporary idle-sleep prevention. The lid
must stay open because caffeinate cannot override clamshell sleep.

Current status: `/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced-awake/status.json`.
The failed attempt is preserved alongside the rerun. A clean rerun must establish
all 194 identities exactly once per framework, 190 passes/four existing skips and
all 190 preceding outcomes before completion is claimed. Focused/mutation checks
and four-target builds remain valid; the complete regression gate remains pending.

The first awake-rerun batch passed 95 cases/two existing skips. All four cases that
failed during sleep now pass unchanged, with matching case identities, no quota or
clock-discontinuity errors and complete owned cleanup. Three batches remain; see
`paced-awake/progress-verification-1.json` in the evidence directory.


## Redis scheduled retry endpoint isolation completion (2026-11-03)

Four approved sync/async × endpoint-order cases pass through native Redis and the
production in-memory scheduler, command processor, handler and outbox mediator.
Two subscriptions share a factory, registry, scheduler and native topic while their
captured aliases retain independent default/composed writers and destination queues.
Repeated deliveries preserve current payloads and all caller snapshots. No permanent
production change is needed.

Three independent production-only mutations produce twelve intended assertion
failures: wrong queue with a cached consumer selector, wrong framing with a reused
publication writer, and stale decoded body with a cached default representation.
Each exact restoration returns four passes. Initial and final optimized focused
checks pass four cases per framework. All four Redis library targets build with zero
warnings/errors. Approved test/helpers, all 6,526 preceding C# files and protected
inputs are unchanged; only the approved test is added.

Complete paced regressions pass **190 cases, zero failures and four existing skips
per framework**. All 194 identities run exactly once, preserving all 190 prior
outcomes. Real idle intervals are at least 3,605 seconds. No quota or clock-discontinuity
errors are reported in this rerun and owned containers are removed. The earlier
sleep-interrupted failure is retained separately under `paced/`; it is not erased
or counted as a successful run. This establishes complete
batched coverage, not a single-process full-suite pass. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced-awake/final-verification.json`.

Invalid/missing configuration, scheduling/dispatch failure recovery, cancellation,
durable persistence, concurrency and broader feature release gates remain open.
Whole-feature production readiness, hosted CI, commits, pushes and deployment are
not claimed.
