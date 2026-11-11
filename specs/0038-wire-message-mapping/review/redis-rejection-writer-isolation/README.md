# Redis rejection-writer isolation

Status: approved and complete on 2026-10-31 under the user-authorized paced regression plan.

Review [the four cases](When_redis_subscriptions_select_rejection_writers_should_capture_each_selection_and_isolate_repeated_payloads.cs).
They reuse the unchanged approved [rejection writer helper](../redis-rejection-writers/RedisRejectionWireWriter.cs).

## Contract and behavior

Four sync/async × legacy-first/custom-first cases create three consumers through
one factory, selecting legacy, custom and composed invalid-message/dead-letter
writers. All consumers are created before both subscription writer settings are
replaced with a decoy. Each consumer must retain both selections captured at
creation, including null/default selections.

Each consumer receives four distinct source messages and alternates invalid-message
and dead-letter rejection. Each route is used twice. A native Redis observer checks
each exact stored string, independent destination counters and queues, unchanged
source payloads and remaining source queue order. Application IDs/bodies and rejection
metadata remain correct. Each selected custom/composed writer is called twice; the
decoy is never called. Each legacy oracle conversion owns a fresh serializer.

The four cases exercise 48 native forwards. They establish sequential endpoint and
payload isolation, capture of both settings, and repeated default/composed conversion
without accumulated serializer content. They do not establish concurrent serializer
safety, mapping failure during fallback, delayed retry mapping, cancellation,
native failures after partial progress, atomic forwarding or crash redelivery.

## Results

All four unchanged approved cases passed initially and after a clean optimized
rebuild on .NET 9 and .NET 10. No production correction was needed. Three independent
.NET 10 mutations produced twelve intended native-string assertion failures: caching
both first selections (4), resolving live subscription settings (4), and accumulating
serializer content (4). Every byte-exact restoration returned four passes. Diagnostic
builds/JIT settings established exact assertion locations without test changes.
Final focused and regression runs used the unchanged optimized assemblies and ordinary JIT.

The initial unfiltered full suites each reported 172 passes/two ServiceStack quota
failures/four existing skips. Both failures were prior async reader-recovery cases.
Those failed runs remain preserved; they have not been relabeled as passing.

The user subsequently authorized running the unchanged tests in smaller batches with
real waiting periods, without payment or dependency/code/test changes. That amended
the original single-process validation plan. All four paced batches completed on
2026-10-31 at 03:11 UTC. Each batch passed 87 cases with two existing skips. Combined
results per framework are **174 passed, zero failed, four existing skips**. Every one
of the 178 identities ran exactly once per framework; all 174 preceding outcomes are
preserved and all four new cases pass. The two earlier quota-failing cases pass on
both frameworks. No batch reported a quota error.

Measured idle intervals before the batches were 3,606, 11,279, 4,597 and 4,507 seconds,
all above the required 3,605 seconds (values rounded). All four owned brokers and their
runners were removed. No license was supplied, no counters or quota checks were
modified, and code, dependencies, tests and timeouts were unchanged. Current source,
protected-file and assembly hashes still match the inputs used by the successful runs.
These results establish complete batched regression coverage. They do not establish
a passing single-process full suite or cross-batch process-state interactions.

All four library targets previously built with zero warnings/errors. Initial/final
test dependency builds reported 604 existing warnings, none in the new file. All
6,522 preceding C# sources remain unchanged; only the approved isolation test was
added and its reused helper is unchanged. No further build was needed because the
verified optimized inputs were reused. Hosted CI was not run.

Focused/mutation and historical quota-failure evidence is retained in
`/private/tmp/brighter-4170-redis-rejection-writer-isolation/`. The paced plan, TRX,
logs, cleanup, `summary.json` and independently checked `final-verification.json`
are retained in `/private/tmp/brighter-4170-redis-paced-regressions/`.

The optional licensed rerun script remains unused; a license is not required for the
completed paced approach. ServiceStack's [quota documentation](https://servicestack.net/download)
describes the free allowance. The approved regression gate is cleared and this batch
is complete. Whole-feature production readiness still has other open workstreams.

The next [four-case fallback recovery draft](../redis-rejection-fallback-recovery/README.md)
remains outside compiled sources, unbuilt/unrun pending its separate test review.

## Approved execution plan (completed with paced regression amendment)

Adopt the test unchanged and reuse the compiled helper unchanged. Current behavior
is expected to pass. Establish behavioral RED with three independent production-only
.NET 10 mutations:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Cache both first factory rejection-writer selections, including null, and use them for every consumer | Exact native string at the first endpoint with a different selection | 4 |
| Resolve both subscription rejection writers at each rejection instead of capturing them at consumer creation | Exact native string at the first rejection, which must not use the decoy | 4 |
| Reuse one RedisMessagePublisher per DefaultRedisWireMessageWriter instance without resetting it | Exact native string once repeated default/composed conversion accumulates earlier content | 4 |

Require twelve intended assertion failures. Setup/compiler/timeout failures do not
count. Diagnostic rebuild/JIT settings may establish exact assertion locations
without changing the test. Restore production bytes exactly after each mutation
and require four passes. If initial behavior differs, retain evidence and confine
any correction to this reviewed isolation contract.

Finish with focused passes on both frameworks, ordinary optimized full suites and
all four library targets built. Preserve all 174 preceding identities/outcomes and
add four new passes: expected complete matrices are 174 passes/four existing skips
each. Run each full suite once on a separate fresh ready broker; preserve and
investigate failures without retrying behavioral regressions until green.

Use the pinned Redis image, native Docker SDK runners, no host ports, unique keys
and owned cleanup. Async operations use a 45-second token; receives request one
second. External process/hang limits bound sync/native calls and disposal. Save
logs before removing owned containers. No dependency/workflow or public signature
changes are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
four cases, unchanged helper reuse, any evidenced minimum isolation correction,
twelve reviewed mutation failures with exact restorations, native Docker validation,
full affected regressions, four library builds, owned cleanup and local reports.
Hosted calls, commits, deployment and publication are excluded. The compiled test
matches the approved draft and reuses the unchanged helper. The user-approved paced
regression amendment is complete. Further fallback tests require their own review.
