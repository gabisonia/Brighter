# Scheduled Redis retry registration failure containment

Status: approved and complete (2026-11-06). Native Redis regressions pass 198 cases/four existing skips per framework; Core and InMemory baselines are preserved.

The [scheduler audit](../../scheduler-path-audit.md#redis-scheduled-retry-failure-audit-2026-11-02)
identifies a gap beyond the completed writer-recovery tests. A missing topic/type
registration throws during producer lookup, outside send-error handling. The real
in-memory timer callback currently has no exception boundary or guaranteed cleanup.
Host termination is a source-derived risk until this package reproduces it.

## Review files and installation

| Draft | Destination after approval |
| --- | --- |
| [Four parent cases](When_a_scheduled_redis_publication_is_missing_should_preserve_the_host_and_allow_recovery.cs) | Redis tests, `MessagingGateway/` |
| [Native scenario](Probe/Program.cs) | New `tests/Paramore.Brighter.Redis.SchedulerFailureProbe/` executable fixture |
| [Real timer observer](Probe/ObservedSystemTimeProvider.cs) | Same fixture |
| [Scheduler log collector](Probe/SchedulerFailureLogs.cs) | Same fixture |
| [Recovery writer](Probe/RecoveryWireWriter.cs) | Same fixture |
| [Fixture project](Probe/Paramore.Brighter.Redis.SchedulerFailureProbe.csproj) | Same fixture |
| [Proposed Redis test project](Paramore.Brighter.Redis.Tests.csproj.proposed) | One added project reference to the fixture |

The fixture is test infrastructure and is not a published package. It references
the existing Redis gateway; it adds no package references or versions. The parent
uses its own runtime configuration and dependency manifest to execute the referenced
fixture on the same framework. A child-process failure becomes an ordinary xUnit
assertion, preserving the parent test runner and unrelated cases. Native cleanup is
owned by the parent so it also runs after a child crash. The child has a 45-second
scenario bound; the parent allows 70 seconds for startup/exit and terminates a stuck
child tree. Existing external process and hang bounds remain in place.

## Four cases and observable contract

The cases cover sync/async × missing retry alias/wrong event type. A correctly
registered ordinary publication is present as a fallback decoy. The invalid retry
publication has either the right event type with a different alias, or the right
alias with a different event type. The incoming message is received from native Redis;
its body is revised before scheduling through the subscription-selected retry path.

1. A five-second retry is accepted without an early send or outbox deposit. The
   caller remains unchanged. The child emits an acceptance marker only after those
   preconditions pass.
2. On callback lookup failure, the process survives. The scheduler logs one Error
   or higher event containing the original `ConfigurationException` and scheduler
   ID. Exactly one outstanding message retains its application ID, revised body,
   topic, event type, handled count, marker and local publication selector. No
   native counter, retry delivery or ordinary-publication fallback is allowed.
3. Explicit clearing through the still-invalid registry continues to throw
   `ConfigurationException` to its caller. Containment belongs to the asynchronous
   timer boundary; registry and direct-dispatch contracts remain visible.
4. A valid message scheduled through the same scheduler with the same scheduler ID
   must be accepted and delivered. This proves failed-job cleanup and continued
   operation. The valid message settles independently of the retained failure.
5. A new correctly configured registry/mediator clears the same in-memory outbox.
   One composed/default payload reaches only the selected queue, with matching
   metadata, correct TTL and no local selector on the wire. The failed message
   settles, and the caller and original native payload remain unchanged.

The time provider delegates to `TimeProvider.System.CreateTimer` with the original
callback state, due time and period. It signals completion only after the real
callback returns normally. It does not catch callback exceptions, fake time, invoke
private methods or replace scheduler/handler/registry behavior. The log collector
uses the public logging provider extension inside the child, avoiding shared test
process logging state. No new request or handler types are registered.

Recovery here means rebuilding application configuration around a retained
in-memory outbox within one surviving process. It is not persistence across restart,
hot mutation of an existing registry, automatic retry, or an exactly-once guarantee.

## Proposed production scope after behavioral RED

Add exception containment to the `FireSchedulerMessage` branch of
`InMemoryScheduler.Execute`: log callback exceptions with their original exception
and scheduler ID, and always invoke the existing generation-aware cleanup in
`finally`. Preserve the successful dispatch path and cleanup's generation checks.
Add public scheduler XML remarks explaining message-callback logging, lack of an
automatic retry, and the need to recover already-deposited messages via the outbox.

This is a shared Core change, explicitly included for review. Do not catch exceptions
in `ProducerRegistry.LookupBy`, change outbox settlement, bypass invalid selection,
fallback to the ordinary publication, redesign public APIs, or alter native Redis
mapping. Request callbacks (`FireSchedulerRequest`) are a separate behavior and are
not changed or claimed covered by these four message cases.

## Validation after approval

Record unchanged Core and InMemory project baselines on .NET 9 and .NET 10. Adopt
the reviewed fixture and parent test exactly; restore the new project references
using existing central package versions. Build the fixture through the Redis test
project and run all four native parent cases on each framework before production
changes. Require each intended RED to reach the child's acceptance marker and fail
the parent exit assertion, with an unhandled `ConfigurationException` from the real
`ProducerRegistry` → scheduler callback path in captured diagnostics. A missing host,
dependency-loading failure, setup failure, timeout or unrelated exception is not RED.

Implement the scoped message-callback correction and require four passes on each
framework. Then independently apply these named production mutations on .NET 10:

| Mutation | Required failure | Cases |
| --- | --- | ---: |
| Rethrow the caught message-callback exception | Parent exit assertion, with accepted scenario and uncaught lookup exception | 4 |
| Omit failed-message cleanup while retaining successful cleanup | Child reports `A completed failed job must release its scheduler ID.`; parent exit assertion | 4 |
| Suppress the scheduler's failure log while retaining containment and cleanup | Child reports `Failed callback must log its original configuration exception and scheduler ID once.`; parent exit assertion | 4 |

Require twelve intended assertion failures, exact production-byte restoration after
each mutation and four restored passes. Preserve existing uncommitted work through
snapshots; never restore files against repository HEAD. Return semantic fixture or
contract changes for review before executing them.

Finish optimized focused runs on both frameworks, all Core and Redis library targets,
and full Core/InMemory regressions on both frameworks with prior outcomes retained.
Complete disjoint native Redis batches must preserve all 198 preceding identities
and add four passes: 198 passes and four existing skips across 202 identities per
framework. Keep exact outcome/identity comparisons, source and assembly manifests,
native command-count measurements, conservative free-quota allocation, and real
3,605-second idle intervals after focused Redis work and between Redis batches.
No license, quota reset, clock manipulation, dependency removal or test removal.
The child architecture is for host-failure containment, not a quota workaround.

Retain the pinned native Redis/SDK runners, no published host ports, owned cleanup
and failed-run evidence. Keep the laptop lid open during native tests. Passing this
package does not close request-callback failures, other scheduler failures,
cancellation, durable persistence or the wider feature release gates.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require **“STOP after writing the test and ASK FOR APPROVAL”** and prohibit running
new tests without approval. This is the concrete
[/test-first](../../../../.claude/commands/tdd/test-first.md) package for that review.

Approval covers these four parent cases, the child fixture and one test-project
reference, the scoped shared Core message-callback fix/XML remarks, twelve mutation
failures/restorations, native focused and paced regressions, Core/InMemory regression
checks, builds, cleanup and local evidence updates. It excludes commits, pushes,
hosted CI, deployment, new packages and broader scheduler redesign.


## Compilation correction (2026-11-06)

Initial build exposed a `RedisSubscription` naming collision with ServiceStack and
missing nullable context in the executable fixture. The fixture now qualifies the
Brighter subscription type and enables nullable annotations in its project. Test
behavior, assertions and runtime bounds are unchanged. The failed build is retained
as setup evidence, not behavioral RED; original reviewed bytes remain preserved.


## Scheduled Redis retry registration: reproduced failure and initial fix (2026-11-06)

All four approved sync/async × missing-alias/wrong-type cases reproduce native retry
acceptance followed by an unhandled ConfigurationException from ProducerRegistry
escaping the production InMemoryScheduler callback. Each child process exits 134;
the parent xUnit process survives and fails at its exit assertion. This occurs on
both .NET 9 and .NET 10. Evidence is under
`/private/tmp/brighter-4170-redis-scheduled-retry-registration/initial-red-*.trx` and
`red-verification.json`.

The message-callback branch now logs the original exception with its scheduler ID
and guarantees existing generation-aware cleanup in finally. All four initial green
cases pass per framework: retained message data, direct dispatch errors, scheduler-ID
reuse, later healthy dispatch and explicit recovery through corrected configuration.
Request callbacks and public signatures are unchanged.

Unchanged full baselines pass 1,716 Core cases/seven existing skips and 167 InMemory
cases on each framework. A sandbox pipe failure and then a temporary output-location
failure were retained as setup evidence; repository-local output resolves the Core
architecture checks without changing them. The fixture's ambiguous subscription type
and nullable project context were corrected without changing assertions or behavior.

Mutation validation and final optimized/build/regression checks are in progress.
Rethrowing the callback and omitting failed-job cleanup each produce four intended
parent assertion failures with exact restoration and four restored passes. The
log-suppression mutation's first build had an unused-variable error; it is retained
as compilation evidence, not behavioral RED. Its catch binding is corrected and
validation resumes without rerunning completed mutations. No complete Redis
regression result or whole-feature production readiness is claimed yet.


## Scheduled Redis retry registration: final focused validation (2026-11-06)

All four approved cases pass on both frameworks in final optimized builds. All twelve
named mutation failures are verified, with exact restorations and four passing cases
after each. Eight Core/Redis library target builds pass with zero warnings/errors.
Full post-fix Core suites retain 1,716 passes/seven existing skips and InMemory retains
167 passes on each framework; every baseline identity and outcome is preserved.
All 6,528 other preceding C# files remain unchanged, and fixture/project changes match
the approved scope plus the recorded non-behavioral compilation corrections.
The focused native broker is removed.

The complete Redis regression schedule has 4 disjoint paced
batches, first eligible at `2026-11-06T08:53:15.366933+00:00`. Expected coverage is 202
identities exactly once per framework, with 198 passes/four existing skips and every
preceding outcome retained. These results remain pending. Allocation uses measured
native command counts plus a conservative allowance; real 3,605-second idle gaps,
source/assembly pinning, owned cleanup and stop-on-failure are enforced. Child
processes isolate callback failures and do not change the quota/pacing contract.

Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-registration/focused-verification.json`.
Live schedule: `/private/tmp/brighter-4170-redis-scheduled-retry-registration/paced/status.json`.
Successful completion independently verifies all identities/outcomes, input hashes,
real waits and cleanup before writing completion reports. Keep the laptop lid open
for native runs; idle-sleep prevention cannot prevent lid sleep. This establishes no
unrun CI or whole-feature production readiness.


## Scheduled Redis retry registration failure containment completion (2026-11-06)

Four approved sync/async × missing-alias/wrong-event-type cases reproduce an accepted
native retry followed by an unhandled registry ConfigurationException terminating
its child process with exit code 134, on both frameworks. The parent xUnit process
survives and preserves the actual callback diagnostics. The initial ambiguous
RedisSubscription type and nullable-context build issues were corrected without
changing assertions or behavior; the failed build and original reviewed files are
retained separately from behavioral RED.

InMemoryScheduler's message-callback branch now logs dispatch exceptions with the
scheduler ID and always performs generation-aware cleanup. Direct invalid-registry
clearing still throws to callers. The failed message remains in the outbox, the same
job ID accepts later healthy work, and a correctly configured registry explicitly
recovers the original payload to its selected native queue. Only InMemoryScheduler.cs
changes production behavior; request callbacks and public signatures are unchanged.

Three independent mutations produce twelve intended parent assertion failures:
rethrowing the callback error, omitting failed-job cleanup, and suppressing the error
log. Exact restorations each return four passes. Initial/final optimized native checks
pass four cases per framework. All eight Core/Redis library target builds have zero
warnings/errors. Full Core regressions retain 1,716 passes/seven existing skips and
InMemory retains 167 passes on each framework, with every baseline outcome preserved.
The initial Core output-location failure is retained as setup evidence; moving build
output under the ignored repository bin directory restores the architecture checks
without altering tests.

Complete paced Redis regressions pass **198 cases, zero failures and four existing
skips per framework**, covering each of 202 identities exactly once and preserving
all 198 preceding outcomes. Real idle intervals are at least 3,605 seconds, no quota
or host clock errors are reported, and owned containers are removed. This is complete
batched coverage, not a single-process full-suite claim. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-registration/paced/final-verification.json`.

All 6,528 other preceding C# files remain unchanged. Additions are the four-case test
and four child-fixture sources, one fixture project and one test-project reference.
The compiled fixture matches its recorded effective contents after the two routine
compilation corrections; no package versions or test semantics changed.

This establishes message-callback containment and explicit in-process outbox recovery.
Request-callback failures, submission/native-send errors, cancellation, durable
persistence, broader transport checks and final release gates remain open. No hosted
CI, commit, push, deployment or whole-feature production readiness is claimed.
