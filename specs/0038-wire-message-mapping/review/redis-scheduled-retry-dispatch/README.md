# Redis scheduled retry dispatch

Status: approved and complete (2026-11-02) under the user-approved paced regression plan.
Both frameworks pass 182 cases/four existing skips, with all preceding outcomes retained.
The preceding fallback recovery batch is complete: 178 passes/four existing skips
per framework under the approved paced regression plan.

Review [the four cases](When_a_redis_retry_is_scheduled_should_map_at_dispatch_through_the_registered_publication.cs).
The existing [publication writer](../../../../tests/Paramore.Brighter.Redis.Tests/TestDoubles/RedisPublicationWireWriter.cs)
and [immediate retry writer](../../../../tests/Paramore.Brighter.Redis.Tests/TestDoubles/RedisRecoverableRequeueWireWriter.cs)
are reused unchanged. No package, lockfile, project reference or workflow change is proposed.

## Contract being reviewed

This batch characterizes the existing scheduler/publication path before attempting
any cross-scheduler retry-selection redesign. `RedisSubscription.RequeueWireMessageWriter`
is documented for immediate retries. Positive-delay retries schedule an application
message; the production callback resolves its registered publication and maps at
dispatch. A publication writer is therefore the explicit configuration under test.

This does **not** implement or sign off per-subscription delayed writer selection,
queue-specific scheduled retry routing, durable scheduler persistence, callback
failure recovery, concurrent configuration changes or dispatch-time telemetry.
Those broader issue requirements remain open. In particular, successful results
must not be used to mark every delayed-retry mapping task complete.

## Public production path and assertions

Four cases cover sync/async requeue × custom/default dispatch mapping. They use a
native Redis broker and public consumer/producer factories, the real
`InMemorySchedulerFactory`, `CommandProcessor`, `FireSchedulerMessageHandler`,
outbox mediator and in-memory outbox. No scheduler spy, mock processor, generated
pump or manual callback invocation is used. The real scheduler waits five seconds.
No virtual clock or quota-related time/counter changes are involved.

The native observer seeds source IDs 1 and 2, counter 41, and source/sibling recipient
queues. After receiving ID 1, the application revises its body and schedules it.
The subscription immediate-retry writer is a decoy. A separate publication writer
is configured at deposit, then replaced by the selected custom writer or null
immediately after scheduling. This specifically checks selection at dispatch.

Before the due time, all writers must remain unused, the counter and source payloads
remain unchanged, no retry is visible, and the outbox is empty. The same consumer
must receive and acknowledge ID 2 and complete an empty receive before the delay.
A stopwatch assertion rejects an invalid before-delay observation on an overloaded
or suspended host rather than silently accepting it as timing evidence.

The real scheduler callback must publish exactly native ID 42, with one entry in
each recipient queue, configured expiry and no mutation of either original stored
payload. Custom mapping must equal `W:` plus the revised Unicode body. Default
mapping must parse with the legacy reader and retain the original application ID,
topic, revised body and marker. The selected writer runs once only for custom
mapping; deposit and immediate-retry writers remain unused. The outbox must settle.
The fan-out/new-native-ID assertions preserve the current delayed-publication
behavior; they do not claim immediate-retry queue isolation.

A 45-second token bounds async work and native observation, with one-second receive
requests and the established external 1,000-second process/two-minute hang bounds.
Scheduler disposal precedes native-key cleanup. Use temporary host idle-sleep
prevention, keep the lid open, record clock interruptions and retain failed evidence.

## Approved execution requested

Adopt the draft unchanged; do not alter the reused helpers. Initial behavior is
expected to pass. Establish behavioral RED with these independent .NET 10 mutations
in Redis production code only:

| Mutation | Intended assertion failures | Count |
| --- | --- | ---: |
| Ignore the publication writer in the two ordinary dispatch send paths | Custom native string must equal `W:` plus revised body; default cases still pass | 2 |
| Retain the in-flight entry when accepting a positive-delay retry in both consumer paths | Receive of the next source message must have no error | 4 |
| After ordinary dispatch mapping succeeds, publish an empty string | Native ID 42 must contain a nonempty string | 4 |

Require ten intended failures and four passes after each exact source restoration.
Setup, compiler, callback crash and timing failures do not count as behavioral RED.
If observed behavior differs, retain evidence and return semantic contract/test
changes for review; do not widen this batch into a shared scheduler redesign.

Finish with ordinary optimized focused runs on both frameworks, four Redis library
targets built and complete disjoint paced regression coverage. Preserve all 182
preceding case identities/outcomes and add four passes: expected totals are
182 passes/four existing skips per framework. Use fresh pinned native Redis brokers,
Docker SDK runners, no host ports, unique keys and only owned-container cleanup.
Maintain at least 3,605 real seconds between regression batches and after focused
runs. Verify each of the 186 cases exactly once per framework in the completed
regression attempt. Preserve interrupted attempts separately. Do not supply a license
or change tests/dependencies to avoid the quota.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". This draft follows the
[/test-first workflow](../../../../.claude/commands/tdd/test-first.md) in review-before
mode. Approval covers the four reviewed cases, unchanged helper reuse, native
execution, ten named mutation failures/restorations, focused and paced regressions,
four library builds, cleanup and local evidence. Any evidenced minimum correction
must preserve the reviewed publication-selection contract. Shared scheduler/API
redesign, hosted calls, commits, deployment and publication are outside this batch.


## Focused execution complete; paced regressions pending

The user approved this batch. The test was adopted unchanged; both reused helpers
remain byte-identical. Initial and final ordinary optimized runs each pass four
cases on .NET 9 and .NET 10. Three independent production-only mutations produce
all ten reviewed assertion failures; each exact restoration returns four passes.
No production correction is needed. All 6,524 preceding C# files and eight protected
dependency/workflow inputs retain their snapshot hashes. Four Redis library targets
build with zero warnings/errors. Test dependency rebuilds retain 604 existing
warnings, with none in the new test. The owned focused broker is removed.

Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-dispatch/`. The complete
regression schedule and status are in `paced/plan.json` and `paced/status.json`.
The first batch is eligible at 2026-11-01T21:12:23Z, then each subsequent batch waits
at least 3,605 real seconds after the preceding batch finishes. Each framework has
an 89-case batch and a 97-case batch, with expected combined totals of 182 passes
and four existing skips. Temporary idle-sleep prevention and host clock-discontinuity
checks apply; the Mac lid must remain open. The batch is not complete until all
186 cases per framework and all prior outcomes are independently verified.


## Redis scheduled retry dispatch completion (2026-11-02)

Four approved sync/async × custom/default cases pass through native Redis and the
production in-memory scheduler, command processor, scheduler handler and outbox
mediator. They establish publication writer selection at dispatch, delayed native
visibility, next-message progress, native counter/recipient fan-out, expiry and
preservation of original native payloads. No production correction is needed.

Three production-only mutations produce ten intended assertion failures: two
custom-payload failures when the publication writer is ignored (two default cases
still pass), four next-receive failures with a retained in-flight entry, and four
empty-native-string failures. Each exact restoration returns four passes. Final
optimized focused runs pass four cases per framework; all four Redis library targets
build with zero warnings/errors. Approved tests/helpers and all 6,524 preceding C#
files and protected inputs are unchanged.

Complete paced regressions pass **182 cases, zero failures and four existing skips
per framework**. All 186 cases run exactly once per framework, preserving all 182
prior outcomes. Real idle intervals are at least 3,605 seconds, with no quota or
clock-discontinuity errors; owned containers are removed. This is complete batched
coverage, not a single-process full-suite pass. Evidence and independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-dispatch/paced/final-verification.json`.

This closes registered-publication dispatch characterization only. Per-subscription
delayed writer selection, queue-specific scheduled retry routing, callback failure
recovery, durable schedulers and the wider feature release gates remain open.
No hosted CI, commit, push, deployment or publication is claimed.
