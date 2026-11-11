# Redis rejection fallback recovery

Status: approved and complete (2026-11-01) under the user-approved paced regression plan.
Both frameworks pass 178 cases/four existing skips, with every prior outcome retained.

Review [the four cases](When_redis_rejection_falls_back_to_a_dlq_writer_should_preserve_the_source_on_mapping_failure_and_recover.cs).
They reuse the unchanged approved [rejection writer helper](../redis-rejection-writers/RedisRejectionWireWriter.cs).

## Contract and behavior

Four sync/async × throw/null cases reject an unacceptable message with only a DLQ
configured. A recoverable DLQ writer fails on its first call and succeeds on its
second. The configured invalid-message writer and retry writer are decoys. Native
recipient queues exist for both routes, so accidental forwarding is observable.

The initial fallback mapping failure must propagate the original exception instance
or throw InvalidOperationException for null. It must restore the application topic,
preserve application ID/body/marker and retain the in-flight entry. Neither route
may acquire a counter, payload or queued ID. The next source ID remains queued and
another receive must remain blocked. The same source can then be rejected again:
exactly one native DLQ payload with the selected writer's string is published, with
the configured expiry and refreshed original-topic/rejection metadata. The invalid
route remains untouched. Only the DLQ writer is called, exactly twice. The same
consumer then receives and acknowledges its next original delivery and becomes empty.
Both original native source payloads remain unchanged.

The prior mapping-failure cases configure both destinations and use DeliveryError
for their DLQ route. These cases exercise Unacceptable's fallback with no invalid
destination. They do not establish atomic forwarding, native failure recovery,
concurrency, cancellation, delayed retry mapping or crash redelivery. Payloads are
single-line Unicode.

## Execution requested

Adopt the test unchanged and reuse the helper unchanged. Current behavior is
expected to pass. Establish behavioral RED through four independent production-only
.NET 10 mutations:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Select the invalid writer when Unacceptable falls back to the DLQ | Mapping error must be present | 4 |
| Leave the destination topic on the source when mapping fails | Source topic must equal the original application topic | 4 |
| Remove the in-flight entry before rejection mapping | Receive after mapping failure must throw ChannelFailureException | 4 |
| Publish an empty string after successful mapping | Exact native DLQ string after the second rejection | 4 |

Require sixteen intended failures. The last mutation must preserve the original
mapping failure and only replace the mapped string at native publication. Setup,
compiler and timeout failures do not count. Diagnostic rebuild/JIT settings may
establish exact assertion lines without test changes. Restore production bytes
exactly after each mutation and require four passes. If initial behavior differs,
retain evidence and confine any correction to the reviewed fallback contract.

Finish with focused passes on both frameworks, ordinary optimized batched regression coverage and
all four library targets built. Preserve all 178 preceding identities/outcomes and
add four new passes: expected matrices are 178 passes/four existing skips each.
Use complete, disjoint batches with fresh ready brokers and at least one real hour
between batches, following the user-approved free-quota pacing. Verify every case
identity/outcome exactly once per framework. Retain and investigate failures without
retrying behavioral regressions until green. Report this as batched coverage.

Use the pinned Redis image, native Docker SDK runners, no host ports, unique keys,
45-second async tokens and one-second receives. External process/hang limits bound
sync/native calls and disposal. Save logs and remove only owned containers. No
dependency/workflow or public signature changes are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
four cases, unchanged helper reuse, any evidenced minimum fallback correction,
sixteen reviewed mutation failures with exact restorations, native Docker validation,
full affected regressions, four library builds, owned cleanup and local reports.
Hosted calls, commits, deployment and publication are excluded. The user explicitly
approved this batch. The compiled test and reused helper remain byte-for-byte
identical to their approved versions.

## Execution evidence

Evidence is retained in `/private/tmp/brighter-4170-redis-rejection-fallback-recovery`.
Initial focused matrices are four passes per framework. The four reviewed mutations
each fail all four cases at the intended assertion, followed by four restored passes.
No production correction is needed. The full batched regression gate passed; every case identity/outcome is verified.

Final ordinary optimized focused runs pass four cases per framework. All four library
targets build with zero warnings/errors. The test dependency rebuild has 604 existing
warnings and no errors, with no diagnostic attributed to the new test. All 6,523
preceding C# files and eight protected dependency/workflow inputs match their snapshots.
The owned focused broker is removed. The regression schedule is in `paced/plan.json`
and records status in `paced/status.json`. It starts no earlier than
2026-10-31T23:17:46Z and waits at least 3,605 real seconds between batches. Its first
batches cover 89 cases each; second batches cover 93 each, including all four new cases.
Each framework must finish with 178 passes/four existing skips across 182 unique cases.


## Redis fallback regression host interruption (2026-11-01)

The first paced .NET 9 batch stopped with 86 passes, two existing skips and one
failure in the existing synchronous rejection-boundary `no-channels` case. No
quota error was reported; both owned containers were removed. Later batches did
not run. The new fallback recovery cases were not part of this first batch.

The failed case spans 00:24:45–01:24:42 UTC in TRX wall timestamps but reports only
307 ms of execution. It fails when rereading an original source payload seeded with
a ten-minute expiry. Redis reports 37 expired keys. macOS power logs confirm repeated
sleep periods in this interval. This supports expiry during host suspension; it does
not establish a production regression. The failed TRX, native logs, and diagnosis
remain in `/private/tmp/brighter-4170-redis-rejection-fallback-recovery/paced` and
`sleep-interruption-diagnosis.json` in its parent directory.

After investigation, all four unchanged batches are rerunning from scratch under
`paced-awake/`, using identical verified assemblies, a temporary `caffeinate -i -s`
assertion and host-clock discontinuity detection. The lid must remain open; this
assertion cannot override lid closure. Real waits of at least 3,605 seconds remain.
No production, dependency, test, assertion, expiry or timeout changes were made.
Completion is still pending; the failed attempt is not counted as a passing gate.

The first sleep-protected rerun batch passed 87 cases/two existing skips, including
the previously failing unchanged case. Runner wall/monotonic durations both measure
96.45 seconds, with no clock discontinuity or quota error. Owned containers are
removed. Three scheduled batches remain; this is not a completed regression gate.


## Redis fallback recovery completion (2026-11-01)

The approved four sync/async × throw/null fallback cases pass initially and after
restoration on both frameworks. Four independent production-only mutations produce
sixteen failures at the reviewed assertions; each exact restoration returns four
passes. No production correction is needed. Final optimized focused runs pass four
cases per framework, and all four library targets build with zero warnings/errors.
The test dependency rebuild retains 604 existing warnings, none in the new test.

The user-approved paced regressions finish with **178 passes, zero failures and four
existing skips per framework**. Every one of the 182 cases runs exactly once per
framework; all 178 prior outcomes are retained and the four new cases pass. Real
idle periods before the batches exceed one hour. No quota errors or license were
needed; owned containers are removed. All 6,523 preceding C# files, eight protected
inputs, approved test/helper bytes and final regression assemblies retain their
recorded hashes. This establishes complete batched coverage; it does not claim a
single-process full-suite pass. The fallback batch is complete; delayed retries,
broader lifecycle coverage and the full feature's other release gates remain open.

Evidence: `/private/tmp/brighter-4170-redis-rejection-fallback-recovery`, with the
independent final result in `paced-awake/final-verification.json`. No hosted CI, commit,
push, deployment or publication is claimed.
