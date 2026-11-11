# Redis rejection boundaries

Status: approved, characterized and validated on 2026-10-30.

Review [the twelve cases](When_redis_rejection_uses_fallback_or_encounters_native_failure_should_preserve_routing_and_release_the_consumer.cs).
They reuse the approved [rejection writer helper](../redis-rejection-writers/RedisRejectionWireWriter.cs).

## Contract and behavior

Twelve sync/async × six-path cases characterize the existing boundaries around
configured rejection writers:

| Path | Required observation |
| --- | --- |
| Unacceptable with only a DLQ, null DLQ writer | Legacy native envelope reaches the DLQ. |
| Unacceptable with only a DLQ, custom DLQ writer | The DLQ writer's exact string reaches the DLQ; the invalid writer is not called. |
| Neither channel configured | Reject returns true, calls neither writer, leaves the application topic unchanged and does not add originalTopic metadata. |
| DeliveryError with only an invalid channel | Reject returns true and calls neither writer; metadata is refreshed but nothing is published. |
| Invalid-channel native counter contains a noninteger | Mapping runs once, native INCR fails, Reject retains its existing true/no-throw behavior, publishes nothing and releases the in-flight gate. |
| Dead-letter native counter contains a noninteger | The same native-error contract applies through the DLQ writer. |

Native observation verifies exact fallback strings/counters/queues, unchanged
noninteger counters, absent destination payloads and no wrong-route publication.
The source queue retains its second ID until Reject completes. The same consumer
then reads and acknowledges that valid message and reaches an empty receive.
Stored source payloads remain unchanged.

These tests explicitly characterize existing native-error behavior: a true result
does not guarantee forwarding when Redis publication fails. They do not turn Redis's
destructive queue pop into a reliable/redeliverable delivery. Mapping errors retain
the separately tested retry contract. Other native failure windows, cancellation,
concurrency, process failure, fallback mapping failure, per-endpoint rejection
selection isolation and arbitrary binary/multiline forwarding remain separate.

## Results

The unchanged approved test passed all twelve cases initially on both frameworks.
No production correction was needed. Each of the four independent .NET 10 mutations
produced exactly four intended assertion failures and eight unaffected passes.
Diagnostic rebuilds and JIT settings verified the exact reviewed assertion lines.
All four byte-exact restorations returned twelve passes. The approved test, helper,
assertions and time bounds were unchanged.

A clean optimized rebuild and ordinary JIT then passed all twelve focused cases on
both frameworks. Both full optimized suites ran once on separate fresh brokers:
170 passed, zero failed and four existing skips each. All 162 preceding identities
and outcomes were retained with twelve new passes. Four library targets build with
zero warnings/errors. Initial and final test dependency builds report 604 existing
warnings, none in the new file. Hosted CI was not run.

All 6,521 preceding C# files remain byte-identical. Only the approved test was added;
the reused helper is unchanged. Dependency, lockfile and workflow hashes are unchanged.
All three owned brokers were removed after saving logs; unrelated containers were
untouched. Evidence is retained in `/private/tmp/brighter-4170-redis-rejection-boundaries/`.

The next [four-case rejection-writer isolation draft](../redis-rejection-writer-isolation/README.md)
checks captured selections across three endpoints and repeated forwarding through
both routes. It reuses the same helper and remains outside the compiled suite,
unbuilt/unrun pending its own review.

## Approved execution plan (completed)

Adopt the test unchanged and reuse the compiled helper unchanged. Current behavior
is expected to pass. Establish behavioral RED with these independent production-only
.NET 10 mutations:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Select the invalid-message writer for Unacceptable even when routing falls back to the DLQ | Exact native fallback payload in both fallback modes | 4 |
| Invoke an invalid-message writer when no destination is eligible, without publishing | Invalid writer call count remains zero in no-channels/no-dlq modes | 4 |
| Propagate native SendMapped/SendMappedAsync errors instead of keeping the current catch behavior | Rejection error must be null in both native-error modes | 4 |
| Keep the source in-flight entry after native publication failure | Recovery receive error must be null in both native-error modes | 4 |

Require sixteen intended failures; each mutation's other eight cases must pass.
The first mutation must only change fallback writer selection, preserving native
invalid/dead-letter selection. The second must preserve the absence of native
publication so it reaches the writer-call assertion. Setup/compiler/timeout failures
do not count. Use diagnostic rebuild/JIT settings if needed for exact assertion
locations, without changing tests. Restore all production bytes exactly after each
mutation and require twelve passes. If initial behavior differs, retain evidence
and confine any correction to these reviewed boundaries.

Finish with focused passes on both frameworks and ordinary optimized full suites,
all four library targets built, and all 162 preceding identities/outcomes retained
plus twelve new passes. Expected complete matrices are 170 passes/four existing
skips each. Run full suites once on separate fresh ready brokers; preserve and
investigate failures without retrying behavioral regressions until green.

Use the pinned Redis image and native Docker SDK runners, no host ports, unique
keys and owned cleanup. Async operations use a 45-second token; receives request
one second. External process/hang limits bound sync/native calls and disposal.
Save logs before removing owned containers. No dependency/workflow or public
signature changes are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
twelve cases, reuse of the unchanged approved helper, any evidenced minimum boundary
correction, sixteen reviewed mutation failures with exact restorations, native Docker
validation, full affected regressions, four library builds, owned cleanup and local
reports. Hosted calls, commits, deployment and publication are excluded. The compiled
test matches the approved draft and reuses the unchanged approved helper. Further
isolation and lifecycle tests require their own review.
