# Redis immediate requeue writer

Status: approved, implemented and validated on 2026-10-30.

Review [the ten cases](When_a_redis_requeue_writer_is_selected_should_preserve_native_controls_and_recover_from_mapping_failure.cs)
and [the recoverable retry writer](RedisRecoverableRequeueWireWriter.cs).

## Contract and behavior

RedisSubscription.RequeueWireMessageWriter will optionally select an
IAmAWireMessageWriter<string> for immediate (zero/null delay) requeue. Null selection
retains legacy serialization. The consumer factories capture the selection per
consumer, independently of publication writers and the subscription reader.
Existing public constructor signatures remain compatible.

Ten cases cover sync/async × legacy/custom/composed/throw/null modes. A native
observer seeds a legacy delivery at source ID 1, counter 41 and a sibling recipient.
The consumer reads it through the public factory, then changes its body for retry.
The application topic deliberately differs from the native source topic.

A successful immediate retry must replace the source payload at the same native ID,
enqueue that ID only on the original queue, retain counter 41, refresh configured
expiry, and release the in-flight gate. It must not fan out to sibling queues,
allocate a new native ID or store under the application topic. The exact legacy,
custom or composed native string is independently asserted. Writers leave the
application message's ID, topic, revised body and marker unchanged.
Each legacy oracle conversion uses a fresh RedisMessagePublisher because that
public serializer accumulates output across calls on the same instance.

A thrown writer error must propagate as the original instance; null must throw
InvalidOperationException. Before native mutation, both must preserve the old
stored string, empty source/sibling queues, counter and in-flight gate. Retrying
the same message through the same consumer must then succeed with the configured
writer. There is no default fallback or silent success.

The observer removes the retried native ID after inspecting its content. It then
seeds a separate valid legacy message: the same consumer must receive and acknowledge
that message and reach an empty receive. This proves gate release without claiming
that a default reader understands arbitrary custom retry formats.

This batch covers immediate requeue only. Delayed/scheduler-backed retry retains
its existing selection path pending separate implementation and tests. Native
failures after partial progress, atomic publication/visibility to competing
consumers, cancellation, endpoint isolation, concurrent mapping, retry-format reader
compatibility and rejection-writer selection remain separate.

## Results

Both initial builds reported the expected missing RequeueWireMessageWriter property
(CS0117). After adding only the reviewed public auto-property, both frameworks
produced eight intended behavioral failures: custom/composed native strings differed
in four cases, and throw/null modes silently succeeded in four cases. Both legacy
cases passed. Compilation failure was recorded separately from behavioral RED.

Both factories now capture the retry writer per consumer. Immediate retries map and
validate before storing the replacement, enqueueing the existing native ID and
removing the in-flight entry. Null selection uses the default adapter; thrown errors
remain unchanged and null results throw InvalidOperationException. Existing public
constructors and delayed retry behavior are unchanged.

All ten approved cases pass unchanged on .NET 9 and .NET 10. Four independent
mutations produced all 32 intended assertion failures, with the expected unaffected
cases passing. Each exact restoration returned ten passes; final focused results
pass on both frameworks. No fixture, assertion, helper or bound changes were needed.

Both unfiltered complete suites passed 134 cases with four existing skips, retaining
all 128 preceding identities/outcomes plus ten new passes. All four library targets
built with zero warnings/errors. Initial and green dependency builds each reported
604 existing warnings, none in new/changed files; the final restored build was clean.

Of 6,515 preceding C# files, 6,512 remain byte-identical. Only the consumer, factory
and subscription changed, with the two approved compiled test/helper additions.
Dependency, lockfile and workflow hashes are unchanged. All three owned brokers were
removed after saving logs; unrelated containers were untouched. TRX, logs, mutation
snapshots, source verification and cleanup evidence are retained in
`/private/tmp/brighter-4170-redis-immediate-requeue-writer/`.

The next [retry-writer isolation draft](../redis-requeue-writer-isolation/README.md)
checks per-consumer selection capture and repeated native payloads across three
subscriptions sharing a factory. It remains outside the compiled suite, unbuilt/unrun.

## Approved execution plan (completed)

Adopt both files unchanged. The missing public selection property is expected to
cause an initial compile failure; record it but do not count it as behavioral RED.
Add only the nullable public auto-property with contract XML documentation to make
the tests compile, leaving selection unused. Then run both frameworks to establish
eight intended native/mapping failures with the two legacy cases passing.

Implement the minimum factory/consumer wiring and immediate mapping boundary.
Mapping and null validation must precede queue/storage mutation and in-flight
removal. Keep existing native routing, ID reuse, expiry and public constructors.
Require all ten cases to pass on both frameworks before mutations.

Apply these independent production-only .NET 10 mutations:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Ignore the configured retry writer | Custom/composed native stored string (4); throw/null error presence (4) | 8 |
| Move the single enqueue operation before invoking the selected writer | Empty source queue immediately after mapping failure in throw/null cases | 4 |
| Skip replacement of the source stored payload but enqueue and report success | Exact native retry string in all modes | 10 |
| Keep the source in-flight entry after a successful retry | Recovery receive error must be null in all modes | 10 |

Require 32 intended assertion failures. In the first mutation the two legacy cases
must pass; in the second the six non-failing modes must pass. Setup/compiler/timeout
failures do not count. Restore production bytes exactly after each mutation and
require ten passes. Keep reviewed cases, assertions and bounds unchanged.

Finish with focused passes on both frameworks, all four Redis library targets built,
and both full suites preserving all 128 preceding identities/outcomes plus ten new
passes. Expected complete matrices are 134 passes/four existing skips each. Run
each full suite once on a separate fresh ready broker; retain and investigate
failures without retrying behavioral regressions until green.

Use the pinned Redis image, native Docker SDK runners, no host ports, unique keys
and owned cleanup. Async operations use a 45-second token; receives request one
second. External process/hang limits bound native sync calls and disposal. Save
logs before removing owned containers. No dependency/workflow changes are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
ten cases, the minimum public selection property needed to compile and observe RED,
evidenced factory/consumer changes and documentation, 32 reviewed mutation failures
with exact restorations, native Docker validation, full affected regressions, four
library builds, owned cleanup and local reports. Hosted calls, commits, deployment
and publication are excluded. The compiled test/helper match the approved draft.
Further isolation and lifecycle tests require their own review.
