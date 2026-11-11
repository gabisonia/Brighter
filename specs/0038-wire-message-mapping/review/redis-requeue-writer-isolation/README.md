# Redis retry-writer isolation

Status: approved, characterized and validated on 2026-10-30.

Review [the four cases](When_redis_subscriptions_select_retry_writers_should_capture_each_selection_and_isolate_repeated_payloads.cs)
and [the endpoint writer](RedisEndpointRequeueWireWriter.cs).

## Contract and behavior

Four sync/async × legacy-first/custom-first cases create three consumers through
one factory: legacy, custom and composed retry mappings. All consumers are created
before all subscription writer settings are replaced with a decoy. Each consumer
must retain the selection captured at creation, including a null/default selection.

Each endpoint receives and immediately requeues two distinct application messages.
An independent Redis observer verifies each exact stored legacy/custom/composed
string, reused native IDs, queue ordering and absence of a publication counter.
It removes the retry from the end of the source list while preserving the next
original delivery at the front. The same consumer must receive that next original,
retry it, and finish with an empty receive. Each selected writer is called twice;
the decoy is never called. Every legacy oracle conversion owns a fresh serializer.

These four cases exercise 24 immediate native requeues across different endpoint
orders. They establish sequential selection and payload isolation and guard against
accumulating serializer output. They do not claim concurrent consumer/serializer
safety, custom-format retry-reader compatibility, delayed retries, rejection mapping,
cancellation, native failure recovery or crash redelivery.

## Results

All four unchanged approved cases passed initially on .NET 9 and .NET 10. No
production correction was needed. Three independent .NET 10 mutations produced
twelve intended native-string assertion failures. Every byte-exact restoration
returned four passes; final focused results pass on both frameworks. No approved
fixture, helper, assertion or time-bound changes were needed.

Both unfiltered complete suites passed 138 cases with four existing skips, retaining
all 138 preceding identities/outcomes and adding four passes. All four library
targets built with zero warnings/errors. Initial and final restored dependency
builds each reported 604 existing warnings, none in the new files.

All 6,517 preceding C# files remain byte-identical. Only the approved test/helper
are added to compiled sources. Dependency, lockfile and workflow hashes are unchanged.
All three owned brokers were removed after saving logs; unrelated containers were
untouched. TRX, logs, mutation snapshots, source verification and cleanup evidence
are retained in `/private/tmp/brighter-4170-redis-requeue-writer-isolation/`.

The next [rejection-writer draft](../redis-rejection-writers/README.md) covers twenty
invalid/dead-letter selection and mapping-recovery cases. It remains outside the
compiled suite, unbuilt/unrun pending its own approval.

## Approved execution plan (completed)

Adopt the test/helper unchanged and run both frameworks. Current behavior is
expected to pass. Establish behavioral RED with these independent .NET 10 mutations:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Cache the first factory retry-writer selection, including null, and use it for every consumer | Exact native string at the first endpoint whose selection differs | 4 |
| Resolve the subscription's current retry writer at each retry instead of capturing it when creating the consumer | Exact native string at the first retry, which must not use the decoy | 4 |
| Reuse one RedisMessagePublisher per DefaultRedisWireMessageWriter instance without resetting it | Exact native string once repeated default/composed conversion accumulates prior content | 4 |

Require twelve intended assertion failures. Setup/compiler/timeout failures do not
count. Restore all production bytes exactly after each mutation and require four
passes. If initial behavior differs, preserve evidence and confine any correction
to the reviewed isolation contract. Do not weaken cases, assertions or bounds.

Finish with focused passes on both frameworks, four library targets built, and
complete suites retaining all 138 preceding identities/outcomes plus four new passes.
Expected complete matrices are 138 passes/four existing skips each. Run each full
suite once on a separate fresh ready broker; preserve and investigate failures
without retrying behavioral regressions until green.

Use the pinned Redis image, native Docker SDK runners, no host ports, unique keys
and owned cleanup. Async operations use a 45-second token; receives request one
second. External process/hang limits bound sync/native operations and disposal.
Save logs before removing owned containers. No dependency/workflow changes or
public signature changes are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
four cases, any evidenced minimum isolation correction, twelve reviewed mutation
failures with exact restorations, native Docker validation, complete affected
regressions, four library builds, owned cleanup and local reports. Hosted calls,
commits, deployment and publication are excluded. The compiled test/helper match
the approved draft. Further rejection and lifecycle tests require their own review.
