# Native Redis scheduling-conflict recovery

Status: draft for review (2026-11-07). No build or test execution; no production edits.

This is the next package in the [completion plan](../../completion-plan.md).
The consumer already restores its captured native ID when scheduler submission
throws. Existing delegation tests do not establish that a received native delivery
retains the correct ID, or that the conflicting real scheduled job is preserved.
These cases therefore start as characterization candidates with named mutations.

## Review and installation

The [two-case draft](When_a_scheduled_redis_retry_conflicts_should_preserve_the_delivery_and_allow_recovery.cs)
belongs in `tests/Paramore.Brighter.Redis.Tests/MessagingGateway/` after approval.
Cases cover sync and async consumer factories/requeue paths. They reuse the existing
`RedisPublicationWireWriter`, default writer/reader, native Redis clients, real
`InMemoryScheduler`, `CommandProcessor`, scheduled-message handler and outbox mediator.
There are no new helper classes, project references, packages or public APIs.

## Observable contract

1. Seed two native source deliveries as IDs `1` and `2`, with the topic counter at
   `41`. Receive the first delivery, revise its Unicode body, and retain its ID,
   event type, application topic, handled count and marker. A separate valid job
   occupies the fixed scheduler ID with a one-hour delay and `OnConflict.Throw`.
2. A positive-delay retry of the received delivery throws `InvalidOperationException`
   containing that scheduler ID. The source object, native payloads, queue, counter
   and sibling queue are unchanged. No writer or outbox deposit is allowed.
3. Another receive on the same consumer still throws `ChannelFailureException`
   identifying the original application message. The second delivery remains queued;
   failure did not silently release the in-flight gate.
4. Reschedule the existing job to one second. It publishes its original parked ID
   and payload as native ID `42`, through the selected composed/default writer.
   This proves the rejected retry neither replaced nor deleted the existing job.
   The failed source delivery remains unchanged. Cancel any remaining timer entry
   through the public scheduler API before continuing.
5. An immediate retry through the same consumer must requeue native ID `1`, producing
   queue order `2, 42, 1` without incrementing the counter. Its default payload retains
   the revised body, application ID/topic, handled count and marker. This distinguishes
   restoration of the correct native ID from restoration of an arbitrary nonempty ID.
6. Receive and acknowledge the second source delivery. Independently remove the
   observed parked-job native ID `42`, then receive the recovered original ID `1`.
   Submit a five-second scheduled retry through the same consumer and scheduler.
7. Before dispatch, no new payload, counter increment, outbox entry or writer call
   appears. On dispatch, exactly one composed payload appears as ID `43`, only in
   the selected queue. It retains source data, TTL and application metadata, excludes
   the local producer selector, and settles its outbox entry. The unrelated ordinary
   publication is unused and both source objects remain unchanged.

The scenario has a 45-second cancellation bound. Native publication polling uses
100-millisecond intervals and checks both stored payload and outbox completion.
The real scheduler's public dispose operation finishes before final assertions and
again in cleanup. Cleanup removes only this test's unique keys, including the
deliberately wrong ID used by a mutation. No private-state inspection, fake scheduler
or timer manipulation bypasses the production path.

The next receive gate and immediate retry establish in-flight restoration. The later
scheduled retry establishes continued selected-writer dispatch. These assertions
do not promise atomic broker operations, process-restart durability, automatic retry
or exactly-once delivery.

## Named mutations and scope

Run the initial two native cases on .NET 9 and .NET 10. If they pass unchanged,
prove that they guard behavior with these independent mutations to the positive-delay
catch blocks in `RedisMessageConsumer.Requeue` and `RequeueAsync`:

| Mutation | Intended assertion failure | Cases |
| --- | --- | ---: |
| Omit restoration of the captured in-flight entry | The subsequent receive no longer reports the original in-flight delivery | 2 |
| Restore native ID `999` instead of the captured ID | Immediate recovery queues `999` instead of the original `1` | 2 |
| Return `true` after restoration instead of rethrowing submission failure | No `InvalidOperationException` reaches the retry caller | 2 |

Require all six intended failures. Restore exact pre-mutation production bytes and
require both cases to pass after each restoration. A build error, timeout or setup
error is not the intended failure. Preserve the reviewed test and helper bytes.

No permanent production change is expected. If initial execution reveals a defect,
limit the correction to submission error propagation and restoration in the Redis
consumer's existing positive-delay paths. Changes to shared scheduler policy,
public configuration, mapping, persistence, cancellation or other transport behavior
are not included in this package.

## Final validation

Run optimized focused checks on both frameworks and build all Redis library targets.
Record native command counts before allocating the complete Redis regression suite
to disjoint quota-safe batches. Preserve all 202 preceding identities and outcomes,
adding two passes: **200 passes and four existing skips per framework**, with every
one of the 204 identities executed exactly once across the final batches.

Use the established real 3,605-second idle intervals after focused Redis work and
between regression batches. Retain all failure evidence and exact source/assembly
manifests. Do not remove tests, alter dependencies, reset quotas, manipulate clocks
or buy a license. A completed batched suite is not a single-process full-suite claim.

Missing scheduler configuration, native-send failures, pre-cancelled submission,
durable persistence and wider lifecycle paths remain separately tracked in the
completion plan. This package is not whole-issue production sign-off.

## Focused result, 2026-11-07

Both native cases pass on .NET 9 and .NET 10. All three named mutations fail
both cases at the intended assertions and pass after byte-for-byte restoration.
All pre-existing source/build inputs remain unchanged. The Redis library builds
on all four supported targets. The final focused run uses 299 native commands per
framework. Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-submission`.

The full regression gate remains pending. It will run once with the native-send
failure and cancellation cases, retaining all prior test identities/outcomes.
