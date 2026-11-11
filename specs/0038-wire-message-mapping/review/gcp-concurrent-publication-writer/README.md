# Concurrent GCP publication writer

Status: approved, adopted unchanged and validated. All three cases pass on both
frameworks, including both full regressions. The named mutation produces three
intended failures. The full net10.0 run retains one known factory-replacement failure;
full-feature production readiness remains open.

The endpoint-isolation batch sends sequentially through separate producers. This next
batch checks two overlapping operations through the same public factory-created
producer using a thread-safe shared writer. Review [the three cases](When_concurrent_gcp_sends_share_a_writer_should_keep_native_messages_isolated.cs)
and [the helper](GcpConcurrentWireWriter.cs).

## Behavior

Sync, async and bulk modes each send two distinct binary messages concurrently. Two
explicit long-running tasks invoke the production send APIs; each bulk operation owns
one source message. The writer reuses the existing GcpNativeProbeWireWriter mapping,
then waits at a two-party barrier before returning its newly created native result.
Both mapping operations must overlap within twenty seconds. Failure to establish that
precondition is reported as a send failure, not accepted as native isolation evidence.

An independent native subscriber requires both expected binary payloads before checking
application IDs, custom attributes, native IDs and source preservation. Comparing the
native payload set first detects reused native objects without depending on delivery
order or which operation wins a race. The gateway must retain unordered native controls
despite the writer supplying an ordering key. Writer calls must total two. This does
not claim arbitrary user writers are thread-safe, or cover concurrent batch internals,
multiple producers, cancellation, scheduling, rejection or every concurrency path.

## Execution and named mutation

The approved execution plan was: adopt the test/helper into MessagingGateway/TestDoubles, preserving
these review copies. Reuse the existing native probe writer; no new request/handler
or logger registration is needed. Run three cases per framework against fresh isolated,
readiness-checked Pub/Sub emulators. All native builders are EmulatorOnly; no hosted
project or ADC is involved. Keep the existing sixty-second native-operation budget,
twenty-second mapping barrier, fifteen-second deletes and process/test hang bounds.
Each case creates one topic/subscription pair and publishes two synthetic messages.

If the baseline passes, prove RED with this production-only mutation on .NET 10:
add an instance-level cached PubsubMessage to GcpMessageProducer, and, after each
configured writer invocation/null guard, use Interlocked.CompareExchange to retain
and return the first mapped result for subsequent operations. The writer still runs
twice and reaches the barrier. All three cases must fail at
`Assert.Equal(expectedBodies, actualBodies)` because both sends use one payload.
Compilation, barrier/setup failures or missing-ID assertions do not count.

Restore the producer exactly and require three passes on both frameworks. If initial
behavior is RED, diagnose retained native evidence before a minimum concurrency fix.
Do not weaken assertions, remove the overlap precondition, change timeouts, or repeat
until green. Build all four library targets and run both full available Pub/Sub
regression matrices. Preserve the previous 477 identities plus these three cases
(480 expected with the same selection), including all skips and historical lifecycle
failures. Record unavailable database/hosted coverage explicitly. Clean up only owned
resources and containers; no dependency upgrade is proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". This concrete batch is
prepared through the test-first review workflow and approved by the user. Approval covered
adoption, isolated native tests, the named mutation and exact restoration, any
evidence-supported minimum concurrency correction, affected regressions/library builds,
owned cleanup and local reports. Hosted calls, unrelated changes, deployment, commits
and publication are excluded.


## Execution outcome (2026-10-30)

All three baseline and restored cases pass on net9.0/net10.0. The native-result-cache
mutation fails all three net10.0 cases at the native payload multiset assertion:
expected ["VwAA/wGA", "VwD+AAJ/"] versus actual ["VwD+AAJ/", "VwD+AAJ/"]. The
producer is restored byte-for-byte. No test changes or production correction are needed.

Full available matrices: net9.0 425 passed / 0 failed / 55 skipped; net10.0 424 passed /
1 failed / 55 skipped. Each retains all 477 prior identities plus the three new passes.
The known synchronous/two-client factory-replacement case fails at line 185 on net10.0:
a successful native nack and replacement build are followed by an empty receive.
The preceding net9.0 async ordered-nack failure passes now; neither change establishes
a lifecycle fix. Full regressions run once, without exclusions or retry-until-green.

All four library targets build without warnings/errors. All 6,486 preceding src/tests
C# files remain unchanged and both adopted files match their approved snapshots.
All three owned emulators are removed. No hosted calls, credentials, dependency
upgrades, commits, deployment or publication are involved.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-publication-writer/` includes all
focused/mutation/restored/full TRXs and logs, source snapshots, library build,
final-verification.json and cleanup.json. Initial infrastructure launch failures
executed no tests and are retained separately. See the
[full validation entry](../../validation.md#gcp-concurrent-publication-writer-2026-10-30).

The next [concurrent pull reader batch](../gcp-concurrent-pull-reader/README.md) is a
concrete review-only draft, unbuilt/unrun and not adopted into the compiled suite.
