# Concurrent GCP pull reader

Status: approved, adopted unchanged and validated. Both cases pass on both frameworks,
including both full regression runs. Four mutations produce eight intended failures.
Each full run retains one known synchronous ordered-nack failure; full-feature
production readiness remains open. All owned emulators are removed.

The writer concurrency batch covers outgoing messages. This test-first draft covers
incoming mapping through two public-factory-created pull consumers sharing a safe
reader. Review [the two cases](When_gcp_pull_consumers_share_a_reader_should_keep_overlapping_deliveries_isolated.cs)
and [the reader helper](GcpConcurrentPullWireReader.cs).

## Behavior

Sync and async cases each create two isolated topic/subscription pairs. Each pair
receives one distinct binary message from the native SDK. Explicit long-running
workers receive through separate consumers. The shared reader composes the existing
GcpNativeProbePullReader and waits at a two-participant barrier after mapping.
Two completed barrier crossings must be observed before payload assertions, so a
mapping timeout converted to an unacceptable message cannot count as isolation proof.

Before asserting per-consumer metadata, the test compares the two mapped binary bodies
as a multiset. Each mapped ID, body and marker must belong to that consumer's native
delivery. Cross-topic lookup uses the unique source marker rather than assuming native
IDs differ across topics. The gateway must replace fabricated reader receipt handles and handled
counts with each delivery's own native values. Receipts must differ. Native bodies,
IDs and attributes must remain unchanged, and mapped messages/headers must be separate.

Each consumer then acknowledges its own mapped message. After twelve seconds (past
the ten-second native ack deadline), a native marker is published to each subscription.
A six-second observation per subscription must see its marker and no original message.
The acknowledgement-omission mutation below validates this bounded observation; it
is not an unlimited no-redelivery guarantee. This batch does not cover streaming,
concurrent settlement, repeated receive calls on one consumer, nack or reader failures.

## Approved execution

Adopt the two files unchanged into MessagingGateway/TestDoubles. Run the two cases on
.NET 9 and .NET 10 against isolated, readiness-checked pinned Pub/Sub emulators. All
native builders use EmulatorOnly. There are no hosted calls or ADC, new dependencies,
request/handler types or logging registrations. The existing GcpNativePullReader
category already participates in CI.

Keep the ninety-second operation token, twenty-second mapping barrier, twelve-second
receive polling window, ten-second consumer disposal and fifteen-second delete bounds,
plus process/hang limits. Await both receives before cleanup. Attempt every owned
consumer/subscription/topic cleanup even if an earlier cleanup fails.

## Named production mutations

Apply each mutation independently on .NET 10; never change the tests to obtain RED.

1. Cache the first completed mapped MessageBody across consumer instances using a
   static field and Interlocked.CompareExchange in GcpPullMessageConsumer.ReadWireMessage,
   after the reader invocation and before native metadata assignment. Preserve each
   mapped header and receipt. Both cases must pass the overlap precondition and fail at
   `Assert.Equal(expectedBodies, actualBodies)` with duplicate mapped bodies.
2. Omit the native ReceiptHandle override in ReadWireMessage. Both cases must fail the
   exact native receipt equality; setup, barrier and body assertions must have passed.
3. Omit the native HandledCount override in ReadWireMessage. Both cases must fail the
   exact native handled-count equality (3 or 7 versus the reader's 777).
4. Omit the native Acknowledge calls in both AckByHandle and AckByHandleAsync, retaining
   successful method completion. Both cases must fail the native post-deadline
   `Assert.DoesNotContain` because an original delivery is observed again.

Timeout, compile or unrelated failures do not count as the intended evidence. Restore
GcpPullMessageConsumer byte-for-byte after each mutation and require both cases green.
Finish with both frameworks green, all four gateway library targets built, and both
full available regressions run once with the previous 480 identities plus these two
(482 expected using the same selection). Retain all historical failures/skips; do not
retry until green. If the initial test exposes a defect, diagnose it and make only the
minimum evidence-supported correction. Record database/hosted coverage as unavailable.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers adoption,
scoped native tests, the four named mutations and exact restorations, an evidenced
minimum correction if needed, affected regressions/library builds, owned cleanup and
local reports. It excludes hosted calls, unrelated changes, commits, deployment and
publication. The user approved this scope; execution evidence is recorded below.


## Execution outcome (2026-10-30)

Both cases pass initially on net9.0/net10.0. Four net10.0 production mutations yield
eight intended failures: duplicate mapped bodies, fabricated receipt handles, the
reader's 777 handled count, and the unacknowledged native original returning alongside
the post-deadline marker. Every exact restoration passes both net10.0 cases; the final
net9.0 check also passes both. No production correction or test change is needed.

Each full available matrix records 426 passed / 1 failed / 55 skipped, retaining all
480 prior case identities plus the two new passes. Both failures are the previously
recorded generated synchronous ordered-nack case at Reactor line 152, where the nacked
message is absent from the final set. The preceding net10.0 factory-replacement
failure passes here; this does not establish a fix. Full regressions run once with
unchanged selection and time bounds. Historical lifecycle gates remain open.

All four library targets build without warnings/errors. All 6,488 preceding src/tests
C# files remain unchanged, and the adopted test/helper match approval byte-for-byte.
The test dependency graph retains warnings, including CS0618 for the deliberate legacy
MessageType compatibility assertion. All three owned emulators are removed. No hosted
calls, credential use, dependency upgrades, commits, deployment or publication occur.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-pull-reader/` includes all
initial/mutation/restored/final/full TRXs and logs, approved snapshots, saved mutants,
source verification, library build, final-verification.json and cleanup.json. See the
[full validation entry](../../validation.md#gcp-concurrent-pull-reader-2026-10-30).

The next [concurrent stream-reader batch](../gcp-concurrent-stream-reader/README.md)
is a concrete unbuilt/unrun review draft outside the compiled suite.
