# ASB concurrent publication writer — validated

Status: approved and validated. Three native cases pass on .NET 9 and .NET 10,
initially and after mutation restoration. Six mutations produce 18 intended assertion
failures. Both full available suites pass 588 cases, zero failures/skips, retaining
all 585 previous case/outcome pairs. No production correction was needed.

## Approved contract

Queue and topic publications in one registry share a thread-safe writer instance.
Three cases exercise sync, async and bulk calls through two distinct producers.
Dedicated workers start each operation. The helper constructs a fresh native result
and waits at a two-participant, 20-second barrier before returning it, proving that
both mappings overlap. It delegates mapping to the approved AsbExternalWireWriter,
which remains unchanged. No broker I/O is mocked or replaced.

Native SDK receivers verify per-source IDs, bytes and trace state, custom metadata
shape, and distinct five-minute/seven-minute publication TTLs. Each source maps once.
Bulk batches preserve original source IDs. Source routing, identity, body and selected
metadata remain unchanged. Each received native message is completed in a finally
block. Dedicated isolated queue/topic destinations prevent unrelated traffic.

The test awaits both operations before disposing the shared barrier. The barrier and
native receives are bounded; async broker calls use the 90-second token. Sync sends
also rely on the external test-runner timeout. No handler or closed-generic logging
registration is introduced.

This characterizes a safe shared strategy across two producers. It does not promise
thread safety for user-owned mutable writer state, concurrent operations on a single
producer, a throughput target or a stress-test guarantee. Bulk operations each contain
one source; existing packing/splitting tests retain their separate scope.

## Named production mutations

Apply each mutation to both individual and batch mapping paths where applicable.

1. Ignore the configured writer: writer invocation count fails.
2. Invoke the selected writer twice per source: count fails after two barrier phases.
3. Cache the first completed native body across producer instances under a lock and
   use it for both sends, leaving each message's other fields unchanged: one endpoint's
   native-body assertion fails regardless of which operation wins the race.
4. Replace each mapped body with empty bytes: native-body assertion fails.
5. Replace custom native MessageId with the original source ID: native-ID assertion fails.
6. Force both publication TTLs to five minutes: topic native-TTL assertion fails.

The cache mutation must act after the writer returns, and preserve native identity,
metadata and message object ownership. An SDK concurrency exception or a barrier
timeout does not count as detection of the body-isolation defect.

## Validation evidence

Each named mutation produces three failures at its intended assertion. The shared-body
cache mutation fails on native body comparison in every sending mode, without barrier
timeouts or SDK exceptions being counted as evidence. All mutations are restored.

All 1,171 non-generated production C# files match their pre-batch hashes. The approved
test/helper match their compiled copies byte-for-byte; the reused external writer is
unchanged. All library targets build. Both full available suites pass 588 cases on
their first runs, zero failures/skips, retaining exactly all 585 preceding outcomes.
Previous session-lock instability remains unresolved despite passing here.

Evidence: `/private/tmp/brighter-4170-asb-concurrent-writer/` contains initial/restored
matrices, mutations.json, full regression TRXs, exact baseline comparisons and source
verification. Dedicated fixture containers are stopped after validation. Nothing
committed or pushed.

Reader concurrency, scheduled sends, cancellation, session-lock instability, eight
DLQ-blocked cases, 52 unavailable hosted classes and global release gates remain open.
No commits or pushes are requested.
