# RabbitMQ Sync concurrent mapping isolation

Status: validated with the approved
[completed-properties cache amendment](../rmq-sync-concurrent-cache-amendment/README.md).
All eight approved cases pass initially and after both restoration checkpoints on
.NET 9 and .NET 10. Twelve mutations produce 34 intended failures. Both final full
suites pass 291 cases with one existing skip and retain every prior outcome. All
four approved files, 1,280 production files, dependencies, HEAD and stash match
their snapshots. The two original shared-properties attempts remain recorded as
insufficient mutation evidence. No production fix was needed. The next
[four Kafka adapter cases](../kafka-default-wire-mapping/README.md) await review;
scheduler-backed retry and the remaining global rollout are still open.

## Contract and cases

| Test | Cases | Contract |
| --- | ---: | --- |
| [Concurrent writer operations](When_rmq_sync_shares_a_writer_across_concurrent_operations_should_keep_native_payloads_and_controls_isolated.cs) | 6 | Replacement/composition for publication, immediate retry and invalid forwarding. Two independent endpoints share one writer instance. Each native result retains its own member, ID, topic and bytes, while gateway controls remain authoritative. Source identity/topic/body/member survive. |
| [Concurrent consumer reads](When_rmq_sync_shares_a_reader_across_concurrent_consumers_should_keep_messages_and_delivery_controls_isolated.cs) | 2 | Replacement/composition through two consumers sharing one reader. Returned messages and bags are distinct; mapped identity/topic/body remain per delivery, while native count, tag, redelivery and persistence override conflicting reader values. Both native deliveries can be acknowledged. |

The [writer helper](SyncConcurrentWireWriter.cs) creates fresh mapping state and
counts calls using Interlocked. Dedicated workers start both endpoint operations
through a bounded barrier. Retry and invalid-forwarding calls additionally meet
inside both `WriteMessage` and native-property callbacks, proving actual overlap.
Each mapping carries a different member, identity and binary payload. The same
selected writer is used by both endpoints, so shared native property storage would
mix their data.

The Sync producer currently locks sends across producer instances. Publication
cases start callers together but do not require mapper/callback overlap or remove
that lock. They check isolation through the existing serialized path. No throughput
or new thread-safety guarantee for a single consumer or channel is proposed.

The [reader helper](SyncConcurrentWireReader.cs) creates a fresh message and waits
at a bounded barrier before returning. Both consumer calls therefore overlap in
the shared reader. Native messages have different members, counts and persistence;
the reader intentionally supplies conflicting delivery controls. No custom writer
or reader is expected to make unsafe user state thread-safe; these helpers are
safe shared strategies, and the gateway must not introduce shared per-call state.

All eight cases use the public Sync factory APIs, real native setup/inspection,
unique queues/exchanges and the `RMQ` collection. `TaskCreationOptions.LongRunning`
provides dedicated workers, and the async tests await their completion. Barriers
have ten-second bounds, receives ten seconds, native inspection five seconds and
post-disposal settlement observations two seconds. These are bounded checks, not
stress tests or broker-outage guarantees. Tests do not use reflection, internal
access, mocks, new handler types or dependencies. Owned broker resources are cleaned
up even when an assertion fails.

## Validation after approval

Copy both tests and helpers unchanged into `MessagingGateway` and `TestDoubles`.
Run all eight cases on .NET 9 and .NET 10. Preserve any actual failure before making
the minimum required correction. If green on arrival, prove behavioural RED using
the following named production mutations on .NET 9. Filter writer cases by their
actual `operation` argument in TRX (0 publication, 1 retry, 2 invalid forwarding).

| Mutation | Cases | Required failure |
| --- | ---: | --- |
| Ignore selected publication writer | 2, operation 0 | Writer invocation count, line 155 |
| Ignore selected retry writer | 2, operation 1 | Writer invocation count, line 155 |
| Ignore selected invalid writer | 2, operation 2 | Writer invocation count, line 155 |
| Reuse the first completed properties object per connection configuration at publish time, under a lock (approved amendment) | 4, operations 1/2 | Native member isolation, line 174 |
| Publish an empty mapped body in both PublishMessage and RequeueMessage | 6 writer cases | Native payload, line 179 |
| Force transient persistence in both publishing methods | 6 writer cases | Native persistence, line 181 |
| Ignore configured reader | 2 reader cases | Reader invocation count, line 100 |
| Replace both mapped results with a static first-result cache, using Interlocked.CompareExchange after reader invocation | 2 reader cases | Distinct message instances, line 102 |
| Omit gateway handled-count override after configured reading | 2 reader cases | Native handled count, line 114 |
| Omit gateway persistence override after configured reading | 2 reader cases | Native persistence, line 115 |
| Omit gateway redelivery override after configured reading | 2 reader cases | Native redelivery, line 116 |
| Omit gateway delivery-tag override after configured reading | 2 reader cases | Native tag range, line 117 |

Require **34 intended failures across twelve mutations**. The original native-properties cache
mutation caused concurrent writes to shared headers and did not meet the intended
assertion gate in all cases. Its two attempts remain preserved. The approved
amendment caches completed properties per shared connection configuration at the
native publish calls. All four selected cases then fail at native member line 174,
without altering the test helpers. Publication remains excluded from this mutation
because the producer's existing send lock prevents overlapping mapping.

Verify exact selected counts and assertion locations from TRX, restore all
production bytes, rebuild normally and rerun all eight focused cases on both
frameworks. Then run both complete Sync suites, expecting **291 passes and one
existing skip each**, retaining every preceding 283 passing case by exact name.
Build all four library targets and both test targets; check new warnings, source
and dependency scope, approved files, saved Git state, local links and whitespace.
Stop task-owned brokers and retain all evidence. Compile errors, deadlock timeouts
and unrelated assertion failures do not count as mutation evidence.

Scheduler-backed consumer retry and other transport rollout remain open; see
[remaining scope](../../tasks.md). Nothing is committed, pushed or published by this
batch, and the repository review gear remains unchanged.
