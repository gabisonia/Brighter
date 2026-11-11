# RabbitMQ Sync native delayed retry mapping

Status: validated without production changes. All 26 focused cases pass initially,
after mutation restoration and after the equivalent assertion-style cleanup on
.NET 9 and .NET 10. Seven mutations produce 20 intended failures. Both full Sync
suites pass 283 cases with one existing skip and retain every prior outcome. All
1,280 production files and dependencies are unchanged. The approved
[immediate-retry cleanup](When_rmq_sync_selects_a_requeue_writer_should_preserve_native_controls_and_recover_from_mapping_failures.cs)
resolves xUnit2031. One further equivalent cleanup at native-test line 173 uses
`Assert.Contains` instead of `Assert.True(keys.Any(...))`, resolving xUnit2012; the
original approved draft and initial evidence remain preserved. No affected test
file has warnings in the final build. The
[writer helper](../rmq-sync-requeue-writer/SyncRetryProfileWireWriter.cs) is unchanged.
The subsequent [concurrency batch](../rmq-sync-concurrent-mapping/README.md) records
its focused/full results and the remaining mutation amendment review.

## Contract

The [eight cases](When_rmq_sync_maps_a_native_delayed_retry_should_preserve_delay_queue_isolation_and_mapping_recovery.cs)
combine replacement/composition with success, writer throw, writer null and
native-properties callback throw. They use `RmqSubscription.RequeueWireMessageWriter`
through the public consumer factory with native delay enabled. A separately selected
invalid-message writer throws, so retry must use its own selection.

Native setup declares an application delayed exchange and a queue-specific delayed
retry exchange using the existing direct topology. Two durable queues bind to the
same application topic. The setup message reaches both queues; the neighbor's copy
is consumed before the operation. The retry must reach only the original queue.
This checks custom mapping with the direct topology; existing default-mapping tests
cover topic and fanout exchange types separately.

The requested delay is three seconds. The writer supplies 60,000 milliseconds, a
conflicting ID, transient persistence, handled count, delivery marker and two
original-ID variants. The gateway must replace these with requested/native controls.
The received native delivery must arrive after the requested delay and carry the
plugin's consumed `x-delay` value of -3000 milliseconds, a fresh ID, persistence,
handled count 3, retry marker `"1"`, exact custom/binary body and tenant profile.
The source's uppercase original-ID key and value must survive as the sole original-ID
entry. The source ID/topic/body/count/persistence/bag must remain unchanged.

Mapping failures return false and retain the original native message, which is
inspected after consumer disposal. Repairing the strategy and receiving through a
new consumer permits the delayed retry. Failure recovery is polled for two seconds,
retry arrival for five seconds after disposal, and extra source/neighbor deliveries
for two seconds after inspection. The elapsed timer starts immediately before the
successful retry call. These observations are bounded; publication and acknowledgement
remain separate, with no broker-outage or exactly-once guarantee.

The test uses the `RMQNativeDelay` collection, existing native-plugin fixture and
`RMQ_NATIVE_DELAY_URI`. All exchanges and queues have unique names and are cleaned
up. There are no new packages, mocks, closed handler generics or production interfaces.
Custom retries are inspected natively; the default reader is not expected to decode
arbitrary replacement formats.

## Validation after approval

Copy both draft files unchanged into the Sync test project's `MessagingGateway`
folder, adding the eight native cases and applying the one-line cleanup to the
existing recovery test. Run all eighteen immediate-retry cases and eight native
cases on .NET 9 and .NET 10, for 26 focused passes on each. Confirm xUnit2031 is gone
and the new files introduce no warnings. If green on arrival, prove the
following named mutations; if a real defect appears, preserve its evidence and
make only the change required by the approved contract before proving the mutations.

| Mutation | Selected cases | Required failures |
| --- | --- | --- |
| Ignore selected retry writer | failureMode 0 | 2 at profile presence, line 219 |
| Acknowledge source before mapping | failureMode 1/2/3 | 6 at original native delivery, line 160 |
| Omit retry source acknowledgement | failureMode 0 | 2 at original source routing, line 213, or extra source, line 241 |
| Override requested native delay with one millisecond after mapping | failureMode 0 | 2 at elapsed delay, line 217, or exact consumed delay header, line 228 |
| Publish retry through application exchange/topic instead of queue-specific retry exchange/key | failureMode 0 | 2 at native routing, line 213, or neighbor isolation, line 242 |
| Empty mapped retry body | failureMode 0 | 2 at exact payload, line 221 |

Also reapply the preceding `original-variants` mutation to the four immediate-retry
success cases: the amended `Assert.Single` at line 211 must fail in all four cases.
This verifies that the predicate overload retains the original guard.

Require **20 intended failures across seven mutations** on .NET 9, with actual case
selection and assertion locations verified from TRX. Restore production bytes,
rebuild normally and rerun all 26 focused cases on both frameworks. Run complete Sync
suites expecting **283 passes and one existing skip per framework**, retaining all
275 preceding passes by exact name. Verify all four library targets, dependency and
source scope, approved test/helper bytes, unchanged saved Git state, local links
and whitespace. Stop task-owned brokers and record all findings. Compile errors,
timeouts or unrelated failures are not mutation evidence.

Scheduler-backed retry, simultaneous mapping and the other transport rollout remain
open; see [remaining scope](../../tasks.md). Nothing is committed, pushed or published
by this review batch, and the repository review gear remains unchanged.
