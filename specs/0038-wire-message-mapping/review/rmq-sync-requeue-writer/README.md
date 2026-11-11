# RabbitMQ Sync immediate retry writer selection

Status: approved files copied unchanged, implemented and validated. All eighteen
cases pass on .NET 9 and .NET 10 initially after implementation and after mutation
restoration. Fourteen mutations produce 60 intended failures. Both complete Sync
suites pass 275 cases with one existing skip, retaining every prior outcome. Four
Sync production files change; the other 1,276 files and dependencies are unchanged.
Initial missing-setting compile errors and two mutation-harness corrections remain
recorded. The xUnit2031 assertion-style warning from this batch is resolved by the approved
one-line cleanup in the subsequent [native-delay batch](../rmq-sync-native-retry-writer/README.md).
That batch rechecks all eighteen immediate cases, proves the original-ID guard and
records the later full-suite results. The original reviewed file remains here for
traceability.

## Contract and eighteen cases

Add nullable `RmqSubscription.RequeueWireMessageWriter`, using the existing
`IAmAWireMessageWriter<RmqWireMessage>` contract. The consumer factory captures the
selection separately for each consumer. Null retains default mapping. Existing
public constructors and dependencies remain unchanged. This setting is independent
of invalid-message and publication writers.

The writer controls body and application properties for direct consumer retry.
The gateway owns the fresh native message ID, source-queue destination, source
persistence, current handled count, retry delivery marker (`"1"`), requested delay
and original-ID chain. Immediate retry removes a writer-supplied delay. Original-ID
handling removes all writer-supplied case variants using ordinal comparison, then
writes the source's existing original-ID key/value, preserving its spelling; with
no source original ID, use the canonical key and current source message ID. Other
AMQP headers are not globally normalized. Multiple conflicting original-ID entries
already in the source are outside this batch, matching the Async contract.

Mapping must not mutate the source ID, topic, body, handled count, persistence or
bag. Writer throw/null and native-properties callback failure make `Requeue` return
false, leaving the original native delivery available. Repairing the strategy and
receiving through a new consumer allows one mapped retry and source acknowledgement.
Publication and acknowledgement remain separate operations; this does not establish
atomicity, broker-outage safety or absence of arbitrarily late duplicates.

| File | Cases | Coverage |
| --- | ---: | --- |
| [Mapping, controls and recovery](When_rmq_sync_selects_a_requeue_writer_should_preserve_native_controls_and_recover_from_mapping_failures.cs) | 16 | Replacement/composition × first/subsequent retry × success/writer throw/writer null/callback throw. Native inspection verifies exact binary/custom body, fresh ID, persistence, count, delivery marker, delay cleanup and authoritative original identity. The invalid-message writer is deliberately throwing, proving retry selects its own strategy. |
| [Consumer selection isolation](When_rmq_sync_subscriptions_select_requeue_writers_should_keep_each_consumers_mapping_isolated.cs) | 2 | Create custom/default consumers through one factory in both orders before receiving. Each preserves its own profile, payload, identity, native controls and source bag. |

[SyncRetryProfileWireWriter](SyncRetryProfileWireWriter.cs) composes the default or
replaces the body/native properties. It supplies conflicting native ID, persistence,
count, delivery tag, delay and canonical/uppercase original-ID headers. Assertions
inspect real RabbitMQ.Client 6 deliveries independently of production readers.
After disposal, source recovery is polled for two seconds, retry arrival for five,
and absence of an extra delivery for two. These are bounded observations.

Tests use the public Sync factory/consumer APIs, native confirmed setup publications,
unique owned queues/exchanges, and the existing RabbitMQ 4.2 fixture. They join the
`RMQ` collection. No mocks, internal test access, new request type, logger generic,
package or asynchronous consumer/factory is added. A reader must understand the
chosen retry format when the application consumes it; these tests inspect retries
natively rather than claiming default-reader compatibility with arbitrary formats.

Native delayed retry, scheduler-backed retry and simultaneous writer execution
remain separate validation work. This batch exercises `TimeSpan.Zero` only. The
new selection is for direct retry publication; scheduler-backed publication keeps
its existing mapping path until that separate contract is implemented.

## Validation after approval

Copy all three files unchanged into the Sync test project. Record the expected
missing-setting compile failure on both targets, which is not behavioural RED.
Implement the minimum subscription/factory/consumer/publisher selection and native
controls required by these tests, with XML documentation. Run eighteen cases on
both frameworks, then prove these named production mutations on .NET 9. Restrict
recovery cases by actual `failureMode`, `compose` and `existingOriginal` TRX names;
use diagnostic compilation/JIT settings for exact assertion locations.

| Mutation | Selected cases | Required failures and assertion |
| --- | --- | --- |
| Ignore selected retry writer | Recovery, failureMode 0 | 4: profile presence, line 201 |
| Acknowledge source before mapping | Recovery, failureMode 1/2/3 | 12: original native delivery, line 144 |
| Omit source acknowledgement | Recovery, failureMode 0 | 4: original source seen instead of retry at line 196, or extra source at line 221 |
| Skip gateway regeneration of native ID | Recovery, failureMode 0 | 4: writer-owned ID rejected, line 199 |
| Keep writer's transient persistence | Recovery, failureMode 0 | 4: native persistence, line 205 |
| Keep writer's handled count | Recovery, failureMode 0 | 4: count, line 208 |
| Keep writer's delivery tag | Recovery, failureMode 0 | 4: retry marker, line 209 |
| Keep writer's delay on immediate retry | Recovery, failureMode 0 | 4: no delay header, line 210 |
| Omit removal of writer original-ID variants | Recovery, failureMode 0 | 4: one authoritative original-ID entry, line 211 |
| Always replace source original identity with canonical current ID | Recovery, failureMode 0, existingOriginal true | 2: preserved source key/value, line 212 or 213 |
| Cache first factory retry selection, including null | Isolation | 2: selection-specific profile, line 121 |
| Empty the mapped retry body | Recovery, failureMode 0 | 4: native bytes, line 203 |
| Publish through the application exchange/topic instead of direct source-queue routing | Recovery, failureMode 0 | 4: native routing key, line 196 |
| Write handled count into the source bag during serialization | Recovery, failureMode 0 | 4: source bag values unchanged, line 185 |

Require **60 intended failures across fourteen mutations**. Restore every production
byte, rebuild with normal optimization and rerun all eighteen focused cases. Then
run both full Sync suites, expecting **275 passes and one existing skip each**, with
all 257 preceding passes retained by exact name. Build all four Sync library targets,
verify approved files, source/dependency scope and saved Git state, record every
finding and stop task-owned brokers. Compile errors, runner timeouts, wrong case
selections and unrelated assertions are not mutation evidence.

No commit, push, publication or review-gear change is authorized. Full #4170 remains
open beyond this batch; see [remaining scope](../../tasks.md).
