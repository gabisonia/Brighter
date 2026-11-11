# Scheduler paths requiring wire-mapping validation

This records source findings and the explicitly identified AWS destination and
native-delivery regression evidence. Unvalidated findings are not runtime or
compatibility evidence. The [completion gates](tasks.md#full-implementation-completion-gates)
remain open.

## Dispatch through the configured producer

`FireSchedulerMessageHandler` posts the callback command. Both sync and async
`OutboxProducerMediator.CreateMessageFromRequest` paths special-case that command
and return its contained `Message`. This is the path that can reach publication
selection and actual producer trace propagation after scheduling.

| Scheduler | Source path | Validation still required |
| --- | --- | --- |
| In-memory | Timer dispatches `FireSchedulerMessage` | SQS/SNS dispatch tracing, RabbitMQ Async/Sync queue-targeted retries, SQL Server/Kafka/MQTT retry selection and GCP/RocketMQ delayed-publication callbacks are validated; see the gateway-specific evidence below |
| AWS V3/V4 callback | `AwsSchedulerFiredHandler` sends `FireSchedulerMessage` | Distinct destinations, actual callback-to-writer dispatch, automatic callback-pump settlement and explicit outbox recovery after writer failure are validated in both SDKs; hosted IAM remains separate |
| Azure | `AzureSchedulerFiredHandler` sends `FireSchedulerMessage` | Native callback and Service Bus writer integration |
| Quartz | `QuartzBrighterJob.Execute` sends a deserialized callback | Job completion, native RabbitMQ payload/writer/traceparent and explicit writer-failure recovery validated; PostgreSQL JobStoreTX/System.Text.Json process restart validated; other providers and crash recovery remain separate |
| TickerQ | `BrighterTickerQSchedulerJob.FireSchedulerMessageAsync` sends a deserialized callback | Serialized selection, native RabbitMQ writing/trace timing and not-before correction validated; EF Core SQLite process restart validated on both frameworks; other providers and crash recovery remain separate |
| Hangfire | `BrighterHangfireSchedulerJob.FireSchedulerMessageAsync` sends a deserialized callback | Serialized selection, native RabbitMQ writing/trace timing and separate-process PostgreSQL persistence validated; other provider-specific configurations remain outside that evidence |

The callback's serialization is an internal transport representation. It must
retain the original message until a producer performs application wire mapping.
Mapping at schedule creation cannot establish dispatch-time trace context.

## AWS native-target bypass and callback routing

Both `AwsScheduler.CreateTargetAsync` implementations serialize SNS publish or
SQS send requests themselves when `UseMessageTopicAsTarget` resolves a native
destination. They cannot execute an application writer later inside the AWS
service. Full custom mapping therefore needs a callback execution path while
preserving existing native targets for applications that use them.

The approved destination-selection tests reproduced callback routing to the
business endpoint in both SDKs. Both implementations now resolve the callback
from `schedulerMessage`, whose topic is `SchedulerTopic`. The V4 lookup also
handles null/empty SNS topic pages before falling back to SQS, retaining pagination.
All sixteen cases pass on both frameworks and six named mutations produce 24
intended failures. All four full scheduler suites pass 70 cases each and all four
gateway suites pass 607 each, without failures/skips; see the
[review record](review/aws-scheduler-destinations/README.md).

Existing native-delivery regressions also reproduced loss of fractional timestamp
precision. Both serializers now use invariant round-trip timestamps; their full
header assertions remain unchanged. This is a native wire-text correction, separate
from adding configurable mapping. All four final scheduler regressions pass.

The destination cases inspect stored native schedules. The subsequent
[native callback cases](review/aws-native-callback-mapping/README.md) receive actual
callbacks fired by the emulator, decode them with the public mapper and dispatch
them through a command processor and the registered AWS/Brighter scheduler
handlers. Both SQS and SNS business writers run with the later producer trace,
state and baggage, while the original scheduling object remains unchanged.
Eight named mutations produce sixteen intended failures. All four full AWS
gateway suites pass 611 cases each, and both Core suites retain 1,716 passes and
seven existing skips. No production correction is needed for this callback path.

These initial callback cases explicitly dispatch the received callback through
handlers; they do not run a service-activator pump. The subsequent
[automatic callback-pump package](review/aws-callback-pump/README.md) validates
callback settlement and explicit outbox recovery after business-writer failure.
It does not establish automatic business redelivery or hosted AWS IAM enforcement.

Source: [V4 scheduler](../../src/Paramore.Brighter.MessageScheduler.AWS.V4/AwsScheduler.cs),
[V3 scheduler](../../src/Paramore.Brighter.MessageScheduler.AWS/AwsScheduler.cs),
[existing callback test](../../tests/Paramore.Brighter.AWSScheduler.V4.Tests/Scheduler/Messages/Sqs/When_Scheduling_A_Sqs_Message_Via_FireScheduler_Async.cs).

The [eight native SNS source-preservation cases](review/aws-native-source-preservation/README.md)
now reproduce source-header mutation when correlation is absent. Both schedulers
keep the generated value local to serialization and retain a supplied ID exactly.
All cases pass on both frameworks; six mutations produce sixteen intended failures.
Exact source serialization, including tenant metadata and deposit telemetry, stays
unchanged. All four full Scheduler suites pass 74 cases and all four gateway suites
retain 611. These tests inspect stored schedules without firing them; they do not
prove source preservation in every other scheduler or transport.

## RabbitMQ scheduler-backed retries

`RmqMessageConsumer.RequeueAsync` supplies `_requeueWireMessageWriter` to its
direct/native-delay publisher. Its scheduler branch now prepares a private message
copy using the subscription-captured `ScheduledRequeuePublicationTopic`; callback
dispatch uses that publication's writer while retaining the source queue as the
destination. The direct retry writer and scheduled publication selection are
separate configuration choices.

The [Async selection package](review/rmq-scheduled-selection/README.md) and
[Sync selection package](review/rmq-sync-scheduled-selection/README.md) validate
selected/default mapping and queue isolation. Subsequent positive-delay and
unsupported-delay fallback packages bring the complete suites to 566 passes/two
skips and 299 passes/one skip respectively per framework. The earlier missing
scheduler selection is implemented; these counts do not close unrelated release gates.

Source: [consumer](../../src/Paramore.Brighter.MessagingGateway.RMQ.Async/RmqMessageConsumer.cs).

## Quartz completion and zero-delay findings

`QuartzBrighterJob.Execute` previously dispatched either recognized payload and
then unconditionally threw the missing-payload exception. The
[approved completion tests](review/quartz-job-completion/README.md), with the
[positive-delay amendment](review/quartz-job-positive-delay/README.md), reproduce
four failures per framework at the completion assertion after successful delivery.
The correction returns after each successful dispatch. Missing-payload errors
remain, and dispatch exceptions are not caught or suppressed by the correction.

All five cases pass on both frameworks. Four mutations produce nine intended
assertion failures. Both full Quartz suites pass 53 cases, retaining all 48
baseline outcomes. Only the two return statements change production behavior.
The [native Quartz/RabbitMQ batch](review/quartz-rmq-wire-dispatch/README.md)
now validates actual message/request scheduling through the configured writer,
original payload/identity, source preservation, native controls and dispatch-time
traceparent. All eight cases pass on both frameworks; four mutations produce 24
intended failures. Both full RabbitMQ suites pass 548 cases with two existing
skips; both full Quartz suites retain 53 passes. Production source is unchanged.
The [writer-recovery batch](review/quartz-rmq-writer-recovery/README.md), with its
approved [trace-chain amendment](review/quartz-rmq-recovery-trace-chain/README.md),
now validates retained original outbox data after throwing/null writers, followed
by one confirmed custom native publication under a fresh recovery trace. The chain
is recovery activity → clear span → publish span. Three mutations produce 24
intended failures; both full RabbitMQ suites pass 556 cases/two existing skips,
Quartz retains 53 passes each and Core retains 1,716 passes/seven existing skips
each. Production source is unchanged. This uses in-memory outbox/Quartz stores;
database persistence, other transports and automatic retry policies remain separate.

The original zero-delay cases failed earlier: delay overloads calculate a timestamp
that the absolute-time overload rejects as past. This is a separate scheduler
finding. The approved completion fixture uses one second to reach its intended
assertions; zero-delay production behavior remains unchanged.

Source: [Quartz job](../../src/Paramore.Brighter.MessageScheduler.Quartz/QuartzBrighterJob.cs).


## Redis scheduler-backed retries: registered-publication characterization

`RedisMessageConsumer.Requeue`/`RequeueAsync` route positive delays through a lazy
producer and scheduler, removing the in-flight entry on acceptance and restoring
it if schedule submission throws. The internal producer does not carry the
subscription's immediate retry writer. `InMemoryScheduler` later dispatches
`FireSchedulerMessage` through the production handler/outbox mediator, which
resolves the registered publication. Merely setting the internal producer's
writer would not preserve that selection through the callback registry lookup.

The [four-case review draft](review/redis-scheduled-retry-dispatch/README.md) tests
this real callback path with registered custom/default publication selection,
source preservation, next-message progress and native fan-out. The approved batch passes all four focused cases and ten intended mutation failures, with full paced Redis regressions at 182 passes/four existing skips per framework.
Per-subscription delayed mapping and queue-specific scheduling are addressed by the
separate approved implementation below; this characterization alone does not
establish those behaviors.


## Redis scheduled retry selection: implementation under validation

The [approved package](review/redis-scheduled-retry-selection/README.md) reuses
`Message.ProducerTopicHeaderName` on a private scheduled copy to select a registered
retry publication. Explicit Redis publication destination-topic/queue configuration
separates registry lookup, logical message topic and native storage/delivery. The
factory captures the subscription selector; null preserves the existing path.
No shared scheduler API or serialized writer delegate changes are required.
Four intended behavioral RED failures and four initial/final GREEN passes are verified
per framework. All eighteen mutation failures and exact restorations are verified;
complete paced regressions pass 186 cases/four existing skips per framework. Durable
scheduler persistence, multi-endpoint isolation and failure/cancellation coverage
remain separate work.

## Redis scheduled retry failure audit (2026-11-02)

This is source inspection while the approved selection/isolation schedules wait for
real quota intervals. No new failure test has been adopted, compiled or executed,
and no production change is made. These findings do not turn pending lifecycle
coverage into a passing release gate.

| Boundary | Current source behavior | Remaining evidence or decision |
| --- | --- | --- |
| Missing scheduler | Positive-delay Requeue/RequeueAsync throws before removing the in-flight entry. | Native-received message must retain its original payload and remain recoverable through the same consumer. |
| Submission throws | The consumer removes its in-flight entry, submits the scheduled copy, then restores the captured native ID in its catch block. The original exception is rethrown. | Exercise a real scheduler conflict, prove no replacement job/native write, then cancel the conflicting job and recover. |
| Pre-cancelled async submission | Redis forwards the token to an async scheduler, but InMemoryScheduler.ScheduleAsync(Message, TimeSpan, CancellationToken) does not inspect it before creating a timer. The sync-only fallback also schedules without inspecting the token. | Decide and test the cancellation contract before changing shared behavior. This is distinct from cancelling an already-created job by scheduler ID. |
| Missing alias or mismatched event type | Publication lookup occurs at callback dispatch, after scheduler acceptance. ProducerRegistry.LookupBy throws ConfigurationException for an absent topic/type pair. Lookup is outside the producer-send resilience wrapper. | Prove failure containment and recovery with the actual handler/registry. Use an isolated process for the real-time callback path so an escaping exception cannot terminate unrelated tests. |
| Writer/null/native send failure | The producer-send resilience wrapper catches exceptions and normally returns false; the dispatch branch marks the outbox entry only when sending succeeds. | Observe retained original outbox data, untouched or partially-written native state as appropriate, then recover explicitly after fixing the writer/native condition. Do not infer automatic scheduler retry. |
| Process restart before the due time | InMemoryScheduler stores a Message reference in timer state; this path performs no durable scheduler serialization. | A separate durable scheduler/outbox integration must prove selector/type preservation and recovery across restart. The current in-memory cases cannot establish it. |

### Callback exception containment is a shared scheduler concern

Status update (2026-11-07): the message and local request callback findings below
are resolved by the completed [message registration](review/redis-scheduled-retry-registration/README.md)
and [request handler failure](review/inmemory-scheduled-request-failures/README.md)
packages. The following paragraphs record the original control-flow finding.

[InMemoryScheduler.Execute](../../src/Paramore.Brighter/InMemoryScheduler.cs) calls
`BrighterAsyncContext.Run(() => processor.SendAsync(message))`, then calls
`CleanupTimer`. There is no catch/finally around that call. CommandProcessor.SendAsync
records an exception on its span and rethrows; BrighterAsyncContext unwraps task
exceptions. Therefore an uncaught registry/handler/outbox exception can escape the
timer callback and skip its normal cleanup. This is a source-derived control-flow
finding. Host termination and recovery behavior have not been reproduced here.

[FireSchedulerMessageHandler](../../src/Paramore.Brighter/Scheduler/Handlers/FireSchedulerMessageHandler.cs)
posts the message through the real command processor. Post deposits before clearing;
[OutboxProducerMediator](../../src/Paramore.Brighter/OutboxProducerMediator.cs) unwraps
FireSchedulerMessage to its embedded message, then looks up the producer by local
selector and event type. The send-failure catch does not cover producer lookup.
A missing retry publication must consequently be distinguished from an ordinary
writer exception when designing recovery coverage.

A correction to this timer boundary would affect shared Core behavior and multiple
transports. It is outside the approved Redis-only selection and characterization
packages and needs its own concrete test/contract review. No Core edits are bundled
into the currently queued work.

### Existing cancellation tests cover different operations

The Redis `When_cancelling_a_scheduled_message_should_not_republish_it` file exercises
ConformanceHarnessMessageScheduler, not the production InMemoryScheduler plus Redis
publication selector. The in-memory scheduler project has production CancelAsync(id)
cases using its time provider, but those do not establish pre-cancelled submission
semantics. The older Redis delayed-consumer tests use SpySchedulerSync/Async and
messages that were not received natively; they establish delegation, not native
in-flight restoration after a scheduling failure.

### Next validation order

After the approved gates finish, prioritize callback exception containment and
invalid registration, then native submission-conflict recovery, writer/native-send
outbox recovery, pre-cancelled submission, and durable scheduler restart behavior.
Each should keep an explicit contract and distinguish scheduling acceptance,
native publication and eventual recovery. The current true Requeue return value
establishes accepted submission, not successful later publication.


## Redis scheduled retry endpoint isolation completion (2026-11-03)

Four approved sync/async × endpoint-order cases pass through native Redis and the
production in-memory scheduler, command processor, handler and outbox mediator.
Two subscriptions share a factory, registry, scheduler and native topic while their
captured aliases retain independent default/composed writers and destination queues.
Repeated deliveries preserve current payloads and all caller snapshots. No permanent
production change is needed.

Three independent production-only mutations produce twelve intended assertion
failures: wrong queue with a cached consumer selector, wrong framing with a reused
publication writer, and stale decoded body with a cached default representation.
Each exact restoration returns four passes. Initial and final optimized focused
checks pass four cases per framework. All four Redis library targets build with zero
warnings/errors. Approved test/helpers, all 6,526 preceding C# files and protected
inputs are unchanged; only the approved test is added.

Complete paced regressions pass **190 cases, zero failures and four existing skips
per framework**. All 194 identities run exactly once, preserving all 190 prior
outcomes. Real idle intervals are at least 3,605 seconds. No quota or clock-discontinuity
errors are reported in this rerun and owned containers are removed. The earlier
sleep-interrupted failure is retained separately under `paced/`; it is not erased
or counted as a successful run. This establishes complete
batched coverage, not a single-process full-suite pass. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced-awake/final-verification.json`.

Invalid/missing configuration, scheduling/dispatch failure recovery, cancellation,
durable persistence, concurrency and broader feature release gates remain open.
Whole-feature production readiness, hosted CI, commits, pushes and deployment are
not claimed.


## Redis scheduled retry writer recovery completion (2026-11-04)

Four approved sync/async × throwing/null cases pass through native Redis and the
production in-memory scheduler, command processor, handler and outbox mediator.
Failed callbacks leave exactly one outstanding message with its revised payload,
metadata and selected publication. Native counters, payloads and queues remain
unchanged. Explicit recovery sends one composed/default payload to the selected
queue and settles the outbox while preserving the caller and original native records.
No permanent production change is needed. This does not establish automatic retries
or exactly-once delivery across arbitrary failures.

Two independent production-only mutations produce eight intended assertion failures:
premature settlement loses the pending message, and bypassing the selected writer
fails the required failed-attempt assertion. Exact restoration returns four passes
after each mutation. Initial and final optimized focused checks pass four cases per
framework. Four Redis library targets build with zero warnings/errors. All 6,527
preceding C# files and 281 protected build/dependency/workflow inputs are unchanged;
only the approved test and writer helper are added.

Complete paced regressions pass **194 cases, zero failures and four existing skips
per framework**. All 198 identities run exactly once, preserving all 194 prior
outcomes. Real idle intervals are at least 3,605 seconds, no quota or host clock
errors are reported, and owned containers are removed. This establishes complete
batched coverage, not a single-process full-suite pass. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-writer-recovery/paced/final-verification.json`.

Invalid/missing registration, submission and native-send failures, cancellation,
durable persistence, concurrency and broader transport/release gates remain open.
Whole-feature production readiness, hosted CI, commits, pushes and deployment are
not claimed.


## Registration failure containment review (2026-11-05)

The [four-case review package](review/redis-scheduled-retry-registration/README.md)
uses independent child processes to turn a real timer callback crash into a parent
xUnit assertion. Its proposed shared Core scope is message-callback containment,
error logging and guaranteed generation-aware cleanup. Direct registry/dispatch
errors remain visible. Cases also require retained outbox data, reuse of the failed
job ID for healthy work and explicit recovery through corrected configuration.
The native fixture delegates to real system timers and adds no package dependencies.
This is a review draft only: host termination is not yet reproduced, the tests have
not been adopted or executed, and no production correction is claimed. Request
callbacks and durable recovery remain separate.


## Scheduled Redis retry registration failure containment completion (2026-11-06)

Four approved sync/async × missing-alias/wrong-event-type cases reproduce an accepted
native retry followed by an unhandled registry ConfigurationException terminating
its child process with exit code 134, on both frameworks. The parent xUnit process
survives and preserves the actual callback diagnostics. The initial ambiguous
RedisSubscription type and nullable-context build issues were corrected without
changing assertions or behavior; the failed build and original reviewed files are
retained separately from behavioral RED.

InMemoryScheduler's message-callback branch now logs dispatch exceptions with the
scheduler ID and always performs generation-aware cleanup. Direct invalid-registry
clearing still throws to callers. The failed message remains in the outbox, the same
job ID accepts later healthy work, and a correctly configured registry explicitly
recovers the original payload to its selected native queue. Only InMemoryScheduler.cs
changes production behavior; request callbacks and public signatures are unchanged.

Three independent mutations produce twelve intended parent assertion failures:
rethrowing the callback error, omitting failed-job cleanup, and suppressing the error
log. Exact restorations each return four passes. Initial/final optimized native checks
pass four cases per framework. All eight Core/Redis library target builds have zero
warnings/errors. Full Core regressions retain 1,716 passes/seven existing skips and
InMemory retains 167 passes on each framework, with every baseline outcome preserved.
The initial Core output-location failure is retained as setup evidence; moving build
output under the ignored repository bin directory restores the architecture checks
without altering tests.

Complete paced Redis regressions pass **198 cases, zero failures and four existing
skips per framework**, covering each of 202 identities exactly once and preserving
all 198 preceding outcomes. Real idle intervals are at least 3,605 seconds, no quota
or host clock errors are reported, and owned containers are removed. This is complete
batched coverage, not a single-process full-suite claim. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-registration/paced/final-verification.json`.

All 6,528 other preceding C# files remain unchanged. Additions are the four-case test
and four child-fixture sources, one fixture project and one test-project reference.
The compiled fixture matches its recorded effective contents after the two routine
compilation corrections; no package versions or test semantics changed.

This establishes message-callback containment and explicit in-process outbox recovery.
Request-callback failures, submission/native-send errors, cancellation, durable
persistence, broader transport checks and final release gates remain open. No hosted
CI, commit, push, deployment or whole-feature production readiness is claimed.


## In-memory scheduled request failure containment (2026-11-07)

The [approved request callback package](review/inmemory-scheduled-request-failures/README.md)
reproduces four real timer crashes on each framework, covering sync/async Send and
Publish. The scoped Core fix logs the original dispatch exception and scheduler ID,
then guarantees existing generation-aware cleanup. Direct dispatch still throws;
the same scheduler ID accepts and completes later healthy work. Failed local
requests are not retained or retried automatically.

All four initial/final focused cases pass on both frameworks. Three named mutations
produce twelve intended failures and exact restorations return twelve passes.
Full Core suites pass 1,720 cases/seven existing skips; full InMemory suites pass
167 cases/no skips per framework. Every baseline identity/outcome is preserved.
Four Core library targets build with zero warnings/errors. Approved test sources
are unchanged; only `InMemoryScheduler.cs` changes production behavior.

This closes the local request-handler exception boundary identified in the previous
message-callback audit. Submission/native-send recovery, cancellation, restart
persistence, broader transport coverage and final release checks remain open.
Evidence: `/private/tmp/brighter-4170-inmemory-scheduled-request-failures/`.

## SQL Server and PostgreSQL retry applicability — 2026-11-08

SQL Server delegates positive-delay retries to its configured scheduler. Its captured
`ScheduledRequeuePublicationTopic` now selects the registered retry publication on
a private message copy, retaining the logical topic as the native queue destination.
Four native timer/command-processor/outbox cases and four selector mutation failures
validate that path; full suites pass 396 cases/four existing skips on each framework.
Zero-delay retries retain `RequeueWireMessageWriter` and the direct SQL insertion path.

PostgreSQL performs delayed sends and retries using native `visible_timeout` SQL
expressions. Its `SendWithDelay` methods do not delegate to the `Scheduler` property;
its requeue methods update content through the selected requeue writer in the same
SQL statement that updates visibility. There is no callback publication selector on
this path. The initial native suite covers selected delayed sends and immediate
selected requeue, while existing control tests cover delayed requeue. This source
applicability disposition does not claim a separate persisted scheduler restart or
an additional compound positive-delay/custom-requeue runtime test.

### MQTT scheduled retry selection — 2026-11-09

Validated native callback dispatch uses the subscription-captured
`ScheduledRequeuePublicationTopic` on a private message copy. A typed registry entry
for that lookup topic and original CloudEvents type selects the publication writer;
the actual MQTT destination remains the original topic under the configured prefix.
Four sync/async custom/default cases pass per framework and all four fail when the
selector stamp is omitted. Full MQTT regressions pass 121 cases/four existing skips
per framework. Immediate retry/rejection writers have separate captured selectors.

### AWS automatic callback pump recovery — 2026-11-09

Actual Floci Scheduler callbacks run through the native queue, generic public JSON
mapper, ServiceActivator Proactor and production scheduler handlers. Business-writer
throw/null leaves the original outbox message pending while the callback queue is
settled. Explicit outbox recovery uses the selected SQS/SNS writer and fresh telemetry.
Eight cases pass per SDK/framework; sixteen body-corruption failures and four full
627-case regressions establish this boundary. This does not provide automatic business
redelivery, durable outbox or hosted AWS permissions validation.

### Remaining native retry and broker-control applicability — 2026-11-09

Current source review distinguishes operations that create a new application wire
message from broker operations on an existing delivery:

| Path | Current behavior and mapping boundary |
| --- | --- |
| SQS retry | Changes native visibility; it does not publish or invoke a writer. Explicit rejection producers have separate captured writer selections. |
| GCP pull/stream retry | Releases the native ack ID or completes the stream callback as nack; native retry policy controls delivery. No scheduler callback or inverse-reader writer is involved. Brighter-created rejection producers have separate captured writers, validated by 24 native cases per framework. |
| RocketMQ retry | Changes the native delivery's invisible duration, with existing broker limit handling. It does not republish application bytes. |
| PostgreSQL retry | Updates native row visibility and rewrites content through the captured requeue writer; no scheduler callback. Direct delayed sends also map before the SQL insert. |
| GCP delayed publication | Passes the original Brighter message to the configured scheduler; callback dispatch returns through ordinary producer lookup and the same selected immediate/bulk mapping boundary. Four native default/composed sync/async cases now pass through real InMemoryScheduler callbacks and typed registry/outbox dispatch, including current telemetry and ordering; the selected-writer bypass fails both custom cases. |
| RocketMQ delayed publication | Native delay topics map before sending; other topic types delegate to the configured scheduler. Eight native default/composed sync/async cases now validate both native delay and real scheduler callbacks, with selected payload, current telemetry, delay, native key/tag/destination and settlement. The selected-writer bypass fails all four custom cases. |
| ASB direct topic retry | Abandons the existing delivery, retaining broker-owned content and settlement. |
| ASB queue/forwarding retry | Native mode captures the consumer publication writer; the global Azure scheduler captures `RequeueWireMessageWriter`. Mapping follows trace propagation and preserves custom body while retry identity/session/partition/count remain scheduler-owned. Twelve new SDK-I/O cases and 524 prior local cases pass per framework; hosted native acceptance remains open. |

The ASB queue-retry mapping bypass is corrected locally in both adapters. Twelve
behavioral RED cases become GREEN per framework. The complete local inventory passes
536 cases with every prior outcome retained; 80 native-service test classes are not
part of that local inventory. Existing hosted queue/session scheduling controls must
still pass on the new implementation. Hosted Service Bus access currently requires
refreshing Azure authentication; SDK-I/O evidence alone cannot close that gate.

Source review is an applicability assessment, not runtime proof or whole-issue sign-off.

### Kafka scheduled retry publication selection — 2026-11-09

Four native cases per framework establish the captured publication selector through
real InMemoryScheduler callbacks, typed producer lookup and outbox dispatch. The
logical topic, payload and CloudEvents type remain unchanged; the alias topic is
empty, local selector metadata stays off the wire and only the original offset is
committed. The selector omission fails all four format assertions. Both final full
suites pass 412 cases including the eight new dead-letter cases. Existing native
header cleanup and acknowledge-after-registration behavior are preserved.
