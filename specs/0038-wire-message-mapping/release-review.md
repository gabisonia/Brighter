# Wire mapping release review

## Completion checkpoint — 2026-11-11

**Release decision: blocked.** Implementation and the local requirement review are
complete; production readiness is not established. The finite
[five-step plan](completion-plan.md#active-completion-plan--2026-11-11) supersedes
earlier execution queues. No new gateway capabilities or unchanged broad reruns
are queued. Existing work is preserved; no commits or pushes.

The [native SDK probe](review/asb-sdk-dlq/README.md) reproduces the ASB topic/DLQ
fault without Brighter on both frameworks. Four queue controls pass per framework.
Opening empty DLQ receivers first is not a reliable workaround: topic failures
remain on .NET 9, while the .NET 10 follow-up passes. The probe establishes that
Brighter is unnecessary for that failure, not the exact SDK/emulator/backend cause.
All eight diagnostic containers are stopped. This work changes no production or
compiled test source and does not justify another full regression run.

The [lifecycle disposition table](local-review.md#lifecycle-findings) specifies the
missing observation for each historical ASB, GCP and Kafka finding. These are open
release findings. Neither the new SDK reproduction nor earlier passing matrices
explain them. There is no supported additional production fix from this review.

### Resume criteria and acceptance order

1. **Healthy isolated ASB fixture:** run the original eight reader-recovery cases
   on each framework, then the ten named mutations with fixture reset between
   mutations. Preserve the [approved contract](review/asb-reader-failure-recovery/README.md).
   Verify native retry/scheduler identity, timing, duplicate detection and settlement.
   Establish the [lost-lock precondition](review/asb-cancellation-abandon-failure/README.md)
   before interpreting cleanup assertions or applying its six mutations. Its retained
   test is outside the compiled project; it must be included in the isolated validation
   checkout before invoking its filter. No existing uncompiled case is claimed as run.
2. **Native x86-64 SQL Server 2022:** run the complete MSSQL matrix on both targets
   against a dedicated test database and retain TRX, server version, host/container
   architecture and source manifest. Compare all identities with the existing
   396-pass/four-skip SQL Edge evidence; explain any differences. ARM64 emulation
   is not a substitute. The suite creates/deletes test database objects.
3. **Historical lifecycle findings:** resume only with the failing correlated
   observation described in the disposition table or a concrete discriminating
   hypothesis. Any demonstrated correction requires RED and affected regression
   evidence. A passing repeat cannot close these findings.
4. **Final-source CI and release review:** validate the actual dirty source snapshot,
   including untracked feature files. The current workflow has push/PR triggers and
   no manual dispatch; running an older remote commit cannot certify this checkout.
   No publishing is authorized. After all gates close, recheck source continuity,
   required outcomes/skips and API/build evidence before changing the release decision.

Prepared commands from the repository root, after the named fixture preconditions
are satisfied (repeat with `-f net10.0` and a separate results directory):

```sh
dotnet test tests/Paramore.Brighter.AzureServiceBus.Tests/Paramore.Brighter.AzureServiceBus.Tests.csproj -c Release -f net9.0 --filter FullyQualifiedName~AsbReaderFailureRecoveryTests --logger trx --results-directory /tmp/brighter-4170-final-asb-net9 --blame-hang-timeout 5m
dotnet test tests/Paramore.Brighter.MSSQL.Tests/Paramore.Brighter.MSSQL.Tests.csproj -c Release -f net9.0 --logger trx --results-directory /tmp/brighter-4170-final-mssql-net9 --blame-hang-timeout 5m
```

These are starting acceptance commands, not claims that all gates reduce to two
test invocations. ASB credentials and dedicated entity selection must be supplied
through the existing fixture configuration without logging credentials. No Azure
login, hosted test, SQL host request, paid resource or remote CI run was initiated
in this checkpoint. The current source check is retained in
[verification](review/completion-checkpoint-verification.json).

## Execution priority — 2026-11-10

The [earlier plan](completion-plan.md#earlier-local-execution-order--2026-11-10)
puts local work first: ASB in-memory session-lock diagnosis, GCP shutdown/recovery
review, Kafka follow-up only when a new discriminating hypothesis or failure exists,
then final local implementation/evidence reconciliation. The bounded ASB/GCP
source/evidence review is now recorded in the [local review](local-review.md), with
both lifecycle findings still unresolved. Kafka has no new failure to capture.
The [final local requirement review and handoff](local-review.md) are now complete.
They retain every unresolved lifecycle and deferred acceptance gate. The follow-up
corrects stale Kafka ownership and AWS scheduler documentation without changing code.

Hosted ASB acceptance, supported x64 SQL Server validation and remote CI are held
for later. They remain release requirements, but do not block the local queue or
justify repeated access checks. No passing rerun closes an unexplained failure.
The issue remains not production-ready; no tests, requirements or recorded failure
evidence are removed by this priority change. No commits or pushes.

## Bulk tracing correction — 2026-11-10

The [local requirement review](review/bulk-dispatch-trace/README.md) reproduced
stale stored trace context during bulk ASB/GCP outbox dispatch. Both bulk paths
now propagate the producer context before invoking the selected/default writer.
Twelve RED observations become passing regressions; ASB's complete local inventory
passes 540 cases per framework and GCP's available native inventory passes 459
with 55 existing skips per framework. Every prior identity is retained.

All declared targets of the two gateways build without warnings/errors, and eight
rebuilt API comparisons pass. Main-workspace focused checks confirm the final
files. ASB publication XML now describes bulk writer selection correctly. Existing
test-project warnings remain; the new tests emit none in the final builds.
Historical lifecycle findings and deferred native acceptance remain open. The
earlier build/input and regression checkpoints below precede this correction.

## Current checkpoint — 2026-11-09

The full issue is **not production-ready**. The [completion plan](completion-plan.md)
tracks remaining acceptance work. All changes remain local; no commits or pushes.

Current validated and integrated packages include RabbitMQ Async (566 passes/two
existing skips per framework), RabbitMQ Sync (299 passes/one skip), MQTT (121
passes/four skips), RocketMQ (136 passes/two skips), SQL Server (396 passes/four
skips) and PostgreSQL (341 passes on its declared .NET 10 test target). AWS SDK v3/v4
callback-pump recovery passes four complete 627-case suites; the new tests are
integrated and confirmed from the main workspace. Kafka dead-letter and
scheduled retry selection pass both complete 412-case suites and fourteen combined
main-workspace acceptance/ownership cases per framework. GCP rejection selection
passes 24 native cases each and twelve intended cache mutation failures; real scheduled callbacks add four passing cases per framework. Regular full matrices
record 457 passes/55 skips on net9 and 456 passes/one ordered-nack failure/55 skips on
net10; the generated diagnostic full net9 run also reproduces that failure. Native
lease-order controls support the emulator stream-membership finding, while separate
historical shutdown findings remain open. Main-workspace focused checks pass.

Redis's certified quota-paced aggregate preserves all 207 identities per framework:
203 passes/four skips. It is an aggregate, not a single-process full-suite pass.
Suspended attempts are retained separately and excluded from certification.

Provider-specific durable process restart is validated for Hangfire/PostgreSQL,
Quartz/PostgreSQL JobStoreTX/System.Text.Json and TickerQ/EF Core SQLite. Their full
suites pass 54, 53 and 54 cases respectively per framework. Those probes do not
establish arbitrary persistence-provider or database/broker crash recovery.

Final Release and Debug solution builds pass with zero errors and all 7,248 source/build
inputs unchanged; repository warnings remain. Main-workspace confirmation passes four
GCP and eight RocketMQ scheduling cases per framework. No issue-owned test container
remains running; logs and containers are retained.

ASB retry writer selection is integrated with 536 local SDK-I/O passes per framework;
hosted scheduling acceptance remains open.

All 68 compiled API comparisons across 18 changed production assemblies pass against
HEAD with complete dependency resolution. The separately checked compiler-generated
async-state-machine attribute differences are retained and classified.

Remaining gates are ASB native scheduling/DLQ/session/lost-lock coverage and its unresolved
in-memory lock failure, GCP historical streaming failures, the retained Kafka native
disposal stall, supported x86-64 SQL Server 2022 runtime coverage, and unrun remote CI. The typed composition design is
the issue's permitted alternative to a mapping DSL. No unrun hosted coverage or CI
success is claimed. Historical checkpoints below retain their original scope and
counts; use this section and the completion plan for current status.

## Historical scope review — 2026-10-30

Full-feature production readiness remains open. The historical results below are
scoped evidence, not a cumulative sign-off on every transport or execution path.

| Workstream | Remaining work |
| --- | --- |
| Remaining gateway integrations | MQTT publication writer/subscription reader selection and bounded reader/writer throw/null recovery are validated (99 passes/four existing skips on both frameworks). Redis publication writer/subscription reader selection, bounded throw/null recovery, selected legacy/native reader error boundaries, immediate retry selection/recovery, sequential retry endpoint isolation and invalid/dead-letter writer selection/recovery plus fallback, absent/ineligible-destination and native-counter error boundaries are validated (170 passes/four existing skips on both frameworks). Its approved four-case rejection-writer isolation tests and twelve mutation failures pass. The user-authorized paced regression run completed with 174 passes/zero failures/four skips per framework, preserving all prior outcomes without a license or code/dependency/test changes. The earlier single-process quota failures remain recorded separately. The subsequent four-case fallback recovery batch is approved: initial/final focused tests pass on both frameworks, all sixteen intended mutation failures are verified with exact restoration, and four library targets build cleanly. Its complete paced regressions pass 178 cases/four existing skips per framework with all preceding outcomes preserved. Redis fallback mapping recovery is validated; delayed retry mapping, other native failure windows and wider lifecycle coverage remain open. RocketMQ, SQL Server and PostgreSQL still lack configured wire-reader/writer integration. Each gateway needs native-control, default-compatibility, failure and applicable batch/stream/transaction validation; broader MQTT lifecycle paths also remain open. |
| Azure Service Bus | Reader/writer, batch, scheduling and selected session/cancellation paths are implemented and validated. Session-lock instability, DLQ-blocked recovery, abandonment failure during cancellation and unavailable hosted coverage remain open. |
| GCP Pub/Sub | Publication, pull and streaming selection, mapping-failure recovery and pre-submission send cancellation are implemented. Native observation reproduces missing ordered redelivery after a successful correlated nack, including in a native SDK control with no Brighter dependency; a native protocol control isolates the emulator stream-handoff stall. All 24 hosted SDK observations recover both messages with completed native nacks and verified cleanup, and all twelve hosted Brighter ordered-nack cases pass through public factories. All sixteen hosted factory shutdown/native-or-replacement cases also pass with verified cleanup. All eight hosted buffered-redelivery shutdown cases pass with verified unread readiness and cleanup. The retained emulator restoration/regression gate and broader lifecycle/concurrency paths remain open. Full affected regressions retain unresolved lifecycle failures. Cancellation and writer throw/null after confirmed bulk progress are characterized. Sequential custom/default publication writer isolation is validated with twelve intended mutation failures; the concurrent same-producer writer batch adds three passing sync/async/bulk cases and three intended cache-mutation failures. Shared pull-reader overlap and native acknowledgement are validated in two cases with eight intended mutation failures. Shared stream-reader overlap, native wrapper ownership and independent callback/native acknowledgement are validated in two cases with ten intended mutation failures. The latest full matrices record net9.0 429 passes/55 skips and net10.0 428 passes/one known synchronous ordered-nack failure/55 skips; earlier lifecycle findings remain open despite intermittent passes. Other partial batches, remaining lifecycle/concurrency/rejection/scheduler coverage and the earlier buffered-redelivery finding remain open. |
| Kafka | Investigate the observed .NET 9 commit/revoke test stall in native assignment during disposal (stack evidence retained); Gated in-flight rejection, confirmed broker retry failure and tested writer/null-result recovery and async caller cancellation and internal confirmation timeout during mapping are validated; resolve or explicitly disposition late acknowledgements, rebalance during forwarding, pre-rejoin delivery, other cancellation windows and missing-confirmation scenarios, scheduled retry mapping, transactions/batches and automatic pump recovery. Sequential and selected reassignment evidence does not establish general concurrency safety. |
| Scheduling | AWS automatic callback-pump failure recovery; scheduler-backed retry mapping; TickerQ and Hangfire execution/persistence; remaining Quartz persistence configurations and transport combinations. See the scheduler source audit. |
| Configuration and examples | Review the requested configuration DSL, document the agreed API, and provide executable non-CloudEvents telemetry examples plus final ADR/documentation updates. |
| Final release verification | Run the final cross-transport compatibility/failure/concurrency matrix and available native regressions, inspect CI evidence, and reconcile every acceptance requirement with implemented code or an explicitly agreed scope change. |

These are implementation and validation workstreams, not a short release checklist.
No defensible total completion estimate follows from the number of open checkboxes:
several cover whole gateways or several execution paths.

This review covers the local AWS SDK V4 and RabbitMQ Async implementation.
This is the completed first-increment review, not full-feature sign-off. The user
subsequently required full implementation. A reread of the issue also identified
missing direct acceptance evidence for relocating all three telemetry fields
outside CloudEvents and restoring consumer tracing. The SQS gateway-to-tracer
cases cover that scenario. The subsequent real-pump batch validates SQS Reactor
and Proactor handlers in both SDKs: all four complete AWS suites pass 607 cases,
and both Core suites pass 1,716 with seven existing skips. At that checkpoint,
SNS/RabbitMQ telemetry, scheduler paths and other transports remained open; later
scoped results are recorded below. The full implementation checklist
in tasks.md is now the completion criterion.

The subsequent native AWS callback batch validates actual emulator-fired SQS
callbacks followed by registered handler/command-processor dispatch to custom
SQS and SNS business writers. Dispatch-time traceparent, tracestate and baggage
are observed independently on the native output. Eight mutations produce sixteen
intended failures; all four AWS suites pass 611 cases and both Core suites retain
1,716 passes/seven skips. Production source is unchanged in that increment.
Automatic callback-pump settlement, failure redelivery and real AWS IAM remain
unverified. The subsequent native SNS source-preservation batch reproduces and
fixes mutation of the source header when correlation is absent. Six mutations
produce sixteen intended failures; all four full Scheduler suites pass 74 cases,
and all four gateway suites retain 611. Supplied IDs and generated native wire
correlation remain intact. This closes that specific gap, not global readiness.

The subsequent SNS pump batch validates raw and enveloped delivery with telemetry
in native attributes or JSON body, through Reactor and Proactor handlers in both
SDKs. Ten mutations produce eighty intended assertion failures; all four complete
AWS suites pass 619 cases and both Core suites retain 1,716 passes/seven skips.
Production files are unchanged. This closes the ordinary SNS handler-telemetry
coverage gap. The subsequent RabbitMQ pump batch validates custom AMQP headers or
JSON body, dispatch-time producer telemetry, Reactor/Proactor handler context and
acknowledgement verified after closing the consumer channel. Seven mutations
produce 24 intended failures; both full RabbitMQ suites pass 540 cases with two
existing skips and both Core suites retain 1,716 passes/seven skips. All 1,277
production source files match the pre-batch manifest. Scheduler paths, other
gateways and final configuration review remain open; this is not full sign-off.

The Quartz completion correction adds a return after successful message or request
dispatch, preserving the missing-payload error. The amended approved tests reproduce
four completion failures per framework; four mutations then produce nine intended
failures, and both full Quartz suites pass 53 cases without skips. All 48 baseline
outcomes remain. No shared Core/gateway source changes are needed. The subsequent
native Quartz/RabbitMQ batch validates configured writing through actual scheduled
message/request dispatch, including original payload/identity, source preservation,
native controls and producer traceparent. Four mutations produce 24 intended
failures; both full RabbitMQ suites pass 548 cases with two existing skips and both
Quartz suites retain 53 passes. No production correction is needed. Writer failure
recovery now passes with the approved trace-chain assertion: three mutations yield
24 intended failures, both RabbitMQ suites pass 556 cases/two existing skips, both
Quartz suites retain 53 passes and both Core suites retain 1,716 passes/seven skips.
No production correction is needed. Zero-delay timestamp rejection is unchanged.

The RabbitMQ Sync endpoint batch passes with the approved
[callback-based amendment](review/rmq-sync-native-properties/README.md): six cases
per framework, eight mutations yielding 26 intended failures, and both full suites
with 219 passes/one existing skip. All 213 baseline passes remain. The gateway
creates native properties through its channel and then invokes the mapping callback;
no client upgrade or dependency change is needed. Existing public consumer
constructors remain unchanged. The subsequent [four reader-failure cases](review/rmq-sync-reader-failures/README.md)
reproduce and fix batch loss from configured throw/null results. Five mutations
produce 20 intended failures, and both full Sync suites pass 223 cases/one existing
skip while retaining all preceding outcomes. Only the Sync consumer changes.
The [default-parser failure correction](review/rmq-sync-default-parser-failures/README.md)
preserves native bytes, handled count and delivery metadata. Four mutations produce
12 intended failures. Final full suites pass 227 cases/one existing skip each,
retaining all prior outcomes. The first .NET 9 full run encountered a broker storage
I/O error during queue binding; three targeted rechecks per framework and both full
reruns pass without source changes. This does not resolve the broker storage cause.
The [ten forwarding/recovery cases](review/rmq-sync-invalid-forwarding/README.md)
pass on both frameworks with all 1,280 production files unchanged. Both full suites
pass 237 cases/one existing skip each. Six mutation runs produce 42 intended failures,
but an earlier acknowledgement-omission run produces nine failures and one pass.
The approved [bounded-observation amendment](review/rmq-sync-settlement-observation/README.md)
replaces the single immediate source-queue read. All six mutations now produce
42 intended failures, and six additional acknowledgement-omission runs produce
60 intended failures with no misses. Both full suites retain exactly 237 passes
and one existing skip each. The original miss remains recorded, and two-second
observation does not prove atomic publication/acknowledgement or absence forever.
The approved [fourteen invalid-writer cases](review/rmq-sync-invalid-writer/README.md)
pass with the approved [native-tag correction](review/rmq-sync-invalid-writer-native-tag/README.md).
Nine mutations produce 22 intended failures; both full suites pass 251 cases/one
existing skip, retaining every preceding outcome. Three files add subscription,
factory and consumer selection; all other production files and dependencies remain
unchanged. Initial incorrect-assertion evidence is retained. The subsequent
[six publication recovery cases](review/rmq-sync-publication-recovery/README.md)
pass initially and after restoration on both frameworks without production changes.
Five mutations produce 26 intended failures. Full Sync suites pass 257 cases/one
existing skip each, retaining every preceding outcome. The subsequent
[eighteen immediate-retry cases](review/rmq-sync-requeue-writer/README.md) pass on both
frameworks after implementation and restoration; fourteen mutations produce 60
intended failures. Full suites pass 275 cases/one existing skip, retaining all prior
outcomes. Four Sync files add per-consumer retry selection and gateway-owned native
controls without dependency changes. The subsequent
[eight native-delay cases](review/rmq-sync-native-retry-writer/README.md) pass with
the eighteen immediate-retry cases on both frameworks. Seven mutations produce
20 intended failures; final full suites pass 283 cases/one existing skip and retain
all prior outcomes. No production changes are needed. Equivalent assertion-style
cleanups resolve xUnit2031 and xUnit2012, with all focused cases passing afterwards.
The [eight concurrency cases](review/rmq-sync-concurrent-mapping/README.md) pass
initially and after restoration on both frameworks. The approved
[completed-properties cache amendment](review/rmq-sync-concurrent-cache-amendment/README.md)
produces all four intended isolation failures, completing twelve mutations with
34 expected failures. After restoration, all eight focused cases pass again and
both full Sync suites retain exactly 291 passes and one existing skip. No production
changes were needed. The [Kafka default adapters](review/kafka-default-wire-mapping/README.md) are now
implemented and validated with the approved [charset correction](review/kafka-default-content-type/README.md).
All four focused cases pass on both frameworks, ten mutations produce their intended
failures, and both complete Kafka suites pass 269 cases with no failures/skips,
retaining all 265 baseline outcomes. Scheduler-backed retry, remaining Kafka coverage
and full #4170 implementation remain open.

## Public API compatibility

The changed production files retain existing public constructor signatures and
the public RabbitMQ conversion entry point. New settings use transport-specific
closed generic contracts. Core has no new broker SDK dependency.

The source-compatibility review found an overload ambiguity in this existing call:

```csharp
new SqsMessageConsumer(connection, queueName, default)
```

The third argument previously selected `int batchSize`. The new public
reader-taking constructor also accepts a default literal in that position,
and its remaining parameters are optional. The two third-argument types have
no implicit conversion between them to select a better overload.

Existing binaries still have their original constructor. Retaining that binary
signature alone is insufficient to preserve source compatibility.

The correction makes the new reader-taking constructor internal.
`SqsMessageConsumerFactory` is its only production caller and configures the
reader from the subscription. RabbitMQ already uses an internal constructor
for the corresponding factory configuration.

The public-surface test fails before the correction and passes afterward on
.NET 9 and .NET 10. Making the overload public again restores the assertion
failure. The external legacy-call probe reports CS0121 before the correction,
then builds on netstandard2.0, net8.0, net9.0 and net10.0.
Existing custom-reader factory cases still pass through both entry points.
Full AWS regression results are recorded separately in the validation log.

## Wire format and delivery ownership

The later AWS scheduler regression exposed a pre-existing timestamp precision
defect. Native scheduled SNS/SQS timestamps now use invariant round-trip text
instead of culture-dependent `Convert.ToString`. This preserves the exact instant
required by the existing header assertions. Brighter's reader accepts it, but
external consumers tied to the old text format must accept the corrected ISO
representation. No public signature or persisted outbox schema changes.

The writer extraction retains the existing default native fields. The intended
differences are fresh serialized bags for AWS writers, preserved failure payloads
and delivery metadata, and enforcement of RabbitMQ retry original identity.

| Boundary | Reviewed behavior | Evidence and limits |
| --- | --- | --- |
| SQS/SNS publication | Writers supply content; senders retain destination and FIFO controls | Native publication, isolation and overlap cases pass; native scheduler targets excluded |
| SQS consumption | Custom mapping precedes consumer receipt/count attachment; defaults preserve raw/envelope parsing | Constructor correction passes both complete AWS V4 Release suites |
| RabbitMQ publication | Writer output precedes gateway persistence/delay/delivery metadata; confirmation uses original Brighter identity; SDK sequence header overrides a conflicting writer value | Conflict cases and named mutation verified; both full Release suites pass |
| RabbitMQ direct retry | Subscription writer selection; gateway identity/count/persistence/delay enforcement; source acknowledged after publication | Immediate/native delayed retry, failure recovery and overlap cases pass; scheduling exclusions below |
| Reader failure | Throwing/null mappings preserve native payload and settlement metadata; batches keep other deliveries | Failure and mixed-batch cases retained in the final 499 AWS / 512 RabbitMQ passing suites |

Body and mutable nested-header values are not universally deep-copied. Default
RabbitMQ writing retains read-only body memory. Composition must replace values
when changing them; callers must not mutate the source.

RabbitMQ retry-count reconsumption reproduced a defect in both factory paths:
a replacement reader returned count zero on the first retry instead of one.
The consumer now attaches the canonical native handled count after custom
mapping, reusing the internal parser. Without that header, the reader's count
remains. Two successive immediate retries retain counts one and two on both
frameworks. Removing the attachment restores the original assertion failure.
Ten precedence cases also verify that integer and UTF-8 counts override a
reader-supplied count of 99, malformed and unsupported types fall back to zero,
and a missing count header preserves 99. Separate mutations verify both native
precedence and missing-header preservation. Both full Release suites pass with
502 cases and two existing skips per framework. The lifecycle cases also
reproduce and correct count loss after unconfigured default-parser failure.
Native delayed reconsumption retains counts one and two. Both lifecycle
mutations fail at their intended assertions, and all 22 count cases pass after
restoration. Both final full Release suites pass with 512 cases and two existing
skips per framework, retaining every preceding test and outcome.

SNS overlapping output lifetimes are now checked through native envelopes in
both sending modes. The shared-attributes mutation fails both cases at the
intended distinct-ID assertion, and byte restoration returns both frameworks
to GREEN. Both full AWS V4 Release suites pass with 491 cases and no skips or
failures. This does not prove simultaneous execution inside the default converter.
SNS publication isolation also passes in both factory/send modes and both
default/custom creation orders. Four fresh-process shared-writer mutations fail
at mapped-topic equality. Both final full AWS V4 Release suites pass with 495
cases and no skips or failures, retaining every preceding case and outcome.
Scheduled SQS dispatch through the real in-memory scheduler also passes both
factory/send modes with native wire observation. Moving trace propagation until
after sending fails both intended assertions. Byte restoration and both full
AWS V4 Release suites pass with 497 cases and no failures or skips per framework.
Scheduled SNS dispatch through the real in-memory scheduler now passes both
modes as well. Both intended trace mutations fail, byte restoration returns
GREEN and both final AWS V4 Release suites pass with 499 cases, no failures and
no skips. These checks do not establish AWS native scheduler target support.

## RabbitMQ control-header audit

The implemented publication and direct-retry paths were checked against the
existing native assertions. This review adds no production behavior or tests;
the latest RabbitMQ full-suite evidence remains 512 passes and two existing
skips on each framework.

| Control | Production enforcement | Existing evidence |
| --- | --- | --- |
| Delay | `AddDeliveryHeaders` removes writer delay and adds the requested native delay only when positive | Immediate delay removal, custom native delay and conflicting retry-header cases |
| Delivery tag | Publication uses the source tag or removes a fabricated tag; direct retry stamps its own marker | Delivery-tag ownership cases and conflicting retry-header cases |
| Retry handled count | Direct retry overwrites the writer value; consumer attaches the native count when present | Conflicting retry headers, count precedence and immediate/native delayed reconsumption |
| Retry identity | Direct retry generates a new native message ID and preserves the original-ID chain, removing conflicting case variants | Canonical conflict cases and original-ID case-variant cases |
| Persistence | Publication and direct retry overwrite writer delivery mode from `message.Persist` | Custom publication and retry conflict cases |
| Confirmation sequence | SDK confirmation tracking supplies the canonical sequence header; Brighter tracks the source application ID | Conflicting confirmation-header cases and the disabled-tracking mutation |

The audit concerns controls used by these gateway paths. Original-ID matching
is deliberately case-insensitive; other application headers are not globally
normalized. This does not extend the retry writer to scheduler-backed retry or
invalid-message forwarding, and does not claim simultaneous converter execution.

## Shared mapping state review

Source review finds no per-message cache in the supplied default converters.
This is static evidence, distinct from the native overlap tests.

| Converter | State used across calls | Per-call output |
| --- | --- | --- |
| Default SQS writer | Constructor-supplied queue URL | New carrier, attribute dictionary, attributes and serialized bag copy |
| Default SNS writer | Constructor-supplied topic ARN | New carrier, attribute dictionary, attributes and serialized bag copy |
| Default SQS reader | Constructor-supplied raw/envelope mode | New parser and parsed/failure message |
| Default RabbitMQ writer | Private header-name set, initialized once and only read | New properties and header dictionary; body memory and nested user header values may be shared with the source |
| Default RabbitMQ reader | Stateless wrapper; parser's static logger | New header, bag and message; native body storage is not universally deep-copied |

The native overlap/mutation cases establish independence of outputs and detection
of cached state. They do not prescribe instruction-level interleaving inside
converters. Applications must still make custom converter implementations safe
for concurrent use and must not mutate shared input bodies or nested values.
The scoped concurrency review combines these runtime cases and source inspection;
it does not establish exhaustive runtime validation.

## Supported first implementation

Publication writing and subscription reading are implemented for AWS SDK V4
and RabbitMQ Async. RabbitMQ direct immediate and native delayed retries select
the subscription retry writer.

Scheduler-backed retry and invalid-message forwarding retain their existing
mapping. `RequeueWireMessageWriter` configures neither path. These exclusions
must remain explicit in release documentation.

The AWS scheduler also needs a separate mapping contract. With
`UseMessageTopicAsTarget` enabled, `AwsScheduler.CreateTargetAsync` builds SNS or
SQS request payloads using its own conversion methods. Those methods do not
consult the publication writer. The scheduling branch in each producer executes
before its native writer path. This is a code-review finding, not validated
custom mapping support. Native scheduler targets must not be described as
honoring publication writers until this path is implemented and tested.

The original specification still requires the other gateway integrations.
A release containing these two transports must be described as a partial
implementation, without closing the global requirement. The
[approved release boundary](release-boundary-proposal.md) records the user’s
decision to finish this partial increment while retaining the global follow-ups.

## Validation and documentation

The [validation record](validation.md) preserves both the intermittent failures
and their passing repeats. The [fixture review](review/stabilization/README.md)
defines synchronization changes and named mutations. Repeat passes alone do not
prove the causes of the original failures.

[ADR 0086](../../docs/adr/0086-configurable-wire-message-writers.md) and
[ADR 0087](../../docs/adr/0087-configurable-wire-message-readers.md) have finalized
text for the locally approved increment; their upstream status remains Proposed.
The [external documentation draft](external-docs/wire-message-mapping.md) is local
and unpublished. Its three C# examples compile on all four supported AWS gateway
targets. Debug and Release solution builds and the legacy-constructor probe passed.

## Final local readiness assessment

No additional blocker was found in the reviewed, approved paths. The final review
covered the core contracts and native carriers, producer/factory configuration,
consumer delivery metadata, default converter extraction and retry ownership.
No production or test code changed during this documentation checkpoint, so the
existing completed regressions remain the relevant evidence.

| Area | Evidence | Limit |
| --- | --- | --- |
| AWS SDK V4 | Latest Release suites: 503 passed, zero failures/skips on .NET 9 and .NET 10 | Floci 1.5.19; not a real AWS service run |
| RabbitMQ Async | Final Release suites: 512 passed, two existing skips per framework; RabbitMQ 4.2 / 4.3, TLS and native delay | Historical empty original-ID read was not reproduced; exact cause unknown |
| Core and compatibility | Core: 1,716 passed, seven existing skips per framework; solution builds and four-target constructor probe passed | Earlier completed checks; no new core implementation since those checks |
| Scheduled dispatch | SQS/SNS in-memory scheduler tests and trace mutations pass | AWS native targets and other schedulers excluded |
| Concurrency | Isolation, overlapping output and named cache mutations; default shared-state source audit | Does not validate arbitrary custom converters or every thread interleaving |
| CI | Local workflow/filter review | No remote CI run or pass claimed |

The historical empty-read event remains a residual test-observation risk. Bounded
native observation and synchronized confirmation fixtures retain the intended
assertions, detect the named mutations and pass focused repeats and later full
suites. Those results do not prove the cause of the original event.

This increment is ready for review and CI within the approved scope. Global
rollout, native scheduler mapping and upstream ADR acceptance remain separate.
No commit, push, publication or deployment is included in this assessment.

The recovery batch's initial .NET 10 full run had one failure in the existing
native-delay Classic/fanout routing case. All 18 focused repeats and the complete
rerun passed unchanged. The original failure is retained; its cause is not
established and no fix for it is claimed. See the [validation record](validation.md#quartz-scheduled-writer-recovery-validated).


The [eight Kafka endpoint cases](review/kafka-endpoint-wire-mapping/README.md) now
pass on both frameworks. Publication/subscription mapping selection preserves
custom dispatch trace, original confirmation identity and native acknowledgement
offsets. Eight mutations produce 32 intended failures; both complete Kafka suites
pass 277 cases with no failures/skips and retain all 269 preceding outcomes.
The [eight reader-failure cases](review/kafka-reader-failures/README.md) now reproduce
and fix configured throw/null failures on both frameworks. Nine mutations produce
72 intended failures; both complete Kafka suites pass 285 cases with no failures/skips
and retain all 277 preceding outcomes. Only the consumer changes; approved tests and
dependencies are unchanged. The [eight required-header failure cases](review/kafka-default-parser-failures/README.md)
now reproduce and fix payload/metadata loss on both frameworks. Eight mutations
produce 60 intended failures; both complete suites pass 293 with no failures/skips,
retaining all 285 prior outcomes. Only KafkaMessageCreator changes in that increment.
The [eight content-type parser cases](review/kafka-parser-exceptions/README.md)
now reproduce and fix MIME-exception payload/metadata loss on both frameworks.
Nine mutations produce 68 intended failures; both complete suites pass 301 with no
failures/skips, retaining all 293 prior outcomes. Only KafkaMessageCreator changes
in that increment. The [eight explicit-unacceptable cases](review/kafka-explicit-unacceptable/README.md)
pass initially and after restoration on both frameworks without production changes.
Ten mutations produce 80 intended failures; both full suites pass 309 with no failures/skips,
retaining all 301 prior outcomes. The [eight writer-recovery cases](review/kafka-writer-recovery/README.md)
pass initially and after restoration on both frameworks without production changes.
Nine mutations produce 72 intended failures; both full suites pass 317 with no failures/skips,
retaining all 309 prior outcomes. The [six outbox/subscriber recovery cases](review/kafka-outbox-recovery/README.md)
pass initially and after restoration on both frameworks without production changes.
Seven mutations produce 40 intended failures; both full suites pass 323 with no failures/skips,
retaining all 317 prior outcomes. The [eight factory-isolation cases](review/kafka-factory-isolation/README.md)
pass initially after mechanical setup corrections and after restoration on both frameworks.
Eight mutations produce 32 intended failures; both full suites pass 331 with no failures/skips,
retaining all 323 prior outcomes. No production changes were needed. The
[sixteen custom-header cases](review/kafka-custom-header-failures/README.md) reproduce
and fix payload/metadata loss from duplicate and null-valued custom headers on both
frameworks. Ten mutations produce 144 intended failures; both full suites pass 347
with no failures/skips, retaining all 331 prior outcomes. Only KafkaMessageCreator
changes; approved assertions and dependencies remain unchanged. The
[four invalid-message writer cases](review/kafka-invalid-writer/README.md) now pass
on both frameworks with per-subscription selection, mapped rejection metadata,
source-topic restoration and native source acknowledgement. Nine mutations produce
34 intended failures; both full suites pass 351 with no failures/skips, retaining
all 347 prior outcomes. Only subscription/factory/consumer source changes; approved
files and dependencies are unchanged. The
[four rejection recovery cases](review/kafka-invalid-writer-recovery/README.md)
now reproduce and fix false success and lost source offsets after selected-writer
exceptions/null results. Eight mutations produce 32 intended failures; both full
suites pass 355 cases, retaining all 351 prior outcomes. Only KafkaMessageConsumer
changes; the approved test, reused helper and dependencies remain unchanged. The
[two broker-failure cases](review/kafka-invalid-broker-recovery/README.md) now reproduce
and fix false success after negative native delivery confirmations. Rejection waits
for a per-send confirmation before acknowledging the source. Eight mutations produce
16 intended failures; both final full suites pass 357 cases, retaining all 355 prior
outcomes. Only consumer/producer source changes; the helper and dependencies remain
unchanged. The test's SDK enum spelling is corrected without changing its intended
assertion or line numbers. The
[four offset-barrier cases](review/kafka-failed-forward-offset-barrier/README.md)
now reproduce and fix later acknowledgements committing past a failed forward.
That batch capped commits at the earliest failed source offset for the consumer
lifetime and verified recreation-based recovery. Eight mutations produced 32
intended failures; both full suites passed 361 cases, retaining all 357 prior outcomes.
The [four same-consumer recovery cases](review/kafka-failed-forward-same-consumer-recovery/README.md)
now reproduce and fix the stuck limit after a successful retry. Rejection releases
only the matching limit, limited commits retain later acknowledgements, and the
consumer remembers the highest acknowledgement on affected partitions. Eight
mutations produce 32 intended failures; both full suites pass 365 cases, retaining
all 361 prior outcomes. Only consumer source changes; the approved test, reused
helper and dependencies are unchanged. The [four multiple-failure cases](review/kafka-multiple-failed-forwards/README.md)
now reproduce and fix skipped unresolved records after partial recovery. The consumer
retains every outstanding failed native offset per partition and releases only the
successfully recovered record. Commit reduction preserves later acknowledgements
and stops at the earliest remaining failure. Eight mutations produce 32 intended
failures; both full suites pass 369 cases, retaining all 365 prior outcomes. Only
consumer source changes; approved test/helper and dependencies are unchanged.
The approved [four partition-isolation cases](review/kafka-partition-forward-isolation/README.md)
pass initially and after restoration on both frameworks; eight mutations produce
32 intended failures. Both full suites pass 373 cases with no failures/skips and
retain all 369 previous outcomes. All production files and dependencies are unchanged.
Concurrent commits/rebalance
and automatic pump recovery remain unverified or unfinished. Other malformed headers,
remaining failure/recovery paths, retry mapping, transactions, batches, concurrency
and the global #4170 gates remain open.


The approved [lost-partition recovery batch](review/kafka-lost-partition-recovery/README.md)
now passes with its [assignment-observation amendment](review/kafka-lost-partition-assignment-observation/README.md).
The amended cases reproduce the final-commit defect on both frameworks; clearing
forwarding state for lost/revoked partitions fixes all four cases. Eight mutations
produce 32 intended failures. Both full suites pass 377 cases without failures or
skips and retain all 373 preceding outcomes. Only KafkaMessageConsumer changes;
all other production files, dependencies and the reviewed amended test/helper are
unchanged. Earlier fixture failures remain recorded. Concurrent commit/lifecycle
safety, partial revocation, late acknowledgements and automatic pump recovery
remain open; this is not global production sign-off.

## Kafka in-flight rejection commit barrier validated

The approved [offset-metadata amendment](review/kafka-inflight-forward-offset-amendment/README.md)
retains the four synchronous/asynchronous and first-write success/failure cases.
All four reproduce a premature native commit on both .NET 9 and .NET 10: while
forwarding offset 0 is gated, acknowledging offset 1 commits 2 instead of 0.
Registering the unresolved source offset before forwarding fixes the defect;
failure retains the barrier and confirmed success releases it. Later acknowledgements
remain available for the final commit. Source metadata checks after failure and
native mapped key, wire ID and binary-body assertions remain intact.

All four pass on both frameworks before and after mutation restoration. Nine
mutations run against the new cases: eight yield 30 intended failures; releasing
the barrier on failure survives all four new cases because no subsequent commit
is attempted before retry. That same mutation is detected by all four existing
KafkaFailedForwardOffsetBarrierTests at the broker-commit assertion. This is 34
intended failures across ten mutation executions, including the explicitly recorded
survivor. No approved assertion was changed to improve those results.

Both complete Kafka suites pass **381 cases, zero failures and zero skips**,
retaining every one of the preceding 377 passing cases. The library builds for
netstandard2.0, net8.0, net9.0 and net10.0; tests build for net9.0 and net10.0.
Only KafkaMessageConsumer changes in production relative to this batch's baseline;
public declarations, dependency files and the approved amended test/helper remain
unchanged. All mutations are restored. No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-inflight-forward-amended/`, including
RED/green/restored/full TRX files, mutation results, supplementary survivor check,
source snapshots, build logs and verification.json. The original unamended test's
incorrect success-offset expectation remains documented in
`/private/tmp/brighter-4170-kafka-inflight-forward/`.

This proves the barrier for acknowledgements initiated after the writer is entered.
It does not establish safety for commits already reduced before registration,
rebalance during forwarding, late acknowledgements from a prior assignment,
concurrent retries of one record, missing confirmations, or automatic pump recovery.
The full-feature production-readiness gates remain open.

## Kafka immediate retry writer selection validated

The approved [eight retry-writer cases](review/kafka-immediate-requeue-writer/README.md)
cover sync/async calls, null/zero delay and selected/default mapping through the
public subscription and factory. The initial compilation confirms the missing
`KafkaSubscription.RequeueWireMessageWriter` property. With only that nullable
property added as a scaffold, all four selected-writer cases fail because the
writer is never called; all four default cases pass on both frameworks. This
provides behavioral RED independently of the initial compilation failure.

The subscription now exposes the nullable selection, the factory captures it on
the consumer, and the lazy retry producer uses it for its publication. Null
preserves default mapping. Retry selection is independent of invalid-message
selection; no existing constructor signatures or dependencies change.

All eight cases pass on .NET 9 and .NET 10 before and after mutation restoration.
Eight mutations produce 44 intended assertion failures: missing factory/producer
selection, use of the invalid writer, source key/ID/body substituted for mapped
values, omitted source acknowledgement, and committing past the new retry. Native
Kafka observations verify the exact retry offset and contents, an empty invalid
topic, and a commit that advances past the source only.

Both full Kafka suites pass **389 cases, zero failures and zero skips**, preserving
all 381 preceding outcomes. All four library targets and both test targets build
with no new warning diagnostics. Only KafkaSubscription, KafkaMessageConsumerFactory
and KafkaMessageConsumer change relative to this batch's production baseline.
The approved test and reused helper remain byte-identical; all mutations are
restored. No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-writer/`, including the initial
compile diagnostics, behavioral RED/green/restored/full TRX files, mutation logs,
source snapshots, build logs and verification.json. Optimized RED stack traces
point to line 125 while the invocation-count assertion is on line 126; recorded
failure messages confirm expected 1 versus actual 0. The initial .NET 9 harness
stopped on that line-number check; its four intended failures and four default
passes are retained and verified independently.

This closes immediate retry selection only. Writer exception/null recovery,
negative delivery confirmations, missing confirmations, cancellation,
scheduler-backed dispatch, concurrent lazy initialization and consumer-pump retry
behavior remain unverified or unfinished. In particular, send-plus-flush must not
be treated as proof that failed forwarding prevents source acknowledgement.
Global production readiness remains open.

The [broker-rejected retry recovery batch](review/kafka-requeue-broker-recovery/README.md) is now validated; its final results follow below.

## Kafka immediate retry broker-rejection recovery validated

The approved [two broker-recovery cases](review/kafka-requeue-broker-recovery/README.md)
reproduce false success after Kafka rejects an oversized retry on both .NET 9 and
.NET 10. Both sync and async requeue previously returned true. An independent
native publication verifies the broker's message-size rejection before the tested
operation, and native observers verify source commits and retry contents.

Immediate requeue now registers the unresolved source offset and waits for
per-send persistence confirmation. A negative confirmation returns false and
restores source-offset metadata; later acknowledgements remain capped at that
record. Successful retry releases its barrier while retaining later acknowledgements.
The existing confirmation helpers are shared with rejection under names that
reflect forwarding rather than a particular destination. Their five-second bound
remains unchanged. Delayed dispatch calls remain unchanged. The immediate path
handles ChannelFailureException here; other mapping exceptions, cancellation and
missing confirmations remain separate coverage gaps.

Both cases pass on both frameworks before and after mutation restoration. Eight
mutations produce 16 intended failures: ignored negative confirmation, omitted
source-offset restoration, omitted barrier, retained barrier after recovery,
discarded later acknowledgement and replacement of mapped key, wire ID or body.
The restoration mutation fails at the expected missing source-offset lookup; all
other failures are the intended result, commit or native-content assertions.

Both complete Kafka suites pass **391 cases, zero failures and zero skips**,
preserving all 389 preceding outcomes. All four library targets and both test
targets build with no new warning diagnostics. Only KafkaMessageConsumer changes
relative to this batch's production baseline. External public declarations, all
268 dependency/build files, the approved test and reused helper are unchanged.
All mutations are restored; no commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-broker-recovery/`, including
RED/green/restored/full TRX files, mutation logs, source snapshots, build logs and
verification.json. This establishes an explicit caller retry on the same consumer
after confirmed broker rejection. Automatic pump recovery, writer exception/null
handling, cancellation, missing confirmations, general concurrency/rebalance safety
and scheduler-backed retry mapping remain unverified or unfinished. Full-feature
production readiness remains open.

The [writer-failure recovery batch](review/kafka-requeue-writer-recovery/README.md) is now validated; its final results follow below.

## Kafka immediate retry writer-failure recovery validated

The approved [four writer-recovery cases](review/kafka-requeue-writer-recovery/README.md)
cover sync/async retry and writers that throw InvalidOperationException or return
null. Initial runs on both frameworks produce two passes and two intended failures:
the producer already wraps the tested writer exception in ChannelFailureException,
so its recovery path works, while a null native message escapes as
NullReferenceException. The initial runner expected four failures and stopped after
.NET 9; its expectation was corrected after inspecting the producer's wrapping
behavior, then .NET 10 confirmed the same result. No test assertions changed.

The immediate retry catch in each sending mode now handles non-cancellation
exceptions, so null-result failures reach the existing source-offset restoration,
false result and commit-barrier recovery path. OperationCanceledException remains
excluded, preserving the existing propagation policy; its source-restoration and
recovery behavior are not established by these cases. Delayed dispatch is unchanged.

All four cases pass on both frameworks before and after mutation restoration.
Nine mutations produce 36 intended failures: restricting recovery to negative
broker confirmation, reporting success after failure, omitting source-offset
restoration or the barrier, retaining the barrier after recovery, discarding the
later acknowledgement, and replacing the mapped native key, wire ID or body.
The first mutation also establishes sensitivity of the throwing cases that were
green on arrival. Native observations verify commit 0 after the later acknowledgement,
commit 2 after recovery, and exactly one correctly mapped retry at offset 2.

Both complete Kafka suites pass **395 cases, zero failures and zero skips**,
preserving all 391 preceding outcomes. All four library targets and both test
targets build with no new warning diagnostics. Only KafkaMessageConsumer changes
relative to this batch's production baseline: two exception filters and their XML
remarks. External public declarations, all 268 dependency/build files, the approved
test and reused helper are unchanged. All mutations are restored. No commit or
push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-writer-recovery/`, including
initial mixed results, green/restored/full TRX files, mutation logs, source
snapshots, build logs and verification.json. The evidence covers the tested writer
exception and null result, not every possible application exception type.
Cancellation recovery, missing confirmations, concurrent forwarding/rebalance,
scheduler-backed retries and automatic pump recovery remain unverified or unfinished.
Full-feature production readiness remains open.

The next [cancelled retry recovery case](review/kafka-requeue-cancellation-recovery/README.md)
is now validated (396 passing cases per full suite). It permits a
copy from the cancelled attempt because cancellation does not establish whether
Kafka persisted it; it requires source recovery and leaves all retry copies unacknowledged.

## Kafka cancelled immediate retry recovery validated

The approved [cancellation-recovery case](review/kafka-requeue-cancellation-recovery/README.md)
reproduces a missing source offset on both frameworks after caller cancellation
during the retry writer's gated mapping call. Cancellation already propagates,
but the source-offset-presence assertion fails. The initial .NET 9 runner expected
xUnit's default assertion label; xUnit emits the test's custom message instead.
The runner check was corrected and .NET 10 confirmed the same assertion failure.
No test or helper was changed.

Async immediate requeue now restores the extracted source offset before rethrowing
OperationCanceledException. It retains the unresolved barrier. A later source
acknowledgement commits offset 0, and explicit retry with a live token on the same
consumer succeeds and advances the commit to 2. Native observations allow one or
two mapped retry copies: cancellation does not prove the first attempt was absent
from Kafka. This is not an exactly-once claim. Synchronous and delayed paths are
unchanged relative to this batch's baseline.

The case passes before and after mutation restoration on both frameworks. Eight
mutations fail at their intended assertions: lost offset restoration, swallowed
cancellation, premature barrier release, a retained barrier after success, a lost
later acknowledgement, and substituted native key, wire ID or body. All mutations
are restored. Both complete Kafka suites pass **396 cases, zero failures and zero
skips**, retaining all 395 preceding outcomes. All four library targets and both
test targets build with no new warning diagnostics. Only KafkaMessageConsumer
changes relative to the production baseline; public declarations, all 268
dependency/build files, the approved test and existing helper remain unchanged.
No commit or push is performed.

The first .NET 9 full run stalled in the existing Fragile=CI
KafkaMessageConsumerCommitRevokeConcurrency test during native assignment inside
consumer disposal. A non-terminating createdump stack report identified the
blocked call; the stalled test container was then stopped and the full suite rerun
with five-minute inactivity hang diagnostics. The final .NET 9 pass is that rerun,
not a clean first-run result. This intermittent rebalance/disposal stall remains
an unresolved production-readiness concern; no speculative fix or test weakening
was made. The stack report and stalled-run console log are retained alongside the
successful results.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-cancellation-recovery/`, including
initial/green/restored/full TRX files, mutation logs, source snapshots, build logs
and verification.json. This establishes explicit async caller recovery after
cancellation during mapping. Cancellation before entry, internal confirmation
timeouts, synchronous writer-thrown cancellation, general forwarding/rebalance
concurrency, scheduler-backed retries and automatic pump recovery remain open.
The full feature is not yet production-ready.

The next [confirmation-timeout cases](review/kafka-requeue-confirmation-timeout-recovery/README.md)
are now validated (398 passing cases per full suite). They
separate internal timeout failure from caller cancellation and permit late retry
copies without claiming cancellation of broker writes.

## Kafka immediate retry confirmation-timeout recovery validated

The [two approved timeout cases](review/kafka-requeue-confirmation-timeout-recovery/README.md)
initially fail at the no-exception assertion on both frameworks: the internal
five-second deadline escapes as TaskCanceledException even though the caller token
remains live. The synchronous and asynchronous confirmation helpers now convert
internal deadline cancellation into ChannelFailureException. Async conversion
explicitly excludes caller cancellation. Existing immediate-retry failure handling
returns false, restores source metadata and retains the unresolved commit barrier.
These helpers are shared with rejection forwarding, whose existing failure handling
continues to catch the exception; the new deadline scenario specifically tests retry.

Both timeout cases pass before and after mutation restoration on both frameworks.
The existing caller-cancellation recovery case also passes on both frameworks.
Native observations establish commit 0 after a later source acknowledgement and
commit 2 after retry with the same consumer; one or two correctly mapped retry
copies are permitted because deadline expiry cannot roll back a broker write.
The confirmation deadline cannot interrupt a blocked synchronous application writer.

Nine mutation variants produce 18 intended assertion failures: propagating timeout,
reporting success after timeout, losing the source offset, prematurely releasing
the barrier, retaining it after successful retry, losing the later acknowledgement,
and substituting the native key, wire ID or body. The first mutation attempt did
not compile because its exception variable became unused under warnings-as-errors;
that temporary mutation was corrected and rerun. The compile failure is retained
as runner evidence and is not counted as a behavioral mutation failure. No test
assertions changed, and all mutations are restored.

Both full Kafka suites pass **398 cases, zero failures and zero skips**, preserving
all 396 preceding outcomes. Both full runs include five-minute inactivity hang
diagnostics. All four library targets and both test targets build without new
warning diagnostics. Only KafkaMessageConsumer changes relative to this batch's
production baseline. Public declarations, delayed-dispatch calls, all 268
dependency/build files, the approved test and reused helper remain unchanged.
No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-confirmation-timeout-recovery/`,
including initial/green/restored/cancellation/full TRX results, mutation logs,
source snapshots, build logs and verification.json. The prior .NET 9 native
assignment/disposal stall remains unresolved even when this batch passes. These
cases do not establish every missing-callback, broker-outage, restart, cancellation,
rebalance, concurrency, scheduler or automatic-pump behavior. Full-feature
production readiness remains open.

## Kafka disposal stall investigation — not reproduced, still open

The retained stack from the first cancellation-recovery .NET 9 full run shows
`rd_kafka_assign` waiting during a rebalance callback invoked by
`rd_kafka_consumer_close`. The managed path runs through consumer Close and disposal
inside the existing `KafkaMessageConsumerCommitRevokeConcurrency` test. That
snapshot is not waiting on Brighter's commit semaphore, and does not establish
which component caused the native wait. No speculative shutdown change was made.

The unchanged existing RoundRobin/CooperativeSticky cases were repeated five times
per framework against separate Kafka brokers, with five-minute hang diagnostics.
All **20 case executions passed**, generally in seven to eight seconds per case.
The built test artifacts reference Confluent.Kafka and librdkafka.redist 2.15.1.
All production and compiled test source files match the audit baseline. These
focused repetitions do not reproduce the full-suite history or resolve the stall;
they are not another full regression run and do not replace the preceding 398-case
results. No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-disposal-stall-audit/` contains all ten
TRX/log pairs, per-framework duration reports, the source manifest and verification.
The original stack remains in the cancellation-recovery evidence directory.
A future reproduction should collect native group/assignment diagnostics and
identify the assignment strategy and full-suite preconditions before changing
shutdown behavior or dependencies.

At the preceding checkpoint, the next [Azure native wire-writer batch](review/asb-native-wire-writer/README.md)
is drafted outside compiled projects: twelve queue/topic, sync/async, default/custom
cases plus one strategy helper and isolated emulator entity configuration. It is
unbuilt and unrun pending test review. Service Bus test credentials are not
configured in this shell; isolated emulator provisioning is part of the proposed
execution plan, and any unavailable hosted regression coverage must remain explicit.


## Azure Service Bus individual native writer selection

The [approved writer batch](review/asb-native-wire-writer/README.md) adds nullable
`AzureServiceBusPublication.WireMessageWriter` and the composable
`DefaultAzureServiceBusWireMessageWriter`. Individual sends select the writer,
then apply configured publication TTL. Custom body and application metadata are
preserved. The static publisher API and batch mapping remain unchanged. The shared
delayed-send path also selects the writer, but delayed native delivery has not been
validated by this batch.

The corrected tests initially fail only on the two missing public APIs. After
implementation, all twelve queue/topic, sync/async, default/custom native cases pass
on .NET 9 and .NET 10 against a dedicated Service Bus emulator. Ten approved
mutations produce 52 intended assertion failures on .NET 9. Temporary mutation
build errors (obsolete body accessor and unreachable TTL branch) were corrected
before recording behavioral results; neither counts as a successful mutation.
All mutations are restored. Both frameworks pass 539 available cases: 536 in the
main regression run plus three existing session mapping cases omitted by the
initial class-discovery filter. There are zero failures or skips. The source-based
scope manifest covers all 65 locally available existing test classes plus the new
native class; 52 hosted test classes are explicitly unexecuted. No hosted Azure
credentials were available, and the isolated static fixture is not a replacement
for their broader infrastructure.

All library targets (netstandard2.0, net8.0, net9.0, net10.0) build; final library and
test builds report zero warnings/errors. Source verification finds only the two
intended existing production files changed, one new default writer, and the two
approved test/helper files. No dependencies changed. The approved files match the
review copies exactly. Work remains uncommitted and unpushed.

Evidence: `/private/tmp/brighter-4170-asb-native-wire-writer/` contains build logs,
TRX files, ten mutation outcomes, the regression scope manifest, container image
IDs and source verification. This closes individual immediate-send writer mapping
only. ASB readers/settlement, batches, scheduling, failures/null results, sessions,
concurrency, endpoint isolation, handler telemetry and hosted compatibility remain
open, as do the Kafka disposal stall and other cross-transport release gates.


## Azure Service Bus native reader selection and settlement

The [approved reader batch](review/asb-native-wire-reader/README.md) adds nullable
`AzureServiceBusSubscription.WireMessageReader` accepting `ServiceBusReceivedMessage`
and the reusable `DefaultAzureServiceBusWireMessageReader(subscription)`. Production
consumers pass the actual native delivery to the configured reader, then attach its
native lock token and sequence number. Custom identity, subject, correlation and
binary body survive mapping. Existing default mapping and the public creator API
remain compatible; the native accessor is internal on the existing internal wrapper.
Configured readers explicitly reject legacy wrappers that cannot supply native SDK
deliveries; this compatibility boundary is documented, not separately validated here.

All twelve queue/topic, sync/async, implicit-default/explicit-default/custom cases
pass on .NET 9 and .NET 10. Native SDK sends keep producer and reader evidence
independent. Each case proves nack/redelivery with a fresh lock, then acknowledgement
with a native peek at the explicit sequence number. Ten mutations produce **64
intended assertion failures** on .NET 9. Omitting lock attachment and reusing a stale
mapped message are caught at the receive assertion because dispatch rejects the
invalid lock; the later metadata/fresh-instance assertions are not reached.

The initial nack-omission mutation also saw cross-case identity failures after the
fixture's one-minute locks expired. Those contaminated outcomes are preserved but
excluded from clean mutation evidence. The repeat used five-minute locks only for
that mutation: all twelve cases then failed at the intended second receive, verified
from TRX stack locations. The original one-minute fixture was restored before final
regressions. Fixture image pulls delayed an intermediate restart; subsequent runs
pin the same cached SQL/emulator image IDs and disable pulls. No production dependency
changed. All mutations are restored.

Each framework passes **551 available ASB regressions, zero failures/skips**, covering
all 65 existing locally available classes plus the writer and reader native classes.
The preceding 539 cases are retained. Fifty-two hosted test classes remain unexecuted;
this isolated emulator result does not close hosted compatibility. All four library
targets build. The approved legacy MessageType assertion produces one CS0618 warning
per test target, alongside existing test warnings; incremental zero-warning builds
do not erase those diagnostics.

The initial approved draft failed at subscription construction before sending. The
user approved the generic subscription/request-type and explicit Reactor/Proactor
correction before it was applied and rerun. No assertions or helper semantics changed.
The compiled test and helper match the corrected review copies byte for byte. Source
verification limits this batch to the subscription, consumer, internal wrapper, new
default reader, and approved test setup correction; the helper is unchanged. All work
remains uncommitted and unpushed.

Evidence: `/private/tmp/brighter-4170-asb-native-wire-reader/` contains build logs,
TRX files, mutation reports including the excluded first nack run, repeat-stack
verification, fixture configuration, scope manifest and source verification.
Reader exceptions/null results, mixed batches, sessions, retry counts, forwarding,
concurrent reuse, endpoint isolation and handler telemetry remain open. The earlier
Kafka disposal stall and broader transport/scheduler release gates remain open.


## ASB reader exception/null recovery — historical pre-correction checkpoint

The [approved recovery batch](review/asb-reader-failure-recovery/README.md) first
reproduced eight failures on .NET 9: four InvalidOperationException cases and four
NullReferenceException cases. AzureServiceBusConsumer now creates an unacceptable
message with the original ID and binary body for non-cancellation reader exceptions
and null results, then attaches native lock/sequence metadata. Cancellation still
propagates. No dependencies or public API changed; only this existing production
file changed in the batch. All library targets build.

Do not treat the subsequent eight-pass native matrices on both frameworks as final
sign-off. Fresh isolated runs reproduced a native DLQ completion fault on the first
topic case on both frameworks, with seven other cases passing. Emulator logs show an
internal duplicate SequenceNumber:1 insertion, topic abort, and forced link detachment
during CompleteMessageAsync on the native DLQ observer. The native payload/reason
assertions had passed before that cleanup failure. Earlier shared-entity runs also
showed redelivery contamination after failed cases. Per-case entity overrides now
isolate the runner; they do not by themselves resolve the first-topic fault.

Mutation validation is incomplete. The initial exception-bypass mutation reproduces
eight exceptions, but the null-check mutation's additional failures are contaminated
and are not accepted as clean mutation evidence. Every mutation is restored. The
compiled test/helper remain the originally approved versions. A review-only correction
uses ReceiveAndDelete for the independent native DLQ observation and removes explicit
DLQ completion cleanup. All assertion lines are preserved; the correction is unrun
and awaits approval. Brighter rejection and healthy-message acknowledgement remain
under test. No speculative Brighter change was made for the emulator's internal fault.

Evidence: `/private/tmp/brighter-4170-asb-reader-failure-recovery/`, especially
`emulator-logs/log2026102622.txt` (duplicate-sequence error and entity abort),
`isolated-green-net9-isolated.json`, `isolated-green-net10-isolated.json`, original
and contaminated TRX/logs, and `source-verification.json`. Hosted compatibility,
mutation completion, mixed-batch behavior and the wider release gates remain open.
Work remains uncommitted and unpushed.

Existing available regressions pass **551 cases on each framework, zero failures/skips**, retaining exactly the previous case multiset. This excludes the eight new recovery cases and does not complete their validation.


## ASB recovery observer correction applied — broker validation blocked

The approved ReceiveAndDelete correction is now applied and executed; compiled test
and helper match their review copies. It removes only independent DLQ completion
cleanup and preserves all assertions. The earlier pending-approval checkpoint above
is historical. No further approval is needed for this correction.

Fresh isolated runs still produce seven passes and one failure on each framework:
the first topic case receives no native DLQ message. Changing from AMD64 emulation
to the native ARM64 image of the same registry release does not fix it, nor does an
additional 45-second readiness delay. ARM64 image digest:
`sha256:4dab6cd564aa723b99a32af955959acc739bfe831778fe131bd138dd977ca09c`.
A subsequent queue-only mutation run also hits the native DLQ fault. Logs record
`Cannot add the same item 'S.SP-SequenceNumber:1' more than once.` followed by entity
abort. Queue-only execution is therefore not a reliable workaround.

The queue exception-bypass mutation produces four expected exceptions. The next
null-check mutation produces three failures instead of its expected two because of
the DLQ fault, so that run is not accepted as clean mutation evidence. The ten-mutation
plan remains incomplete. Every mutation is restored; no speculative production
change was made for the emulator failure.

Final restored library and test builds succeed. Existing regressions pass **551 on
.NET 9 and 551 on .NET 10, zero failures/skips**, with exact prior case/outcome multisets
verified. These runs exclude the eight recovery cases and do not establish their
correctness. Fifty-two hosted test classes remain unexecuted. A healthy broker is
required to finish recovery and mutation validation; hosted credentials are not
currently configured. The batch and broader production-readiness gates remain open.

Evidence: `/private/tmp/brighter-4170-asb-reader-failure-recovery/`, including
`corrected-baseline-net9.trx`, `corrected-baseline-net10.trx`,
`corrected-baseline-verification.json`, `queue-mutations.json`,
`queue-only-emulator-logs/`, `corrected-emulator-logs/`, and source verification.
The dedicated recovery containers are stopped after validation. Changes remain
uncommitted and unpushed.


## ASB small-message batch writer selection validated

The [approved six-case batch](review/asb-batch-wire-writer/README.md) reproduced ignored
writer selection: queue and topic custom cases expected three writer calls and observed
zero; the four default cases passed. Four ASB production files now map each source once
before packing, apply publication TTL, and pass the native message through batch helpers
while retaining original source IDs. Existing public helper signatures retain default
mapping. Dependencies and the approved test are unchanged.

All six cases pass on .NET 9 and .NET 10, initially and after restoration. Seven named
mutations produce **26 intended assertion failures**: bypassed writer (2), empty explicit
default (2), omitted TTL (6), native IDs replacing source IDs (2), repeated first source
(6), double mapping (2), and mutated source subject (6). The uniqueness mutation exposed
a reporting-script assumption about xUnit custom messages; all six original stacks
confirm the intended assertion. The reporter was corrected without changing tests.
All production mutations are restored.

Final available suites pass **557 cases on each framework, zero failures/skips**. Exact
case/outcome comparison retains all prior 551 cases and adds only the six new passes.
All library targets build. Source verification limits this increment to four existing
ASB production files and the approved test. No public API or dependency changes.

This closes small-message batch writer selection only. Native batch splitting, oversized
single-message fallback, failures/cancellation, callbacks and sessions remain unverified
by these cases. Fifty-two hosted classes remain unrun; the eight reader-recovery cases
remain separately blocked by the emulator DLQ fault and are not included in the 557.
The Kafka disposal stall, remaining transports/schedulers and global release gates remain
open. Full production readiness is not achieved.

Evidence: `/private/tmp/brighter-4170-asb-batch-wire-writer/` contains RED/green/restored
TRXs, mutations.json, the preserved original report, full regression TRXs, exact baseline
comparisons and source verification. The dedicated fixture containers are stopped after
validation. No changes committed or pushed.


## ASB native batch splitting validated

The [approved splitting cases](review/asb-batch-wire-splitting/README.md) negotiate native
batch capacity and send three distinct custom binary messages, each 60% of that capacity,
through public bulk producers for queues and topics. Both cases pass on .NET 9 and
.NET 10 initially and after mutation restoration. They establish three nonempty native
batches, exactly one writer call per source, preserved original batch IDs, native custom
mapping and publication TTL, distinct deliveries, and unchanged source metadata/body.
No production correction was needed after the preceding batch-selection implementation.

Six mutations produce **12 intended assertion failures**: remapping on split, default
metadata on retry, dropping a completed batch, losing a retry source ID, reusing the
first native message, and losing TTL on retry. The initial default conversion mutation
also expanded binary bytes and reached the wrong batch-shape assertion; it is excluded
from clean mutation proof. Repeating with unchanged native body bytes isolates the
intended metadata loss. An initial ID-loss mutation compile error is also excluded;
correcting its obsolete property reference yielded the intended source-ID failure.
Tests remain byte-identical to approval and all production source hashes are restored.

Both final available suites pass **559 cases, zero failures/skips**, retaining the exact
557 prior case/outcome pairs and adding only the two splitting cases. All library targets
build. Eight DLQ-recovery cases remain separately blocked by the emulator fault; 52 hosted
test classes remain unavailable. Oversized single-message fallback, cancellation,
failure recovery, sessions and broader production-readiness gates remain open.

Evidence: `/private/tmp/brighter-4170-asb-batch-wire-splitting/` contains all original and
corrected mutation evidence, initial/restored and full-regression TRXs, baseline comparisons
and source verification. The dedicated fixture containers are stopped after validation.
No changes committed or pushed.


## ASB oversized fallback construction validated

The [approved six cases](review/asb-oversized-batch-fallback/README.md) exercise native
batch-capacity rejection with oversized messages first, middle and last for queues and
topics. Public bulk creation preserves custom binary content, native metadata,
publication TTL and original source IDs in the single-message fallback. Normal batches
remain usable, each source is mapped once, and empty trailing native batches are excluded.
Native SDK receivers verify and settle the two normal messages in each case. The oversized
fallback itself is inspected through its public Content API and is deliberately not sent.

All six cases pass on .NET 9 and .NET 10 initially and after restoration. Seven named
mutations produce **38 intended assertion failures**: default fallback conversion (6),
mapped ID replacing original bookkeeping ID (6), lost fallback TTL (6), dropped fallback
(6), remapped fallback (6), discarded normal batches (6), and returned empty batches (2).
The last mutation fails only oversized-last cases; all four other cases pass as expected.
Every mutation is restored and production source hashes match the batch baseline.
The approved test is unchanged. No production correction or dependency change was needed.

Both final available suites pass **565 cases, zero failures/skips**, retaining all 559
previous case/outcome pairs and adding only these six passes. All library targets build.
Fifty-two hosted classes remain unrun and eight DLQ-recovery cases remain blocked by the
emulator fault. Successful oversized delivery, rejection/recovery, sessions, cancellation,
concurrency and broader transport/scheduler release gates are not established here.

Evidence: `/private/tmp/brighter-4170-asb-oversized-batch-fallback/` includes initial and
restored matrices, all seven mutation results, full-regression TRXs, exact baseline
comparisons and source verification. Dedicated fixture containers are stopped after
validation. Nothing committed or pushed. Full production readiness remains incomplete.


## ASB oversized send rejection and explicit recovery

The [approved two cases](review/asb-oversized-send-recovery/README.md) pass on .NET 9
and .NET 10 initially and after mutation restoration. A native SDK control establishes
MessageSizeExceeded for the oversized body in this fixture. Sending the mapped fallback
through the public bulk producer exposes the same non-transient failure, and a subsequent
healthy batch through the same producer reaches the native queue/topic with custom mapping,
TTL and original bookkeeping IDs intact. This does not identify the enforcement location
or prove automatic retry, outbox recovery or successful oversized delivery.

Six mutations produce **12 intended assertion failures**: swallowed failure, changed
exception type, changed reason, lost healthy-message writer, fallback remapping during
send, and cleared failed source body. Tests remain unchanged and production source hashes
are restored. All library targets build; no production correction was required.

.NET 9 passes **567 cases** on its first final run. The first .NET 10 run passes 566 and
fails a session-lock renewal case (queue/async/buffer size 1). The focused unchanged class
rerun passes 15 of 16 and fails a different session case (topic/sync/buffer size 1).
Both fail with MessageLockLost during in-memory receiver completion. The next unchanged
full .NET 10 run passes **567 cases**. Successful final runs retain exactly all 565 prior
case/outcome pairs plus the two new cases; both failed runs remain recorded.

Session-lock renewal instability is an unresolved readiness finding. These existing
tests use two-second locks and real-time delays, but no causal diagnosis is established.
A later green run does not resolve it. No tests, timing or production were changed to
obtain that repeat. Eight DLQ-blocked cases and 52 hosted classes remain excluded.
The wider transport/scheduler, successful oversized-delivery and release gates remain open.

Evidence: `/private/tmp/brighter-4170-asb-oversized-send-recovery/` includes mutation,
initial/restored and full-regression TRXs, the focused failed class repeat,
lock-renewal-instability.json, exact baseline comparisons and source verification.
Dedicated fixture containers are stopped after validation. Nothing committed or pushed.


## ASB session-lock timing audit — unresolved

The [bounded audit](session-lock-renewal-audit.md) investigated the two prior .NET 10
MessageLockLost failures without changing existing tests/helpers. Temporary production
timing diagnostics were removed and original source rebuilt afterward. The full 16-case
lock-renewal class passes on both frameworks under diagnostics; the four short-lock
session cases pass 20 repeated executions with diagnostics and 20 without. All 72
executions pass, but none reproduces the original failures.

The focused diagnostic samples show at least approximately 1,001 ms of lock lifetime
remaining when completion begins, with a maximum 3.16 ms stop-to-completion interval.
These samples do not establish the cause of the earlier failures. No production fix,
timeout adjustment or test weakening was made; all production hashes are restored.
The session-lock readiness finding remains open. Capturing timing during an actual
failure or constructing a deterministic reproduction is needed before claiming resolution.
Evidence is under `/private/tmp/brighter-4170-asb-session-lock-audit/`.


## Full-suite diagnostic follow-up

Two further .NET 10 runs exercised the entire available 567-case ASB suite with
temporary renewal/expiry/settlement timing diagnostics and session-to-delivery
identifiers. Each run used a fresh dedicated emulator fixture. Both passed all 567
cases, with exact prior case/outcome multisets retained. The earlier failure did not
recur. These runs preserve the suite composition; they cannot recreate unknown host
scheduling conditions from the original failures, and instrumentation can affect timing.

All temporary source diagnostics were restored byte-for-byte and library/test projects
rebuilt successfully. Existing tests/helpers were unchanged. The session-lock finding
remains unresolved: neither the 72 earlier focused executions nor these two full runs
establish a cause or a fix. Further green repetitions alone are not a release criterion.

Evidence: `/private/tmp/brighter-4170-asb-session-lock-full-audit/` contains both full
TRXs, detailed timing logs, source backups, scripts and verification.json. The dedicated
fixture containers are stopped after the audit. Nothing committed or pushed.


## ASB batch writer failure recovery validated

The [approved four cases](review/asb-batch-writer-failure-recovery/README.md) reproduce
a null configured writer result escaping batch creation as NullReferenceException.
The two writer-exception cases already pass. AzureServiceBusMessageProducer now checks
for null before applying publication TTL and raises InvalidOperationException; writer
exceptions preserve their original identity. Only this guard and its XML exception
documentation change in production. Public signatures and dependencies are unchanged.

Four native queue/topic × exception/null cases pass initially after the fix and after
mutation restoration on both frameworks. They check failed creation, source preservation,
writer invocation counts and subsequent healthy sending through the same producer.
Six mutations produce **20 assertion failures**: removed guard (2), partial success (4),
wrapped writer exception (2), lost recovery writer (4), cleared failed body (4), and
premature publication (4). The last mutation fails three immediate-peek assertions and
one downstream native-ID assertion: the topic peek briefly sees empty before the
prematurely published first message becomes visible. This limits the peek's standalone
guarantee; the later identity check still detects the actual defect. The original report
and reviewed assertion evidence are preserved. No approved tests/helpers were changed.

Both final available suites pass **571 cases, zero failures/skips**, on their first runs,
retaining exactly all 567 prior outcomes plus four new passes. All library targets build
and all mutations are restored. Previous session-lock instability remains unresolved;
eight DLQ-recovery cases and 52 hosted classes remain excluded. Other ASB and global
production-readiness requirements remain open.

Evidence: `/private/tmp/brighter-4170-asb-batch-writer-failure-recovery/` contains RED,
initial/restored matrices, mutation results and assertion-location review, final full
TRXs, baseline comparisons and source verification. Dedicated fixture containers are
stopped after validation. Nothing committed or pushed.


## ASB individual writer failure recovery

The [approved eight-case batch](review/asb-individual-writer-failure-recovery/README.md)
covers queue/topic, sync/async and writer exception/null. Initial RED passes the four
throwing cases and fails all four null cases with an accidental NullReferenceException.
A minimal null-result guard now produces InvalidOperationException inside the existing
ChannelFailureException wrapper. Original writer exception identity, same-producer
recovery, native mapping/TTL and source preservation are verified.

Six named mutations produce 40 intended assertion failures; all are restored. Approved
tests/helpers are unchanged. Both available ASB suites pass **579 cases, zero failures
or skips**, on their first runs, retaining all 571 previous outcomes. All library
targets build. Session-lock instability remains unresolved; eight DLQ-blocked cases
and 52 hosted classes remain excluded. Other ASB and global release requirements stay
open. Scheduled sends, cancellation and broker-failure recovery are outside this batch.

Evidence: `/private/tmp/brighter-4170-asb-individual-writer-recovery/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB publication writer isolation

The [approved six-case batch](review/asb-publication-writer-isolation/README.md)
validates custom/default writer and TTL isolation between queue and topic publications
in one registry, through sync, async and bulk sends. Both placements of the custom
writer pass initially and after mutation restoration on .NET 9 and .NET 10.

Six named mutations produce 36 intended assertion failures. All 1,171 non-generated
production C# files remain unchanged; approved test/helper are unchanged. All library
targets build. Both full available suites pass **585 cases, zero failures/skips**,
on their first runs, retaining exactly all 579 preceding outcomes.

This covers sequential publication isolation, not concurrency or two distinct custom
writer implementations. Session-lock instability remains unresolved despite passing
here. Eight DLQ-blocked cases and 52 unavailable hosted classes remain excluded;
scheduled sends, cancellation, reader isolation and broader release gates stay open.
Evidence: `/private/tmp/brighter-4170-asb-publication-isolation/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB concurrent publication writer

The [approved three-case batch](review/asb-concurrent-publication-writer/README.md)
validates one safe shared writer across queue/topic producers through sync, async and
bulk sends. A bounded barrier inside the writer establishes overlapping mappings.
Native SDK observers verify per-message identity, bytes, trace state and publication
TTL. Cases pass initially and after mutation restoration on .NET 9 and .NET 10.

Six mutations produce 18 intended assertion failures, including shared native-body
cache contamination detected by payload assertions. All mutations are restored; all
1,171 non-generated production C# files and approved tests/helpers are unchanged.
All library targets build. Both full available suites pass **588 cases, zero failures
or skips**, on their first runs, retaining exactly all 585 previous outcomes.

This is bounded concurrency across two distinct producers, not a stress test or a
thread-safety guarantee for arbitrary user state or one shared producer. Reader
concurrency, scheduled sends, cancellation, session-lock instability, eight DLQ-blocked
cases, 52 unavailable hosted classes and broader release gates remain open.
Evidence: `/private/tmp/brighter-4170-asb-concurrent-writer/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB concurrent reader

The [approved two-case batch](review/asb-concurrent-reader/README.md) validates sync
and async consumers sharing one safe reader across queue/topic deliveries. A bounded
barrier proves overlapping mapping calls. Native delivery records verify per-message
identity/body and lock/sequence ownership. Native peeks observe each locked delivery
before acknowledgement and removal afterward.

All six named defects have intended assertion evidence. Five initial mutations yield
ten intended failures. The initial whole-Message cache catches sync sharing but async
rejects the corrupted lock before the identity assertion; that result is preserved and
not counted as identity evidence. A refined cache mutation copies the cached header
before attaching native delivery metadata, isolating the same identity defect; both
cases then fail at the intended mapped-ID assertion. Final qualified evidence totals
twelve intended failures, with the initial cache attempt reported separately.

All mutations are restored. All 1,171 non-generated production C# files and approved
test/helper are unchanged. All library targets build. Both full available suites pass
**590 cases, zero failures/skips**, on their first runs, retaining exactly all 588
preceding outcomes. This covers two distinct consumers and subsequent individual
acknowledgements, not concurrent settlement or repeated reads on one consumer.

Session-lock instability, scheduled sends, cancellation, eight DLQ-blocked cases,
52 unavailable hosted classes and broader release gates remain open. Evidence:
`/private/tmp/brighter-4170-asb-concurrent-reader/`. Dedicated fixture containers are
stopped after validation. Nothing committed or pushed.


## ASB scheduled custom writer

The [approved four-case batch](review/asb-scheduled-wire-writer/README.md) validates
native queue/topic scheduled sends through sync/async producer interfaces. A five-second
delay preserves custom wire identity, body and metadata, publication TTL and source
state. Native scheduled timestamps and observed delivery satisfy the approved bounds
with one-second tolerance. Cases pass initially and after restoration on both frameworks.

Six mutations produce 24 intended assertion failures, including immediate-send bypass
and scheduling without the requested delay. All mutations are restored; all 1,171
non-generated production C# files and approved test/helper remain unchanged. All library
targets build. Both full available suites pass **594 cases, zero failures/skips** on
their first runs, retaining exactly all 590 prior outcomes.

This covers direct producer scheduling with a custom writer, not the separate Azure
scheduler package, default scheduling, cancellation or scheduled failure recovery.
Session-lock instability, eight DLQ-blocked cases, 52 unavailable hosted classes and
broader release gates remain open. Evidence: `/private/tmp/brighter-4170-asb-scheduled-writer/`.
Dedicated fixture containers are stopped after validation. Nothing committed or pushed.


## ASB send cancellation recovery

The [approved four-case batch](review/asb-send-cancellation-recovery/README.md)
validates queue/topic × immediate/scheduled async sends cancelled deterministically
after custom mapping and before native submission. The existing ChannelFailureException
wrapper retains an inner OperationCanceledException-compatible cause. The same producer
recovers with an active token, preserving custom mapping, TTL and source state.

Six mutations produce 24 intended assertion failures. All mutations are restored;
all 1,171 non-generated production C# files and approved tests/helpers are unchanged.
All library targets build. Both full available suites pass **598 cases, zero failures
or skips**, on their first runs, retaining exactly all 594 previous outcomes.

The immediate empty peek remains best-effort; these cases do not establish broker
atomicity for cancellation during I/O or retract an accepted message. Bulk/consumer
cancellation and scheduled-message cancellation remain separate. Session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader release
gates remain open. Evidence: `/private/tmp/brighter-4170-asb-send-cancellation/`.
Dedicated fixture containers are stopped after validation. Nothing committed or pushed.


## ASB bulk cancellation recovery

The [approved four-case batch](review/asb-bulk-cancellation-recovery/README.md)
reproduces cancellation being ignored after the second source maps into an existing
batch. Both frameworks initially fail both creation cases because no exception is
thrown; both cancelled-submission/reuse cases already pass. A token check immediately
after mapping now aborts creation before packing and leaves the third source unvisited.
The same producer recovers, and a batch cancelled before submission remains reusable
with an active token, without remapping.

Only the producer guard and XML documentation change. Approved test and reused helpers
are unchanged. Six mutations produce 16 intended assertion failures; all are restored.
All library targets build. Both full available suites pass **602 cases, zero failures
or skips**, on their first runs, retaining exactly all 598 prior outcomes.

Immediate peek remains best-effort. In-flight cancellation, oversized fallback,
consumer cancellation, session-lock instability, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader release gates remain open. Evidence:
`/private/tmp/brighter-4170-asb-bulk-cancellation/`. Dedicated fixture containers are
stopped after validation. Nothing committed or pushed.


## ASB pre-entry receive cancellation — incomplete

The [approved two-case batch](review/asb-receive-cancellation-recovery/README.md)
reproduces ignored pre-entry cancellation in the queue case. A minimal token check
at ReceiveAsync entry and XML exception documentation are implemented. Fresh isolated
queue cases pass on both frameworks. Topic cases stop earlier at a null immediate
native peek, exposing a setup visibility race. Original cross-framework leftovers
also caused source-ID mismatches; fresh per-case fixtures eliminate that contamination.
All raw results are retained.

A [bounded initial visibility wait](review/asb-receive-cancellation-visibility/README.md)
is drafted outside compiled projects for review. Original test/helper are unchanged;
mutation validation has not started. Both existing full available suites retain
exactly **602 passes, zero failures/skips** against the guard, excluding this pending
batch. All library targets build. This batch is not complete and does not add two
validated cases to the baseline. Other release gaps remain open.

Evidence: `/private/tmp/brighter-4170-asb-receive-cancellation/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB pre-entry receive cancellation — completed with approved visibility amendment

The [approved initial visibility wait](review/asb-receive-cancellation-visibility/README.md)
is adopted unchanged. Queue and topic cases now pass on both frameworks, initially
and after mutation restoration. The production change remains the ReceiveAsync entry
cancellation check and XML exception documentation. Pre-cancelled receives do not map
or consume the delivery, and the same consumer recovers with delivery count one and
supports native acknowledgement.

Six mutations produce 12 intended assertion failures. All are restored. Compiled test
matches the approved amendment; the reused reader is unchanged. All library targets
build. Both final full available suites pass **604 cases, zero failures/skips** on
their first runs, retaining exactly all 602 prior outcomes. The original visibility
race and cross-framework leftovers remain recorded. Fresh fixtures isolate each
focused/mutation case. A harness precondition abort occurred before changing source;
it was corrected to target only the receive-entry guard and is not mutation evidence.

This closes the previously pending batch only. Cancellation during broker waits,
mapping, buffered deliveries or settlement remains separate. Session-lock instability,
eight DLQ-blocked cases, 52 unavailable hosted classes and broader release gates stay
open. Evidence: `/private/tmp/brighter-4170-asb-receive-cancellation/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB cancellation during mapping

The [approved two-case batch](review/asb-mapping-cancellation-recovery/README.md)
reproduces ignored cancellation after the reader returns on both frameworks. A
post-mapping token check alone reports cancellation but fails both recovery receives
on both frameworks: reset alone does not release the native delivery promptly. The
final correction explicitly abandons the current native delivery before propagating
cancellation through the existing reset path. Recovery on the same consumer occurs
before the original lock expires, with a fresh lock and delivery count two.

Six mutations produce 12 intended assertion failures, including omitted explicit
release reproducing empty recovery receives. All are restored; approved test/helper
and reused reader remain unchanged. Only AzureServiceBusConsumer changes. All library
targets build. Both full available suites pass **606 cases, zero failures/skips** on
their first runs, retaining exactly all 604 previous outcomes. Original and guard-only
failures are preserved. The initial Docker address-pool exhaustion was resolved by
removing the previous completed batch's stopped fixture/network, before any tests ran.

Scope remains one non-session delivery and cancellation observed after mapping.
Buffered multi-message cancellation, broker-wait/settlement cancellation, session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader release
gates remain open. Evidence: `/private/tmp/brighter-4170-asb-mapping-cancellation/`.
This batch's fixture containers and network are removed after validation. Nothing
committed or pushed.

## ASB buffered mapping cancellation

The [approved queue/topic batch](review/asb-buffered-mapping-cancellation/README.md)
reproduces incomplete recovery on both frameworks: the earlier mapped delivery
remains locked while the cancellation-triggering delivery recovers. The correction
abandons the received batch before propagating cancellation through the existing
reset path. Both messages then recover before original lock expiry with fresh locks,
correct mapping and native delivery metadata, and successful acknowledgement.

Six mutations produce 12 targeted failures: ten xUnit assertion failures and two
mapped-identity cardinality failures at the deliberate Single lookup. Those two
are not described as xUnit assertion failures. A mutation-only compilation error
from unqualified LINQ is preserved and excluded; its corrected version detects the
intended incomplete recovery. All mutations are restored and approved sources match
compiled copies byte-for-byte. Only AzureServiceBusConsumer changes among 1,171
non-generated production C# files; no public signatures or dependencies change.

Both full available suites pass **608 cases, zero failures/skips**, on their first
runs, retaining exactly all 606 prior outcomes. The explicit two-message native
receive precondition holds in every execution; SDK batch-size variability remains
a documented setup limitation. Unvisited buffered deliveries, abandonment failures,
broker-wait/settlement cancellation, sessions, session-lock instability, eight
DLQ-blocked cases, 52 unavailable hosted classes and broader release gates remain
open. Evidence: `/private/tmp/brighter-4170-asb-buffered-cancellation/`. Nothing
committed or pushed.

This batch's isolated emulator containers and network were removed after validation.

## ASB cancellation with an unvisited buffered delivery

The [approved queue/topic characterisation](review/asb-unvisited-buffered-cancellation/README.md)
cancels after mapping two of three native deliveries. All three recover on the same
consumer with correct custom mapping, native body/sequence metadata and successful
acknowledgement. The previously unvisited message has exactly one mapping invocation
and native delivery count two, proving it was acquired by the cancelled receive.
Recovery precedes the earliest recorded original lock deadline. The third original
lock itself is not exposed to the reader, as documented in the approved contract.

All four initial and four restored executions pass without production changes. Six
named mutations produce 12 intended xUnit assertion failures on .NET 9, including
incomplete release, extra mapping after cancellation, and explicit mapped-identity
failure when the reader is lost. No setup or compile failure is counted. All mutations
are restored. The adopted test matches the approved draft; both reused helpers and
all 1,171 non-generated production C# files remain unchanged.

Both full available suites pass **610 cases, zero failures/skips**, on their first
runs, retaining exactly all 608 previous case/outcome pairs. All library targets
build. Native batch acquisition is established in every execution, but SDK maximum
batch size still does not guarantee a minimum for future runs. Abandonment failures,
broker-wait/settlement cancellation, sessions, session-lock instability, eight
DLQ-blocked cases, 52 unavailable hosted classes and broader transport/release gates
remain open. Evidence: `/private/tmp/brighter-4170-asb-unvisited-cancellation/`.
Nothing committed or pushed.

This batch's isolated emulator containers and network were removed after validation.

## ASB abandonment failure during cancellation — precondition not met

The [approved native queue/topic batch](review/asb-cancellation-abandon-failure/README.md)
was built and executed on .NET 9 and .NET 10. All four executions returned
OperationCanceledException instead of ServiceBusException(MessageLockLost), failing
at line 96 after confirming two mapper calls and one completed public Nack callback.
The chosen repeat-abandonment mechanism did not produce a native settlement failure.
The evidence does not distinguish SDK behavior from emulator behavior. It does not
establish a cleanup defect, and recovery assertions were not reached.

No production changes or named mutations were applied. The unchanged approved test
and helper remain in the review directory, with adopted copies moved to the evidence
directory outside the compiled project. They were not weakened or skipped. All
1,171 non-generated production C# files are unchanged. A replacement mechanism must
establish a native abandonment failure while other buffered locks are still live,
or validation needs a suitable hosted environment. This gate remains unchecked.

After removing only these two newly adopted files from the compiled project, both
full available suites pass **610 cases, zero failures/skips**, retaining exactly all
610 baseline case/outcome pairs. All library targets build. Evidence:
`/private/tmp/brighter-4170-asb-abandon-failure/` contains original initial TRXs,
logs, preserved adopted files, restored-baseline TRXs and source verification.
No commits or pushes. Existing session-lock instability, eight DLQ-blocked cases,
52 unavailable hosted classes and broader release gates remain unresolved.

The isolated emulator containers and network were removed after verification.

## ASB cancellation during an empty receive

The [approved queue/topic batch](review/asb-broker-wait-cancellation/README.md)
reproduces ignored cancellation on both frameworks: the pending ten-second receive
returns successful emptiness after its wait. Passing the caller token to the native
SDK receive through an internal wrapper overload fixes cancellation. A filtered
OperationCanceledException catch resets the receiver and preserves direct caller
cancellation rather than wrapping it in ChannelFailureException. Same-consumer
recovery retains custom mapping, native metadata and successful acknowledgement.

The public receiver-wrapper interface and its existing signatures remain unchanged.
External wrapper implementations retain their existing receive path; this change
does not promise in-flight cancellation support for arbitrary external wrappers.
No detached receive, new public API or dependency change is introduced. Only the
consumer and internal native wrapper change among 1,171 non-generated production
C# files. The approved test and reused reader remain unchanged.

Six mutations produce 12 intended xUnit assertion failures, including an elapsed-time
failure when cancellation is checked only after the ten-second wait. All mutations
are restored. All library targets build. Both full available suites pass **612 cases,
zero failures/skips**, on their first runs, retaining all 610 prior outcomes.
Evidence: `/private/tmp/brighter-4170-asb-wait-cancellation/`. Nothing committed or pushed.

This covers an empty non-session receive after warm-up. Arrival/cancellation races,
provisioning, session acquisition, settlement cancellation, the unavailable native
abandonment-failure mechanism, session-lock instability, eight DLQ-blocked cases,
52 unavailable hosted classes and broader transport/release gates remain open.

The isolated emulator containers and network were removed after validation.

## ASB acknowledgement cancelled before entry

The [approved queue/topic batch](review/asb-acknowledgement-cancellation/README.md)
reproduces ignored pre-entry cancellation on both frameworks. AcknowledgeAsync now
checks the token before accessing the channel or receiver and documents the exception.
Both cases confirm the native delivery remains present after cancellation, mapped
content and lock metadata stay intact, and an active-token retry on the same mapped
message and consumer completes the delivery without another receive or mapping.

Four named mutations produce eight intended xUnit assertion failures. Checking the
token only after native completion fails the retained-delivery assertion even though
it reports cancellation. All mutations are restored. Approved test and reused reader
remain unchanged. Only AzureServiceBusConsumer changes among 1,171 non-generated
production C# files, adding the entry guard and XML documentation; no public
signatures, receiver-wrapper contracts or dependencies change.

All library targets build. Both full available suites pass **614 cases, zero
failures/skips**, on their first runs, retaining all 612 previous outcomes. Evidence:
`/private/tmp/brighter-4170-asb-ack-cancellation/`. Nothing committed or pushed.

In-flight settlement cancellation, nack/reject cancellation, receive-arrival races,
provisioning, session acquisition, abandonment-failure recovery, session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. Pre-entry cancellation does not establish
rollback or safe retry after a native settlement may already have committed.

The isolated emulator containers and network were removed after validation.

## ASB negative acknowledgement cancelled before entry

The [approved queue/topic batch](review/asb-nack-cancellation/README.md) reproduces
ignored pre-entry cancellation on both frameworks. NackAsync now checks the caller
token before accessing the channel or receiver and documents the exception. The
cancelled call retains the delivery, while active-token retry permits redelivery on
the same consumer with a fresh lock, delivery count two and preserved custom/native
mapping. Final acknowledgement removes the native message.

Five mutations produce ten intended xUnit assertion failures. In particular, checking
cancellation after native abandonment allows the competing receiver to acquire the
message and fails its Assert.Null in both cases. Thus the bounded observation detects
premature release in this fixture. All mutations are restored. Approved test and
reused reader remain unchanged. Only AzureServiceBusConsumer changes among 1,171
non-generated production C# files: the entry guard and XML documentation. No public
signatures, wrapper contracts or dependencies change.

All library targets build. Both full available suites pass **616 cases, zero
failures/skips**, on their first runs, retaining all 614 prior outcomes. Evidence:
`/private/tmp/brighter-4170-asb-nack-cancellation/`. Nothing committed or pushed.

In-flight settlement cancellation, reject cancellation, receive-arrival races,
provisioning, session acquisition, abandonment-failure recovery, session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. Existing NackAsync native error handling and
rollback after a settlement may have committed are not established by this batch.

The isolated emulator containers and network were removed after validation.

## ASB rejection cancelled before entry

The [approved queue/topic batch](review/asb-reject-cancellation/README.md) reproduces
ignored pre-entry cancellation on both frameworks: RejectAsync returns successfully
without a cancellation exception. An entry guard now rejects the call before channel
or receiver access, with XML exception documentation. The native delivery and mapped
metadata remain intact, permitting later acknowledgement on the same consumer and
mapped message without redelivery or another mapping.

Three mutations produce six intended xUnit assertion failures. Removed-guard cases
return successfully and fail the expected cancellation assertion; no emulator failure
or hang is counted. This narrower path does not encounter the anticipated blocker,
but it does not validate DLQ reading or resolve the eight earlier blocked cases.
All mutations are restored. Approved test and reused reader remain unchanged. Only
AzureServiceBusConsumer changes among 1,171 non-generated production C# files, adding
the entry guard and XML documentation; no public signatures or dependencies change.

All library targets build. Both full available suites pass **618 cases, zero
failures/skips**, on their first runs, retaining all 616 prior outcomes. Evidence:
`/private/tmp/brighter-4170-asb-reject-cancellation/`. Nothing committed or pushed.

In-flight settlement/rejection cancellation, successful DLQ validation, receive-arrival
races, provisioning, session acquisition, abandonment-failure recovery, session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. Pre-entry rejection cancellation does not
establish rollback after a native settlement may already have committed.

The isolated emulator containers and network were removed after validation.

## ASB cancellation during session acquisition

The [approved queue/topic batch](review/asb-session-acquisition-cancellation/README.md)
reproduces ignored cancellation on both frameworks: an empty session acquisition
waits for the native timeout and returns without the expected cancellation exception.
The native provider now forwards the caller token to AcceptNextSessionAsync. Queue
and topic consumers propagate caller cancellation directly. The same consumer can
subsequently acquire a newly published session, preserve custom mapping and native
session/lock metadata, and acknowledge the delivery.

A scoped AsyncLocal carries the token through the existing virtual acquisition hook
and restores its previous value in finally. This preserves existing subclass dispatch
and the public provider/receiver interfaces. Existing external providers retain their
original call path; arbitrary external-provider cancellation is not established.
The native forwarding consumer uses the same token path, but has no dedicated
forwarding-session test in this batch. Five production files change among 1,171
non-generated C# files: the base, queue, topic and forwarding consumers, and the
internal ServiceBusReceiverProvider. No dependency changes.

The approved native-client helper bounds SDK acquisition to ten seconds with no
retries. Empty session-enabled entities exercise pending acquisition, potentially
including connection setup; the test does not locate the precise AMQP request instant.
Both cases require direct cancellation within five seconds and same-consumer recovery.
All four initial cases fail the intended cancellation assertion. Six named mutations
on .NET 9 produce twelve intended xUnit assertion failures. All mutations are restored;
approved test/helper sources, entity fragment and reused reader are unchanged.

All library targets build. Restored focused cases and both full available suites pass
on their first runs: **620 cases, zero failures/skips per framework**, retaining all
618 prior outcomes. Evidence: `/private/tmp/brighter-4170-asb-session-cancellation/`.
Nothing committed or pushed.

Session contention/renewal, receive-arrival races, provisioning, in-flight settlement
cancellation and abandonment-failure recovery remain unvalidated. Session-lock renewal
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. This batch does not establish production readiness.

The isolated emulator containers and network were removed after validation.

## Session-lock failure-state diagnostic audit — inconclusive

The [approved helper amendment](review/asb-session-lock-failure-diagnostics/README.md)
adds temporary diagnostics to the existing in-memory receivers, preserving test
assertions. It separates closed receiver, absent token and expired lock states and
records session receive/renewal wall-clock and monotonic timestamps. No behavioral
production correction was made.

Both focused lock-renewal classes passed 16/16 cases, on .NET 9 and .NET 10. Both
bounded full diagnostic .NET 10 runs passed all 620 available cases. The original
session-lock test failure did not recur. The smallest observed session lifetime
remaining at receive was 1,996.4882 ms of the configured 2,000 ms; these samples do
not show setup consuming the initial lock budget. They do not exclude that hypothesis
in the archived failures, nor establish a renewal/settlement scheduling cause.

Each full diagnostic run recorded three lock-exception events: two closed receivers
with absent tokens, and one present token whose expiry had passed. All tests passed,
and none of those entities matched a recorded session-receive entity. These events
are not reproductions of the original failing session test. The adapter captures
stderr at run level, so no individual test attribution is claimed for those events.
Logging itself can perturb scheduling. These are in-memory diagnostic observations,
not native broker session-renewal validation.

All 6,454 non-generated production/test C# files match their pre-audit hashes after
restoration. Both restored full available suites pass **620 cases, zero failures or
skips**, with exact baseline case/outcome equality. Library and test builds pass.
A sandboxed build initially stalled without output; only its owned processes were
stopped, sources verified restored, and the approved audit rerun with required access.
That infrastructure interruption is not counted as test evidence.

Evidence: `/private/tmp/brighter-4170-asb-session-failure-diagnostics/`, including raw
TRXs/logs, per-run event extracts, timing-summary.json, source hashes and verification.
No tests were added, assertions weakened, or permanent source changes made. Nothing
committed or pushed. The session-lock readiness finding remains open. Further green
repetitions alone are not a release criterion; capture an actual failing session path
or establish a separately reviewed deterministic reproduction before changing behavior.

The isolated diagnostic emulator containers and network were removed after validation.

## Native session renewal control — expiry cause not established

The [approved native queue/topic batch](review/asb-native-session-renewal/README.md)
initially passes on both .NET 9 and .NET 10 without production changes. Each case
receives a custom-mapped session delivery from an entity configured with a five-second
lock, waits eight seconds, acknowledges it, and confirms removal by native peek.

Disabling only the session renewal budget produces two intended Assert.Null failures
at the settlement assertion (line 131), but the recorded exception is
ObjectDisposedException: the native ServiceBusReceiver is already closed. Neither
case reports ServiceBusException with a lock-loss reason. Thus the control detects
the absence of renewal, but does not directly establish the approved native
lock-expiry prerequisite or explain why the SDK receiver closed. The runner records
validated=false and stops. No test or expected exception was weakened to accept this
result, and the other three mutations were not run. These are assertion failures,
not setup failures, but are insufficient for the stronger planned expiry claim.

The mutation is restored. All 1,171 non-generated production C# files match pre-batch
hashes; reused reader and native-client helper are unchanged. The approved test is
preserved byte-for-byte in its review directory and temporary evidence, outside the
compiled project pending resolution of the prerequisite. No production fix or new
passing baseline case is claimed.

Restored library and test builds pass. Both full available suites pass **620 cases,
zero failures/skips**, retaining the exact prior case/outcome multisets. Evidence:
`/private/tmp/brighter-4170-asb-native-session-renewal/`, including four initial passing
cases, the two control failures, mutation/source verification and both regression TRXs.
Nothing committed or pushed.

A follow-up needs direct evidence linking native session expiry to receiver closure,
or a separately reviewed control that observes native session-lock state and release.
Do not reclassify ObjectDisposedException as a native lock-loss reason without that
evidence. The intermittent in-memory session-lock finding, eight DLQ-blocked cases,
52 unavailable hosted classes and broader release gates remain open.

The isolated emulator containers and network were removed after validation.

## Native session SDK investigation

The [version-specific SDK trace](asb-native-session-sdk-trace.md) identifies how session
link closure can dispose the transport before acknowledgement, causing the observed
ObjectDisposedException. It does not establish why the broker closed the link. A
[review-only amendment](review/asb-native-session-expiry-observation/README.md) directly
asserts advancement of the native session expiry while retaining mapping, completion
and removal checks. No new tests ran or production sources changed; baseline is 620
per framework and the native renewal gate remains open pending review and validation.

## Native ASB session-expiry observation — validated

The [approved amendment](review/asb-native-session-expiry-observation/README.md)
validates native emulator session renewal with a custom wire reader for queue and
topic deliveries. A real SDK client records the accepted receiver and original
broker-reported SessionLockedUntil before returning it to Brighter. After eight
seconds, the original five-second lock has expired, the native expiry has advanced,
and the receiver remains open. The same mapped message is acknowledged successfully
and native peek confirms removal. Custom identity/body/headers, native session/lock
metadata and exactly one reader invocation are preserved.

All four initial cases pass without production changes. Disabling the session renewal
budget makes both cases fail the explicit native-expiry-advance assertion at line 135.
This establishes the renewal dependency directly, independently of the prior
ObjectDisposedException result. Bypassing the reader, replacing the mapped body and
omitting native completion each produce the intended assertion failure in both cases.
Four named mutations therefore yield eight intended assertion failures on .NET 9;
all mutations are restored. Restored focused cases pass on both frameworks.

All library targets and tests build. Both expanded full available suites pass
**622 cases, zero failures/skips**, retaining all 620 prior case/outcome pairs and
adding exactly the two new cases. All 1,171 non-generated production C# files match
pre-batch hashes. Adopted test and both observing helpers match the approved drafts;
the reused external reader and approved fixture fragment are unchanged. No production
correction, public contract change or dependency change was needed.

Two harness corrections are preserved in the evidence: the reader-bypass validator
initially expected line 116 instead of the actual reader-count assertion at line 111;
raw failures were validated without changing or rerunning the test. The first .NET 9
regression filter omitted the new class and passed only the 620 baseline cases.
The exact-count check caught this omission; the filter was expanded and the complete
622-case suite passed. Neither correction changed test expectations or masked a failure.

Evidence: `/private/tmp/brighter-4170-asb-session-expiry/`, including raw TRXs, initial
and restored focused results, mutations.json, the original validator report,
baseline-only-net9.0 artifacts, expanded regressions and source verification.
Nothing committed or pushed.

This closes the narrow native emulator renewal/mapping/completion gate. It does not
prove the causal link event behind the earlier disposed-receiver exceptions, resolve
the intermittent in-memory session-lock failure, or establish hosted Service Bus
behavior. Session contention, renewal failure recovery, other session/cancellation
paths, eight DLQ-blocked cases, 52 unavailable hosted classes and broader transport
and release gates remain open. Overall production readiness is not established.

The isolated emulator containers and network were removed after validation.

## Native session renewal after partial settlement — validated

The [approved queue/topic batch](review/asb-session-partial-settlement/README.md)
receives two native deliveries sequentially on the same session receiver, with both
initially unsettled. After acknowledging the first, the native session remains open.
The test records SessionLockedUntil after that settlement, waits eight seconds with
a configured five-second lock interval, verifies the recorded expiry has elapsed
and the current native expiry has advanced, then completes the second delivery.
Both messages preserve custom/native mapping and distinct lock/sequence metadata;
reader count remains two and final native peek confirms removal.

All four initial cases pass without production changes. Five named mutations on
.NET 9 produce ten intended assertion failures: stopping renewal after the first
settlement fails expiry advancement (line 178); closing the session after the first
acknowledgement fails the immediate open-receiver assertion (line 168); bypassing the
reader fails reader count (line 110); replacing custom body fails equality (line 115);
omitting completion fails final native peek (line 197). Both queue/topic cases detect
each defect. All mutations are restored, and restored focused cases pass on both
frameworks without test changes or harness corrections.

All library targets and tests build. Both full available suites pass **624 cases,
zero failures/skips**, retaining all 622 prior case/outcome pairs and adding exactly
the two new cases. All 1,171 non-generated production C# files match pre-batch hashes.
The adopted test matches the approved draft; all three reused helpers are unchanged,
and the approved PT5S session entity fragment is present in the isolated configuration.
No production correction, public API change or dependency change was required.

Evidence: `/private/tmp/brighter-4170-asb-partial-settlement/`, including initial and
restored focused results, ten mutation failures, both expanded regressions, exact
baseline comparisons and source verification. Nothing committed or pushed.

This closes the narrow native emulator partial-acknowledgement renewal gate for two
deliveries received sequentially on one session. It does not cover concurrent calls,
native multi-message batch sizing, abandonment/rejection, competing consumers, renewal
failure recovery or hosted Service Bus. The intermittent in-memory session-lock
finding, eight DLQ-blocked cases, 52 unavailable hosted classes and broader transport
and release gates remain open. Overall production readiness is not established.

The isolated emulator containers and network were removed after validation.

## Native session renewal after partial abandonment — validated

The [approved queue/topic batch](review/asb-session-partial-abandonment/README.md)
receives two native deliveries sequentially on the same session receiver. After
abandoning the first, it verifies that the session stays open and its native expiry
advances past the value recorded after abandonment while the second remains unsettled.
Only then does it receive the first message again, requiring native delivery count
two, a fresh lock token, original sequence and preserved native/custom mapping.
Both deliveries are acknowledged, reader count remains three, and final native peek
confirms removal. Successful NackAsync return alone cannot satisfy the test.

All four initial cases pass without production changes. Six named mutations on
.NET 9 produce twelve intended assertion failures, two for each defect: stopping
renewal fails expiry advancement (line 178); closing after abandonment fails the
immediate open-receiver assertion (line 168); omitting native abandonment fails
redelivery Assert.Single (line 183); bypassing the reader fails reader count (line
110); replacing custom body fails equality (line 115); omitting completion fails
final native peek (line 226). All mutations are restored. Restored focused cases
pass on both frameworks; no test changes or harness corrections were needed.

All library targets and tests build. Both full available suites pass **626 cases,
zero failures/skips**, retaining all 624 prior case/outcome pairs and adding exactly
the two new cases. All 1,171 non-generated production C# files match pre-batch hashes.
The adopted test matches the approved draft, all three reused helpers are unchanged,
and the approved PT5S session entity fragment is present in the isolated configuration.
No production correction, public API change or dependency change was required.

Evidence: `/private/tmp/brighter-4170-asb-partial-abandonment/`, including initial and
restored focused results, twelve mutation failures, both expanded regressions,
exact baseline comparisons and source verification. Nothing committed or pushed.

This closes the narrow native emulator partial-abandonment/renewal/redelivery gate.
It does not validate abandonment failure recovery, rejection/DLQ, concurrent calls,
competing consumers, native multi-message batch sizing or hosted Service Bus. The
intermittent in-memory session-lock finding, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader transport/release gates remain open. Overall production
readiness is not established.

The isolated emulator containers and network were removed after validation.

## Native session renewal-budget expiry and recovery — validated

The [approved queue/topic batch](review/asb-session-renewal-budget-recovery/README.md)
observes one native session delivery with a five-second lock and six-second renewal
budget. After thirteen seconds, native SessionLockedUntil has advanced beyond its
original value and is in the past: renewal occurred and then stopped. The same
consumer reacquires the session through a different native receiver and obtains the
same message with delivery count two, fresh lock, original sequence and preserved
native/custom mapping. Reader count remains two. Acknowledgement and native peek
confirm removal.

The approved recovery contract permits immediate recovery or one ChannelFailureException
followed by one successful receive, preserving the gateway's existing replacement
boundary. A green result alone does not distinguish those permitted paths; no claim
of transparent single-call recovery is made.

All four initial cases pass without production changes. Five named mutations on
.NET 9 produce ten intended assertion failures: ignoring the budget fails native
expiry (line 134); disabling renewal fails initial expiry advancement (line 132);
clearing the reader during receiver reset fails recovery reader count (line 153);
replacing custom body fails equality (line 114); omitting completion fails final
native peek (line 184). Both cases detect every defect. All mutations are restored;
restored focused cases pass on both frameworks. No test or harness amendments were
needed during execution.

All library targets and tests build. Both full available suites pass **628 cases,
zero failures/skips**, retaining all 626 prior case/outcome pairs and adding exactly
the two new cases. All 1,171 non-generated production C# files match pre-batch hashes.
The adopted test matches the approved draft, all three reused helpers are unchanged,
and the approved native entity fragment is present. No production correction,
public API change or dependency change was required.

Evidence: `/private/tmp/brighter-4170-asb-budget-recovery/`, including initial/restored
focused runs, ten mutation failures, both expanded regressions, exact baseline
comparisons and source verification. Nothing committed or pushed.

This closes the narrow native emulator budget-expiry and bounded same-consumer
recovery gate. It does not validate arbitrary renewal failures, network partitions,
contention, concurrent calls, abandonment failure recovery, DLQ or hosted Service Bus.
The intermittent in-memory session-lock finding, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader transport/release gates remain open. Overall production
readiness is not established.

The isolated emulator containers and network were removed after validation.

## GCP rollout source audit

The initial [source audit](gcp-rollout-audit.md) found an unimplemented wire-mapping rollout:
publication/subscription selection is absent and producer, pull and stream paths call
the static parser directly. Pull AckId and streaming callback receipt ownership must
be preserved separately. The audit records implementation order and the mixed Pub/Sub,
Firestore and Spanner test-project boundary. No tests ran or production code changed;
ASB remains at 628 validated cases per framework. Overall readiness remains open.


## GCP native publication writer selection

The [writer batch](review/gcp-native-wire-writer/README.md) reproduces ignored custom
mapping in six sync/async/bulk × ordering cases on each framework. Publication-level
selection, a default parser adapter and a shared producer mapping step fix both send
paths. The gateway retains control of the native ordering key.

All six initial, restored and final CI-marked cases pass on net9.0 and net10.0.
Six named production mutations produce 30 intended assertion failures on net10.0;
all are restored. The corrected review sources match their adopted copies.

Combined selected regressions pass 228 cases with 30 existing skips per framework.
Exact comparison retains all 252 pre-change outcomes and adds only the six writer
cases. Main runs pass 220 cases; comparison catches eight streaming purge cases
omitted by a namespace filter. Those eight pass in separate runs on both frameworks.
The initial comparison failure and both filters are preserved; no missing case is
silently dropped from the final comparison.

Initial missing SDK assemblies are restored without dependency edits. Two draft
references to Header.Id are corrected to Header.MessageId. The new emulator-only
trait and a two-line CI change include these cases in the emulator job and exclude
them from hosted execution. Final builds pass; CI selection passes six cases on
each framework and the hosted exclusion selects none.

Only two existing production files change; one adapter, one test and one helper are
added. The remaining 6,458 pre-existing production/test C# files are unchanged.
Evidence is retained in `/private/tmp/brighter-4170-gcp-writer/`. The isolated native
emulator is removed after validation. Nothing is committed or pushed.

This is a selected Pub/Sub regression result, not a whole-project pass. Full stream
conformance, Firestore, Spanner, Cloud Storage and hosted-only suites remain unrun.
Reader selection, writer failure recovery, concurrency/isolation, scheduler paths and
broader transport/release gates remain open. Overall production readiness is not
established; the separate ASB baseline remains 628 per framework.


## GCP native pull reader selection

The [pull reader batch](review/gcp-native-pull-reader/README.md) adds subscription-level
PullWireMessageReader selection through both public consumer factory paths. A default
adapter retains parser behavior. Both receive paths map once, then attach the native
AckId and resolve delivery counts from the native envelope. An internal constructor
overload carries the selection; the existing public constructor remains unchanged.

Both initial RED runs fail because the reader is never invoked. Initial and restored
implementation runs pass both sync/async cases on net9.0 and net10.0. Native observation
confirms requeue before a 30-second lease expires and absence after acknowledgement
past the ten-second deadline, with a fresh marker as the positive observation control.
The unavailable native counter retains wire HandledCount=7 over the reader's 777.

Eight named production mutations produce 14 intended failures on net10.0. Native
release omission fails the pre-expiry observation; acknowledgement omission exposes
the original native ID after expiry. All mutations are restored. The test and helper
remain byte-for-byte identical to their reviewed drafts.

Each complete selected Pub/Sub regression run passes 230 cases with 30 existing skips.
Exact comparison retains all 258 prior outcomes and adds only the two reader cases.
All eight streaming purge cases are included. Regressions use independent saved green
binaries whose hashes remain unchanged throughout mutation execution. The only change
after saving those binaries removes an obsolete parser comment; restored focused
checks and final builds pass.

The emulator CI filter includes the new category, and the hosted exclusion selects
none. Four existing production files change and one default adapter is added; the
other 6,459 pre-existing production/test C# files remain unchanged. Evidence is in
`/private/tmp/brighter-4170-gcp-reader/`, including raw runs, mutations, exact outcome
comparisons, source hashes and the scoped workflow delta. The isolated emulator is
removed after validation. Nothing is committed or pushed.

This is not a whole-project pass. Full streaming conformance, Firestore, Spanner,
Cloud Storage and hosted-only suites remain unrun; Fragile=CI cases remain excluded.
Streaming mapping, failed/null readers, mixed batches, isolation/concurrency,
populated counters, rejection metadata and scheduler paths remain open. Overall
production readiness is not established. The separate ASB baseline remains 628.


## GCP native streaming reader selection

The [stream reader batch](review/gcp-native-stream-reader/README.md) adds
StreamWireMessageReader selection, a default adapter and factory wiring while
preserving the public consumer constructor. Receive maps once, then restores the
actual native callback receipt and native delivery-count policy. Existing
acknowledgement, nack, requeue and disposal implementations are unchanged.

Four unchanged reviewed cases fail at reader invocation before integration, then
pass initially and after mutation restoration on both target frameworks. The tests
observe native redelivery, SDK callback Ack/Nack completion and continued delivery
of a fresh marker. They do not claim durable acknowledgement or hosted behavior.
Eight production mutations produce 28 intended failures, including bounded failures
when nack, requeue or acknowledgement is omitted. All mutations are restored.

The established Pub/Sub selection passes 234 cases with 30 existing skips on each
framework, retaining all 260 preceding outcomes exactly. Expanded stream coverage
adds 163 outcomes, overlapping eight purge cases. Combined unique final results:
net9.0 364 passes and 55 skips; net10.0 363 passes, one failure and 55 skips.

The pre-implementation net10.0 stream baseline has four failures: ordered nack
redelivery, sync/async disposal with buffered redelivery, and sync disposal with an
unsettled message. The final run retains the async buffered-redelivery failure;
no previously passing stream case regresses. Prompt disposal succeeds, but a
replacement channel receives an empty message within its twenty-second window
instead of the original native ID (test line 87). The other baseline failures
passing later does not mean they are fixed. These lifecycle failures remain open
release blockers and require a separate diagnosis.

The new category is included in emulator CI and excluded from hosted execution;
selection checks pass four cases per framework and hosted selection finds none.
Final normal builds pass with existing warnings. Four existing production files
change, one adapter is added and 6,462 other existing C# source files are unchanged.
The test and helper match their approved drafts exactly. Regression binaries are
preserved independently of mutation builds. Evidence, filters, TRX and exact outcome
comparison are in `/private/tmp/brighter-4170-gcp-stream-reader/`.

No dependencies are changed; nothing is committed or pushed. This is not a
whole-project pass. Fragile=CI, Firestore, Spanner, Cloud Storage and hosted-only
checks remain excluded. Mapping exceptions/nulls, isolation/concurrency, populated
counters, rejection and scheduler paths, remaining transports and the existing
stream lifecycle failures prevent a production-ready claim.

## GCP streaming reader failure recovery — scoped validation

The [approved eight-case batch](review/gcp-stream-reader-failures/README.md)
reproduces throwing/null reader failures on both frameworks and passes after the
stream consumer creates an unacceptable message from native identity, topic and
raw bytes. Gateway-owned callback receipt and handled count remain available for
explicit nack/requeue and acknowledgement. Mapping recovery and subsequent native
delivery pass without changing the approved test or helper.

Thirteen temporary production mutations produce 96 intended assertion failures
on net10.0. All mutations are restored; eight focused cases pass again on each
framework. Library builds pass for netstandard2.0, net8.0, net9.0 and net10.0.
The existing emulator CI category already includes the new cases; remote CI is unrun.

The complete affected Pub/Sub selection retains all 419 preceding case identities
and adds eight passing cases. Results are **370 passes, two failures and 55 skips
on net9.0**, and **371 passes, one failure and 55 skips on net10.0**. Failures concern
ordered async nack/redelivery and async unsettled disposal on net9.0, and sync
unsettled disposal on net10.0. All three passed in the preceding saved full results.
The earlier net10.0 buffered-redelivery failure passes this time, without a related fix.

All three failing cases pass three focused pre-change binary repetitions and one
current-build recheck. The full-suite failures are not reproduced by those controls;
their causes remain unresolved and the change is not declared regression-free.
Only the stream consumer changes among existing C# files; 6,470 other hashed files
are unchanged. Test/helper match their approved drafts exactly. No dependency or
public-constructor changes, commits or pushes are included.

Evidence: `/private/tmp/brighter-4170-gcp-stream-failures/`, including raw TRX,
mutation assertions, control runs, exact case comparisons and source verification.
The two emulators created for this batch are removed. This closes the scoped
stream mapping-recovery behavior. Pull recovery, lifecycle findings, unavailable
hosted/database coverage and broader feature gates remain open.

## GCP full pre-change lifecycle controls

The complete combined Pub/Sub filter was run against the preserved binaries from
before the streaming reader-recovery correction, on separate isolated emulators.
No production or compiled test source changed during this investigation.

Results: net9.0 **363 passes, one failure, 55 skips**; net10.0 **364 passes,
zero failures, 55 skips**. All 419 prior case identities are present.
The net9.0 ordered async nack/redelivery test fails at the same missing-nacked-ID
assertion as the current build. This establishes that this specific failure
predates the reader-recovery correction, without identifying its cause.

Neither full control reproduces the current build's unsettled-disposal failures.
Those remain unresolved; the passing control does not prove a regression or
disprove one. Existing buffered tests do not observe actual callback arrival,
and async disposal tests do not await the disposal task's result. The sync disposal
tests do propagate faults through Task.Wait.

The [next eight diagnostic cases](review/gcp-stream-shutdown-observation/README.md)
observe native shutdown nack completion, actual buffered arrival and disposal
task completion, then compare native-pull and replacement-stream recovery.
They are outside compiled projects, unbuilt and unrun pending review. No speculative
shutdown correction, timeout relaxation or SDK update is made.

Evidence: `/private/tmp/brighter-4170-gcp-lifecycle/`, including full-control TRX/logs,
parsed failures and pinned SDK source inspected for the diagnostic boundary.
Feature-wide production readiness remains open.

## GCP native shutdown observation results

The approved [eight shutdown diagnostics](review/gcp-stream-shutdown-observation/README.md)
are adopted. A compile-only correction qualifies `Grpc.Net.Client.GrpcChannelOptions`;
no behavior, bounds or assertions change. The focused cases pass on both frameworks
before mutations and again with production sources restored. Native traces observe
actual buffered callbacks, successful shutdown nacks, and recovery through independent
native pull or a replacement stream. These tests intentionally bypass the factory.

Four approved production mutations produce 28 failures. Omitting shutdown produces
eight missing-nack assertions; omitting callback cancellation produces eight observed
disposal timeouts; omitting consumer Accept produces four replacement-callback timeouts
while four native-pull cases pass. Premature native Ack produces four expected buffered
arrival timeouts and four earlier missing-nack assertions, despite successful unwanted
native Acknowledge requests in their traces. Those last four outcomes differ from the
approved assertion expectation. The [assertion-order amendment](review/gcp-stream-shutdown-ack-observation/README.md)
is a review draft only; the compiled assertions remain unchanged. The task is not
marked complete pending that review. Every mutation is restored byte for byte and
followed by eight passing net10.0 cases; the final net9.0 focus also passes eight.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 380 | 0 | 55 | 435 |
| net10.0 | 379 | 1 | 55 | 435 |

Both runs retain all 427 prior case identities and add exactly eight passing cases.
The net10.0 failure is the existing async unsettled-disposal case, at the replacement
message ID assertion on line 83: an empty message arrives from the bounded receive.
The same case failed on net9.0 in the preceding reader-recovery batch. This run
confirms the finding remains open; it does not establish its cause. Passing ordered
nack and buffered-disposal cases do not close earlier intermittent failures.

The GCP library builds netstandard2.0, net8.0, net9.0 and net10.0 with zero warnings
or errors. The emulator CI filter includes GcpNativeStreamLifecycle; hosted runs
already exclude its Requires=PubSubEmulator trait. Hosted/Spanner/remote CI coverage
is not claimed. All 6,472 source hashes are compared: the test's type qualification
is the only post-adoption C# difference, and all existing C# files are unchanged.
The review copy and compiled test match. No production fix, dependency update or
timeout relaxation is introduced. All three batch-owned emulators are removed.

Evidence: `/private/tmp/brighter-4170-gcp-shutdown-observation/` contains full TRXs,
native traces, emulator logs, builds, four mutation/restoration pairs and
`final-verification.json`. Feature-wide production readiness remains open.

## GCP shutdown assertion-order amendment completed

The approved [assertion-order amendment](review/gcp-stream-shutdown-ack-observation/README.md)
is adopted exactly. The only compiled C# change moves the existing forbidden native
Ack assertion before the shutdown-nack assertion. All operations, cases and bounds
remain unchanged. The amended premature-Ack mutation produces four forbidden-Ack
failures at line 157 and four buffered-arrival failures at line 136. The other three
approved mutations are rechecked: eight missing-shutdown-nack assertions, eight
disposal timeout assertions and four replacement-acknowledgement timeouts, with
four native-pull cases passing in the last mutation. All **28** failures now reach
their intended assertions. Each mutation is byte-restored and followed by eight
passing net10.0 cases; final focused runs pass eight on both frameworks.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 379 | 1 | 55 | 435 |
| net10.0 | 379 | 1 | 55 | 435 |

All 435 prior case identities remain present, including eight passing diagnostics.
The net9.0 ordered async two-message nack case fails at line 158; the net10.0 sync
counterpart fails at line 152. Both are the first final assertion, checking the
**nacked message's redelivery**, not the other queued message. The sync failure is
newly observed in this batch and its cause is not established. Disposal tests pass
this run; that nonreproduction does not close the prior intermittent findings.

A source-line audit also corrects the earlier full pre-change control description:
its line 158 failure checks the nacked message's ID. Earlier wording that called it
a missing second/other message was incorrect. The generated source hashes match the
saved recovery-batch snapshot, and the preserved control TRX identifies line 158.
The baseline failure still demonstrates that the async ordering failure predates
the reader-recovery change; the correction changes its interpretation, not its
outcome or timing.

The GCP library builds all four targets with zero warnings/errors. All 6,472 existing
src/tests C# files are compared: only the approved test assertion order differs,
and all production mutations are byte-for-byte restored. The compiled test exactly
matches the approved amendment. No production correction, dependency update or
bound increase is introduced. All three emulators created for this batch are removed.

Evidence: `/private/tmp/brighter-4170-gcp-shutdown-ack-observation/`, including mutation
and restored TRXs, full-suite results, native traces, emulator logs, library builds,
source snapshots and `final-verification.json`. Earlier artifacts are preserved.
The scoped observation amendment is complete; feature-wide readiness remains open.

The [next factory shutdown draft](review/gcp-factory-stream-shutdown/README.md) covers
real factory-created channels, stopped-cache replacement and SDK-owned connections
with one/two native clients. Its eight cases and four named mutations are outside
compiled projects, unbuilt and unrun pending review. It does not claim to explain or
cover the ordering failure, and changes no existing test or production code.

## GCP factory shutdown characterization completed

The approved [eight factory shutdown cases](review/gcp-factory-stream-shutdown/README.md)
now run through the public channel factory with one/two configured native clients,
SDK-owned stream connections and same-subscription cache replacement. The initial
net9.0 run fails during subscription construction because the draft omitted the
required request type. Adding the existing TestDoubles.MyCommand type corrects that
fixture setup only; it is not behavioral RED or a production correction. The original
approved file is preserved, and no assertion, bound or operation sequence changes.

All eight configured cases pass on both frameworks. Four approved production
mutations produce **24 intended assertion failures**: eight pending callbacks when
native shutdown is omitted, eight disposal timeouts when cancellation is removed,
four missing second factory builds when stopped-cache eviction is omitted, and four
replacement acknowledgement timeouts when Accept is omitted. Native-pull branches
pass the last two mutations. Each mutation is restored byte for byte and followed
by eight passing net10.0 cases; final focus passes eight on both frameworks.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 388 | 0 | 55 | 443 |
| net10.0 | 388 | 0 | 55 | 443 |

Both matrices retain every one of the 435 prior case identities and add exactly
eight passing cases. The earlier ordered-nack and disposal failures are not
reproduced. Their cause remains unresolved; no production code changes in this
batch, so these passes do not establish a fix for the intermittent findings.

All four GCP library targets build with zero warnings/errors. The test's diagnostic
MessageType log emits the existing property's CS0618 deprecation warning in test
builds. All 6,472 pre-existing src/tests C# files match their saved hashes. The only
new compiled source is the approved test plus its required request-type configuration;
it matches the review copy. Existing CI categories select it for the emulator and
exclude it from hosted runs. No hosted/Spanner/remote CI coverage is claimed. All
three isolated emulators created for this batch are removed; no commit is made.

Evidence: `/private/tmp/brighter-4170-gcp-factory-shutdown/`, including initial setup
failure TRXs, configured green runs, native traces, four mutation/restoration pairs,
full-suite results, emulator logs, library builds and `final-verification.json`.
This closes the scoped factory characterization, not feature-wide readiness.

At the factory-shutdown checkpoint, source inspection identified the next mapping
gap: unprotected pull-reader throw/null results. The [eight recovery cases](review/gcp-pull-reader-failures/README.md)
were then drafted outside compiled projects for review. The approved execution and
correction are recorded in the following section; ordering, streaming lifecycle and
mixed batch findings remain separate gates.

## GCP pull reader recovery completed

The approved eight pull-reader recovery cases and configured reader were adopted
unchanged. Before implementation, all eight fail at the initial Assert.Null(error)
on both frameworks: four escaping reader exceptions and four null dereferences.
The pull consumer now catches thrown/null reader results, logs the mapping failure,
constructs an unacceptable message from native ID/topic/raw bytes without reparsing
invalid MIME, and attaches the native receipt and delivery count. The caller retains
settlement ownership and the configured reader remains selected after recovery.
Public Receive/ReceiveAsync remarks describe that behavior. No Nack, timeout, SDK,
factory, streaming or scheduler policy changes are included.

All eight cases pass initially after correction and in final restored runs on both
frameworks. Eight named production mutations produce **64 intended assertion
failures**, covering the unprotected reader, invalid-parser fallback, reader bypass,
fabricated receipt, premature acknowledgement, missing release, missing acknowledgement
and loss of the configured reader after failure. Each is restored exactly and followed
by eight passing net10.0 cases. The first auto-ack mutant did not compile because
MessageHeader.MessageType is obsolete and production warnings are errors. It was
replaced with a local failure flag in the temporary mutant; tests and intended behavior
were unchanged. That compile error is not counted as RED, and its log is retained.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 395 | 1 | 55 | 451 |
| net10.0 | 395 | 1 | 55 | 451 |

Every one of the 443 prior case identities is retained, with exactly eight new passing
cases per framework. Observed full-run failures:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

The ordered nack failures are the previously recorded missing nacked-message IDs
at the first final Assert.Contains (generated Proactor line 158 / Reactor line 152).
These failures predate this pull-only correction; the async failure was also observed
in the preserved pre-stream-recovery control. Their causes remain unresolved.
These remain release blockers; a passing focus run is not substituted for the
full result. The baseline comparison records changed outcomes explicitly rather
than claiming every previous pass is retained.

All four GCP library targets build with zero warnings/errors. Of 6,473 pre-existing
src/tests C# files, 6,472 remain byte-identical; only GcpPullMessageConsumer changes.
The two added compiled files exactly match the approved review copies. Existing CI
traits include native emulator execution and exclude hosted runs. No hosted,
Firestore, Spanner or remote-CI validation is claimed.

Evidence: `/private/tmp/brighter-4170-gcp-pull-failures/`, including initial RED,
green, mutation/restoration and full TRXs/JSON, logs, before/after source copies,
source hashes, all-target build and `final-verification.json`. The first full runner
launch encountered a Docker name collision before any tests ran; its logs are kept
separately and the runner names were corrected. All three owned emulators are
removed after validation; unrelated containers are untouched. No commit is made.

At the pull-recovery checkpoint, the [writer recovery draft](review/gcp-writer-failure-recovery/README.md)
was outside compiled projects pending review. Its approved execution is recorded in
the subsequent writer-recovery section. It covers sync/async/single-
message bulk throw/null behavior, no publication on mapping failure, source preservation,
real publication after explicit retry and continued custom mapping. Mixed batches,
concurrency, rejection/scheduler selection, historical lifecycle failures and the
remaining gateway integrations still prevent feature-wide production readiness.

## GCP writer failure recovery completed

The approved six cases and writer were adopted unchanged. Initial runs on both
frameworks reproduce three null-result failures at the InvalidOperationException
type assertion: the actual exception is NullReferenceException. The three throwing
cases already pass. An explicit null guard in GcpMessageProducer.CreateWireMessage
now produces the reviewed error before native publication. Existing thrown exceptions
retain their identity. Public send XML remarks describe mapping failure and the
possibility that earlier bulk messages have already been published.

All six corrected cases pass on both frameworks. Six approved production mutations
produce **30 intended assertion failures**: three removed null guards, six configured
writer bypasses, three changed exception identities, six fallback publications,
six lost ordering keys and six omitted native publications. Each mutation is restored
exactly and followed by six passing net10.0 cases; both final focused suites pass six.
The tests prove native publication absence on mapping failure, explicit same-producer
retry, fresh-message delivery, source preservation and gateway ordering control.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 402 | 0 | 55 | 457 |
| net10.0 | 401 | 1 | 55 | 457 |

All 451 prior case identities remain, with exactly six new passing cases per framework.
The comparison records existing outcome changes explicitly. Full-run findings:

- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

The first full launch raced emulator startup, producing connection-refused setup
failures. Both invalid runs were stopped and their logs/runner exit records archived;
they do not establish behavioral RED or a valid full matrix. Fresh emulators passed
an explicit TCP-listener readiness probe before the complete runs above. Source and
tests were unchanged for the rerun.

All four library targets build with zero warnings/errors. Of 6,475 pre-existing
src/tests C# files, 6,474 remain byte-identical; only GcpMessageProducer changes.
The two new compiled sources exactly match the approved review copies. Existing CI
traits include emulator execution and exclude hosted runs. No hosted, Firestore,
Spanner or remote-CI coverage is claimed. All batch-owned emulators are removed;
unrelated containers remain untouched, and no commit is made.

Evidence: `/private/tmp/brighter-4170-gcp-writer-failures/` contains RED/green,
mutation/restoration and full TRXs/JSON, archived startup failures, readiness evidence,
emulator logs, source snapshots, all-target builds and `final-verification.json`.

At the writer-recovery checkpoint, the [send-cancellation draft](review/gcp-send-cancellation/README.md)
was pending review. That batch has since been approved and validated; see its
execution record for the correction and remaining lifecycle failures. It covers cancellation before
entry or during mapping for immediate async and single-message bulk sends. Native
in-flight cancellation, partial batches, reader isolation/concurrency, scheduler and
rejection mapping, historical lifecycle failures, hosted checks and the remaining
five gateway integrations still prevent feature-wide production readiness.

## GCP send cancellation before native submission completed

The approved four cases and configured writer were adopted unchanged. All four
initially fail on both frameworks at the OperationCanceledException assertion:
the send completes without cancellation. Immediate individual sends now check the
caller token before mapping and immediately before native submission. Bulk sends
apply those checks per message. Cancellation retains the caller's token and prevents
submission at those boundaries; the active-token retry uses the same producer and
selected writer. Once submitted, the existing native publication await remains.
Public XML remarks and exception tags state that cancellation cannot recall an
already submitted message. Scheduler and batch-creation behavior remain unchanged.

All four corrected cases pass on both frameworks. Five named production mutations
produce **16 intended assertion failures**: two removed entry checks, two removed
post-mapping checks, four lost caller tokens, four publications before cancellation
and four omitted recovery publications. Each mutation is restored exactly and followed
by four passing net10.0 cases; both final focused suites pass four. Tests observe no
native publication for cancellation before entry or during mapping, then real retry
and fresh-message publication with preserved source data and native ordering.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 405 | 1 | 55 | 461 |
| net10.0 | 404 | 2 | 55 | 461 |

All 457 prior case identities remain, with exactly four new passing cases per framework.
Existing outcome changes are recorded explicitly. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.Stream.GcpStreamDisposeWithUnsettledMessageAsyncTests.When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_async_should_return_promptly — Assert.Equal() Failure: Values differ
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpFactoryStreamShutdownRecoveryTests.When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(useAsync: False, clientCount: 2, nativeRecovery: False) — Assert.Equal() Failure: Values differ
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

The factory shutdown failure is newly observed in this batch, not established as a
pre-existing failure of that case. Its net10.0 sync/two-native-client/replacement-channel
trace records successful disposal, a cancelled callback and a second factory build,
then MT_NONE after the 20-second receive window (test line 155). It uses native
publication and the unchanged streaming path, outside the producer method changed
here. Three focused runs of the saved pre-change binaries and one current run each
pass all eight factory cases. This non-reproduction does not resolve the full-suite
finding or establish its cause. No lifecycle fix or wider bound is introduced.
The 322 saved baseline binary artifacts retain their hashes, and the baseline producer
source matches the pre-batch snapshot exactly. The async ordered-nack failure remains
the previously recorded missing nacked-message ID at generated Proactor line 158.
The net9.0 async unsettled-disposal case also reproduces its previously recorded
empty replacement receive at line 83; its cause remains open.

All four library targets build with zero warnings/errors. Of 6,477 pre-existing
src/tests C# files, 6,476 remain byte-identical; only GcpMessageProducer changes.
The two added compiled sources exactly match their approved review copies. Both full
runs start only after their fresh emulator listener is ready. Existing CI traits
include emulator execution and exclude hosted runs; no hosted, Firestore, Spanner
or remote-CI coverage is claimed. All three batch-owned emulators are removed;
unrelated containers are untouched, and no commit is made.

Evidence: `/private/tmp/brighter-4170-gcp-send-cancellation/` contains RED/green,
mutation/restoration and full TRXs/JSON, native lifecycle traces and focused controls,
baseline artifact hashes, readiness evidence, emulator logs, source snapshots,
all-target build, cleanup record and `final-verification.json`.

At the send-cancellation checkpoint, the [partial-bulk cancellation draft](review/gcp-partial-bulk-cancellation/README.md)
was pending review. It has since been approved and characterized below. It covers a confirmed
first publication followed by cancellation before or during the second mapping,
then explicit caller retry of only the remainder. This adds no general progress API
or duplicate-free whole-batch retry guarantee. Other partial batches, in-flight
cancellation, reader concurrency, scheduler/rejection mapping, hosted checks,
historical lifecycle findings and five remaining gateway integrations remain open.

## GCP cancellation after partial bulk progress characterized

The [approved two cases](review/gcp-partial-bulk-cancellation/README.md) are adopted
unchanged. Both pass initially and after exact mutation restoration on net9.0 and
net10.0 without a production change. A real first Publish response triggers either
immediate cancellation or cancellation during the second mapping. The tests verify
the caller token, exactly one native publication, the expected writer inputs and
unchanged sources. Independent native pull/ack observes the confirmed prefix and
both explicitly selected remainder messages retried through the same producer.

Four named production mutations produce **six intended assertion failures**: moving
the entry check outside the loop visits an unwanted second mapping (one failure);
removing the post-mapping check publishes an unwanted second message (one failure);
default-token cancellation loses token identity (two failures); omitting gateway
ordering enforcement changes independently observed native ordering (two failures).
Each mutation is restored exactly and followed by two passing net10.0 cases. Both
final focused suites pass two cases. No setup or compile failure is counted as RED.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 407 | 1 | 55 | 463 |
| net10.0 | 406 | 2 | 55 | 463 |

All 461 prior case identities remain, with exactly two new passing cases per framework.
Outcome changes are recorded in final-verification.json. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

These ordered-nack failures are the missing nacked-message ID at the first final
assertion: Reactor line 152 and Proactor line 158. The previously failing async
unsettled-disposal and sync/two-client factory replacement cases pass in these runs;
this does not establish a fix.

The prior ordered-nack, unsettled/buffered disposal and sync/two-client factory
replacement findings remain open. This characterization changes no streaming code
and establishes no lifecycle fix. The known remainder belongs to this deterministic
fixture: no progress API or duplicate-free whole-batch retry guarantee is introduced.

All 6,479 pre-existing src/tests C# files remain byte-identical. The one added compiled
test matches its approved review copy; both reused helpers are unchanged. All four
library targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero warnings/errors.
Test-project builds retain existing warnings. Both full runs wait for fresh native
emulator readiness. Existing CI traits include these tests in emulator execution
and exclude hosted runs. No hosted, Firestore, Spanner or remote-CI result is claimed.
All three batch-owned emulators are removed; unrelated containers are untouched.
Nothing is committed or published.

Evidence: `/private/tmp/brighter-4170-gcp-partial-bulk-cancellation/` contains
initial/restored, mutation and full TRXs/JSON; emulator readiness and logs;
source hashes and exact snapshots; all-target build; cleanup.json and
final-verification.json.

At the partial-bulk cancellation checkpoint, the [writer-failure draft](review/gcp-partial-bulk-writer-failures/README.md)
was pending review. It has since been approved and characterized below. It covers configured
writer throw/null after confirmed progress. Other partial-batch and concurrency
paths, in-flight cancellation, scheduler/rejection mapping, historical lifecycle
findings, hosted checks and five remaining gateway integrations still prevent
feature-wide production readiness.

## GCP writer failure after partial bulk progress characterized

The [approved two cases](review/gcp-partial-bulk-writer-failures/README.md) are adopted
unchanged. Both pass initially and after mutation restoration on net9.0/net10.0
without a production change. After a confirmed first Publish, the second mapping
either throws the writer's original exception or returns null. The bulk preserves
the original exception instance or documented null-result error, visits only the
first two source objects and publishes only the first. Independent native pull/ack
verifies the prefix, then the explicitly selected two-message remainder retried
through the same producer. Source IDs, bytes, topics, partitions and bags stay intact.

Four production-only mutations produce **six intended assertion failures**: removing
the null guard produces NullReferenceException (one); replacing the writer exception
loses instance identity (one); deferring mapping failure until after the loop visits
the third source (two); omitting gateway ordering enforcement changes independently
observed native ordering (two). Each mutation is restored exactly and followed by
two passing net10.0 cases. Both final focused suites pass two. No build/setup failure
is counted as behavioral RED.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 409 | 1 | 55 | 465 |
| net10.0 | 409 | 1 | 55 | 465 |

All 463 prior case identities remain, with exactly two new passing cases per framework.
Outcome changes are recorded in final-verification.json. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.Stream.GcpStreamDisposeWithUnsettledMessageTests.When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_should_return_promptly — Assert.Equal() Failure: Values differ
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set

The net9.0 failure is the previously observed empty replacement receive in the
synchronous unsettled-disposal case at line 82. The net10.0 failure is the missing
nacked-message ID at the first final assertion in generated Reactor line 152.
The factory diagnostic cases pass in these runs; that does not establish a fix.

Historical ordered-nack, unsettled/buffered disposal and sync/two-client factory
replacement findings remain open. No streaming code changes in this characterization,
and it establishes no lifecycle fix. The known remainder is specific to the fixture;
there is no general progress API or duplicate-free whole-batch retry guarantee.

All 6,480 pre-existing src/tests C# files remain byte-identical. The sole added compiled
test matches its approved review copy; both reused helpers remain unchanged. All four
library targets (netstandard2.0/net8.0/net9.0/net10.0) build without warnings or errors.
Test-project builds retain warnings. Both full runs wait for fresh native emulator
readiness. Existing CI traits select emulator execution and exclude hosted execution;
no hosted, Firestore, Spanner or remote-CI result is claimed. All three batch-owned
emulators are removed; unrelated containers remain untouched. Nothing is committed
or published.

Evidence: `/private/tmp/brighter-4170-gcp-partial-bulk-writer-failures/` contains
initial/restored, mutation and full TRXs/JSON, emulator readiness/logs, source hashes
and exact snapshots, all-target build, cleanup.json and final-verification.json.

At the partial-bulk writer-failure checkpoint, the [factory native shutdown observation amendment](review/gcp-factory-native-shutdown-observation/README.md)
was pending review. It has since been approved and characterized below. It adds native nack/ack
RPC evidence to the existing eight factory cases through the public SDK settings
hook, retaining native connection ownership, case identities, assertions and bounds.
Other batch/concurrency paths, scheduler/rejection mapping, hosted checks, historical
lifecycle findings and five remaining gateway integrations still block feature-wide
production readiness.

## GCP factory native shutdown observation characterized

The [approved amendment](review/gcp-factory-native-shutdown-observation/README.md)
replaces the existing factory test unchanged from review, retaining all eight case
identities. All eight pass initially and after restoration on net9.0/net10.0 without
a production correction. The public SDK ApiSettings.Interceptor hook captures native
unary RPCs while the factory-created SDK clients retain their own connections.

Each case observes a successful deadline-zero ModifyAckDeadline request for the
exact subscription and first factory generation, started after disposal began and
containing acknowledgement IDs. No completed native acknowledgement precedes recovery.
The original disposal, callback, recovery, identity/content, replacement-build and
receipt-acknowledgement assertions remain. SDK versions and all time bounds remain
unchanged. Native acknowledgement IDs are recorded, but this amendment does not capture
streaming delivery responses to independently associate each acknowledgement ID with
a native message ID. Every scenario publishes only one message to its unique subscription.

Four production-only mutations produce **24 intended assertion failures**: omitting
native shutdown fails all eight native-nack assertions; removing callback cancellation
causes eight disposal timeouts; omitting stopped-client eviction causes four missing
replacement builds; omitting Accept causes four replacement-acknowledgement timeouts.
Each exact restoration returns eight net10.0 passes; final focused suites pass eight
on both frameworks. No setup/build failure is counted as behavioral RED.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 408 | 2 | 55 | 465 |
| net10.0 | 410 | 0 | 55 | 465 |

All 465 prior case identities remain unchanged, including eight passing amended cases
on each framework. No new cases are added. Outcome changes are recorded in
final-verification.json. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.Stream.GcpStreamDisposeWithUnsettledMessageAsyncTests.When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_async_should_return_promptly — Assert.Equal() Failure: Values differ
- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set

The net9.0 failures are the missing nacked-message ID in generated Reactor line 152
and the empty replacement receive in async unsettled disposal at line 83. Both have
been observed previously. The net10.0 matrix is green in this run; it does not close
those intermittent findings.

The historical ordered-nack, unsettled/buffered disposal and sync/two-client factory
replacement findings remain open. Successful focused observation and later passing
recovery do not establish their cause or a fix. Native RPC completion remains distinct
from independently observed redelivery.

Of 6,481 pre-existing src/tests C# files, 6,480 remain byte-identical; only the approved
factory test changes. All production sources and reused helpers are unchanged, and
no compiled source is added. All four library targets build without warnings/errors.
Test-project builds retain warnings. Full runs wait for fresh emulator readiness.
Existing CI traits select emulator execution and exclude hosted execution; no hosted,
Firestore, Spanner or remote-CI result is claimed. All three batch-owned emulators are
removed; unrelated containers remain untouched. Nothing is committed or published.

Evidence: `/private/tmp/brighter-4170-gcp-factory-native-shutdown/` contains
initial/restored, mutation and full TRXs/JSON; successful native nack observations;
original test and source snapshots; readiness/emulator logs; library build;
cleanup.json and final-verification.json.

At the factory-observation checkpoint, the [ordered-nack observation draft](review/gcp-ordered-nack-native-observation/README.md)
was outside compiled projects pending review. Its approved execution and unresolved failure are recorded below. Six sync/async × default/
one/two-client cases correlate native streaming delivery acknowledgement IDs with
nack RPCs while preserving immediate receive/ack sequencing. Three named mutations
are specified. Other lifecycle, concurrency, scheduler/rejection, hosted and gateway
rollout requirements remain open; full-feature production readiness is not established.


## GCP ordered-nack observation: native failure reproduced

The [approved six cases](review/gcp-ordered-nack-native-observation/README.md) and two
passive helpers are adopted unchanged. Initial runs pass six cases on both frameworks.
All three approved production mutations produce six intended missing-native-nack
assertions each: **18 intended failures**. Exact production restoration is verified.
The first two restoration runs pass six cases each. The third has **five passes and
one failure**: async/default SDK clients successfully nack the observed first native
ack ID, but that message is not redelivered within thirty seconds. The original
receipt completes with Nack and the other message is received/acknowledged.

Raw StreamingPull observation contains no second native delivery of the missing
message. The full net9.0 matrix independently reproduces the same diagnostic failure.
Both failing traces include an overlapping lease extension and second-message ack;
completion timestamps do not establish the server's internal processing order.
Final focused runs pass six on each framework, but do not clear either captured failure.
**The restoration gate remains unresolved; characterization is not marked complete.**
No production correction, timeout increase, client-count reduction or SDK upgrade
is made.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 413 | 3 | 55 | 471 |
| net10.0 | 415 | 1 | 55 | 471 |

All 465 prior case identities remain, with six diagnostics added. Full failures:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpOrderedNackRpcRecoveryTests.When_nacking_an_ordered_gcp_delivery_should_send_a_native_nack_and_recover_both_messages(useAsync: True, clientCount: 0)
- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async
- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async

Read-only inspection of the pinned emulator 0.8.36 jar identifies a possible
interaction between deferred ordered acknowledgements and stream ownership changes.
This remains a hypothesis: current traces lack server stream ownership and processing
order. It does not establish a Brighter defect or a hosted Pub/Sub defect.
The [native SDK control](review/gcp-ordered-nack-native-control/README.md) is prepared
outside compiled tests, unbuilt/unrun pending review, to investigate without Brighter
in the delivery path. Its bounded outcomes will not replace the Brighter regressions.

All 6,481 pre-existing src/tests C# files are byte-identical to the pre-batch snapshot;
only the three approved compiled files are added. All four library targets build with
zero warnings/errors. Existing test-build warnings remain. All three owned emulators
are removed after saving logs; no unrelated container is changed. Nothing is committed
or published. Hosted/Firestore/Spanner/remote-CI validation is not claimed. Historical
disposal, other GCP mapping/concurrency/scheduler gaps and five remaining gateway
integrations continue to block full-feature production readiness.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-observation/` contains all
initial/mutation/restored/final/full TRXs and JSON, native trace summaries, exact source
snapshots/hashes, emulator bytecode inspection, all-target build, readiness/cleanup and
`final-verification.json`. The failed restoration is retained as
`restored-disconnect-receipt.trx` and `restored-failure-native-trace.txt`.


## GCP ordered nack reproduced without Brighter

The [approved native SDK control](review/gcp-ordered-nack-native-control/README.md)
builds without warnings/errors and executes unchanged. The dependency manifests contain
Google.Cloud.PubSub.V1 3.38.0 and Google.Api.Gax.Grpc 4.13.1, with no Brighter production
assembly. Both frameworks run the fixed four rounds × default/one/two clients.

| Native control | Observations | Both IDs recovered | Missing first ID within 30s | Correlated native nacks completed |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 12 | 11 | 1 | 12 |
| net10.0 | 12 | 11 | 1 | 12 |

Both missing-redelivery observations occur in round 3 (the fourth round), with client
count unset and Environment.ProcessorCount 11. Each records only one raw delivery of
the first native message, a successful nack for its exact observed ack ID, and the
second message's successful acknowledgement. No Brighter receipt, mapper, channel or
factory participates. All one/two-client observations recover both IDs; this is not a
recommendation to change the default count or evidence that a lower count is always safe.
The concurrent framework runs and small fixed sample do not establish a failure rate.

This demonstrates that Brighter is not necessary to trigger the observed failure.
It does not yet distinguish an SDK defect from emulator behaviour, prove permanent
message loss, establish hosted behaviour, or clear the existing Brighter restoration
gate. The same overlapping ack/nack/lease-extension pattern remains visible. No
production correction is justified by these observations alone.

The [prepared protocol control](review/gcp-ordered-nack-protocol-control/README.md)
removes high-level subscriber handling and automatic lease management. Its four native
RPC/stream-membership combinations and separately recorded recovery intervention are
outside compiled tests, unbuilt/unrun pending review. See also the local
[upstream reproduction report](gcp-ordered-nack-upstream-reproduction.md), not published.

All 6,484 pre-existing src/tests C# files remain byte-identical. No production or compiled
test source changes, no new test assertions/mutations, and no dependency upgrades occur.
The two owned emulators are removed after saving logs. The earlier full Brighter matrix
remains the regression checkpoint: net9.0 413 passed/3 failed/55 skipped; net10.0 415
passed/1 failed/55 skipped, with all four library targets built. Those suites are not
rerun because this control changes no production/test source. Other release gaps remain
open; nothing is committed or published.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-native-control/` contains approved
source snapshots, build/dependency artifacts, all 24 RESULT lines and full native
traces, both isolated failure traces, per-framework JSON, readiness/emulator logs,
source hashes, cleanup.json and final-verification.json.


## GCP ordered-nack emulator protocol stall isolated

The [approved protocol experiment](review/gcp-ordered-nack-protocol-control/README.md)
uses generated gRPC service clients directly: no Brighter, high-level subscriber
handler, automatic lease management, substituted response or emulator-internal call.
After correcting readiness setup, both frameworks complete the same four observations:

| Add a second stream | Complete second ack before first nack | net9.0 redelivery within 30s | net10.0 redelivery within 30s |
| --- | --- | --- | --- |
| No | No | Yes | Yes |
| No | Yes | Yes | Yes |
| Yes | No | Yes | Yes |
| Yes | Yes | No | No |

For the stalled combination, a successful correlated nack is followed by thirty seconds
without the first native message. Repeating only the second message's original ack
then releases the first message onto stream 1 with the same native ID/body and a new
ack ID: 37.620 ms after the intervention on net9.0 and 46.755 ms on net10.0. The initial
failure remains recorded; subsequent recovery is not counted as passing that bound.
The new stream's readiness is independently observed through a separate marker before
settling either target message.

This isolates a failure mode in the pinned emulator's ordered delivery protocol that
does not require Brighter or high-level SDK handling. Read-only bytecode inspection is
consistent with a deferred acknowledged entry preventing ownership transfer after the
earlier delivery is nacked. The live server's internal state was not instrumented.
This does not prove every historical failure has the same cause or establish hosted
Pub/Sub behaviour. Original Brighter tests, defaults and bounds remain intact; their
restoration gate and unrelated lifecycle findings remain open.

The first run exposed a readiness setup race on both frameworks: a marker reached the
original stream before the second was registered. Two single-stream observations per
framework completed, then setup aborted. These logs and nonzero exits are preserved
under initial-setup-error; they are not behavioural RED. The sole executable correction
acknowledges such an early marker and waits for a subsequent marker on stream 1 within
the same fifteen-second setup bound. Exact comparison confirms all settlement,
observation and recovery code remains unchanged. Initial failed runners were stopped
after reporting their unhandled setup error to avoid waiting for crash-dump writing.
A fresh pair of emulators is used for the corrected matrix.

Both initial and corrected project builds have zero warnings/errors. All 6,484 existing
src/tests C# files remain byte-identical. Both initial and both corrected owned emulators
are removed, and native logs are saved. The prior full Brighter regression/library
checkpoint remains unchanged; it is not rerun for this standalone diagnostic. No
production fix, dependency upgrade, test skip, commit or publication is included.

The local [upstream reproduction report](gcp-ordered-nack-upstream-reproduction.md)
now includes the discriminating protocol matrix. The next [hosted control draft](review/gcp-ordered-nack-hosted-control/README.md)
explicitly targets the locally configured brighter-pubsub-validation project, whose
ADC file exists but validity/permissions are unverified. It is unbuilt/unrun pending
review; no hosted API call or credential disclosure occurs. Its results cannot replace
Brighter hosted integration or broader release validation.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-protocol-control/` contains
approved/executed source snapshots, readiness-only.patch, initial setup failures,
all eight final observations, isolated failure/intervention traces, dependency/build
artifacts, source hashes, emulator logs, cleanup.json and final-verification.json.

## Hosted native SDK comparison (2026-10-29)

The approved [hosted control](review/gcp-ordered-nack-hosted-control/README.md)
ran its unchanged four-round × default/one/two-client matrix against
`brighter-pubsub-validation`, sequentially on both frameworks. Every observation
recovered both application IDs within the original thirty-second loop and recorded
successful native nack completion correlated to the first delivery. Native traces
show two copies of the first message, preserving its native ID, application ID,
binary body and ordering key, with a new acknowledgement ID on redelivery.

| Framework | Observations | Correlated native nacks | Both IDs recovered | Missing first ID |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 12 | 12 | 12 | 0 |
| net10.0 | 12 | 12 | 12 | 0 |

Execution used local macOS/Darwin 25.6.0 arm64, eleven reported processors,
Microsoft.NETCore.App 9.0.20/10.0.11 and unchanged Pub/Sub 3.38.0/GAX gRPC 4.13.1.
The emulator comparison ran in Linux containers, so this is not a controlled
single-variable comparison of server implementations. The raw protocol experiment
independently isolates the emulator handoff stall; these hosted observations did
not reproduce missing redelivery. Neither result establishes absence of intermittent
hosted failure or validates Brighter's hosted integration.

Both builds have zero warnings/errors; both processes exit zero, completing every
owned-resource delete. Read-only project listings independently verify that all
24 owned topics and 24 owned subscriptions are absent. Exactly 48 synthetic messages
were published. ADC was used without copying or printing credentials. No auth,
permission, quota, setup or cleanup error occurred. Traces retain eleven/net9 and
twelve/net10 client-cancelled lease-extension RPCs after the RESULT records during
subscriber shutdown; these do not replace the successfully completed nacks.

All 6,484 pre-existing src/tests C# files and the approved executable/project remain
byte-identical. No production change, mutation, SDK upgrade, timeout increase,
client-count reduction, skipped assertion, repeated-until-green run, commit or
publication is included. This standalone control does not rerun or supersede the
Brighter regression matrix or clear its failed restoration gate.

Evidence is in `/private/tmp/brighter-4170-gcp-ordered-nack-hosted-control/`:
source and review snapshots, build log, lock/dependency artifacts, runtime/environment
records, both full native logs and twelve-row results, `final-verification.json`,
read-only post-delete listings and `cleanup.json`.

The next [hosted Brighter adapter](review/gcp-ordered-nack-hosted-brighter/README.md)
is prepared through the test-first review workflow, unbuilt and unrun. It reuses the
existing six sync/async × client-count diagnostic assertions through public Brighter
factories, with hosted targeting shown in a mechanical diff. Its proposed twelve
observations across both frameworks use at most twelve resource pairs and twenty-four
synthetic messages in the same explicitly named project. Earlier SDK-only approval
does not execute this new adapter. Other GCP lifecycle, mapping, generated-pump and
broader release requirements remain open.

## Hosted Brighter ordered-nack comparison (2026-10-30)

The approved [hosted Brighter adapter](review/gcp-ordered-nack-hosted-brighter/README.md)
passes all twelve fixed observations against `brighter-pubsub-validation`. The matrix
runs sync/async operations with SDK-default, one and two subscriber clients once per
framework. Unlike the preceding SDK-only control, these observations use Brighter's
public producer/channel factories and actual send/receive/nack/acknowledge paths.

| Framework | Fixed cases | Passed | Failed | Correlated native nacks | Both application IDs recovered |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 6 | 6 | 0 | 6 | 6 |
| net10.0 | 6 | 6 | 0 | 6 | 6 |

The existing diagnostic assertions execute unchanged: binary body, application/native
identity, native ordering key, initially pending receipt, successful native deadline-zero
request correlated to the first delivery, receipt Nack completion and both recovered
application IDs. The original immediate receive/ack sequence and all observation bounds
remain intact. Independent trace verification finds two raw copies of every first
message with the same identity/body/order and a new acknowledgement ID on redelivery.

Both frameworks build with zero errors and four total CS0618 warnings: two uses of
the existing obsolete MessageHeader.MessageType per framework in the copied diagnostic.
Those checks and traces were retained, not suppressed or rewritten. The linked gateway
and core are built from the current source baseline. Execution uses local Darwin 25.6.0
arm64, eleven processors, .NET runtimes 9.0.20/10.0.11, Pub/Sub 3.38.0 and GAX gRPC
4.13.1. Six client-cancelled native RPC observations per framework remain in the full
traces; all required nacks complete successfully and no setup/cleanup failure occurs.

Both processes exit zero. The twelve unique topic/subscription pairs were deleted,
and subsequent read-only project listings independently confirm all twelve topics and
twelve subscriptions are absent. Exactly twenty-four synthetic messages were published.
ADC was used locally; no credential was copied or printed. No repeat-until-green,
mutation, default-client reduction, timeout increase, dependency upgrade or production
correction is included. All 6,484 existing src/tests C# files and the approved adapter
code/project remain byte-identical.

This supplies bounded hosted evidence for the ordered-nack path, closing that specific
missing comparison. It does not demonstrate absence of intermittent failures, reproduce
every provisioning/pump detail of the generated tests, or clear the retained failing
emulator restoration/regression evidence. No full regression suite was rerun for this
unchanged-source environment comparison. Buffered/unsettled disposal, factory replacement,
other mapping/lifecycle/concurrency coverage and overall production readiness remain open.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-hosted-brighter/` contains approved
snapshots, source hashes, build/artifact/dependency records, runtime/environment records,
both full logs and six-row result files, `final-verification.json`, post-delete listings
and `cleanup.json`. No commit, deployment or publication occurred.

The next [hosted factory shutdown adapter](review/gcp-hosted-factory-stream-shutdown/README.md)
is prepared under the test-first review workflow, unbuilt and unrun. It reuses the
existing eight sync/async × one/two-client × native/replacement recovery assertions,
with the hosted adaptation shown in a mechanical diff. The proposed sixteen cases
across both frameworks create at most sixteen resource pairs and sixteen synthetic
messages in the same explicitly named project. It targets the separate shutdown/
replacement finding; it does not cover buffered-after-requeue disposal. Previous
ordered-nack approval does not execute this new batch.

## Hosted factory stream shutdown comparison (2026-10-30)

The approved [hosted factory adapter](review/gcp-hosted-factory-stream-shutdown/README.md)
passes all sixteen fixed cases against `brighter-pubsub-validation`. Each framework
runs sync/async × one/two subscriber clients × native Pull/replacement-channel recovery.
The public factory/channel path and existing assertions execute unchanged.

| Framework | Fixed cases | Passed | Failed | Observed disposal duration |
| --- | ---: | ---: | ---: | --- |
| net9.0 | 8 | 8 | 0 | 0.135493–0.175947 seconds |
| net10.0 | 8 | 8 | 0 | 0.128783–0.185121 seconds |

Every case disposes within the original ten-second bound, observes a successful
native deadline-zero request during shutdown for the exact subscription and first
SDK client generation, observes no native acknowledgement from that generation,
and satisfies the pending callback completion/cancellation assertions. Native Pull
or a replacement channel recovers the same native/application identity and binary
body. Replacement cases build generation two, obtain a distinct receipt and complete
its Ack reply. Native-Pull cases retain one SDK stream generation. The shutdown nack
assertion observes a nonempty ack-ID set; it does not newly correlate that set to a
raw StreamingPull response. The trace verifier preserves that limit.

Both frameworks build successfully with zero errors and two total CS0618 warnings
from the existing MessageHeader.MessageType trace, one per framework. Execution uses
Darwin 25.6.0 arm64, eleven processors, .NET runtimes 9.0.20/10.0.11, Pub/Sub 3.38.0
and GAX gRPC 4.13.1. All 6,484 existing src/tests C# files and the approved adapter
code/project remain byte-identical. No mutation, implementation change, SDK upgrade,
client-count change, timeout increase or repeat-until-green is included.

Both processes exit zero, including cleanup. Sixteen synthetic messages were published
across sixteen unique topic/subscription pairs. Subsequent read-only project listings
independently confirm all sixteen owned topics and sixteen owned subscriptions are
absent. ADC remained local; no credentials were copied or printed. No assertion,
setup, auth, quota or cleanup failure occurred. No commit, deployment or publication.

This closes the missing bounded hosted factory shutdown/replacement comparison.
It does not establish a universal cause for the historical empty replacement receives,
erase the retained emulator failures, exercise buffered-after-requeue disposal, or
complete broader lifecycle/concurrency/mapping validation. No full regression suite
was rerun for this unchanged-source environment comparison. Production readiness
remains open.

Evidence: `/private/tmp/brighter-4170-gcp-hosted-factory-stream-shutdown/` contains
approved/source snapshots, isolated build/dependency artifacts, environment/runtime
records, complete traces, per-framework eight-row results, disposal timings and
`final-verification.json`, read-only post-delete listings and `cleanup.json`.

The next [buffered-redelivery hosted adapter](review/gcp-hosted-buffered-stream-shutdown/README.md)
is prepared through the test-first review workflow, unbuilt and unrun. It reuses four
existing buffered cases per framework, with actual public buffer readiness/peek
assertions before disposal. Its explicit adaptation replaces the old manually
constructed SDK client/no-op disposal callback with an SDK-owned client and lets the
production consumer own shutdown/cleanup. It composes the public gateway components
with real SDK collaborators; it does not claim to drive the channel factory or pump.
The proposed eight cases create at most eight resource pairs and eight synthetic
messages in the same named project. Earlier factory approval does not execute it.

## Hosted buffered-redelivery shutdown comparison (2026-10-30)

The approved [buffered-shutdown adapter](review/gcp-hosted-buffered-stream-shutdown/README.md)
passes all eight fixed cases against `brighter-pubsub-validation`. Each framework
runs sync/async × native Pull/replacement-consumer recovery with one SDK-owned
subscriber client. Every case establishes unread buffered redelivery through the
public Reader.WaitToReadAsync/TryPeek assertions before disposal; no readiness sleep
or assumption replaces that precondition.

| Framework | Fixed cases | Passed | Failed | Observed disposal duration |
| --- | ---: | ---: | ---: | --- |
| net9.0 | 4 | 4 | 0 | 0.121941–0.237365 seconds |
| net10.0 | 4 | 4 | 0 | 0.121745–0.164711 seconds |

The buffered delivery has the original native ID/body, a distinct pending receipt
and an incomplete callback before shutdown. All cases complete disposal inside the
original ten-second bound, observe a successful deadline-zero native request started
after shutdown begins, prohibit premature native acknowledgement and satisfy callback
cancellation/Nack assertions. Native Pull or a replacement consumer then recovers the
same application/native identity and bytes. Replacement acknowledgement completes.
Trace verification independently confirms BUFFERED precedes DISPOSE, the completed
shutdown nack and the same native ID on recovery. The assertion observes a nonempty
ack-ID set, not a new raw StreamingPull/ack-ID correlation check.

The approved ownership adaptation executes unchanged: SubscriberClientBuilder owns
connections; GcpStreamConsumer owns native stop/disposal. The old no-op disposal
callback/shared SDK implementation is not used. Public gateway components are composed
with real SDK collaborators; this comparison does not claim to drive the factory or
generated pump. The preceding sixteen-case hosted comparison covers the factory path.

Both builds succeed with zero warnings/errors. Both processes exit zero. Eight
synthetic messages were published across eight unique topic/subscription pairs;
read-only project listings independently confirm all eight topics and eight subscriptions
are absent after deletion. There are no assertion, setup, auth, quota or cleanup errors.
ADC remained local and credentials were not copied or printed.

Execution uses Darwin 25.6.0 arm64, eleven processors, .NET runtimes 9.0.20/10.0.11,
Pub/Sub 3.38.0 and GAX gRPC 4.13.1. All 6,484 existing src/tests C# files and the
approved adapter code/project remain byte-identical. No production correction,
mutation, SDK upgrade, timeout/client-count change, retry-until-green, commit,
deployment or publication is included. No full regression matrix is rerun for this
unchanged-source environment comparison.

This closes the missing bounded hosted buffered-redelivery comparison. It does not
establish the cause of every historical emulator failure, erase the retained failed
restoration/regression evidence, or validate general concurrency, mapping or pump
behavior. Overall issue #4170 production readiness remains open.

Evidence: `/private/tmp/brighter-4170-gcp-hosted-buffered-stream-shutdown/` contains
approved/source snapshots, isolated build/dependency artifacts, runtime/environment
records, full traces, four-row per-framework results, disposal timing verification,
`final-verification.json`, read-only post-delete listings and `cleanup.json`.

The next [publication writer-isolation review](review/gcp-publication-writer-isolation/README.md)
is prepared through the test-first workflow, unbuilt/unrun and not adopted into the
compiled suite. Six cases cross sync/async/bulk sends with publication creation order,
using custom A/default/custom B endpoints in one public producer factory. Native body/
attribute isolation, source preservation and writer selection are checked. Two named
production-only mutations, exact restorations, library builds and full available
Pub/Sub regression matrices are proposed. This next batch uses isolated emulators;
no hosted calls are proposed.

## GCP publication writer isolation (2026-10-30)

The approved [six-case isolation batch](review/gcp-publication-writer-isolation/README.md)
is adopted and validated against isolated native Pub/Sub emulators. Sync/async/bulk
sends preserve custom A/default/custom B mapping when the same public factory creates
all three producers in either publication order. Native binary bodies, endpoint/source
attributes, canonical default identity, writer selection/call counts and source
preservation are checked. This establishes sequential endpoint isolation, not concurrent
operations or scheduler/rejection mapping.

All six cases pass initially on both frameworks. The initial build identifies xUnit2031
in the new test; an equivalent Assert.Single(collection, predicate) overload removes
that warning without changing its predicate, assertion order, data or bounds. The
approved original and two-line style diff are retained. Both focused suites pass again.

Two named production-only mutations on .NET 10 produce twelve intended native-body
assertion failures. Ignoring configured writers removes custom A's binary prefix in
all six cases. Reusing the first publication's writer adds A's prefix to the default
endpoint in forward creation order, and applies B's prefix to A in reverse order.
Every mutation is restored byte-for-byte and followed by six passes. Final focused
runs pass six cases on each framework. No implementation correction is needed.

| Framework | Passed | Failed | Skipped | Total | New isolation cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 421 | 1 | 55 | 477 | 6 |
| net10.0 | 422 | 0 | 55 | 477 | 6 |

Both full matrices preserve all 471 prior case identities and add exactly six. The
net9.0 failure is the previously recorded generated async ordered-nack case:
`StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async`,
at its missing-redelivery Assert.Contains on line 158. The prior sync ordered-nack
and async/default-client native diagnostic failures pass in this run; the previous
net10.0 async ordered-nack failure also passes. Those outcome changes do not establish
a fix or erase the retained intermittent failures. No retry-until-green or test
exclusion is used. Full-feature production readiness remains open.

All four GCP library targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero
warnings/errors. The larger test dependency graph builds with repository warnings;
the final build has no warning attributed to either new file. All 6,484 pre-existing
src/tests C# files remain unchanged. Exactly the approved test/helper are added; the
helper is byte-identical to approval and the test differs only by the equivalent
assertion style cleanup. Both production mutation targets are restored exactly.

All three batch-owned emulators are removed and their logs retained. There are no
hosted calls, credential use, dependency upgrades, unrelated container changes,
commits, deployments or publication. This Pub/Sub selection does not execute the
unavailable Firestore/Spanner/hosted suites or constitute whole-project verification.

Evidence: `/private/tmp/brighter-4170-gcp-publication-writer-isolation/` contains approved
snapshots, all source hashes, `assertion-style-only.patch`, initial/style/mutation/
restored/final focused TRX/log/JSON records, both full 477-case matrices and outcome
comparisons, all-target library build evidence, emulator logs, `final-verification.json`
and `cleanup.json`.

The next [concurrent writer batch](review/gcp-concurrent-publication-writer/README.md)
is a concrete unbuilt/unrun review draft. Three cases overlap two mappings through
the same factory-created producer in sync/async/bulk modes using a thread-safe writer
and explicit barrier. One named native-result-cache mutation, exact restoration,
library builds and full available regressions are proposed. This next batch remains
local to isolated emulators and does not include hosted execution.

## GCP concurrent publication writer (2026-10-30)

The approved [three-case concurrent writer batch](review/gcp-concurrent-publication-writer/README.md)
is adopted unchanged. Two operations overlap through the same public-factory-created
producer in sync, async and bulk modes. A two-party reader-independent writer barrier
requires overlapping mappings. Independent native observation checks both distinct
binary bodies before per-message identity, custom attributes and source preservation;
the gateway must still clear a writer-supplied ordering key for unordered publication.
Each bulk operation owns one source message; this does not prove concurrent batch
internals, arbitrary user-writer thread safety, scheduling or all concurrency paths.

All three cases pass initially on both frameworks. The named production-only native
result cache mutation then makes all three .NET 10 cases fail at the intended payload
multiset assertion: expected ["VwAA/wGA", "VwD+AAJ/"] versus actual
["VwD+AAJ/", "VwD+AAJ/"]. Both writer calls and the overlap barrier still complete;
compilation/setup/timeout errors are not counted as mutation evidence. The producer
is restored byte-for-byte, then all three cases pass again on both frameworks.
No implementation correction is needed.

| Framework | Passed | Failed | Skipped | Total | New concurrency cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 425 | 0 | 55 | 480 | 3 |
| net10.0 | 424 | 1 | 55 | 480 | 3 |

Both full matrices retain all 477 preceding case identities and add exactly three,
all passing. Each full matrix runs once against its own fresh, readiness-checked
pinned emulator. No retries, exclusions or altered time bounds are used to turn a
behavioral failure green. Full-run failures are retained:

- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpFactoryStreamShutdownRecoveryTests.When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(useAsync: False, clientCount: 2, nativeRecovery: False)` — Assert.Equal() Failure: Values differ


The sole failure is the previously recorded synchronous/two-client factory replacement
case at line 185: shutdown's deadline-zero ModifyAckDeadline completes successfully,
disposal completes and a second native client generation is built, but replacement
Receive returns MT_NONE after twenty seconds. This is retained as a redelivery finding,
not attributed to the new writer characterization or claimed fixed by earlier hosted
passes. The preceding net9.0 async ordered-nack failure passes in this run; the net10.0
factory case passed in the preceding run. Neither outcome change clears the historical
intermittent lifecycle gates. All other prior case outcomes are unchanged.

All 6,486 pre-existing src/tests C# files remain byte-identical. Exactly the approved
test and helper are added; their compiled and review copies match their approved
snapshots. All four gateway library targets build with zero warnings/errors. Test dependency builds retain
repository warnings; none is attributed to the new files. No production, dependency,
or CI selection change is needed for this characterization.

The first sandboxed emulator launch was blocked, and an early runner invocation
found no built assembly and executed zero tests. These infrastructure attempts are
retained separately as launcher-before-build-net9.0.log/json; they are not behavioral
runs. After the build completed, both initial focused suites ran once and passed.

All three owned emulators are removed and their logs retained; unrelated containers
are untouched. No hosted API, credentials, Firestore/Spanner suite, remote CI,
commit, deployment or publication is involved. The selected Pub/Sub regressions do
not establish whole-project or full-feature production readiness. Historical emulator
lifecycle failures, remaining gateway integrations and other release gates remain open.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-publication-writer/` contains
approved snapshots, all source hashes, native-cache mutant, initial/mutation/restored
TRX/log/JSON records, both full 480-case matrices with prior-identity/outcome comparison,
all-target library build, emulator logs, final-verification.json and cleanup.json.

The next [concurrent pull reader batch](review/gcp-concurrent-pull-reader/README.md)
is prepared as an unbuilt/unrun review draft. Two consumers share a safe reader and
must preserve overlapping mapping, per-delivery receipts/counts and subsequent native
acknowledgement. Four named mutations, exact restoration, affected regressions and
owned cleanup are proposed; no hosted execution is included.

## GCP concurrent pull reader (2026-10-30)

The approved [two-case shared pull-reader batch](review/gcp-concurrent-pull-reader/README.md)
is adopted unchanged. Separate public-factory-created consumers share a safe reader;
sync/async receives overlap through a two-party mapping barrier. Both successful
barrier crossings must be observed before comparing mapped bodies as a multiset.
Each consumer retains its own mapped identity, native content and custom marker,
while the gateway replaces fabricated receipt handles and handled counts with that
delivery's native values. Native inputs remain unchanged and mapped objects stay
separate. Delivery lookup uses source markers rather than cross-topic ID uniqueness.

Each consumer acknowledges its own message. After twelve seconds, beyond the native
ten-second ack deadline, a six-second native observation on each subscription sees
its fresh marker and no original message. This is bounded settlement evidence;
it does not establish unlimited absence of duplicates or streaming/concurrent settlement.

Both baseline cases pass on net9.0 and net10.0 without a production change. Four
named production-only mutations on net10.0 yield eight intended failures:

- Cached mapped body: both cases expect ["UgAA/wGA", "UgD+AAJ/"] but observe
  ["UgAA/wGA", "UgAA/wGA"] at the payload multiset assertion.
- Omitted receipt override: both cases observe reader-invalid-receipt instead of the
  corresponding native acknowledgement handle.
- Omitted handled-count override: both cases expect native count 3 but observe the
  configured reader's 777.
- Omitted native acknowledgement: the observer sees the original delivery alongside
  the fresh marker, failing Assert.DoesNotContain in both cases.

Every mutation is restored byte-for-byte and followed by two net10.0 passes. The final
net9.0 run also passes both cases. The last net10.0 restoration is its final focused
run. No compile/setup failure is accepted as mutation evidence, no assertions/bounds
are altered, and no behavioral retry-until-green is used.

| Framework | Passed | Failed | Skipped | Total | New reader cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 426 | 1 | 55 | 482 | 2 |
| net10.0 | 426 | 1 | 55 | 482 | 2 |

Both full available Pub/Sub matrices run once on separate fresh, readiness-checked
pinned emulators. All 480 preceding case identities remain, plus exactly two new
passing cases. Full-run failures are retained:

- net9.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Assert.Contains() Failure: Item not found in set
- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Assert.Contains() Failure: Item not found in set

Changes from the immediately preceding full matrices:

- net9.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Passed → Failed.
- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Passed → Failed.
- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpFactoryStreamShutdownRecoveryTests.When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(useAsync: False, clientCount: 2, nativeRecovery: False)` — Failed → Passed.


Both current failures are the generated synchronous ordered-nack case at Reactor
line 152: the expected nacked-message ID is missing from the final redelivery set.
This known emulator finding predates this pull-reader batch. The preceding net10.0
synchronous/two-client factory replacement failure passes here. All other prior
case outcomes remain unchanged. No streaming source changed in this increment.

A passing historical failure does not establish its cause or a fix. The emulator
lifecycle gates, broader mapping/scheduler paths and remaining gateway integrations
remain open. These results do not establish full-feature production readiness.

All 6,488 pre-existing src/tests C# files remain byte-identical. Exactly the approved
test/helper are added, and their compiled/review copies match the approved snapshots.
All four library targets build with zero warnings/errors. The test dependency graph
retains repository warnings, including CS0618 for the new test's deliberate legacy
MessageHeader.MessageType compatibility assertion. The approved assertion is retained;
no test change is needed and the helper introduces no warning. Existing CI categories
already select these cases; there is no workflow or dependency change.

All three owned emulators are removed and their logs retained; unrelated containers
are untouched. There are no hosted calls, credential use, commits, deployment or
publication. The available Pub/Sub selection excludes unavailable Firestore/Spanner
and hosted suites, and is not whole-project or remote-CI verification.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-pull-reader/` contains approved
snapshots, 6,488 source hashes, four saved production mutants, initial/mutation/restored/
final TRX/log/JSON records, both full 482-case matrices with case/outcome comparisons,
all-target library build, emulator logs, final-verification.json and cleanup.json.

The next [concurrent stream-reader batch](review/gcp-concurrent-stream-reader/README.md)
is a concrete review-only draft, unbuilt/unrun and outside the compiled suite.
Two factory-created streaming consumers share a safe reader; mapping, receipt ownership,
callback completion and subscription-specific native acknowledgement must remain
independent. Five named mutations, exact restorations, library builds and affected
regressions are proposed, using isolated emulators only.

## GCP concurrent stream reader (2026-10-30)

The approved [shared stream-reader batch](review/gcp-concurrent-stream-reader/README.md)
is adopted unchanged. Two sync/async cases establish overlapping mapping across
separate public-factory consumers sharing a safe reader, with one SDK client per
subscription. Exact mapped body/identity/marker isolation, unchanged native inputs,
native wrapper/count ownership and independent callback completion are verified.
Each consumer's acknowledgement also completes a native Acknowledge RPC for its
own subscription with one nonempty acknowledgement ID. This is bounded native
settlement evidence; it does not decode streaming delivery ack IDs independently
or establish unlimited absence of redelivery or concurrent settlement.

Both initial cases pass on .NET 9 and .NET 10 without production corrections.
Five independent production mutations on .NET 10 produce ten intended failures:
cached bodies fail payload multiset equality; omitted receipt override fails exact
wrapper identity; omitted count override exposes 777 instead of native 3; omitted
Accept leaves the callback incomplete; replacing its completed Ack with native Nack
leaves the native acknowledgement incomplete after the original callback succeeds.
All mutations fail at their reviewed assertion, not setup or compilation. Both
production targets are restored byte-for-byte after each mutation, followed by two
passes. Final focused runs pass both cases on each framework.

| Framework | Passed | Failed | Skipped | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 429 | 0 | 55 | 484 | 2 |
| net10.0 | 428 | 1 | 55 | 484 | 2 |

Each full available Pub/Sub matrix runs once on a fresh readiness-checked pinned
emulator. All 482 preceding case identities remain, plus exactly two new passing
cases. The .NET 10 failure is the known generated synchronous ordered-nack case:
StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.
When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second,
at the final Assert.Contains redelivery assertion. The same case changes from
failed to passed on .NET 9; all other preceding outcomes are unchanged. Passing in
one run establishes neither a root cause nor a fix. Earlier lifecycle findings and
broader release gates remain open. There is no retry-until-green or bound change.

All 6,490 pre-existing src/tests C# files are byte-identical. Exactly the approved
test/helper are added and match their review and approved snapshots. All four GCP
library targets build with zero warnings/errors. The test dependency graph retains
3,320 repository warnings; neither new file introduces a warning. Existing CI
categories select the new cases; no workflow/dependency change is needed.

All three owned emulators are removed, logs retained and unrelated containers left
untouched. No hosted calls, credential use, commits, deployment or publication occur.
The selection excludes unavailable Firestore/Spanner and hosted coverage and is not
whole-project or remote-CI verification. Whole-feature production readiness remains
open, including the five remaining gateway integrations.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-stream-reader/` contains source
hashes, approved snapshots, five saved mutants, initial/mutation/restored/final
TRX/log/JSON results, both full matrices and outcome comparisons, the all-target
library build, final-verification.json, emulator logs and cleanup.json.

The next [MQTT publication writer batch](review/mqtt-publication-writer/README.md)
is a concrete review-only draft, unbuilt/unrun and outside the compiled suite.
Six sync/async cases cover legacy, custom and composed default payloads with native
broker observation, gateway-owned routing/QoS and source preservation. Four named
mutations propose eighteen assertion failures. Implementation, native regression
baselines and execution await the repository's test-review approval.

## MQTT publication writer (2026-10-30)

The approved [publication writer batch](review/mqtt-publication-writer/README.md)
is implemented. MqttPublication selects an optional native writer, and the reusable
DefaultMqttWireMessageWriter preserves full-Message JSON serialization. Both producer
paths forward the publication selection through internal publisher overloads. The
publisher applies its configured topic prefix and source header topic, then QoS
AtLeastOnce, after mapping. Existing public constructors/methods and base Publication
callers remain compatible. No internal member is exposed for testing.

Both unchanged full baselines pass 79 cases with four existing skips. A stale restored
asset graph initially omitted MQTTnet; a locked restore fixes compilation without
source, package-version or lockfile changes. The first immediate broker readiness
probe precedes startup and is refused; bounded readiness polling succeeds before
any tests run. Neither setup problem is counted as behavioral RED.

The six approved cases and helper are adopted unchanged. With only the new public
contracts/default adapter present, both frameworks pass the two legacy cases and
fail all four custom/composed cases at exact native payload equality. Wiring writer
selection makes all six pass. The independent native MQTT client observes exact
bytes, gateway topic and QoS; source data stays unchanged and each selected writer
runs once. The observer also subscribes to incorrect candidate topics, so routing
mutations fail topic equality rather than timing out.

Four independent .NET 10 production-only mutations yield eighteen intended failures:
ignoring the writer fails four payload assertions; omitting the gateway topic
fails six topic assertions; omitting QoS fails four QoS assertions; replacing default
JSON with raw source bytes fails four legacy/composed payload assertions. Unaffected
cases pass. Every mutation is restored byte-for-byte and followed by six passes.
Final focused cases pass on both frameworks; no assertions or time bounds change.

| Framework | Baseline passed/skipped | Final passed | Failed | Skipped | Total |
| --- | --- | ---: | ---: | ---: | ---: |
| net9.0 | 79 / 4 | 85 | 0 | 4 | 89 |
| net10.0 | 79 / 4 | 85 | 0 | 4 | 89 |

Both complete test assemblies run once on separate fresh brokers, including existing
embedded MQTTnet server tests. All 83 preceding identities and outcomes remain,
plus six new passes. Four generated nack-conformance cases remain skipped under
issue #4240. All three library targets (net8.0/net9.0/net10.0) build with zero warnings
and errors. The test build retains 738 existing warnings; no new or changed file
introduces a warning. Existing MQTT CI selection already covers the new tests.
These are local native-broker results, not a remote CI run.

Of 6,492 preceding src/tests C# files, 6,490 remain byte-identical; only the MQTT
producer/publisher change. Exactly four files are added: the publication, default
writer, approved test and helper. All implemented source matches the saved restored
snapshots. No dependency, workflow or lockfile changes are made in this increment.

Mosquitto matches the repository image, pinned for these runs to
`efrecon/mosquitto@sha256:9f77138ef91011b49f1a10cabb0a919a5f1491b2c5f6ea91ddb29690237941e4`.
The image is linux/amd64 on an arm64 Docker host; SDK runners are native arm64.
No host port is bound. All five owned brokers are removed and logs retained;
unrelated containers are untouched. No hosted calls, credentials, commits,
deployment or publication occur.

Evidence: `/private/tmp/brighter-4170-mqtt-publication-writer/` contains approved
snapshots, source hashes, baseline/RED/green/mutation/restored/final TRX/log/JSON,
saved production mutants, complete regression comparisons, library build,
final-verification.json, broker logs and cleanup.json.

MQTT reader integration, writer failure/cancellation/recovery, retention,
concurrency and remaining scheduler/rejection paths remain open. Redis, RocketMQ,
SQL Server and PostgreSQL integrations and broader release gates also remain open;
this increment does not establish whole-feature production readiness.

The next [MQTT subscription reader batch](review/mqtt-subscription-reader/README.md)
contains six concrete review-only cases and a composing reader. They remain
unbuilt/unrun outside the compiled project pending test-review approval.

## MQTT subscription reader (2026-10-30)

The approved [subscription reader batch](review/mqtt-subscription-reader/README.md)
is implemented. MqttSubscription selects an optional native MQTT application-message
reader. Both public consumer factories capture it through an internal constructor;
all existing public constructor signatures remain compatible. Conversion occurs
synchronously in the SDK callback before the existing non-null buffer guard.
The default path still invokes MqttMessageCreator, preserving its JSON rules and
log-and-drop handling. The reusable DefaultMqttWireMessageReader composes that same
parser and throws FormatException if it cannot return a decoded message.

The reviewed fixture omitted two required base-subscription arguments. The first
.NET 9 run stops all six cases at the missing pump type; the next stops them at the
missing request type. The fixture now explicitly selects Reactor/Proactor from the
case and supplies the existing MyCommand as request metadata. No new request,
handler or logging registration is needed. Original snapshots and both setup-failure
runs remain recorded. Only these two fixture arguments change; the helper, assertions
and time limits are unchanged. Neither setup failure is behavioral RED.

With the corrected fixture and only the public contract/default adapter present,
each framework passes two legacy cases and fails four custom/composed cases at exact
body equality. Factory selection and callback mapping make all six pass. Native
observations verify the configured body, application identity/topic, marker bag,
one reader call and unchanged payload during mapping. The SDK continues to own
protocol acknowledgement; no settlement or redelivery guarantee is inferred.

Four .NET 10 production-only mutations yield eighteen intended assertion failures:
ignoring the reader fails four custom/composed body assertions; empty default-decoded
bodies fail four legacy/composed body assertions; overriding application topics with
native broker topics fails all six topic assertions; calling the configured reader
twice fails four invocation-count assertions. Unaffected cases pass. Every mutant
is restored byte-for-byte and followed by six passes. Final focused cases pass on
both frameworks. No failure is accepted from compilation or setup, and no behavioral
retry-until-green occurs.

| Framework | Passed | Failed | Skipped | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 91 | 0 | 4 | 95 | 6 |
| net10.0 | 91 | 0 | 4 | 95 | 6 |

Both complete assemblies run once on fresh, readiness-checked isolated brokers.
All 89 preceding identities and outcomes remain, including writer selection,
existing malformed-parser checks and four nack-conformance skips under #4240.
The existing malformed-input tests exercise the public parser; they do not establish
custom-reader failure recovery or native recovery after malformed deliveries.
All three MQTT library targets build without warnings/errors. The green test build
retains 738 existing warnings; no new or changed file introduces a warning.
Existing MQTT CI selection covers the cases; remote CI is not run here.

Of 6,496 preceding src/tests C# files, 6,493 remain byte-identical. Only the MQTT
subscription, consumer and factory change. Exactly three files are added: the
default reader, test and helper. MqttMessageCreator is restored exactly after its
mutation. The preceding writer implementation and tests remain unchanged. The
compiled/review test matches the documented corrected snapshot; the helper matches
its original approval snapshot. No dependency, lockfile or workflow change occurs.

The repository Mosquitto image is pinned to
`efrecon/mosquitto@sha256:9f77138ef91011b49f1a10cabb0a919a5f1491b2c5f6ea91ddb29690237941e4`.
It runs as linux/amd64 on an arm64 host, with native arm64 SDK runners and no host
port bindings. All three owned brokers are removed and logs retained; unrelated
containers are untouched. No hosted calls, credentials, commits, deployment or
publication occur.

Evidence: `/private/tmp/brighter-4170-mqtt-subscription-reader/` contains approval
and corrected-fixture snapshots, source hashes, setup/RED/green/mutation/restored/
final TRX/log/JSON records, saved mutants, both full matrices with outcome checks,
library build, final-verification.json, broker logs and cleanup.json.

This closes successful reader selection, not MQTT or whole-feature production
readiness. The configured-reader exception path currently delegates to the SDK;
throw/null recovery still needs native evidence. Writer failure/cancellation,
retention, concurrency, scheduler/rejection paths, the four remaining gateway
integrations and broader release gates remain open.

The next [MQTT reader failure-recovery batch](review/mqtt-reader-failure-recovery/README.md)
contains four review-only cases. They require the failed mapping to be dropped
without default fallback or a placeholder, followed by the next valid delivery
through the same consumer. Four named mutations propose fourteen intended failures.
The draft is unbuilt/unrun outside the compiled suite pending test-review approval.

## MQTT reader failure recovery (2026-10-30)

The approved [reader failure-recovery batch](review/mqtt-reader-failure-recovery/README.md)
is adopted unchanged. All four sync/async × throw/null cases pass on both frameworks
before production changes. The configured reader observes a native failed delivery,
then the same consumer receives the next valid delivery with exact ID, binary body,
application topic and marker. The reader runs twice and the subsequent 250 ms
observation returns the no-message sentinel. Failed inputs are valid default JSON,
so falling back to the default parser would expose a wrong message identity.

No production correction is needed for this bounded recovery behavior. The existing
buffer guard drops null results; the observed SDK dispatch continues after the
configured InvalidOperationException. The consumer still delegates exception
handling to the SDK. This evidence does not establish a new Brighter catch boundary,
log capture, manual settlement, dead-letter forwarding or unlimited absence of
protocol duplicates. Other exception types, explicit default-reader failure
composition, non-JSON input and reconnect paths remain separate.

Four independent .NET 10 production-only mutations produce fourteen intended failures:

- Latching a reader exception and dropping later mappings fails the good-ID assertion
  in both throwing cases; null cases pass.
- Substituting empty messages for throw/null failures fails all four good-ID assertions.
- Falling back to default JSON decoding after throw/null failures fails all four
  good-ID assertions by delivering the failed message instead of the good one.
- Emptying mapped bodies fails all four exact good-body assertions.

Each mutant is restored byte-for-byte and followed by four passes. The final .NET 9
run also passes four cases; the last restored .NET 10 run is its final focused run.
No test/helper changes, time-bound changes or behavioral retry-until-green occur.

| Framework | Passed | Failed | Skipped | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 95 | 0 | 4 | 99 | 4 |
| net10.0 | 95 | 0 | 4 | 99 | 4 |

Both complete test assemblies run once on separate fresh, readiness-checked brokers.
All 95 preceding identities and outcomes remain, plus four new passes. The four
nack-conformance skips under #4240 are unchanged. All three MQTT library targets
build with zero warnings/errors. The test dependency graph retains 738 existing
warnings; neither new file introduces a warning. Existing MQTT CI selection covers
these tests; remote CI is not run here.

All 6,499 pre-existing src/tests C# files remain byte-identical. Exactly the approved
test/helper are added, matching their review and approval snapshots. The production
mutation target is restored exactly; the reader and writer implementations remain
unchanged. No dependency, lockfile or workflow change occurs.

Native runs use the repository Mosquitto image pinned to
`efrecon/mosquitto@sha256:9f77138ef91011b49f1a10cabb0a919a5f1491b2c5f6ea91ddb29690237941e4`.
The broker is linux/amd64 on an arm64 host; SDK runners are native arm64. No host
port is bound. All three owned brokers are removed and logs retained; unrelated
containers are untouched. No hosted calls, credentials, commits, deployment or
publication occur.

Evidence: `/private/tmp/brighter-4170-mqtt-reader-failure-recovery/` contains approved
snapshots, source hashes, four saved production mutants, initial/mutation/restored/
final TRX/log/JSON records, both full matrices and prior-outcome comparisons, library
build, final-verification.json, broker logs and cleanup.json.

This closes the scoped reader recovery characterization. MQTT writer failures,
cancellation, retention, concurrency, scheduler/rejection and wider lifecycle paths,
the four missing gateway integrations and broader release gates remain open.
Whole-feature production readiness is not established.

The next [MQTT writer failure-recovery batch](review/mqtt-writer-failure-recovery/README.md)
contains four review-only cases for original writer errors, explicit null-result
errors and same-producer recovery with native publication observation. Four mutations
propose fourteen intended failures. The draft is unbuilt/unrun outside the compiled
suite pending test-review approval.

## MQTT writer failure recovery (2026-10-30)

The approved [writer failure-recovery batch](review/mqtt-writer-failure-recovery/README.md)
is adopted unchanged. Initial runs on both frameworks pass the two throwing cases
and fail both null-result cases at the exact exception-type assertion: the actual
error is NullReferenceException. MQTTMessagePublisher now rejects a null mapped
result with InvalidOperationException before native publication. Writer-thrown
errors continue to propagate as their original instance. MqttPublication documents
both outcomes; no existing public signature changes.

All four focused cases pass on both frameworks. An independent native observer
subscribes before the failed send, then observes the next successful message's exact
default JSON bytes through the same producer. The additional 250 ms window records
no further delivery. The selected writer runs twice and the failed source ID,
binary body, topic and marker bag remain unchanged. This is bounded evidence for
these throw/null cases, not an unlimited duplicate-absence or outbox guarantee.

Four independent .NET 10 production-only mutations produce fourteen intended failures:

- Ignoring the configured writer fails all four non-null-error assertions.
- Suppressing mapping/null-result errors fails all four non-null-error assertions.
- Publishing default content before mapping fails all four first-native-payload
  assertions; the required mapping errors still reach the caller.
- Removing the null-result guard fails both null cases at the exception-type
  assertion with NullReferenceException; both throwing cases pass.

Exact byte restoration after each mutation returns four passes. Final focused
results are four passes on each framework. The approved test/helper and time bounds
are unchanged; no behavioral retry-until-green occurs.

| Framework | Passed | Failed | Skipped | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 99 | 0 | 4 | 103 | 4 |
| net10.0 | 99 | 0 | 4 | 103 | 4 |

Both complete assemblies run once on separate fresh, readiness-checked brokers.
All 99 preceding test identities and outcomes remain, plus four new passes. The
four nack-conformance skips under #4240 remain unchanged. All three MQTT library
targets build with zero warnings/errors. The initial test dependency build retains
738 existing warnings; no new/changed file has warnings, and the incremental green
build has zero warnings/errors. Existing MQTT CI selection covers these cases;
remote CI is not run here.

Of 6,501 preceding src/tests C# files, 6,499 remain byte-identical. Only the publisher
null guard and publication XML documentation change. Exactly the approved test/helper
are added, matching review/approval snapshots. Both production files match their
implemented snapshots after mutations. No dependency, lockfile or workflow changes
are made in this batch.

Native runs use the repository Mosquitto image pinned to
`efrecon/mosquitto@sha256:9f77138ef91011b49f1a10cabb0a919a5f1491b2c5f6ea91ddb29690237941e4`.
The broker is linux/amd64 on an arm64 host; SDK runners are native arm64. No host
port is bound. All three owned brokers are removed with logs retained; unrelated
containers are untouched. No hosted calls, credentials, commits, deployment or
publication occur.

Evidence: `/private/tmp/brighter-4170-mqtt-writer-failure-recovery/` contains approved
snapshots, source hashes, initial/green/mutation/restored/final TRX/log/JSON records,
saved mutants, complete matrices and preceding-outcome comparisons, library build,
`final-verification.json`, broker logs and `cleanup.json`.

This closes the scoped writer recovery correction. MQTT cancellation, retention,
concurrency, scheduler/rejection and wider lifecycle paths remain open. Four gateway
integrations and the broader release gates also remain open; the whole feature is
not production-ready.

The next [Redis publication writer batch](review/redis-publication-writer/README.md)
contains six review-only sync/async × legacy/custom/composed cases. Native storage
observations cover exact framing, source-topic routing, counters, two-queue fan-out,
expiry and source preservation. Five named mutations propose 26 intended assertion
failures. The draft is unbuilt/unrun outside the compiled suite pending test-review
approval. Redis production code and compiled tests are unchanged in this batch.

## Redis publication writer selection (2026-10-30)

The approved [Redis writer batch](review/redis-publication-writer/README.md) is adopted
unchanged. RedisMessagePublication now optionally selects an
IAmAWireMessageWriter<string>. DefaultRedisWireMessageWriter composes a fresh legacy
RedisMessagePublisher per conversion, avoiding accumulated serializer state. Both
producer send paths choose the configured/default writer before counter advancement,
storage and queue fan-out. Existing producer signatures and native delivery-control
code remain unchanged.

An initial baseline build exposed missing local ServiceStack assemblies. Locked
restore resolved that setup failure without dependency or lockfile changes. Both
complete unchanged baseline suites then pass 96 cases with four existing skips.
After adoption, the absent adapter causes the expected compilation failure. Adding
only the property/default adapter, without producer selection, produces four intended
payload failures and two legacy passes on each framework. Actual native strings
remain legacy envelopes where the custom/composed strings are required.

Minimal producer wiring makes all six cases pass on each framework. Each case sends
two distinct messages through one producer. Native Redis observation verifies exact
stored strings, source-header topic routing distinct from the publication topic,
counter advancement to two, ordered fan-out to two queues, an unused decoy route,
positive one-hour expiry, two configured invocations and unchanged source IDs,
Unicode text bodies, topics and marker bags. The default compatibility oracle is
an independently created instance of the existing public serializer, not a separate
specification of its format. Arbitrary binary encoding is not established.

Five independent .NET 10 production-only mutations produce 26 intended failures:

| Mutation | Intended failed assertion | Failed | Passed |
| --- | --- | ---: | ---: |
| Ignore the configured writer | First stored payload, custom/composed | 4 | 2 |
| Default adapter emits raw body | First stored payload, legacy/composed | 4 | 2 |
| Keep publication topic instead of source topic | Source counter equals two | 6 | 0 |
| Omit queue fan-out | First recipient queue contains IDs 1 and 2 | 6 | 0 |
| Omit stored-message expiry | First TTL is within 3000–3600 seconds | 6 | 0 |

Every production file is restored byte-for-byte after its mutation, followed by six
passes. Final focused results are six passes on each framework. The test/helper,
assertions and time bounds are unchanged. Async cleanup stack locations point beyond
the assertion even in an optimization-disabled diagnostic run; assertion kinds and
expected/actual value signatures establish the intended failures instead. Both
initial run sets are retained. The evidence parser initially expected untyped xUnit
collection output and a negative no-expiry sentinel; it was corrected to accept the
actual typed display and ServiceStack's maximum TTL value. These script corrections
change neither test assertions nor production behavior; saved mutation failures are
validated without rerunning those mutants.

| Framework | Baseline passed | Final passed | Failed | Existing skips | Final total |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 96 | 102 | 0 | 4 | 106 |
| net10.0 | 96 | 102 | 0 | 4 | 106 |

Both complete final suites run once on separate fresh, readiness-checked brokers,
without a category/Fragile filter. All 100 baseline identities and outcomes remain,
plus six new passes. The four existing nack-conformance skips remain unchanged.
All four library targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero warnings
and errors. The baseline/green test dependency graphs each report 604 existing
warnings; no new or changed source file has a warning. Existing redis-ci selection
includes these tests; no remote CI run is claimed.

Of 6,503 preceding src/tests C# files, 6,501 remain byte-identical. Only
RedisMessageProducer and RedisMessagePublication change. Exactly the default adapter,
approved test and approved helper are added. The shared RedisMessageGateway mutation
target is restored exactly; consumers, other transports, dependencies, lockfiles and
workflows remain unchanged in this batch.

Native validation uses the repository Redis image resolved to
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`;
its reported server version is 8.10.2. Broker and SDK runners use linux/arm64. No host
ports are bound. Unique test keys are cleaned individually. All five owned brokers
are removed with logs retained, and unrelated containers remain untouched. No hosted
calls, credentials, commits, deployment or publication occur.

Evidence: `/private/tmp/brighter-4170-redis-publication-writer/` contains approval and
source snapshots, restore/build logs, baseline and final TRX/log/JSON matrices,
initial/diagnostic/green/mutation/restored/final focused records, saved mutants,
image/version records, `final-verification.json`, broker logs and `cleanup.json`.

This closes successful Redis publication writer selection. Redis reader integration,
writer failures/nulls, consumer retry/rejection mapping, cancellation, scheduling,
concurrency and wider lifecycle gates remain open. RocketMQ, SQL Server and PostgreSQL
integrations and broader cross-transport release work remain open. Whole-feature
production readiness is not established.

The next [Redis subscription reader batch](review/redis-subscription-reader/README.md)
contains six review-only cases covering both public factories, legacy/custom/composed
mapping, native queue ownership, the in-flight acknowledgement gate and stored-input
preservation. Four mutations propose twenty intended failures. The draft remains
unbuilt/unrun outside the compiled suite pending test-review approval.

## Redis subscription reader selection (2026-10-30)

The approved [Redis reader batch](review/redis-subscription-reader/README.md) is
implemented. RedisSubscription.WireMessageReader selects an
IAmAWireMessageReader<string>; DefaultRedisWireMessageReader composes the legacy
RedisMessageCreator. Both public consumer factories capture the configured reader
through a new internal constructor. Existing public consumer signatures remain
unchanged. Both receive paths use the selected reader while retaining native queue
removal, native ID tracking and acknowledgement logic.

The initial build confirms the missing default adapter. Once the contract/default
adapter is present, the build exposes a fixture name collision between Brighter's
and ServiceStack's RedisSubscription. Qualifying the Brighter constructor resolves
it. The next run stops at the existing subscription configuration guard; the fixture
also needs requestType and the corresponding Reactor/Proactor messagePumpType.
Supplying these required arguments resolves setup. The original build/run failures
and each corrected snapshot are retained. No setup failure is counted as behavioral
RED. These constructor-reference/argument fixes are the only test changes: all six
cases, assertions and bounds are unchanged, and the helper is byte-identical to its
approval snapshot.

With the corrected fixture and selection unwired, both frameworks pass two legacy
cases and fail four custom/composed cases at the first mapped-ID assertion. The
legacy parser returns an empty identity for the custom formats. Minimum factory
and consumer wiring then makes all six cases pass on both frameworks.

A separate native client seeds two stored strings and queue IDs 1 and 2, independent
of Brighter publication. Both factory modes return the exact application IDs,
Unicode text bodies, application topic and markers for legacy/custom/composed
input. The application topic differs from the native topic, and mapped IDs differ
from native queue IDs. After the first receive, only ID 2 remains queued. Receiving
before acknowledgement raises ChannelFailureException without removing that ID;
acknowledging the mapped first message allows the second message. Acknowledging the
second permits a subsequent empty receive. Both stored strings remain unchanged,
and configured readers see exactly the original inputs in order.

Four independent .NET 10 production-only mutations produce twenty intended failures:

| Mutation | Intended failed assertion | Failed | Passed |
| --- | --- | ---: | ---: |
| Ignore configured readers | First mapped ID, custom/composed | 4 | 2 |
| Default reader replaces the body | First mapped body, legacy/composed | 4 | 2 |
| Replace application topic with native topic | First mapped topic | 6 | 0 |
| Omit in-flight tracking | ChannelFailureException before acknowledgement | 6 | 0 |

Failure types and source locations match the reviewed assertions. Each production
mutation is restored byte-for-byte and followed by six passes. Final focused
results are six passes on each framework. No assertions or time bounds are relaxed,
and no behavioral regression is retried until green.

| Framework | Passed | Failed | Existing skips | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 108 | 0 | 4 | 112 | 6 |
| net10.0 | 108 | 0 | 4 | 112 | 6 |

Both full suites run once on separate fresh, readiness-checked brokers without a
category/Fragile filter. All 106 preceding identities and outcomes remain, plus six
new passes. Four existing nack-conformance skips remain unchanged. All four library
targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero warnings/errors. The
test dependency build retains 604 existing warnings, with none in new/changed files.
Existing Redis CI selection includes these cases; no remote CI result is claimed.

Of 6,506 preceding src/tests C# files, 6,503 remain byte-identical. Only the
subscription, consumer and consumer factory change. Exactly the default reader,
test and helper are added. The corrected compiled/review fixture matches the
recorded mechanical transformation of the approval snapshot. All implemented
production files match their restored snapshots; the public consumer constructor
signature is verified unchanged. Dependency, lockfile and workflow hashes retain
their pre-batch values, including the pre-existing CI edit. The publication writer
implementation and tests are unchanged.

Native validation uses
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
reporting Redis 8.10.2. Broker and SDK runners use linux/arm64. No host ports are bound.
Unique keys are cleaned individually. All three owned brokers are removed with logs
retained; unrelated containers remain untouched. No hosted calls, credentials,
commits, deployment or publication occur.

Evidence: `/private/tmp/brighter-4170-redis-subscription-reader/` contains approval,
qualified and corrected fixture snapshots, setup/RED/green/mutation/restored/final
TRX/log/JSON records, source/protected-file hashes, saved mutants, full matrices and
preceding-outcome comparisons, library build, `fixture-correction.json`,
`final-verification.json`, broker logs and `cleanup.json`.

This closes successful Redis subscription reader selection and preserves the tested
in-flight acknowledgement behavior. It does not add broker settlement or crash
redelivery. Redis reader/writer failures, malformed/default parsing, retry/rejection
mapping, cancellation, connection recovery, concurrency and scheduling remain open.
RocketMQ, SQL Server and PostgreSQL integrations and broader release work also remain
open. Whole-feature production readiness is not established.

The next [Redis writer failure-recovery batch](review/redis-writer-failure-recovery/README.md)
contains four review-only throw/null cases. They require original writer errors,
explicit invalid-null errors before native mutation and same-producer recovery,
observed through counters, stored payloads and queue entries. Four named mutations
propose fourteen intended failures. The draft remains unbuilt/unrun outside the
compiled suite pending test-review approval.

## Redis writer failure recovery (2026-10-30)

The approved [four-case batch](review/redis-writer-failure-recovery/README.md)
is complete. On each framework, the initial throwing cases passed and both null
cases failed at `Assert.NotNull(error)`: null writer results silently succeeded.
The initial failures precede native-state assertions; they do not establish the
initial counter/storage/queue values. Both send paths now throw
InvalidOperationException on null before native mutation, retaining original writer
exceptions. Publication XML documentation records the contract.

All four unchanged approved cases pass on .NET 9 and .NET 10. Independent native
observations establish absent counter/payload and an empty queue after failure,
then exact legacy content at ID 1, counter 1 and one queue entry after recovery
through the same producer. Source identity, Unicode body, topic and marker remain
unchanged. Four independent .NET 10 mutations produced fourteen intended failures:
ignoring the writer (4), suppressing mapping errors (4), publishing before mapping
(4 at the absent-counter assertion), and accepting null as an empty string (2).
The premature-publication mutation preserved the required mapping error. Each
byte-exact production restoration returned four passes; final focused results pass
on both frameworks. No fixture, assertion or bound changes were needed.

Both unfiltered complete suites ran once on separate fresh ready brokers:
**112 passed, zero failed, four existing skips** each. All 112 preceding identities
and outcomes are retained, with four new passes. The four existing nack-conformance
skips are unchanged. Redis library builds pass on netstandard2.0, net8.0, net9.0
and net10.0 with zero warnings/errors. The initial dependency build reports 604
existing warnings, none in new/changed files; the incremental green build is clean.
Existing CI selection includes the cases; hosted CI was not run.

Source verification retains 6,507 of 6,509 preceding C# files byte-for-byte, changing
only RedisMessageProducer and RedisMessagePublication. The two approved test/helper
additions match their review snapshots exactly. All dependency, lockfile and workflow
hashes match the pre-batch state, including pre-existing edits. Public signatures
remain unchanged.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. TRX, logs, mutation
snapshots, source/protected-file hashes, final verification and cleanup records are
retained in `/private/tmp/brighter-4170-redis-writer-failure-recovery/`.

Redis reader error handling, consumer retry/rejection mapping, native failures after
partial publication, cancellation, concurrency and wider lifecycle coverage remain
open, alongside the other release workstreams. This does not establish whole-feature
production readiness.

The next [Redis reader failure-recovery draft](review/redis-reader-failure-recovery/README.md)
contains four sync/async × throw/null cases requiring a rejectable message retaining
native content/identity, observed invalid-channel publication and same-consumer
recovery. Four reviewed mutations propose sixteen intended failures. It remains
outside the compiled suite, unbuilt/unrun pending test-review approval.

## Redis reader failure recovery (2026-10-30)

The approved [four-case batch](review/redis-reader-failure-recovery/README.md)
is complete. Both frameworks reproduced four intended failures at the captured
receive-error assertion: configured InvalidOperationException escaped Receive and
null reader results caused NullReferenceException. These failures establish the
missing configured-reader boundary; they are not fixture or compilation failures.

Both receive paths now convert configured-reader throw/null into rejectable failure
messages retaining the exact raw stored string, native queue ID and source topic.
Unconfigured default parsing and native connection/command calls remain outside
the mapping catch. Existing public constructors remain unchanged; subscription XML
documentation describes failure conversion and destructive queue removal.

All four unchanged approved cases pass on .NET 9 and .NET 10. Independent native
observations establish that the second source ID remains queued, explicit rejection
publishes one invalid-channel message with original content/identity and rejection
metadata, and the same consumer subsequently reads and acknowledges the valid
delivery. The configured reader sees exactly the original two inputs. This is
single-line Unicode rejection evidence, not arbitrary multiline/binary round-trip
coverage or invalid-channel failure recovery.

Four independent .NET 10 mutations produced sixteen intended assertion failures:
default-parser fallback (4 at failure type), empty native ID (4), empty raw body (4),
and skipped invalid-channel send while reporting successful rejection (4 at the
native counter). Each byte-exact production restoration returned four passes.
Final focused results pass on both frameworks; no approved cases, assertions, helper
or time bounds changed.

Both unfiltered complete suites ran once on separate fresh ready brokers:
**116 passed, zero failed, four existing skips** each. All 116 preceding identities
and outcomes are retained with four new passes. The existing nack-conformance skips
are unchanged. The Redis library builds on netstandard2.0, net8.0, net9.0 and
net10.0 with zero warnings/errors. Initial and green test dependency builds each
report 604 existing warnings, none in new/changed files. Existing CI selection
includes the cases; hosted CI was not run.

Of 6,511 preceding C# files, 6,509 remain byte-identical; only RedisMessageConsumer
and RedisSubscription changed. The two compiled test/helper additions match their
approved snapshots exactly. Dependency, lockfile and workflow hashes match the
pre-batch state, including pre-existing edits.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after saving logs, leaving unrelated containers untouched. TRX, logs, mutation
snapshots, source/protected-file hashes, verification and cleanup evidence are
retained in `/private/tmp/brighter-4170-redis-reader-failure-recovery/`.

The next [reader error-boundary draft](review/redis-reader-error-boundaries/README.md)
contains eight cases covering unconfigured malformed JSON and empty-message
sentinels, configured default-parser composition failures, and native WRONGTYPE
errors followed by same-consumer recovery. Four named mutations propose eight
intended failures. The draft remains outside the compiled suite, unbuilt/unrun
pending test-review approval.

Retry/rejection writer selection, cancellation, connection recovery, concurrency,
automatic pump behavior and wider lifecycle paths remain open, alongside the other
gateway and release workstreams. Whole-feature production readiness remains open.

## Redis reader error boundaries (2026-10-30)

The approved [eight-case characterization](review/redis-reader-error-boundaries/README.md)
is complete. All eight cases passed initially on both frameworks, without a
production correction. The approved test/helper, cases, assertions and time bounds
remain byte-identical.

The native cases establish four sync/async paths: unconfigured malformed JSON still
raises JsonException; an invalid envelope prefix retains the empty-message sentinel;
a configured reader composing the default parser converts malformed JSON into a
failure preserving native ID/topic and complete multiline content; and a native
WRONGTYPE queue command produces ChannelFailureException with a RedisException inner
error without calling the configured reader. After each outcome, including repair
of only the owned wrong-type key, the same consumer reads and acknowledges a valid
message and reaches an empty receive. Stored source strings remain unchanged.

Four independent .NET 10 mutations produced eight intended failures: catching
unconfigured parser errors (2), bypassing the configured catch (2), replacing legacy
sentinels (2), and swallowing native RedisException as an empty receive (2). Each
mutation's other six cases passed. Every byte-exact restoration returned eight
passes, and final focused results pass on both frameworks. Compilation, setup and
timeout failures were not counted.

Both unfiltered full suites ran once on separate fresh ready brokers:
**124 passed, zero failed, four existing skips** each. All 120 preceding identities
and outcomes are retained, with eight new passes. The existing nack-conformance
skips are unchanged. The Redis library builds on netstandard2.0, net8.0, net9.0
and net10.0 with zero warnings/errors. The initial test dependency build reports
604 existing warnings, none in the new files; the final restored build is clean.
Existing CI selection includes these cases; hosted CI was not run.

All 6,513 preceding C# files remain byte-identical. Only the approved test/helper
are added to the compiled sources. Dependency, lockfile and workflow hashes match
the pre-batch state, including pre-existing edits.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. TRX, logs, mutation
snapshots, source/protected-file hashes, verification and cleanup records are
retained in `/private/tmp/brighter-4170-redis-reader-error-boundaries/`.

The next [immediate requeue-writer draft](review/redis-immediate-requeue-writer/README.md)
combines ten sync/async legacy/custom/composed/throw/null cases. It proposes native
ID/routing/counter/expiry preservation, no sibling fanout, mapping failure before
native changes, retained in-flight state on failure and successful recovery through
the same consumer. Four named mutations propose 32 intended failures. A missing
public selection property is expected to need compilation scaffolding before
behavioral RED; the review plan distinguishes those stages. The draft remains
outside the compiled suite, unbuilt/unrun pending test-review approval.

Other parser/native errors, connection recovery, cancellation, concurrency,
arbitrary multiline/binary rejection round trips, retry/rejection writer selection,
schedulers and automatic pump behavior remain open alongside the other gateway and
release workstreams. Whole-feature production readiness remains open.

## Redis immediate requeue writer (2026-10-30)

The approved [ten-case batch](review/redis-immediate-requeue-writer/README.md) is
complete. Both frameworks initially reported CS0117 for the missing public
RequeueWireMessageWriter property. Adding only the approved nullable auto-property
allowed behavioral RED: four custom/composed native-string failures and four missing
throw/null errors, with two legacy passes per framework. The compile failures were
recorded separately and were not counted as behavioral evidence.

Both factories now capture an immediate retry writer per consumer. Null selection
uses the default adapter. Both immediate paths map and validate the string before
storing it at the existing source ID, enqueueing that ID on the source queue and
releasing the in-flight gate. Original writer exceptions propagate; null throws
InvalidOperationException before native mutation. Existing public constructors and
delayed/scheduler-backed retry mapping are unchanged. Subscription XML documentation
records the selected-writer contract and delayed-retry boundary.

All ten unchanged approved sync/async legacy/custom/composed/throw/null cases pass
on both frameworks. Independent native observations establish exact strings, native
ID reuse, source-topic storage despite a different application topic, unchanged
counter, configured expiry, no sibling fanout and source preservation. Mapping
failure leaves the previous stored payload and in-flight gate intact; retry through
the same consumer succeeds. The observer consumes the inspected retry before a
separate valid legacy delivery establishes gate release. This does not claim that
the default reader understands arbitrary custom retry formats.

Four independent .NET 10 mutations produced 32 intended failures: ignoring the
writer (8, with two legacy passes), enqueueing before mapping (4, with six unaffected
passes), skipping payload replacement (10), and retaining the in-flight entry after
success (10). Every byte-exact restoration returned ten passes. Final focused
results pass on both frameworks. No approved test/helper, fixture, assertion or
time-bound changes were needed.

Both complete suites ran once, unfiltered, on separate fresh ready brokers:
**134 passed, zero failed, four existing skips** each. All 128 preceding identities
and outcomes are retained, with ten new passes. Existing nack-conformance skips are
unchanged. Redis library builds pass on netstandard2.0, net8.0, net9.0 and net10.0
with zero warnings/errors. Initial and green dependency builds each reported 604
existing warnings, none in new/changed files; the final restored build was clean.
Existing CI selection includes the tests; hosted CI was not run.

Of 6,515 preceding C# files, 6,512 remain byte-identical; only RedisMessageConsumer,
RedisMessageConsumerFactory and RedisSubscription changed. The two compiled
test/helper additions match the approved snapshots. Dependency, lockfile and
workflow hashes match the pre-batch state, including pre-existing edits.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. Compile/behavioral
failure logs, TRX, mutation snapshots, source/protected-file hashes, verification
and cleanup records are retained in
`/private/tmp/brighter-4170-redis-immediate-requeue-writer/`.

The next [retry-writer isolation draft](review/redis-requeue-writer-isolation/README.md)
contains four sync/async × legacy-first/custom-first cases. Three consumers share
one factory; later subscription-setting changes must not replace their captured
selections. Two distinct retries per endpoint establish repeated native payload
isolation. Three mutations propose twelve intended failures. The draft remains
outside the compiled suite, unbuilt/unrun pending test-review approval.

Endpoint isolation, concurrent mapping, cancellation, native errors after partial
progress, custom retry-reader compatibility, delayed retry/rejection writer selection
and broader lifecycle paths remain open, alongside the other gateway and release
workstreams. Whole-feature production readiness remains open.

## Redis retry-writer isolation (2026-10-30)

The approved [four-case characterization](review/redis-requeue-writer-isolation/README.md)
is complete. All four cases passed initially on .NET 9 and .NET 10 without production
corrections. Approved tests, helper, fixtures, assertions and bounds remain unchanged.

Four sync/async × legacy-first/custom-first cases exercise 24 immediate native
requeues. Three consumers share one factory and retain their captured
legacy/custom/composed writers after subscription settings are replaced with a
decoy. Each endpoint retries two distinct messages; native observations verify exact
strings, reused IDs and queue order without a publication counter. Removing each
retry from the native list leaves the next original delivery available to the same
consumer. Each selected writer runs twice and the decoy never runs.

Three independent .NET 10 mutations produced twelve intended native-string failures:
caching the first factory selection including null (4), resolving the subscription's
current setting on every retry (4), and accumulating serializer content between
default/composed conversions (4). Every byte-exact restoration returned four passes;
final focused results pass on both frameworks. Setup/compiler/timeout failures were
not counted as mutation evidence.

Both unfiltered complete suites ran once on separate fresh ready brokers:
**138 passed, zero failed, four existing skips** each. All 138 preceding identities
and outcomes are retained, with four new passes. The existing nack-conformance skips
are unchanged. Redis library builds pass on netstandard2.0, net8.0, net9.0 and
net10.0 with zero warnings/errors. Initial and final restored dependency builds each
reported 604 existing warnings, none in the new files. Existing CI selection includes
the cases; hosted CI was not run.

All 6,517 preceding C# files remain byte-identical; only the approved test/helper
are added to compiled sources. Dependency, lockfile and workflow hashes match the
pre-batch state, including pre-existing edits.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. TRX, logs, mutation
snapshots, source/protected-file hashes, verification and cleanup records are
retained in `/private/tmp/brighter-4170-redis-requeue-writer-isolation/`.

The next [rejection-writer draft](review/redis-rejection-writers/README.md) combines
twenty sync/async × invalid/dead-letter × legacy/custom/composed/throw/null cases.
It requires independent writer selection, native forwarding with rejection metadata,
mapping failure before native changes, restoration of source topic and retention
of its in-flight entry on mapping failure, and same-consumer recovery. Existing
native publication error handling remains a separate boundary. Four mutations
propose 64 intended failures. Missing public-property compilation scaffolding is
distinguished from behavioral RED. The draft remains outside the compiled suite,
unbuilt/unrun pending test-review approval.

Concurrent mapping, cancellation, native errors after partial progress, custom
retry-reader compatibility, delayed retry/rejection writer selection and broader
lifecycle paths remain open alongside the other gateway and release workstreams.
Sequential endpoint isolation does not establish concurrency safety. Whole-feature
production readiness remains open.

## Redis rejection writers (2026-10-30)

The approved [twenty-case rejection-writer batch](review/redis-rejection-writers/README.md)
is implemented and validated. Both factories capture independent invalid-message and
dead-letter writer selections. Null selection retains legacy encoding. Mapping sees
the chosen destination and refreshed rejection metadata. Mapping exceptions propagate
unchanged, null results throw, and both restore the source topic while retaining its
in-flight entry for a second rejection attempt. Internal mapped-send methods reuse
the producer's native publication code without mapping twice. Existing native-error
handling remains logged, returns true and releases the entry; this is not a guarantee
of forwarding. Public constructors are unchanged; two nullable subscription properties
are the public API additions.

Missing-property compilation errors were recorded separately. Property-only scaffolding
then produced sixteen intended behavioral failures and four legacy passes on each
framework. Optimized traces omitted payload assertion locations. Two incremental
diagnostic attempts failed location verification (missing or preceding sequence points);
a clean diagnostic rebuild verified all sixteen intended assertion lines on both
frameworks. These retained diagnostic attempts are not additional defects or mutation
failures. Approved test/helper bytes, fixtures, assertions and time bounds never changed.

All twenty cases pass on both frameworks. Four independent production mutations
produced 64 intended assertion failures: ignored writers (16), premature in-flight
removal (8), wrong destination (20), and empty native payload (20). Expected unaffected
cases passed, and each byte-exact restoration returned twenty passes. Final focused
runs used a clean optimized rebuild with diagnostic JIT settings. Both full suites
used those optimized assemblies with ordinary JIT and ran once on separate fresh
brokers: **158 passed, zero failed, four existing skips each**. All 142 preceding
identities/outcomes were retained, with twenty new passes per framework.

The Redis library builds for netstandard2.0, net8.0, net9.0 and net10.0 with zero
warnings/errors. Test dependency builds reported 604 existing warnings, none in
new/changed files. Hosted CI was not run. Of 6,519 preceding C# sources, 6,515 are
byte-identical; only consumer, factory, producer and subscription changed. Exactly
two compiled test/helper sources were added, matching approved snapshots. Dependency,
lockfile and workflow hashes remain unchanged.

Validation used native arm64 Redis 8.10.2 pinned to
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. Evidence, including TRX,
diagnostic findings, mutations/restorations, scoped source diff, source/protected-file
verification and cleanup, is retained in
`/private/tmp/brighter-4170-redis-rejection-writers/`.

The next [twelve-case rejection-boundary draft](review/redis-rejection-boundaries/README.md)
covers default/custom DLQ fallback, absent/ineligible destinations and native counter
failures through both routes. It reuses the unchanged approved helper and proposes
four mutations with sixteen intended failures. It remains outside the compiled suite,
unbuilt/unrun pending review. It characterizes the existing lossy native-error behavior,
not reliable forwarding or crash redelivery.

Rejection fallback/native-error boundaries and endpoint isolation, delayed retry mapping,
concurrency, cancellation, native failures after partial progress and broader lifecycle
coverage remain open alongside other gateway, scheduler, configuration and release
workstreams. Whole-feature production readiness remains open.

## Redis rejection boundaries (2026-10-30)

The approved [twelve-case boundary batch](review/redis-rejection-boundaries/README.md)
is characterized and validated without production changes. Sync/async cases establish
legacy/custom DLQ fallback, no eligible destination and native INCR failure through
both rejection routes. Fallback selects the DLQ writer; absent/ineligible destinations
invoke neither writer. A noninteger native counter makes publication fail after mapping:
Reject keeps its existing true/no-throw result, publishes no payload, preserves the
counter and releases the consumer to receive its next source message. A true result
therefore does not guarantee forwarding. Source payloads remain unchanged.

All twelve unchanged approved cases passed initially on both frameworks. Four
independent .NET 10 mutations produced sixteen intended failures at the reviewed
assertion lines: wrong fallback writer (4), writer invocation without a destination
(4), propagated native errors (4), and retained in-flight state after native failure
(4). Each mutation retained eight unaffected passes. Diagnostic rebuilds/JIT settings
established exact assertion locations. Each byte-exact restoration returned twelve
passes. No test/helper, fixture, assertion or time-bound changes were needed.

Final focused runs used a clean optimized rebuild with ordinary JIT and passed twelve
cases per framework. Both unfiltered full suites ran once on separate fresh brokers:
**170 passed, zero failed, four existing skips each**. All 162 preceding identities
and outcomes were retained, with twelve new passes. Four library targets
(netstandard2.0/net8.0/net9.0/net10.0) built with zero warnings/errors. Initial and
final test dependency builds each reported 604 existing warnings, none in the new
file. Hosted CI was not run.

All 6,521 preceding C# sources remain byte-identical. Only the approved test was
added to compiled sources; it matches the reviewed snapshot and reuses the unchanged
approved helper. Dependency, lockfile and workflow hashes remain unchanged. Validation
used native arm64 Redis 8.10.2 pinned to
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after saving logs; unrelated containers were untouched. TRX, logs, mutations,
restoration and source/protected-file verification, runtime settings and cleanup
records are retained in `/private/tmp/brighter-4170-redis-rejection-boundaries/`.

The next [four-case isolation draft](review/redis-rejection-writer-isolation/README.md)
checks both captured writer selections across three legacy/custom/composed endpoints,
in two creation orders, and repeated forwarding through both routes (48 forwards).
It reuses the unchanged helper and proposes three mutations with twelve intended
failures. The draft remains outside the compiled suite, unbuilt/unrun pending review.

Rejection endpoint isolation, fallback mapping failure, native failures after partial
progress, delayed retry mapping, concurrency, cancellation and broader lifecycle
coverage remain open alongside other gateway, scheduler, configuration and release
workstreams. These native-error cases establish neither atomic forwarding nor crash
redelivery. Whole-feature production readiness remains open.

## Redis rejection-writer isolation (2026-10-30)

The approved [four-case isolation batch](review/redis-rejection-writer-isolation/README.md)
passes focused and mutation validation without production changes. Sync/async × two
endpoint orders exercise 48 forwards across legacy/custom/composed writers, capturing
both rejection selections before settings are replaced. Exact native payloads,
independent counters/queues, unchanged source payloads and delivery order are checked;
the decoy is unused and repeated serialization does not accumulate earlier content.

All four cases pass initially and after clean optimized rebuilds on both frameworks.
Three independent .NET 10 mutations produce twelve intended native-string failures:
cache the first two selections (4), resolve live subscription settings (4), accumulate
serializer output (4). Every byte-exact restoration returns four passes. Diagnostic
builds/JIT settings verify the exact assertion line; final focused/full runs use
ordinary JIT. Approved test/helper, fixture, assertions and bounds are unchanged.

Both complete suites ran once on separate fresh brokers: **172 passed, two failed,
four existing skips each**. All four new cases pass. Both failures are prior async
RedisReaderFailureRecoveryTests cases (returnNull true/false), at Assert.Null(error)
on line 101, with ServiceStack.LicenseException reporting the 6,000 Redis requests/hour
free quota. All 174 prior identities remain; 172 prior outcomes are unchanged and two
prior passes fail. The full regression gate remains incomplete. No reruns or quota
bypasses were attempted. The earlier 170-pass/four-skip baseline remains historical
evidence, not proof that this expanded suite passes.

All four library targets build with zero warnings/errors. The standalone sandboxed
build initially stalled/exited nonzero; an escalated build succeeded. Initial/final
test dependency builds report 604 existing warnings, none in the new file. All 6,522
preceding C# files remain byte-identical; only the approved test is added and its
helper is unchanged. Dependency, lockfile and workflow hashes remain unchanged.
Native arm64 Redis 8.10.2 was pinned to
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`;
native Docker SDK runners exposed no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. Evidence is retained in
`/private/tmp/brighter-4170-redis-rejection-writer-isolation/`.

No repository license registration or session SERVICESTACK_LICENSE variable was found;
the runner passes no license variable. ServiceStack documents the [quota](https://servicestack.net/download)
and [license registration](https://docs.servicestack.net/register). A valid license
must be made available and passed to the runner before a licensed full-suite retry.
The user was asked for an existing license file location or secret name, not the key.
Hosted CI was not run.

The next [fallback recovery draft](review/redis-rejection-fallback-recovery/README.md)
contains four throw/null cases and proposes sixteen mutation failures. It is outside
compiled sources, unbuilt/unrun, with execution deferred until this regression gate
and its own review are complete. Delayed retry mapping, native failure windows,
concurrency, cancellation, other gateway integrations and wider release work remain
open. This batch and whole-feature production readiness are incomplete.

Licensed rerun preparation (2026-10-30): the local evidence directory now includes
`licensed-regressions.py` and `licensed-rerun-inputs.json`, pinning 6,523 current C#
sources and 245 optimized assembly/output files plus protected inputs. The script
accepts a user-supplied license file or SERVICESTACK_LICENSE, forwards the variable
by name to Docker, uses a new evidence directory and fresh brokers, preserves the
quota-failure runs and checks all prior outcomes plus the four new cases. Syntax
and missing-license preflight checks pass; missing input starts no Docker commands
and creates no result directory. No licensed retry, new application test or container
was run during preparation. License availability remains the blocking input.

Paced regression amendment (2026-10-30): the user explicitly requested rerunning the
unchanged tests without payment and confirmed smaller batches with real waiting
periods. The new local runner in `/private/tmp/brighter-4170-redis-paced-regressions/`
is active. Four batches contain 89 cases each, two per framework. Static coverage
verification schedules all 178 case identities exactly once per framework. The first
batch is no earlier than 21:24:52 UTC, leaving a full hour plus five seconds after
the prior full runs; later batches wait 3,605 real seconds after the preceding batch
finishes. Each uses a fresh owned broker, ordinary JIT and the unchanged optimized
assemblies, tests and timeouts. Input hashes are verified before every batch.

This is a user-approved change to the validation execution plan, not a change to
application behavior or quota enforcement. Combined results will establish batched
regression coverage and must not be reported as a passing single-process full run.
Expected combined counts are 174 passes/four existing skips per framework. The runner
retains TRX/logs, stops on any failure or input mismatch, and removes owned brokers.
`status.json` reports progress; successful completion creates `summary.json`.
At launch no batch has completed; the regression task remains unchecked. A license
key is not required for this paced plan. Host/Docker availability affects completion.

## Redis paced regression completion (2026-10-31)

The user-approved paced regression amendment is complete. Four disjoint batches of
89 cases each ran on fresh brokers with the unchanged optimized assemblies and
ordinary JIT. Each batch passed 87 cases with two existing skips. Combined results
for each of .NET 9 and .NET 10 are **174 passed, zero failed, four existing skips**.
Every one of the 178 identities ran exactly once per framework, preserving all 174
preceding outcomes and adding four isolation passes. Both formerly quota-failing
async reader-recovery cases pass on both frameworks. No batch reported a quota error.

Completion was recorded at 2026-10-31 03:11:08 UTC. Actual idle intervals before the
batches were 3,606, 11,279, 4,597 and 4,507 seconds (rounded), all above the 3,605-second
minimum. All four owned brokers and their runners were removed; cleanup succeeded
for every batch. No license was supplied and no quota enforcement or counters were
modified. Code, dependencies, tests and timeouts are unchanged. Independent verification
on 2026-10-31 confirms current source, protected-input and assembly hashes still match
the pinned inputs, as well as exact TRX coverage/outcomes and real-time spacing.

This clears the approved batched regression gate and completes rejection-writer
isolation. It does not turn the earlier failed unfiltered runs into passing runs or
establish cross-batch process-state interactions. Historical single-process evidence
remains 172 passes/two quota failures/four skips per framework. The preceding focused
passes, twelve intended mutation failures with exact restorations, and four clean
library builds remain applicable because the verified inputs were unchanged. Hosted
CI was not run.

The plan, TRX, logs, broker statistics, cleanup records, `summary.json` and independently
checked `final-verification.json` are retained in
`/private/tmp/brighter-4170-redis-paced-regressions/`. The task checklist and current
review now reflect completion. The subsequent four-case fallback recovery draft
remains outside compiled sources, unbuilt/unrun pending its own test review, and its
regression plan now follows the user's paced-validation preference. Fallback mapping
failure, delayed retry mapping, other gateway integrations and wider lifecycle/release
coverage remain open; whole-feature production readiness is not established.


## Redis fallback regression host interruption (2026-11-01)

The first paced .NET 9 batch stopped with 86 passes, two existing skips and one
failure in the existing synchronous rejection-boundary `no-channels` case. No
quota error was reported; both owned containers were removed. Later batches did
not run. The new fallback recovery cases were not part of this first batch.

The failed case spans 00:24:45–01:24:42 UTC in TRX wall timestamps but reports only
307 ms of execution. It fails when rereading an original source payload seeded with
a ten-minute expiry. Redis reports 37 expired keys. macOS power logs confirm repeated
sleep periods in this interval. This supports expiry during host suspension; it does
not establish a production regression. The failed TRX, native logs, and diagnosis
remain in `/private/tmp/brighter-4170-redis-rejection-fallback-recovery/paced` and
`sleep-interruption-diagnosis.json` in its parent directory.

After investigation, all four unchanged batches are rerunning from scratch under
`paced-awake/`, using identical verified assemblies, a temporary `caffeinate -i -s`
assertion and host-clock discontinuity detection. The lid must remain open; this
assertion cannot override lid closure. Real waits of at least 3,605 seconds remain.
No production, dependency, test, assertion, expiry or timeout changes were made.
Completion is still pending; the failed attempt is not counted as a passing gate.

The first sleep-protected rerun batch passed 87 cases/two existing skips, including
the previously failing unchanged case. Runner wall/monotonic durations both measure
96.45 seconds, with no clock discontinuity or quota error. Owned containers are
removed. Three scheduled batches remain; this is not a completed regression gate.


## Redis fallback recovery completion (2026-11-01)

The approved four sync/async × throw/null fallback cases pass initially and after
restoration on both frameworks. Four independent production-only mutations produce
sixteen failures at the reviewed assertions; each exact restoration returns four
passes. No production correction is needed. Final optimized focused runs pass four
cases per framework, and all four library targets build with zero warnings/errors.
The test dependency rebuild retains 604 existing warnings, none in the new test.

The user-approved paced regressions finish with **178 passes, zero failures and four
existing skips per framework**. Every one of the 182 cases runs exactly once per
framework; all 178 prior outcomes are retained and the four new cases pass. Real
idle periods before the batches exceed one hour. No quota errors or license were
needed; owned containers are removed. All 6,523 preceding C# files, eight protected
inputs, approved test/helper bytes and final regression assemblies retain their
recorded hashes. This establishes complete batched coverage; it does not claim a
single-process full-suite pass. The fallback batch is complete; delayed retries,
broader lifecycle coverage and the full feature's other release gates remain open.

Evidence: `/private/tmp/brighter-4170-redis-rejection-fallback-recovery`, with the
independent final result in `paced-awake/final-verification.json`. No hosted CI, commit,
push, deployment or publication is claimed.


## Redis scheduled retry dispatch completion (2026-11-02)

Four approved sync/async × custom/default cases pass through native Redis and the
production in-memory scheduler, command processor, scheduler handler and outbox
mediator. They establish publication writer selection at dispatch, delayed native
visibility, next-message progress, native counter/recipient fan-out, expiry and
preservation of original native payloads. No production correction is needed.

Three production-only mutations produce ten intended assertion failures: two
custom-payload failures when the publication writer is ignored (two default cases
still pass), four next-receive failures with a retained in-flight entry, and four
empty-native-string failures. Each exact restoration returns four passes. Final
optimized focused runs pass four cases per framework; all four Redis library targets
build with zero warnings/errors. Approved tests/helpers and all 6,524 preceding C#
files and protected inputs are unchanged.

Complete paced regressions pass **182 cases, zero failures and four existing skips
per framework**. All 186 cases run exactly once per framework, preserving all 182
prior outcomes. Real idle intervals are at least 3,605 seconds, with no quota or
clock-discontinuity errors; owned containers are removed. This is complete batched
coverage, not a single-process full-suite pass. Evidence and independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-dispatch/paced/final-verification.json`.

This closes registered-publication dispatch characterization only. Per-subscription
delayed writer selection, queue-specific scheduled retry routing, callback failure
recovery, durable schedulers and the wider feature release gates remain open.
No hosted CI, commit, push, deployment or publication is claimed.


## Redis subscription-selected scheduled retries: final focused validation (2026-11-02)

The approved five-file Redis implementation passes all four native focused cases
on .NET 9 and .NET 10, including final ordinary optimized builds. The preceding
property-only implementation produced four intended counter assertion failures per
framework. Five independent mutations produce all eighteen reviewed assertion
failures; exact restoration returns four passes after every mutation. Four Redis
library targets build with zero warnings/errors. The test build retains 604 existing
warnings. The approved test/helpers, 6,520 other prior C# files and protected
inputs are unchanged. The focused broker is removed.

The complete paced regression schedule is running, with its first batch eligible at
2026-11-02T18:27:19.877436+00:00. Four disjoint native batches use real 3,605-second
idle intervals, temporary idle-sleep prevention and automatic owned cleanup. New
cases join the smaller prior batch to preserve free-quota headroom. Expected coverage
is 190 cases exactly once per framework: 186 passes and four existing skips, with
all 186 prior outcomes retained. These outcomes are pending, not yet claimed.
Keep the laptop lid open; idle-sleep prevention cannot prevent lid sleep.

Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-selection/focused-verification.json`.
Live schedule: `/private/tmp/brighter-4170-redis-scheduled-retry-selection/paced/status.json`.
Successful completion independently checks identities, outcomes, source/assembly
hashes, real idle gaps and cleanup before updating local completion reports.
Failure stops later batches and retains evidence. Whole-feature production readiness
is not established; multi-endpoint isolation, invalid configuration, scheduler
failure/cancellation/persistence and broader release gates remain open.


## Redis subscription-selected scheduled retries completion (2026-11-02)

The four approved sync/async × custom/default cases pass through native Redis and
the production in-memory scheduler, command processor, scheduler handler and outbox
mediator. Redis subscriptions capture an optional scheduled publication topic; the
registered publication selects its writer, native destination topic and single queue.
The original application topic/type and caller message are preserved. Null options
retain existing routing, and the public consumer constructor remains compatible.
Only five Redis production files change; shared Core and scheduler APIs are unchanged.

Initial behavior fails at the intended native source counter assertion on both
frameworks. Five independent production mutations then produce eighteen intended
assertion failures, with four passes after each exact restoration. Initial and final
optimized focused runs pass four cases per framework; all four Redis library targets
build with zero warnings/errors. The approved test/helpers, 6,520 other preceding C#
files and protected dependency/workflow inputs remain unchanged.

Complete paced regressions pass **186 cases, zero failures and four existing skips
per framework**. All 190 cases run exactly once per framework, preserving all 186
prior outcomes. Real idle intervals are at least 3,605 seconds, with no quota or
clock-discontinuity errors; owned containers are removed. This is complete batched
coverage, not a single-process full-suite pass. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-selection/paced/final-verification.json`.

This closes the reviewed single-endpoint subscription selection and queue-specific
routing batch. Shared-factory multi-endpoint isolation, invalid/missing configuration,
submission/callback failure recovery, cancellation, durable schedulers and broader
feature release gates remain open. No hosted CI, commit, push or deployment is claimed.


## Approved isolation batch: focused validation complete; regressions pending

Four cases pass initially and finally on .NET 9/.NET 10. All twelve intended
mutation failures and exact restorations are verified. Four Redis library targets
build without warnings/errors; all preceding production/test sources and protected
inputs remain unchanged. The approved test alone is added and the focused broker
is removed. Complete paced regressions are queued; no full regression pass is claimed.
Live evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced/status.json`.

First regression eligibility: 2026-11-02T23:45:51.175528+00:00.


## Isolation regression sleep interruption and unchanged rerun (2026-11-03)

The first paced regression attempt stopped during net9.0-batch-1: 91 passed,
four failed and two existing skips. Two new isolation cases saw an empty receive;
two existing reader/immediate-retry cases saw expired native payloads. The runner
reported 24,597 wall seconds versus 176 monotonic seconds and rejected the clock
discontinuity. macOS power logs confirm lid-closed sleep at 00:14 local time and
repeated maintenance sleep during the batch. Redis recorded 51 expired keys and
no quota error. Owned containers were removed. This is not a passing regression
result, and the original evidence remains under
`/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced/`.

On the user's continuation, the complete unchanged suite was restarted after the
required real idle interval had already elapsed. Source inventory, all pinned source,
dependency and assembly hashes, and approved tests/helpers match. No timeout, TTL,
dependency, assertion, license or clock setting was changed. The new run retains
3,605-second gaps between all batches and temporary idle-sleep prevention. The lid
must stay open because caffeinate cannot override clamshell sleep.

Current status: `/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced-awake/status.json`.
The failed attempt is preserved alongside the rerun. A clean rerun must establish
all 194 identities exactly once per framework, 190 passes/four existing skips and
all 190 preceding outcomes before completion is claimed. Focused/mutation checks
and four-target builds remain valid; the complete regression gate remains pending.


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


## Redis scheduled retry writer recovery: focused validation (2026-11-04)

The user approved the four cases, persistent-failure helper and named mutation plan.
All four cases pass initially and in final optimized builds on .NET 9 and .NET 10.
The premature-settlement mutation produces four failures at the outstanding-message
assertion; bypassing the publication writer produces four failures at the failed-write
assertion. Exact restoration returns four passes after each mutation. No permanent
production change is needed. All four Redis library targets build with zero warnings
or errors; the test build retains 604 existing warnings.

The approved test/helper are unchanged, and independent verification preserves all
6,527 preceding C# files and 281 build/dependency/workflow inputs. The focused broker
is removed. Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-writer-recovery/focused-verification.json`
and `launch-verification.json` in the same directory.

Complete paced regressions are queued in four disjoint batches; their outcomes are
pending. Each framework is expected to cover 198 identities exactly once, with 194
passes and four existing skips, preserving all 194 preceding outcomes. The four new
cases measure 277 Redis commands per framework. An allowance of 516 commands puts
the smaller preceding batches at estimated totals of 5,174 and 5,202, under the 5,500
allocation threshold; no extra batch is required. No tests, dependencies, timeout
bounds or license settings are changed to achieve this allocation.

The first batch is eligible at `2026-11-04T16:16:16.683922+00:00`. Real 3,605-second
idle intervals remain required after focused work and between batches. The launcher
uses temporary idle-sleep prevention; keep the lid open during test execution.
Live state: `/private/tmp/brighter-4170-redis-scheduled-retry-writer-recovery/paced/status.json`.
The runner stops on unexpected results and retains evidence. Successful completion
independently verifies outcomes, exact identities, source/assembly hashes, waits and
cleanup before updating completion reports. This is batched coverage; no passing
single-process full suite or whole-feature production readiness is claimed.


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


## Scheduled Redis retry registration: reproduced failure and initial fix (2026-11-06)

All four approved sync/async × missing-alias/wrong-type cases reproduce native retry
acceptance followed by an unhandled ConfigurationException from ProducerRegistry
escaping the production InMemoryScheduler callback. Each child process exits 134;
the parent xUnit process survives and fails at its exit assertion. This occurs on
both .NET 9 and .NET 10. Evidence is under
`/private/tmp/brighter-4170-redis-scheduled-retry-registration/initial-red-*.trx` and
`red-verification.json`.

The message-callback branch now logs the original exception with its scheduler ID
and guarantees existing generation-aware cleanup in finally. All four initial green
cases pass per framework: retained message data, direct dispatch errors, scheduler-ID
reuse, later healthy dispatch and explicit recovery through corrected configuration.
Request callbacks and public signatures are unchanged.

Unchanged full baselines pass 1,716 Core cases/seven existing skips and 167 InMemory
cases on each framework. A sandbox pipe failure and then a temporary output-location
failure were retained as setup evidence; repository-local output resolves the Core
architecture checks without changing them. The fixture's ambiguous subscription type
and nullable project context were corrected without changing assertions or behavior.

Mutation validation and final optimized/build/regression checks are in progress.
Rethrowing the callback and omitting failed-job cleanup each produce four intended
parent assertion failures with exact restoration and four restored passes. The
log-suppression mutation's first build had an unused-variable error; it is retained
as compilation evidence, not behavioral RED. Its catch binding is corrected and
validation resumes without rerunning completed mutations. No complete Redis
regression result or whole-feature production readiness is claimed yet.


## Scheduled Redis retry registration: final focused validation (2026-11-06)

All four approved cases pass on both frameworks in final optimized builds. All twelve
named mutation failures are verified, with exact restorations and four passing cases
after each. Eight Core/Redis library target builds pass with zero warnings/errors.
Full post-fix Core suites retain 1,716 passes/seven existing skips and InMemory retains
167 passes on each framework; every baseline identity and outcome is preserved.
All 6,528 other preceding C# files remain unchanged, and fixture/project changes match
the approved scope plus the recorded non-behavioral compilation corrections.
The focused native broker is removed.

The complete Redis regression schedule has 4 disjoint paced
batches, first eligible at `2026-11-06T08:53:15.366933+00:00`. Expected coverage is 202
identities exactly once per framework, with 198 passes/four existing skips and every
preceding outcome retained. These results remain pending. Allocation uses measured
native command counts plus a conservative allowance; real 3,605-second idle gaps,
source/assembly pinning, owned cleanup and stop-on-failure are enforced. Child
processes isolate callback failures and do not change the quota/pacing contract.

Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-registration/focused-verification.json`.
Live schedule: `/private/tmp/brighter-4170-redis-scheduled-retry-registration/paced/status.json`.
Successful completion independently verifies all identities/outcomes, input hashes,
real waits and cleanup before writing completion reports. Keep the laptop lid open
for native runs; idle-sleep prevention cannot prevent lid sleep. This establishes no
unrun CI or whole-feature production readiness.


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

## PostgreSQL native wire mapping — 2026-11-07

[Native integration evidence](review/postgres-wire-mapping/README.md): 28 focused cases,
76 intended mutation failures/restorations and 317 passing full-project tests preserve
all 289 prior outcomes. The test project targets .NET 10; all four library targets
build with zero warnings/errors. Only the four existing PostgreSQL gateway files change,
with two default adapter exports added. Schema/outbox/build inputs remain unchanged.
Whole-feature readiness and the final cross-gateway matrix remain open.

## SQL Server initial wire mapping — 2026-11-07

[Integration evidence](review/mssql-wire-mapping/README.md): twelve focused cases per
framework, twenty intended mutation failures with exact restorations, and full suites
of 380 passes/four existing skips on .NET 9 and .NET 10 preserve all 372 preceding
outcomes. All four gateway targets build with zero warnings/errors. Existing public
constructors, queue schema and outbox serialization remain unchanged. Only the five
intended existing MsSql gateway files change, with a publication and default adapter
exports added.

Validation uses the repository's native ARM64 Azure SQL Edge fixture. Concurrent
SQL/RocketMQ execution first caused documented memory-allocation errors; stopping the
owned RocketMQ services resolved that contention. A separate amd64 SQL Server 2022
exit-139 failure recurred after isolation and remains unresolved. Stable repository
fixture results do not resolve that environment finding. Preserve it as a final release
gate along with remaining scheduler/rejection/lifecycle combinations. No commits or pushes.

### Quartz durable restart checkpoint — 2026-11-08

The [native process-restart probe](review/quartz-durable-restart/README.md) passes
four baseline and four restored cases, two intended corruption failures and
both full 53-case Quartz suites. Production source is restored exactly. This closes
the selected PostgreSQL JobStoreTX/System.Text.Json process-restart gate only.
TickerQ durable restart, AWS callback recovery and the remaining transport/final
release gates remain open. No commits or pushes.

### RocketMQ rejection selection checkpoint — 2026-11-08

The [native rejection package](review/rocketmq-rejection-selection/README.md)
produces twelve actual RED and GREEN cases per framework. Full regressions preserve
all 118 prior identities/outcomes and add twelve passes: 128 passes/two existing
skips each. All four library targets build; guarded local integration is complete.
Legacy acknowledgement after failed forwarding remains unchanged; recovery uses
a subsequent message. Main-workspace locked restore/rebuild and all twelve native cases pass on both
frameworks, with no host-clock discontinuity.

### TickerQ durable restart checkpoint — 2026-11-08

The [native SQLite restart probe](review/tickerq-durable-restart/README.md) passes
eight baseline/restored cases, both intended corruption checks and both full
54-case suites, preserving prior identities/outcomes. Production code/dependencies
are unchanged. The recorded Hangfire, Quartz and TickerQ provider-specific restart
gates are complete. AWS callback-pump recovery and final transport/release gates
remain open; this is not a whole-issue production-ready claim.

### RabbitMQ unsupported-delay fallback checkpoint — 2026-11-09

The [fallback package](review/rmq-destination-delay-fallback/README.md) fixes native
missing-exchange failures for explicit queues when delay support is disabled.
Eight actual RED failures and eight passing default controls precede the fix.
Full Async suites pass 566 cases/two existing skips and Sync 299 cases/one existing
skip on both frameworks, with all prior outcomes retained and all library targets
built. Suspended attempts are excluded and retained separately. Guarded local
integration and main-workspace native checks pass (ten Async/eight Sync each).
No commits or pushes; remaining whole-issue release gates stay open.

### MQTT consumer and scheduled writer selection checkpoint — 2026-11-09

The [consumer writer package](review/mqtt-consumer-writers/README.md) adds eighteen
native actual RED/GREEN cases per framework for immediate retry, invalid-message
and dead-letter selection, throwing/null writers and recovery. The
[scheduled retry package](review/mqtt-scheduled-selection/README.md) adds four actual
RED/GREEN cases each plus four intended selector-omission failures. Both full final
suites pass 121 cases/four existing skips; every preceding outcome remains. All
library targets build, mutations are restored exactly and guarded integration is
complete. Main-workspace locked rebuild and all 22 native cases pass on both
frameworks without clock discontinuity. Protocol ACK ownership and the
legacy logged/true-result rejection failure contract remain unchanged.

### AWS automatic callback pump recovery checkpoint — 2026-11-09

The [callback recovery package](review/aws-callback-pump/README.md) validates eight
SQS/SNS, sync/async-business-send, throw/null cases per SDK/framework through the
actual Floci scheduler and ServiceActivator Proactor. Failed business sends retain
outbox identity/body; the callback is settled, and explicit outbox recovery delivers
with selected mapping and fresh traceparent/tracestate/baggage. Sixteen intended
body-corruption failures precede exact production restoration. All four complete
AWS suites pass 627 cases, preserving all 619 prior outcomes plus eight new cases.
Four test/helper files are integrated locally; no permanent production change.
S3 endpoint setup failures and clock-discontinuous attempts remain separate from
certified continuous runs. Main-workspace locked restore/rebuild and all eight new native cases pass for
each SDK/framework, with no host-clock discontinuity. Hosted AWS IAM,
durable outbox and automatic failed-business redelivery are not claimed.

### Kafka dead-letter and scheduled retry selection checkpoint — 2026-11-09

The [dead-letter package](review/kafka-dead-letter-selection/README.md) reproduces
and fixes eight cases per framework: selected replacement/composed mapping plus
throw/null failure retaining the source offset and redelivery recovery. The
[scheduled retry package](review/kafka-scheduled-selection/README.md) reproduces
four wrong-format callbacks each, then verifies subscription capture, typed lookup,
original native destination, fresh dispatch telemetry, local selector isolation and
original-offset-only settlement. Omitting the selector gives four intended failures.
After exact restoration, both full suites pass 412 cases, preserving all 400 prior
outcomes. All library targets build and guarded local integration is complete.
Main-workspace restore/rebuild and fourteen combined native/ownership cases
pass on both frameworks without clock discontinuity. No commits or pushes. The historical native
assignment/close stall remains unresolved; no speculative shutdown change was made.

### Executable telemetry example verification — 2026-11-09

Re-reading the certified full TRXs finds sixteen existing tests whose names contain
`non_cloud_events_telemetry` in each AWS SDK/framework suite and four in each RabbitMQ
Async framework suite; all pass. These are the compiled examples linked from
[usage](usage.md#move-telemetry-outside-cloudevents), covering custom attributes/body,
trace restoration and real application pump/settlement where specified by each test.
They ran as part of the complete 627-case AWS and 566-pass/two-skip RabbitMQ matrices;
no extra test execution or broader binary-format guarantee is implied.
Both ADRs now describe the gateway-wide typed composition design and transport-specific
ownership/failure semantics. Documentation remains local and unpublished. The ASB
queue-retry bypass and other release gates remain explicit in the completion plan.

### GCP rejection writer selection — 2026-11-09

The [rejection selection package](review/gcp-rejection-selection/README.md) reproduces
24 ignored-writer failures per framework, then passes all 24 native pull/stream,
sync/async, shared-route/fallback and throw/null recovery cases. A topic-only cache
mutation causes twelve intended shared-route failures; exact restoration passes.

Both pre-change available Pub/Sub suites pass 429 cases/55 skips. Each final suite
has 452 passes, one failure and 55 skips, retaining all preceding identities plus
24 new passing cases. The changed outcome is the historical generated async ordered-
nack missing-ID assertion at line 158. The failure is retained and not retried away.
This is scoped rejection validation, not a clean full-suite or production-ready claim.
All library targets build; guarded local integration is complete and main-workspace
restore/rebuild and all 24 native cases pass on both frameworks without clock discontinuity. No hosted, Firestore, Spanner or remote CI coverage is claimed.

### Final local build and cleanup checkpoint — 2026-11-09

Complete solution builds pass in Release (22,073 warnings/zero errors) and Debug
(20,517 warnings/zero errors). These are successful builds, not warning-free builds.
All 7,244 recorded source/build inputs remain identical throughout; clock continuity
is verified. `git diff --check` passes. The command used `--locked-mode` for restore,
but not every repository project has a package lock file, so this flag alone is not
claimed as a repository-wide locked dependency guarantee.

Two duplicate PostgreSQL test PackageReference declarations were removed. Before/after
resolved targets and library identities/hashes compare exactly equal; no package
version or test source changes were made. The final solution builds include the cleanup.
All newly integrated AWS, RabbitMQ, Kafka, MQTT and GCP acceptance cases pass from the
main workspace. GCP's full matrix retains the separately recorded async ordered-nack
failure on both frameworks; no passing full GCP regression is claimed.

Eleven task-owned fixtures are stopped, with broker logs and containers retained;
unrelated services remain running. Compact [build/source evidence](review/final-build-verification.json)
and [main-workspace results](review/main-workspace-verification.json) are saved locally.
No commits, pushes, remote CI, deployment or publication occurred. Whole-issue release
gates remain explicit in the [completion plan](completion-plan.md).

## ASB queue retry writer selection — 2026-11-09

Both native consumer-created retry scheduling and the global Azure retry adapter
now capture an explicit writer. Mapping follows current trace propagation and
preserves custom binary body/metadata, while the scheduler restores retry and
original identity, session, partition and handled count. Null retains the old raw
binary body path. Writer throw/null leaves scheduling and source completion untouched;
recovery schedules once before completion. Public constructor signatures are unchanged.

Twelve ignored-writer assertion failures per framework establish actual behavioral
RED, including a second baseline with the corrected telemetry fixture. Both final
local inventories pass **536 cases**, retaining all **524** prior identity/outcome
pairs and adding twelve cases. No failures/skips or clock discontinuities. The
inventory covers 66 test classes using local SDK-I/O/static checks; 80 native-service
classes remain outside this inventory. It is not a replacement for the historical
native/emulator 628-case baseline, nor a hosted-delivery claim.

Setup failures (internal constants, timeout argument and ambient baggage) and the
SDK session/partition ordering failure are retained separately. Clearing the mapped
session before restoring source controls resolves the SDK conflict without changing
the legacy no-writer path. Four production files and two new test files were copied
with before/after hash guards. Tests, inventory and compact results are retained in
[the review package](review/asb-retry-writer-selection/README.md).

Hosted scheduling/DLQ/lost-lock acceptance and the retained session-lock finding remain
open. Nothing committed or pushed. Evidence:
`/private/tmp/brighter-4170-asb-retry-writer-selection/`.

Post-integration verification: twelve main-workspace retry-writer cases and all
thirteen Azure scheduler tests pass per framework. Complete Release and Debug
solution builds pass with zero errors, including both changed libraries on
netstandard2.0, net8.0, net9.0 and net10.0. All 7,246 recorded source/build
inputs remain unchanged through both builds; clocks are continuous. Repository
warnings remain (22,073 Release; 821 Debug).
No native fixtures were started for this package. No commits, pushes or remote CI.

## GCP scheduled publication selection — 2026-11-09

Four native cases exercise synchronous/asynchronous delayed publication through
real InMemoryScheduler callbacks, CommandProcessor, typed producer lookup and outbox
submission. Default/composed payload, dispatch tracing/state/baggage, native ordering,
application identity and acknowledgement all pass. The default-writer bypass mutation
fails both custom body checks and preserves both default controls. Exact source
restoration and all four restored cases pass per framework; no production change.

Full Pub/Sub matrices retain all 508 prior identities plus four new passing cases:
.NET 9 has 457 passes/55 skips; .NET 10 has 456 passes/one known generated ordered-nack
failure/55 skips. The .NET 9 historical failure passes on this run without a product
correction; it remains unresolved. All runs have continuous clocks. The one new test
is integrated with hash guards. See [the review package](review/gcp-scheduled-publication/README.md).
No commits or pushes. Separate passive RPC diagnostics are investigating the retained
failure under the existing generated full-suite conditions.

## Public signatures and generated ordered-nack diagnosis — 2026-11-09

The [98-file public declaration audit](review/public-surface-audit/README.md)
finds no removed/changed covered public/protected signatures versus HEAD, with
68 additions. This syntax audit is scoped separately from compiled binary and
default-wire behavioral evidence. GCP's integrated scheduled-publication tests
rebuild and pass four cases per framework in the main workspace.

The [generated ordered-nack full-suite diagnostic](review/gcp-generated-nack-diagnostic/README.md)
reproduces the exact async failure on .NET 9: 456 passes, one failure, 55 skips.
Its native trace proves successful deadline-zero RPC with no native redelivery
within the unchanged bound. A concurrent 60-second lease RPC prevents attributing
this particular trace solely to the independently reproduced emulator stream/ack
stall. Preserve the failure and leave that distinction open. No test assertions
or production behavior changed.

## Fixed GCP lease control and Kafka diagnostic outcome — 2026-11-09

The [GCP raw lease-order control](review/gcp-ordered-nack-lease-control/README.md)
completes eight observations on the pinned emulator. One-stream redelivery succeeds
with either completed lease/nack order; both two-stream variants miss the unchanged
30-second bound on both frameworks. This supports the already isolated emulator
stream-membership boundary, without claiming the exact concurrent server order in
the generated-test trace or dispositioning separate historical shutdown findings.
No production or assertion changes occur. Both owned fixtures are stopped.

The [Kafka native close diagnostic](review/kafka-native-close-diagnostic/README.md)
completes its one planned full .NET 9 suite: 412 passes, zero failures/skips, continuous
clocks, both commit/revoke strategies exercised. Native debug output is retained in
the TRX; the diagnostic source was restored before execution. The archived close
stall is not reproduced and remains unresolved. Both owned fixtures are stopped.

Current usage/external-documentation text no longer labels validated RabbitMQ
scheduler selection, AWS callback-pump recovery or Kafka partition/retry packages
unimplemented. Historical package outcomes remain in the validation/review records.
All local targets in seven current guidance/ADR files resolve.

## Compiled API compatibility — 2026-11-09

All [68 compiled API comparisons](review/binary-api-audit/README.md) pass across
the 18 changed production assemblies and their declared target frameworks versus
Git HEAD, with no unresolved references or compatibility suppressions. Parameter
names are included. The separate all-attributes run reports only 360 compiler
async-state-machine metadata differences; all are retained and classified. A
reversed Core comparison correctly rejects the two missing wire contracts.
Source and binary hashes are recorded. This closes the scoped public API check,
not native runtime, environment or lifecycle gates.

## Final local checkpoint after delayed-publication integration — 2026-11-09

RocketMQ passes 136 cases/two existing skips per framework, retaining all 130 prior
outcomes plus eight new cases. Main confirmation passes all eight on both frameworks;
GCP main confirmation passes four on both. All temporary production mutations and
diagnostic source edits are restored.

Complete main-workspace Release and Debug builds pass with zero errors and unchanged
7,248 source/build inputs. Warning counts are 22,073 and 4,107; neither new scheduling
test file emits warnings. Final Release output hashes match all 68 API comparison
records after rechecking the 24 regenerated binaries. All issue-owned containers are
stopped; existing unrelated services remain untouched. No commit, push, remote CI,
paid resource creation, deployment or publication occurs.

The [completion plan](completion-plan.md) now records four concrete remaining gates:
native ASB retry scheduling, ASB DLQ/lost-lock coverage after Azure authentication,
cause-supported disposition of retained ASB/Kafka/GCP lifecycle findings, and supported
x86-64 SQL Server 2022 runtime coverage. The feature remains not production-ready.


## Original GCP shutdown diagnostic follow-up — 2026-11-09

The [bounded native-input diagnostic](review/gcp-original-shutdown-diagnostic/README.md)
passes all four original unsettled/buffered shutdown cases on each framework, then
passes those four again in one fixed full .NET 9 matrix. All twelve observations
record a successful nack for the last original delivery and recovery through the
replacement subscriber with the same native/application IDs and a fresh ack ID.
The historical shutdown failure is not reproduced and remains unresolved.

The full matrix has **456 passes, one failure and 55 existing skips**, all 512 prior
identities retained, with continuous clocks (559.83 seconds). The failing existing
ordered-nack async/default-client diagnostic observes successful native nack but no
subsequent native redelivery within thirty seconds. Its exact trace is retained in
the review directory. Near-simultaneous ack, lease-extension and nack completions do
not prove server execution order. This does not establish a new Brighter defect.

All 6,614 main production/test sources are unchanged; 974 relevant isolated inputs
match main after temporary instrumentation is restored. Build: zero errors, 3,316
existing warnings. All three owned emulators are stopped. No production correction,
assertion change, dependency update, commit or push occurs. Complete evidence is in
`/private/tmp/brighter-4170-gcp-original-shutdown-diagnostic/`; compact results,
per-case native traces, scripts and verification are retained in the review folder.

A read-only Azure check still fails with AADSTS9002313 and requires interactive
login. Only local Docker contexts are configured; supported x64 SQL Server 2022
coverage remains unavailable. The [Kafka source follow-up](review/kafka-native-close-diagnostic/README.md)
locates the indefinite native assignment queue wait without establishing its cause.
All outstanding release gates remain explicit in the completion plan.


## Original GCP async disposal result now observed — 2026-11-09

The [validated amendment](review/gcp-disposal-result/README.md) adds one await to
each of the two original async shutdown tests after its unchanged timeout assertion.
A scoped temporary production mutation proves the gap: both original tests pass
with a faulted initial disposal; both amended tests fail at the new await on each
framework. Production is restored exactly, and both cases pass afterward.

The first full matrices experience a 44.3-second wall/monotonic divergence, correlated
with a logged host idle-sleep event. Their complete results are retained separately.
The justified repeat inhibits idle sleep for the runner's lifetime and retains all
512 case identities per framework: **457 passed, zero failed, 55 skipped on .NET 9**;
**456 passed, one retained generated async ordered-nack failure, 55 skipped on .NET 10**.
Both clocks remain continuous. All four original shutdown cases pass in both matrices.

Main-workspace integration changes only the two awaits. Its build succeeds with
zero errors and 3,320 warnings; both changed cases pass on each framework from that
build. Verification covers all 6,614 existing production/test source files and
confirms exact production restoration. Owned fixtures are stopped; the sleep
inhibitor is released. No commit, push or dependency change occurs. Historical
lifecycle findings, Azure authentication and supported x64 SQL Server validation
remain open. Complete evidence: `/private/tmp/brighter-4170-gcp-disposal-result/`.


## GCP emulator version comparison — 2026-11-09

The unchanged [raw protocol control](review/gcp-emulator-comparison/README.md)
reproduces the ordered-redelivery stall on both pinned emulator 0.8.36 and official
Google Cloud CLI 592.0.0's emulator 0.8.37. Each of four combinations runs once per
framework/image: sixteen observations, twelve timely redeliveries and four timeouts.
Every timeout follows adding a stream and acknowledging the second message before
nacking the first. A separately recorded repeated acknowledgement releases the
first message in all four cases, preserving its native ID/body with a new ack ID.
Those recoveries do not turn the original timeouts into passes.

The baseline image is amd64; the candidate is native arm64. This exact-image
comparison establishes reproduction on the newer native image, without isolating
JAR version from all other image differences. Build: zero warnings/errors. All
four runs have continuous clocks, all owned fixtures are stopped, and all 6,618
source/build/protocol inputs remain unchanged during the matrix. Full traces and
compact evidence are retained in the review; complete artifacts are under
`/private/tmp/brighter-4170-gcp-emulator-comparison/`.

The tested update does not resolve this control. No project dependency/fixture,
production or test source is changed; no hosted service or Redis is rerun. The
upstream report is updated locally and remains unpublished. Historical shutdown,
ASB/Kafka lifecycle, native ASB acceptance and supported x64 SQL Server gates remain
open. No commit or push occurs.


## Kafka evidence availability and live capture — 2026-11-10

The [live-capture package](review/kafka-live-capture/README.md) preserves four healthy
consumer traces from the prior 412-pass diagnostic TRX and validates capture before
a test host exits. The original cancellation-recovery directory/raw failed stack
is no longer available locally; the earlier interpretation remains historical,
without recovered raw evidence. This does not clear the failure.

A diagnostic-only Linux startup hook redirects native stderr to a private mounted
file. The unchanged existing .NET 10 RoundRobin/CooperativeSticky cases run once:
both pass. While the runner is live, 21,462 bytes of native output are observed and
a nonterminating stack report captures 29 threads, nine with librdkafka frames.
Internal native symbols and queue memory are not supplied by that report. It is a
healthy capture validating the collection method, not a reproduced hang or a full
regression. A failing full-memory/native-thread capture remains required.

The hook and its final comment/license-only amendment build with zero errors and
warnings. All 6,616 source/build inputs remain unchanged during validation, and both
owned broker/schema fixtures are verified stopped. No product/test implementation,
dependency, hosted resource, commit or push changes. The Kafka release gate remains
open, along with the other lifecycle and external acceptance requirements.


## Kafka full native capture validated; fixed suite remains inconclusive — 2026-11-10

The [full-capture package](review/kafka-full-capture/README.md) validates live native
stderr, a 439,771,136-byte ELF memory core, process maps and native task names/states
on the existing .NET 9 commit/revoke cases. Both pass after nonterminating capture.
The healthy report contains 28 threads, eight with librdkafka frames. The exact
packaged library retains `.symtab`; all forty recorded native frames resolve to
containing function ranges, including Kafka main/queue/broker functions. There is
no DWARF type/variable reconstruction and no claim of decoded failing queue state.

A sixty-second diagnostic watchdog is verified against healthy close sequences
and an explicit uncompleted-event control, then used in one full .NET 9 run.
The suite passes all **412 cases, zero failures/skips**, preserving the exact prior
case identities, in 899.577 monotonic seconds. Clocks remain continuous. All four
instrumented consumers close in 5–13 ms; the watchdog never triggers. This is a
passing diagnostic attempt, not a reproduction or fix of the historical stall.
The only memory core collected is clearly labelled healthy validation.

Both fixtures are verified stopped. All 6,616 source/build inputs and recorded
read-only diagnostic binaries remain unchanged. The memory core stays in
`/private/tmp/brighter-4170-kafka-full-capture/`; compact results, native traces,
thread reports, scripts, checksums and TRXs are retained in the review. No product
or test source, assertion, dependency, commit or push changes. Another identical
green run is not a release criterion. The original missing raw stack and unresolved
cause remain explicit limitations. Azure authentication still fails AADSTS9002313;
external ASB and supported x64 SQL Server acceptance remain open.
