# Local implementation review — 2026-11-10

The local requirement review and evidence reconciliation are complete. The issue
is **not production-ready**: the lifecycle findings and external acceptance gates
below remain unresolved. This closes the local review activity, not those gates.

The review combines the retained gateway packages with inspection of current
mapping, dispatch and delivery-identity boundaries. It is not a fresh execution
of every suite or a claim to have reread every line in the working tree.
The demonstrated ASB/GCP bulk tracing defect is fixed and validated in the
[bulk dispatch package](review/bulk-dispatch-trace/README.md).
The follow-up boundary review found no additional demonstrated code defect.

## Requirement disposition

Numbers refer to [the ten behavior requirements](requirements.md#behavior).

| Requirement | Local disposition and supporting evidence |
| --- | --- |
| 1. Endpoint selection | Typed publication/subscription settings exist across the gateway matrix. The [configuration review](configuration-design-review.md) records the API choice and factory/isolation evidence. GCP deliberately has distinct pull and streaming reader types. |
| 2. Default compatibility | Existing constructors and default adapters remain. The [compiled API audit](review/binary-api-audit/README.md), default-wire regressions and latest eight ASB/GCP API comparisons cover their recorded inputs. API compatibility does not establish runtime compatibility in unavailable environments. |
| 3. Complete mapping and composition | Public default adapters support composition; selected readers run before default parsing rather than requiring the default format. [Usage examples](usage.md) and the configuration review cover complete replacements and selective changes. |
| 4. Mapping before consumer tracing | Consumer boundaries produce Brighter messages before handing them to the pump. The [non-CloudEvents telemetry evidence](review/non-cloud-events-telemetry/README.md) and linked AWS/RabbitMQ pump packages cover application handling and settlement. They are not native acceptance for every gateway. |
| 5. Dispatch-time writing | Immediate, scheduler callback and bulk paths are accounted for in the [scheduler audit](scheduler-path-audit.md) and bulk correction. AWS native targets require explicit callback mode for custom mapping; native services cannot execute application converters. Azure callback acceptance remains deferred. |
| 6. Native delivery operations | The boundary inventory below identifies gateway-owned controls and provider-specific settlement policies. Selected retry/forwarding implementations are recorded in the scheduler audit. Native ASB retry, DLQ and lost-lock cases remain open. |
| 7. Source preservation and reuse | Default writers create per-call output; string/JSON formats serialize per call. Kafka and RocketMQ copy mutable body arrays. RabbitMQ retains read-only body memory and borrowed nested header values, requiring replacement before editing. Existing ownership and concurrency packages remain applicable; this is not a new concurrency run. |
| 8. Mapping failures | Existing failure/recovery packages cover publication, outbox and reader boundaries. GCP bulk can publish earlier items before a later failure; forwarding and settlement are not atomic. MQTT, Redis and SQL retain their documented transport-specific failure policies. No universal retry or lossless-delivery guarantee is inferred. |
| 9. Limits and reserved controls | SQS/SNS carriers exclude destination/FIFO operations; RabbitMQ publishers apply delivery controls after conversion; ASB retains SDK batch admission and oversized-send handling. No new field truncation was found in the reviewed writer paths. Payload validity still belongs to SDK/broker validation and the configured writer. |
| 10. Gateway coverage | Every listed gateway has an implementation and retained local evidence in the [current gateway table](completion-plan.md#current-gateway-gaps). Full release acceptance remains open for the explicitly listed runtime and lifecycle gates. |

## Boundary inventory

These are source-inspection conclusions, supported by the existing gateway packages.
They do not replace native acceptance.

| Gateway | Writer boundary and controls | Reader boundary and delivery identity |
| --- | --- | --- |
| SQS/SNS, SDK v3/v4 | `SqsMessageSender` and `SnsMessagePublisher` combine mapped body/attributes with resolved destination and source FIFO controls. | SQS restores the native receipt handle and resolves delivery count after selected reading. |
| RabbitMQ Async/Sync | Publishers apply persistence, delay, delivery and retry identity controls after conversion; confirmation tracks the source identity. Default AMQP type validation remains. | Consumers stamp delivery tag, redelivery, persistence and canonical handled count after selected reading. |
| Kafka | Selected writer or default/header-builder strategy runs before native publication; source identity remains available for confirmation. There is no exported Brighter bulk/transaction lifecycle to map. | Selected results receive the actual topic/partition/offset. Failed mapping retains raw content and the real offset for rejection. |
| ASB | Individual and bulk paths map after tracing; publication TTL and SDK batch admission remain. Retry schedulers separately own retry identity/session/partition/count. | Selected reading retains native lock token and sequence number. Cancellation and native lock behavior remain subject to the unresolved acceptance gates. |
| GCP | Individual and bulk sends map after tracing. Producer configuration and source partition determine native ordering. | Pull restores the ack ID; streaming restores the delivery wrapper. Both resolve delivery counts after mapping. |
| MQTT | Publisher applies configured topic prefix/destination and QoS after conversion. | Mapping runs in the SDK callback. The SDK owns protocol acknowledgement; null/throwing custom results do not become fabricated Brighter deliveries. |
| Redis | String mapping is separate from queue selection and native storage/counters. | Consumer tracks native IDs outside application mapping. Invalid-message behavior retains the existing destructive-read policy documented in the Redis review packages. |
| RocketMQ | Publisher retains topic, keys, tag, FIFO group and delivery timestamp outside the body/property carrier. | Consumer restores the native `MessageView` receipt and resolves broker delivery count. |
| SQL Server | Queue callback maps payload only; queue operations retain row metadata and read/delete behavior. | Reader receives payload and failures use native row identity; this does not add recoverable acknowledgement to read-and-delete. |
| PostgreSQL | Parameterized inserts retain queue/visibility separately from mapped content, including delayed sends. | Normal and large-message paths stamp the actual row receipt after mapping. |

## Corrections and verification

The Kafka usage text now describes the validated body/header-array copies instead
of the superseded shared-array convention. The writer ADR now explains AWS callback
mapping and explicit outbox recovery instead of calling scheduled mapping excluded.
Historical package counts and failure evidence remain intact.

[Final input verification](review/local-handoff-verification.json) checks source
continuity against the bulk package, including its four new files and three changed
existing files. Build configuration is checked against the retained solution-build
inventory. This documentation-only follow-up runs no tests and changes no production
or test code. Local links and whitespace are checked separately.

## Remaining release work

The subsequent [Docker retry](review/asb-reader-failure-recovery/README.md#docker-retry--2026-11-10)
adds fresh native failure evidence: eight passes on .NET 9, then four passes and
four topic failures on .NET 10 with the emulator's duplicate-sequence fault.
It leaves the release gates below open and requires no production correction yet.

The [2026-11-11 SDK-only diagnostic](review/asb-sdk-dlq/README.md) subsequently
reproduces the topic failure on both frameworks without Brighter. Queue controls
pass. An empty-DLQ warmup still fails on .NET 9, so it is not a reliable workaround.
This isolates Brighter out of that reproduction, but does not establish the exact
SDK/emulator/backend cause or close any of the historical lifecycle findings.

- Resolve the historical ASB session-lock, GCP shutdown/replacement and Kafka close
  findings with discriminating failing evidence or a demonstrated correction.
  GCP's separately reproduced emulator ordered-nack behavior does not explain every
  historical shutdown failure. The bounded review below specifies missing evidence.
- Complete native ASB retry/scheduler, reader-failure DLQ and lost-lock cancellation
  acceptance on a healthy service.
- Validate SQL Server 2022 on supported native x86-64 hardware. SQL Edge evidence
  remains separate.
- Complete remote CI/final release verification within the no-commit/no-push constraint.

There is no further unchanged local matrix queued. Resume a lifecycle investigation
when its required evidence exists, or perform deferred acceptance when access is
available. Do not restart credential checks, Redis runs or the same passing Kafka
audit merely because the release gates remain open.

## Earlier bounded lifecycle checkpoint

This checkpoint precedes the [bulk dispatch tracing correction](review/bulk-dispatch-trace/README.md).
The later requirement review reproduced and fixed stale ASB/GCP bulk trace context,
with affected regressions, API checks and main-workspace confirmation. Input counts
and unchanged-source statements below describe the earlier lifecycle review only.

At this earlier checkpoint, bounded lifecycle review and evidence reconciliation
were complete; final implementation/requirement review and release acceptance
remained open. This is not
a production-readiness sign-off. No production/test source, dependency or test
assertion was changed during this review, and no new test execution is claimed.

### Lifecycle findings

| Finding | Local review result | Evidence needed to resolve it |
| --- | --- | --- |
| ASB in-memory session lock | Reviewed helper construction, acceptance, receive, renewal and settlement against the [prior audit](review/asb-session-lock-failure-diagnostics/README.md). The helper starts expiry at construction, but reported passing samples retain nearly all the budget. No supported historical-failure reproduction or correction was found. The original temporary raw-log directory is absent; retained helper copies and the report are available. | A failing session trace with correlated closed/token/expiry state and acceptance/renewal/settlement timestamps, or a deterministic public-path reproduction of a demonstrated defect. |
| GCP original shutdown/replacement delivery | The [native x64 failing shutdown trace](review/x64-acceptance/docker-remaining/gcp/shutdown-analysis.json) now reproduces one original async case on .NET 10. Original-generation redelivery and deadline RPCs are observed; no replacement-generation native delivery is observed before timeout. Disposal completed. | Correlate replacement stream creation/SDK task termination and server deadline ordering. Successful zero-deadline responses do not establish execution order versus a canceled extension. Do not infer a mapping defect or automatically assign the separate ordered-nack cause. |
| GCP ordered nack | The [protocol comparison](review/gcp-emulator-comparison/README.md) reproduces the stall on emulator 0.8.36 and 0.8.37 without Brighter. The newer image is not a fix. The [disposal observation correction](review/gcp-disposal-result/README.md) is already integrated and validated. | Keep the known emulator failure separate from unexplained shutdown history and required final regression disposition. |
| Kafka native close | Both [native x64 passive full runs](review/x64-acceptance/docker-remaining/README.md) abort after 223 passes, before diagnostic capture. Complete x64 cores, exact-library symbols and direct native stderr now establish a wait through native rebalance-protocol request during SDK consumer close in the concurrency class. | Determine queue ownership and the SDK/native rebalance interaction using the failing cores; verify any demonstrated correction against the original full concurrency context. Separate remainder passes do not close this gate. |

The ASB table update and session expiry are separate operations, and production
stops tracked renewal before settlement. Source inspection alone does not establish
that either boundary caused the original failure. Changing these behaviors or
forcing an expired fixture would not resolve the historical finding.

The GCP passing traces likewise do not establish that all earlier shutdown failures
were caused by ordered-nack behavior. The release findings remain open. There is no
new production correction to validate from this bounded review.

### Inputs and evidence reuse at the earlier checkpoint

The current source/build inventory contains all **7,248** inputs recorded for the
latest complete Release/Debug solution build. There are no added or removed inputs.
Only the two previously validated async GCP shutdown tests differ, each by the
single `await disposing;` documented in the disposal-result package. Production,
project/build configuration, samples and benchmarks match that build snapshot.

All **6,616** inputs in the later Kafka capture source manifest match the current
checkout. This includes the strengthened GCP tests. These comparisons establish
input continuity, not a fresh solution build or an all-green runtime matrix.
See [verification](review/local-review-verification.json) for snapshot hashes,
changed-file hashes and the exact-await check.

The prior complete builds preceded the two test awaits; the later GCP build and
focused/full regressions validate that amendment. The known ordered-nack failure
and existing skips remain reported. Unchanged components do not need another
identical test run merely for this documentation correction.

### Tracking corrections and work identified at the earlier checkpoint

The active task list now distinguishes completed local diagnosis from unresolved
release findings. Retry/forwarding selection is implemented across the reviewed
gateways; ASB native acceptance remains outstanding. The scheduler audit retains
the implemented callback and durable-restart evidence, with Azure native acceptance
separate. Historical package results retain their original scope and counts.

The next local work identified then was the final requirement-to-implementation review, using
[requirements](requirements.md), [gateway status](completion-plan.md#current-gateway-gaps)
and [scheduler applicability](scheduler-path-audit.md). This checkpoint does not
claim a fresh line-by-line review of every changed gateway. Fix only a demonstrated
gap, then validate its affected components. Lifecycle findings reopen when the
specific evidence above becomes available.

Hosted ASB, native x64 SQL Server and remote CI remain deferred under the
[local-first plan](completion-plan.md). No Azure login check, host request, Redis
rerun, paid service, commit or push was performed.
