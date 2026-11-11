# Full issue completion plan

Status: final blocker resolution in progress; not production-ready. Updated 2026-11-11;
scope and recorded evidence remain tied to
[#4170](https://github.com/BrighterCommand/Brighter/issues/4170),
[requirements](requirements.md), [tasks](tasks.md) and the recorded test evidence.

## Active completion plan — 2026-11-11

This is the execution plan. The dated checkpoints below retain historical evidence;
they are not additional queues to restart. Implementation and local review are done.
Remaining work is limited to the release blockers below. No new gateway capabilities,
dependency upgrades or broad refactoring are included without a demonstrated need.
All changes remain local: no commits, pushes, paid resources or Redis reruns.

| Order | Deliverable | Required result | Status |
| --- | --- | --- | --- |
| 1 | ASB DLQ cause isolation | Reproduce the Docker topic/DLQ failure using only the native Azure SDK, the same image/configuration and original receive/dead-letter sequence. Record native IDs, sequence numbers, settlement and broker errors. A fixed queue control distinguishes topic-specific behavior. | Complete: SDK-only probe reproduces four topic failures and four queue passes on each framework; no Brighter dependency |
| 2 | ASB failure disposition and acceptance | If Brighter or fixture code is responsible, demonstrate RED, fix it and run affected full regressions. If SDK-only execution reproduces the failure, retain a minimal upstream reproduction and identify the healthy-broker requirement explicitly. Complete outstanding reader-recovery mutations, retry scheduling and lost-lock acceptance only on a fixture meeting their preconditions. | Local disposition complete: SDK-only failure retained; reader mutations, scheduling and lost-lock acceptance await a healthy isolated broker |
| 3 | Lifecycle findings disposition | For ASB session locks, GCP shutdown/replacement and Kafka close, reconcile each retained failure with its exact source/environment and a concrete causal hypothesis. Run a bounded diagnostic only when it can distinguish that hypothesis. Produce a fix with regression evidence, an established external cause, or an unresolved release blocker with the exact missing observation. | Disposition review complete; three historical lifecycle findings remain unresolved with exact missing evidence in local-review.md |
| 4 | Supported environment acceptance | Run SQL Server 2022 on native x86-64 and required hosted ASB cases using existing resources. Collect final-source CI evidence when possible without commit/push. Prepare exact commands/source identification before requesting any missing access. ARM64 emulation and runs of older remote commits cannot satisfy these checks. | External access required; do not poll credentials |
| 5 | Final verification and decision | Check requirement coverage, source/build/API evidence continuity, all mandatory outcomes and remaining skips. Run affected suites for any new fixes; reuse unchanged evidence. Publish a local final report listing either completed release gates or the precise blockers. | Local checkpoint complete; release decision remains blocked by steps 2–4. Recheck affected evidence after those gates close |

### Investigation stop rules

- A diagnostic starts with a hypothesis, fixed scenarios, a finite deadline and the
  observation that would support or reject the hypothesis. Save results before any follow-up.
- No repeated full suite solely to seek a green result. A pass never erases an earlier failure.
- A failed setup is not a behavioral RED. Do not weaken assertions or silently omit
  cases to make an acceptance gate pass.
- Each code correction requires a failing behavioral case, an exact change scope and
  the affected regression checks. Historical unrelated source changes are preserved.
- If a local investigation cannot resolve a gate, name the missing evidence and
  move to the next actionable item. Reopen it only for new evidence or available access.
- The final report must distinguish **implementation complete**, **locally validated**
  and **production-ready**. Only the last requires every mandatory release gate closed.

### First bounded experiment

Use a standalone console probe with only `Azure.Messaging.ServiceBus`, no Brighter
reference. On one fresh Docker fixture per declared framework, perform four topic
and four queue cycles: send a binary message, receive it in PeekLock mode,
dead-letter it, receive it from the DLQ in ReceiveAndDelete mode, then send/complete
a healthy message. Preserve original identity/body assertions and native broker logs.
Each operation has a deadline and the outer runner stops after the fixed scenarios.
An SDK-only duplicate-sequence fault establishes that Brighter is not necessary for
that reproduction; it does not by itself prove which SDK/emulator component is faulty.
If it does not reproduce, compare the SDK call/receiver lifetime with Brighter before
choosing one further discriminating experiment. Do not repeat the same sample blindly.

The first experiment reproduced the same duplicate-sequence broker fault on both
frameworks. The direct SDK receive/dead-letter path is sufficient; Brighter's
lock-token settlement reconstruction is not required. Results and the standalone
probe are retained under `review/asb-sdk-dlq/`.

One follow-up tests an initialization hypothesis: on a fresh fixture, establish an
empty native DLQ receive before the first dead-letter operation, then run the same
eight scenarios with unchanged assertions. The warmup must itself observe an empty
DLQ and its client is disposed before the scenarios. This tests whether initialization
with a queued first message triggers the duplicate load; it is not an assumed fix.

The follow-up also fails all four topic cycles on .NET 9; all eight .NET 10
cycles pass. Empty-DLQ initialization is therefore not a reliable workaround.
The [SDK reproduction report](review/asb-sdk-dlq/README.md) retains both outcomes,
unchanged assertions and exact source provenance. This bounded diagnostic is
finished; no unchanged rerun is queued.

The lifecycle disposition review reuses the [missing-evidence table](local-review.md#lifecycle-findings).
ASB requires correlated failing lock state/timestamps; GCP requires a failing
native-delivery-to-callback trace; Kafka requires a failing native close capture.
These remain release blockers, not new implementation tasks without a demonstrated
cause. The completed local checkpoint is recorded in the
[release review](release-review.md#completion-checkpoint--2026-11-11).

## Completion contract

Every gateway must expose endpoint-selected writing and reading, preserve default
wire compatibility and delivery controls, and support default-mapping composition.
Dispatch-time traceparent, tracestate and baggage must survive custom metadata or
body formats through application handling and settlement. Applicable scheduled,
retry, forwarding, batch, transaction and streaming paths must use the intended
mapping. The configuration choice must address the issue's usability concern.

Passing a scoped package closes that package only. Existing successful evidence
remains valid for its recorded source state; final readiness requires the completed
implementation and final validation. No open implementation row, unexplained bypass,
unresolved regression or unverified mandatory environment can be silently marked done.

## Earlier local execution order — 2026-11-10

Finish work possible with the checkout, existing local fixtures and retained
evidence before returning to external acceptance. Azure authentication, hosted ASB
checks and supported x64 SQL Server validation are explicitly deferred. Do not
recheck credentials, request a host or make those prerequisites for continuing
the local queue. Resume them when the local pass is complete or access is supplied.

This changes priority, not release requirements. A local diagnostic can end with an
explicit unresolved finding; that finding stays open while work advances to the
next actionable item. Do not repeat an unchanged passing matrix to fill the gap.

| Order | Work | Exit criterion |
| --- | --- | --- |
| 1 | Investigate the ASB in-memory session-lock failure locally | Use the [existing failure-state audit](review/asb-session-lock-failure-diagnostics/README.md) to distinguish construction-to-accept delay, renewal scheduling and settlement state. Establish a discriminating reproduction before changing code. A fix requires a failing behavioral regression, restoration/mutation evidence where applicable and affected full regressions. If no supported reproduction is available, record the missing evidence and advance without another unchanged green audit. |
| 2 | Resolve actionable GCP shutdown/recovery gaps | Review the original unsettled-message and buffered-redelivery shutdown paths using retained phase/native traces and the strengthened disposal-result tests. Keep the separately reproduced emulator ordered-nack defect distinct. Correct any demonstrated Brighter/test defect through the public path and validate affected suites; otherwise retain a precise unresolved lifecycle finding. Neither an emulator upgrade nor a successful rerun is an assumed fix. |
| 3 | Address Kafka only with a new discriminating hypothesis or failure | Full native capture and exact-library symbol resolution are validated; the fixed 412-case run is complete. Reuse them if a new failure or concrete reproduction arises. Do not schedule another identical full suite. A change requires a demonstrated cause and a regression that fails for that cause. |
| 4 | Review the final local implementation and reconcile evidence | Review remaining issue-related changes for requirement gaps and defects; reconcile stale task rows against current evidence. Fix only demonstrated gaps, then run the required checks for changed components. Reuse valid build/API/default-wire/example evidence where inputs are unchanged. Produce a local handoff that lists unresolved findings and deferred acceptance separately. |
| 5 | Return to deferred external acceptance and final release decision | Complete the external checks below when access is available. Only mark the issue production-ready after required acceptance and unresolved regression gates have a supported resolution. |

**Local review checkpoint — 2026-11-10:** the bounded ASB and GCP source/evidence
reviews are complete, with unresolved lifecycle findings retained. Kafka has no
new failing capture or discriminating hypothesis, so another matrix is not queued.
The [local review record](local-review.md) separates those outcomes from fixes and
records input verification. The subsequent [requirement disposition and local
handoff](local-review.md#requirement-disposition) complete the local review activity;
external acceptance and historical lifecycle findings remain open. Do not restart the same diagnostic queue
solely because these release findings remain open.

The subsequent [requirement review found and fixed stale trace context in ASB/GCP
bulk dispatch](review/bulk-dispatch-trace/README.md). Both adapters now propagate
the bulk producer context before mapping. New regressions reproduce the defect on
both frameworks; ASB local matrices pass 540 cases each and GCP matrices pass 459
with 55 existing skips each. All prior identities remain, eight rebuilt API pairs
pass, and main-workspace checks confirm integration. This supersedes the earlier
input-continuity checkpoint for these two producer implementations; it does not
close the historical lifecycle or deferred native acceptance gates.

Local stages 1–4 are complete for the available evidence. Stage 5 remains pending;
its outstanding work is enumerated in the [handoff](local-review.md#remaining-release-work).
No further unchanged local test matrix or credential polling is queued. A newly
demonstrated defect reopens the relevant local implementation work.

## Deferred external acceptance

The requested [Docker retry](review/asb-reader-failure-recovery/README.md#docker-retry--2026-11-10)
passes all eight reader/DLQ cases on .NET 9 but reproduces the emulator's duplicate
sequence-number fault on .NET 10 (four queue passes, four topic failures).
Fresh containers and the current published image do not remove the healthy-broker
requirement. This provides local failure evidence without Azure login or cloud costs;
it does not close native ASB acceptance or supported-x64 SQL Server validation.

| Work held for later | Resume condition | Required evidence |
| --- | --- | --- |
| ASB native retry scheduling, DLQ reader failures and lost-lock cancellation | Existing healthy namespace and usable authentication are available; no new paid resource is implied | Native queue/session timing, duplicate detection, settlement and failure/recovery acceptance for the implemented writer selection |
| SQL Server 2022 runtime validation | A supported native x86-64 environment is available | Required runtime matrix on SQL Server 2022; existing SQL Edge results remain separate |
| Remote CI / final external release verification | It can run within explicit authorization and the standing no-commit/no-push constraint | CI evidence or an explicit remaining limitation; local passes do not stand in for an unrun remote check |

Within a stage, implement only behavior required by the contract or a reproduced
defect. An unrelated pre-existing test failure needs a cause and an explicit impact
assessment; an intermittent pass is not a resolution. Do not add new capabilities
merely to accumulate more test cases. If evidence reveals a new blocker, attach it
to the requirement it threatens and update that stage's exit criterion.

## Current gateway gaps

| Gateway | Remaining completion work |
| --- | --- |
| AWS SDK v3/v4 | Endpoint/default mapping, rejection controls and automatic scheduled callback-pump failure/explicit outbox recovery are validated (627 passes per SDK/framework); scoped parity and compiled API checks are complete. Common release gates remain below. |
| RabbitMQ Async/Sync | Direct/native-delay and scheduler retry selection, queue isolation, positive-delay destination routing and unsupported-delay fallback are validated (566/two skips and 299/one skip); scoped parity and compiled API checks are complete. |
| Kafka | Selected dead-letter forwarding and scheduled retry publication lookup are validated (412 passes per framework). The fixed .NET 9 native-close diagnostic also passes all 412 but does not resolve the archived assignment/close stall. The [applicability review](review/kafka-default-body-ownership/README.md#batch-and-transaction-applicability) identifies no exported bulk or transaction lifecycle. |
| Azure Service Bus | Native/global retry writer selection and bulk dispatch trace propagation are implemented with 540 local passes per framework; complete hosted scheduling acceptance, resolve DLQ-blocked cases, session-lock instability and lost-lock cancellation failure injection |
| GCP Pub/Sub | Consumer-created rejection selection, real scheduled callbacks and bulk dispatch trace propagation are validated. Latest full matrices pass 459 cases/55 existing skips on both frameworks; earlier ordered-nack and shutdown findings remain unresolved |
| MQTT | Endpoint/default mapping, consumer-created immediate retry/rejection and scheduled retry selection are validated (121 passes/four skips per framework); scoped integration and compiled API checks are complete. |
| Redis | Selected endpoint/retry/rejection mappings, failure/cancellation recovery and the certified paced aggregate are validated (203 passes/four skips per framework); scoped default-framing/API checks are complete. No quota rerun is needed. |
| RocketMQ | Endpoint/rejection mapping, native delay and real scheduled callbacks are validated (136 passes/two skips per framework); all 130 prior outcomes plus eight new cases are retained. Compiled API checks pass. |
| SQL Server | Configuration/schema integration, rejection and scheduled retry selection pass 396 cases/four skips on both frameworks using canonical ARM64 SQL Edge. Supported x86-64 SQL Server 2022 runtime coverage remains required; the Rosetta startup crash is an unsupported-environment finding. |
| PostgreSQL | Native reader/writer/requeue and rejection mapping pass 341 cases on the declared .NET 10 test target. Native visibility, schema/outbox applicability and compiled API checks are reconciled; see the scheduler audit for limits. |

Detailed test identities, failures and accepted corrections remain in the existing
review packages. This table is an execution index, not a replacement for their evidence.

## Current checkpoint — 2026-11-09

Validated changes are integrated locally, with focused main-workspace confirmation.
Current complete regression results are scoped to each package's recorded inputs:

| Project | Result per declared test framework |
| --- | --- |
| AWS SDK v3 and v4 | 627 passes each on .NET 9 and .NET 10 |
| RabbitMQ Async / Sync | 566 passes/two skips; 299 passes/one skip |
| Kafka | 412 passes |
| ASB local SDK-I/O inventory | 536 passes, all 524 prior outcomes retained; 80 native-service classes excluded and still required separately |
| MQTT | 121 passes/four skips |
| RocketMQ | 136 passes/two skips |
| SQL Server | 396 passes/four skips |
| PostgreSQL | 341 passes on its declared .NET 10 target |
| GCP available Pub/Sub matrix | net9: 457 passes/55 skips; net10: 456 passes/one known async ordered-nack failure/55 skips; rejection and scheduled-callback cases pass |
| Redis paced aggregate | 203 passes/four skips, 207 identities once per framework; not a single-process suite |
| Hangfire / Quartz / TickerQ | 54 / 53 / 54 passes; provider-specific durable restart evidence recorded separately |

After GCP/RocketMQ scheduling-test integration, both complete main-workspace Release
and Debug solution builds pass with zero errors. All 7,248 recorded source/build
inputs remain unchanged; repository warnings remain (22,073 Release, 4,107 Debug).
The new GCP/RocketMQ test files emit no build warnings. Main-workspace confirmation
passes four GCP and eight RocketMQ cases per framework. All 68 compiled API comparisons
match the final Release outputs; 24 regenerated binaries were rechecked and 44 were
byte-identical. `git diff --check` passes. No issue-owned test containers remain running;
containers and logs are retained, and unrelated services remain untouched.

The compiled non-CloudEvents telemetry examples pass within the complete AWS/RabbitMQ
regressions. Both ADRs and local usage/external documentation now describe the typed
composition design, consumer selections and the native retry validation boundary.
The issue remains not production-ready for the concrete gates below.

## Concrete outstanding release gates — evidence inventory, updated 2026-11-10

The following numbered gates retain their identity as evidence requirements, not
requests for new features or the execution order. The active five-step plan above governs execution;
these identities preserve the earlier release evidence requirements.

1. Complete native queue/session scheduling acceptance for the implemented ASB
   retry writer selection. Both adapters pass twelve new SDK-I/O cases and all 524
   previous local outcomes per framework. This does not replace hosted broker
   duplicate-detection, timing and settlement evidence.
2. Complete ASB DLQ reader-failure and lost-lock cancellation checks on a healthy
   service. The emulator reproduces duplicate-sequence/entity-abort failures and
   does not establish the required lost-lock injection. Existing Azure authentication
   requires a refreshed login; no new paid namespace is authorized or provisioned.
3. Resolve or give a cause-supported disposition to the retained ASB in-memory
   session-lock failure, Kafka native assignment/close stall and GCP streaming
   redelivery/shutdown failures. A subsequent passing run is not the disposition.
   The fixed Kafka native-debug run passes 412 cases but is inconclusive. GCP's exact
   generated failure is captured at native input; the lease-order control supports
   the emulator stream-membership finding without resolving separate shutdown history.
   The subsequent [original shutdown diagnostic](review/gcp-original-shutdown-diagnostic/README.md)
   passes twelve shutdown observations with correlated native recovery. Its fixed
   512-case matrix retains a separate ordered-nack failure (456 pass, one fail,
   55 skip), again without native redelivery after successful nack. Shutdown history
   remains unresolved. The [pinned Kafka source review](review/kafka-native-close-diagnostic/README.md)
   identifies the native queue wait to capture next, without claiming its cause.
   The [live-capture follow-up](review/kafka-live-capture/README.md) preserves the
   healthy native traces and validates durable stderr plus native-thread stack
   collection on two existing cases. The original failed raw stack is no longer
   available locally; its historical interpretation is not fresh evidence.
   A failing capture with native memory/thread state remains required.
   [Full native capture is now validated](review/kafka-full-capture/README.md),
   including thread names/states and exact-library internal symbol resolution.
   Its one full .NET 9 diagnostic run retains all 412 identities and passes, with
   no capture trigger. This supplies a collection method, not a reproduced cause;
   another identical green run is not the next release criterion.
   The [async disposal-result gap](review/gcp-disposal-result/README.md) is now fixed
   in the two original shutdown tests: four intended mutation failures prove the
   added awaits, and both main-workspace cases pass per framework. Final continuous-
   clock matrices retain all 512 identities: 457/0/55 on .NET 9 and 456/1/55 on .NET 10.
   The retained .NET 10 failure is ordered redelivery; historical lifecycle gates
   remain open. An interrupted pair of matrices is preserved separately.
   The [emulator version comparison](review/gcp-emulator-comparison/README.md)
   reproduces the same raw-protocol stall on 0.8.36 and 0.8.37, on both frameworks:
   twelve timely redeliveries and four timeouts, with recovery recorded separately.
   The newer native arm64 image does not fix this control; no dependency or fixture
   update is made. This does not resolve the separate historical shutdown findings.
4. Complete SQL Server 2022 runtime validation on a supported x86-64 host. The retained
   ARM64/Rosetta startup crash is an unsupported-environment finding, with no product
   root cause established. Canonical ARM64 SQL Edge regressions remain separately validated.

The main-workspace/build, GCP/RocketMQ delayed-publication and scoped public API
verification gates are now complete. Existing default-wire and executable
non-CloudEvents example evidence is retained. See the [compiled API review](review/binary-api-audit/README.md)
for its exact baseline, target matrix, optional attribute review and final binary hashes.

Remote CI remains unrun. No commit or push will be used to trigger it under the
current instruction; local checks and unavailable hosted coverage must be reported
separately. Completed packages do not authorize removing tests or buying capacity.

The latest 2026-11-10 Azure check still reports AADSTS9002313. The last Docker-context
inventory contains only local Unix sockets, with no configured remote x86-64 host.
These limitations remain recorded for the deferred phase; they do not block the
active local queue and will not be repeatedly rechecked during it.

## Execution constraints

Preserve existing work, tests, dependencies and default behavior. Use native broker
or database fixtures for adapter verification. Retain failure evidence and compare
test identities/outcomes after each approved package. Redis regression batches
must respect the established real-time quota intervals; do not reset quotas,
manipulate clocks, buy a license or remove coverage to get a pass.

Review-after is explicitly authorized for issue #4170 only. Its working state is
recorded in `.current-gear`. Failing-test-first, named mutation checks and full
regressions still apply; completed batches are reviewed after validation.
Do not commit or push any work. Environment-level execution permissions remain
separate from test review.

Implementation and validation do not themselves close the upstream issue, publish
packages, deploy, or establish maintainer acceptance. Record those actions separately
if they become part of the authorized delivery.
