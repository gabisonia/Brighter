# ASB queue retry writer selection

Status: implemented and integrated locally. Local regressions pass; hosted native
scheduling acceptance remains open. No commits or pushes.

Review-after for #4170. Twelve public-factory cases cross native/global Azure retry
scheduling, sync/async and success/throw/null mapping. They use the existing in-memory
Service Bus client/sender/receiver as I/O and inspect native SDK messages. They do not
claim hosted broker behavior. Native mode captures the consumer factory publication's
existing WireMessageWriter; global mode captures AzureServiceBusSchedulerFactory's
explicit RequeueWireMessageWriter. No inverse reader is inferred.

Observe ignored writer RED before implementation. Preserve mapped body/metadata,
deterministic native retry ID, original CloudEvents identity, session/partition,
handled count, fresh trace context, unchanged caller and no source completion on
mapping failure. Recovery schedules once and completes only afterwards. Keep the
legacy no-writer body path and public constructor signatures. Full native acceptance
is still required on a healthy Service Bus namespace; Azure login currently needs
refresh. No commits or pushes.

Evidence: `/private/tmp/brighter-4170-asb-retry-writer-selection/`.

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
this review package.

Hosted scheduling/DLQ/lost-lock acceptance and the retained session-lock finding remain
open. Nothing committed or pushed. Evidence:
`/private/tmp/brighter-4170-asb-retry-writer-selection/`.

Main-workspace confirmation passes twelve selected-writer cases and all thirteen
Azure scheduler tests per framework. These runs include the global adapter and
unchanged ordinary scheduler envelope/context serialization.

Post-integration verification: twelve main-workspace retry-writer cases and all
thirteen Azure scheduler tests pass per framework. Complete Release and Debug
solution builds pass with zero errors, including both changed libraries on
netstandard2.0, net8.0, net9.0 and net10.0. All 7,246 recorded source/build
inputs remain unchanged through both builds; clocks are continuous. Repository
warnings remain (22,073 Release; 821 Debug).
No native fixtures were started for this package. No commits, pushes or remote CI.
