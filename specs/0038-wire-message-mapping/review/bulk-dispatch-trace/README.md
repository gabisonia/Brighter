# Bulk dispatch trace propagation

Status: reproduced, corrected and validated locally. Scoped review-after work for issue
#4170; no commit or push. Requirements 4–6 require mapping to see dispatch-time
trace context on applicable bulk paths as well as individual sends.

## Source finding

`OutboxProducerMediator` creates a producer span for a bulk group and assigns it
to the producer before batch creation. It does not itself inject that span into
each message. Individual ASB/GCP sends call `BrighterTracer.WriteProducerEvent`
before wire mapping; the ASB batch-creation and GCP bulk-send loops omit that
call. A configured writer can therefore receive stored trace headers instead of
the bulk producer context. Existing native batch-selection tests use fixed header
values, which do not distinguish those two contexts.

The ASB publication XML documentation also incorrectly says bulk sends do not use
its writer, although batch creation selects it. Correct that verified API guidance
alongside the bounded behavior change if the regression establishes the gap.

## Behavioral regression

Both tests deposit two messages carrying an old traceparent/tracestate, then invoke
the public bulk outbox clear path with a new parent span and baggage. Factory-created
producers and the real tracer supply the production path. Default and composed or
custom writer cases must emit the actual bulk producer span ID, current tracestate
and baggage, preserve each payload and identity, and clear the outbox.

GCP observes native messages and acknowledgement on a pinned local emulator. ASB
uses SDK-supported in-memory client/sender and message-batch models to capture the
submitted messages for both queue and topic producers. ASB SDK-I/O checks establish
mapping/submission behavior, not hosted broker acceptance, sessions or locks.

First observe assertion failures on stale trace headers without changing production.
Then make the minimal propagation correction before mapping, run both focused
framework matrices and the affected available regressions. Preserve failures and
case identities; keep known GCP emulator findings separate. No Redis rerun, hosted
access, package change or altered timeout is part of this batch.

Raw evidence directories:
`/private/tmp/brighter-4170-gcp-bulk-dispatch-trace/` and
`/private/tmp/brighter-4170-asb-bulk-dispatch-trace/`.

## Result — 2026-11-10

The original code fails all twelve focused observations: two GCP cases and four
ASB cases per framework. Each failure compares the actual producer span with the
old deposited traceparent received by the observer. No production mutation was
needed to establish RED. Builds and fixture setup failures are not counted as RED.

One propagation call is added before mapping in each bulk loop. ASB maps during
batch creation; GCP maps while sending each batch item. Both use the same
`WriteProducerEvent` operation already used by their individual-send paths. The
fix updates traceparent, tracestate and baggage before either default or selected
mapping. It does not change public signatures, scheduler behavior or dependencies.
ASB's publication XML now correctly describes individual and bulk writer selection.

All six new cases pass per framework after the correction. Full affected available
regressions retain every prior identity:

| Matrix | .NET 9 | .NET 10 | Inventory comparison |
| --- | --- | --- | --- |
| GCP available native Pub/Sub suite | 459 passed, 55 existing skips | 459 passed, 55 existing skips | All 512 prior identities plus two new cases |
| ASB local SDK-I/O suite | 540 passed, no failures/skips | 540 passed, no failures/skips | All 536 prior outcomes plus four new cases |

GCP wall/monotonic durations are 567.524/567.518 seconds and 562.347/562.342
seconds; neither run has a clock discontinuity. The 80 native-service ASB classes
remain excluded from the local inventory and deferred. Passing GCP runs do not
resolve its retained ordered-nack or historical shutdown findings.

Both gateways build for .NET Standard 2.0 and .NET 8/9/10 with zero warnings or
errors. All eight rebuilt assembly/target comparisons pass against the existing
compiled API baseline, with no unresolved references or compatibility suppressions.
Main-workspace builds and both focused framework matrices confirm integration.
Existing test-project warnings remain. Two new xUnit2031 warnings were removed by
using `Assert.Single(collection, predicate)` with the same predicate and assertion;
the final focused runs verify that mechanical cleanup. The full matrices used the
equivalent `Assert.Single(collection.Where(predicate))` form, recorded separately.

An initial sandboxed build progressed slowly and was stopped; it returned exit zero
without a complete test build, then native setup was denied. It ran no tests. Its
logs remain under `sandbox-build-interrupted/`; the accepted RED uses the subsequent
complete build with required local access. No timeout, assertion or coverage was
weakened to obtain passing results.

See [verification](verification.json) for source scope, focused/main outcomes,
regression inventories and build warnings; [API comparison](api-comparison.json)
for the eight compiled pairs; and [production patch](production.patch) for this
batch's exact source changes. Adjacent JSON files retain the RED and regression
results. Raw TRXs/build/broker logs remain in the evidence directories above.
Owned emulators are stopped after validation. No hosted resources, commits or pushes.
