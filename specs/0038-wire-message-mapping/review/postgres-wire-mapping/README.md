# PostgreSQL endpoint wire mapping

Status: initial native integration validated; broader scheduler/rejection/lifecycle gates remain open. No commits or pushes.

Completion-plan stage 2 adds endpoint-selected JSON content writing and reading.
The SQL schema still requires JSON/JSONB. Mapping owns only content; the gateway
owns queue names, native row identity, visibility, locks and deletion. Default
serialization and outbox serialization are unchanged. Explicit default adapters
support composition. Custom large-message readers opt into materializing JSON text;
the unconfigured large-message path keeps its existing streaming behavior.

A separate subscription requeue writer is necessary: the reader cannot infer its
inverse, and immediate/native delayed requeue updates the same SQL row. Its default
continues using existing JSON serialization. Consumer factories capture reader and
requeue writer selections at creation, consistent with the other gateways.

## First native contract

Sixteen cases cover sync/async factories, JSON/JSONB, normal/large reads and complete/
composed mappings. Both immediate and native delayed publication must use the selected
writer with dispatch-time trace context. A custom outer JSON object cannot be consumed
by the default parser. The selected reader recovers body, identity and metadata; its
forged receipt handle must be replaced by the actual row identity. Native visibility
prevents early receipt. Requeue uses the selected writer and retains the same row;
acknowledgement deletes it. Other publications and caller body/bag remain untouched.

Named mutations after implementation: bypass producer writer, bypass consumer reader,
omit receipt replacement, and bypass requeue writer. Each must fail the matching
native assertion, followed by exact restoration and a passing run. Additional writer/
reader failure cases and complete Postgres project regressions are required before
this integration is complete.

## Failure contract and validation

Four writer-failure cases and eight reader-failure cases were added before implementation.
Writer exceptions retain their exact instance; null results fail before insertion.
A later send through the same producer succeeds after recovery. Failed custom readers
return an unacceptable message with raw JSON and actual row ID. Rejection deletes that
row; subsequent reads and acknowledgement work after reader recovery.

Two additional mutation obligations extend the original list: producer-writer bypass
also fails the four error-propagation cases; removing reader failure containment fails
the eight rejectable-message cases. Total expected mutation failures: 76.

The first builds recorded missing public APIs. Test setup corrections retained all
assertions: use the rejection-reason constructor, specify Reactor/Proactor, and configure
the OpenTelemetry SDK as existing native telemetry tests do. These setup failures are
not claimed as product regressions. The corrected 28-case focused matrix passes.

## Final native integration evidence, 2026-11-07

The focused 28-case matrix passes. Five named mutations produce 76 intended native
assertion failures and pass after exact source restoration. The receipt-omission
mutation initially failed compilation because removing a null-forgiving dereference
changed nullable flow analysis. That run is not counted; the corrected mutation retains
only the return annotation and fails all 16 intended receipt assertions.

The complete project suite passes 317 cases with no failures or skips, preserving all
289 preceding identities and outcomes. This project targets .NET 10 only. The gateway
builds on netstandard2.0, net8.0, net9.0 and net10.0 with zero warnings/errors.
Four existing gateway files change; two public default adapters and six test/helper
files are added. Existing schemas, outbox serializers, project files and other source
files remain unchanged. No commits or pushes were made.

Evidence: `/private/tmp/brighter-4170-postgres-wire-mapping`. This establishes the native
mapping integration, not completion of the whole cross-gateway release matrix.
