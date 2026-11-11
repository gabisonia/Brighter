# RabbitMQ Sync scheduled retry selection

Review-after for #4170; no commits or pushes. Two native default/custom cases provide
Sync parity for the [Async selection contract](../rmq-scheduled-selection/README.md).
The Sync consumer has one synchronous receive/retry path. The real in-memory scheduler
returns through the command processor and registered producer, with native queue-only
routing, source preservation, dispatch tracing, confirmation and outbox settlement.

The same nullable subscription selector and publication destination queue preserve
existing behavior when unset. Public constructor signatures remain unchanged. A paired
application writer uses the Sync callback-based property contract.

Observe RED before implementation. Then omit the selector, ignore the queue destination,
and alias the source header in three separate production mutations: two intended failures
each, exact restoration and two restored passes. Require complete Sync suites on both
frameworks, original identity/outcome retention and all library targets before integration.

## Completed validation — 2026-11-07

The original two cases fail on both frameworks before implementation. All six
intended mutation failures and exact restorations are verified. Final suites pass
293 cases with one existing skip per framework, preserving all 292 baseline
identities/outcomes and adding two cases. All four library targets build.
The test explicitly waits for the stopped publish span, matching the Async
synchronization correction; all mutation and full-regression checks were repeated.
The six production files and two test/helper files are integrated locally after
source-hash checks. No commit or push was made. This completes the scoped parity
package; broader scheduler/positive-delay compatibility gates remain open.
