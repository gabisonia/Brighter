# ASB session-lock renewal investigation

Status: unresolved. No production fix or test change is justified by the current evidence.
The two earlier MessageLockLost failures remain a production-readiness finding.

## Original failures

During oversized-send recovery validation, the first .NET 10 full suite passed 566
cases and failed queue/async/session/buffer-size-1 in AzureServiceBusLockRenewalTests.
A focused unchanged rerun passed 15/16 and failed topic/sync/session/buffer-size-1.
Both fail when the in-memory receiver completes the delivery. A later unchanged full
suite passed all 567 cases. Source and test changes were not made to obtain that pass.
Original evidence: `/private/tmp/brighter-4170-asb-oversized-send-recovery/`.

## Bounded timing audit

Inspected ServiceBusLock renewal and ServiceBusReceiverWrapper settlement. Completion
stops renewal through ForgetAsync before invoking native completion. That ordering was
a hypothesis worth measuring, not an established cause of the failures.

Temporary production diagnostics recorded renewal scheduling, attempts, completion,
expiry, cancellation/stop and settlement timestamps. Tests and their helpers remained
unchanged. Each diagnostic script restored the exact original source in a finally
block and rebuilt all library/test targets. The test processes used Docker with no
network; these are existing in-memory session/message lock tests, not native broker
compatibility evidence.

- Complete existing lock-renewal class: 16/16 pass on .NET 10 and 16/16 on .NET 9 with diagnostics.
- Four session/buffer-size-1 cases repeated five times on .NET 10: 20/20 pass with diagnostics.
- Same four cases repeated five times after removing diagnostics: 20/20 pass.

These are 72 passing executions of existing cases, not 72 new tests. In the 20 focused
instrumented executions, at least approximately 1,001 ms remained on the latest known
session lock when completion began. The largest measured interval from stopping
renewal to invoking completion was approximately 3.16 ms. The samples do not show a
settlement gap or late renewal sufficient to explain the original failures. Timestamps
use wall-clock time; they are observations, not a proof of scheduling guarantees.

## Conclusion and next diagnostic boundary

Neither instrumented nor uninstrumented bounded repetitions reproduced the failures.
Logging can affect scheduling, and passing repetitions do not establish a fix. Root
cause remains unknown; no speculative renewal-order, timeout or thread-pool change was
made. Tests were not weakened. All production source hashes match the pre-investigation
baseline and restored library/test builds succeed.

A useful next step is to capture renewal/expiry timings in a run that actually fails,
under the original suite conditions, or establish a deterministic reproduction of the
specific race before proposing implementation changes. Additional green repetitions
alone should not close this finding. The readiness task remains unchecked.

Evidence: `/private/tmp/brighter-4170-asb-session-lock-audit/` contains diagnostic and
control TRXs/logs, scripts, source backups, timing-summary.json and audit-summary.json.
All ephemeral diagnostic test containers exit automatically; no broker fixture was
started. Nothing committed or pushed.


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
