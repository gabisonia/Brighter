# ASB session-lock failure diagnostics — completed, inconclusive

Status: approved audit completed. Diagnostic helper changes were temporary and are
restored exactly. Both restored suites retain 620 passing cases per framework.
The original failure did not recur; the readiness finding remains open.

## Why this amendment

The two archived failures originate in InMemoryLockingServiceBusReceiver completion.
Its exception conflates a closed receiver, absent delivery token and expired lock.
Earlier production-only timing audits did not reproduce either failure. Repeating
those green audits would not establish a cause.

The session helper starts its two-second session lifetime at construction, while
its client returns it unchanged from AcceptNextSessionAsync. Receive then copies
that session expiry onto delivery locks. Setup delay could therefore consume the
initial lock budget. This is a source-based hypothesis, not a demonstrated cause of
the archived failures. Renewal/settlement scheduling remains another hypothesis.

## Concrete reviewed changes

The adjacent copies of the two existing helpers add stderr diagnostics only:

- At lock failure: closed state, token presence, expiry, sampled current time,
  completed count and monotonic timestamp/frequency, correlated by entity/token.
- At session receive and renewal start/end: entity/session, current wall-clock ticks,
  session expiry and monotonic timestamp; renewal completion also records count.

EnsureLockIsHeld samples the dictionary lookup and current time once under its
existing gate, preserving the same failure predicate and exception. No artificial
waits, altered lock durations, expiry refreshes, retry changes or weakened assertions.
Only generated fixture IDs and timing/state values are emitted; no credentials or
message payloads. Logging can perturb scheduling, and wall time versus monotonic
clock comparison is diagnostic evidence, not proof of a host clock anomaly.

## Approved execution scope

Temporarily adopt these helper copies and preserve originals byte-for-byte. Build
all library targets and tests. Run the existing 16-case AzureServiceBusLockRenewalTests
class on .NET 9 and .NET 10, then at most two full available .NET 10 suites using
fresh isolated emulator fixtures with the current session entities. Preserve raw
TRXs and stderr, including every failure. Stop the diagnostic runs after a lock
failure and correlate its entity/session events. Do not retry until green.

Restore both helpers in finally and verify all production and test source hashes
against the pre-audit snapshot. Rebuild and run the restored full available suites
on both frameworks, preserving all 620 prior outcomes. Remove only this batch's
isolated containers/network. No commits or pushes.

If no failure occurs, record the audit as inconclusive and keep the readiness gate
open. If a failure occurs, report the exact state and timings before proposing a
separately reviewed behavioral regression test or fix. These are in-memory diagnostic
cases; neither native broker session renewal nor production readiness is established.
There is no production correction or mutation matrix in this diagnostic-only batch.

## Session-lock failure-state diagnostic audit — inconclusive

The approved helper amendment described above
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

## Local follow-up — 2026-11-10

Reviewed the current session receiver, inner locking receiver, client acceptance,
production lock-renewal loop and receiver settlement path. Acceptance returns the
preconstructed session unchanged; receive copies its expiry into the delivery
lock table. Renewal updates that table and the session expiry, while settlement
checks closed state, token presence and expiry under the inner receiver's gate.
Production starts renewal after receive and stops the tracked renewal before
settling the last delivery. These are possible timing boundaries to observe, not
demonstrated causes of the two historical failures.

The original temporary evidence directory named above is no longer present locally.
The diagnostic helper copies and this contemporaneous report remain, but the raw
failure events and timing summary cannot be reanalysed from that directory. The
reported passing audit must not be presented as a newly verified failing trace.

No deterministic reproduction of the historical failure was established. Adding a
setup delay longer than the lock would demonstrate an expired fixture, without
showing that the original failure had that cause. No helper expiry refresh, longer
timeout, production change or unchanged test rerun is justified by this review.
The bounded local review is complete; the release finding remains unresolved.
Reopen diagnosis with a failing session trace that correlates acceptance, renewal
and settlement state, including wall and monotonic timestamps.
