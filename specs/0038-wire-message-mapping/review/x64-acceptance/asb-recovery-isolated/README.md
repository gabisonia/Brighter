# Local Docker Brighter reader-recovery acceptance

2026-11-11. HEAD `2db6bcc9b53bdb1d388b667d8f8bbe6c05c0be33`, branch
`wip/4170-x64-validation` on `gabisonia/Brighter`. No production or compiled test
source changed. No commit, push, history rewrite, Redis run or paid service.

This continues beyond the [SDK-only comparison](../README.md#bounded-asb-comparison)
with the original approved `AsbReaderFailureRecoveryTests`. The bounded question
is whether the Brighter consumer/rejection/recovery path passes on native x64,
and whether previously chained wrong-message-ID failures persist when every case
has fresh isolated entities. It does not repeat the SDK-only sample or the complete
ASB matrix solely to seek a passing run.

## Fixed execution

Each framework uses a new native amd64 emulator and SQL Server 2022 backend, using
the same pinned image digests as the preceding checkpoint, with `--pull never`.
The configuration contains eight unique queue/topic pairs. Each of the eight
original theory rows runs in its own process with an unused pair selected through
the existing environment variables. The second framework receives entirely fresh
containers and entities. No failed entity is reused by a subsequent case.

Per-case VSTest selection uses documented
[DisplayName filters](https://learn.microsoft.com/dotnet/core/testing/selective-unit-tests?pivots=xunit),
and the runner requires exactly one TRX result from every invocation. All eight
identities match the retained historical reader-recovery TRX on each framework.
The runner uses the unchanged test assertions and helper, 90-second inactivity
hang diagnostics and a 150-second outer process deadline. Original test operation
timeouts remain unchanged. Both project targets build successfully with zero errors
and existing warnings; no assertion or test helper was amended.

## Results

| Runtime | Topic passed / failed | Queue passed / failed | Total |
| --- | ---: | ---: | ---: |
| .NET 9.0.17 | 3 / 1 | 3 / 1 | 6 passed, 2 failed |
| .NET 10.0.12 | 4 / 0 | 4 / 0 | 8 passed, 0 failed |

Both .NET 9 failures are the synchronous throwing-reader cases (`useAsync=false`,
`returnNull=false`), on `recovery-topic-0` and `recovery-queue-4`. They fail at line
116 of the unchanged test: native DLQ receive returns null. The preceding fallback
control signal, original ID/body/destination, reader call count, lock/sequence
metadata and successful rejection-return assertions have already passed.

Broker logs correlate each failing entity with duplicate `S.SP-SequenceNumber:1`
and an entity fault. The other six .NET 9 entities pass their complete assertions,
as do all eight .NET 10 entities. None receives an earlier case's ID; cases use
different entities. This removes reuse of an already faulted entity as an explanation
for these two failures. It does not establish why those particular parameter rows
failed or prove a framework-specific root cause from one sample per runtime.

The prior SDK-only queue controls passed; that narrower observation must not be
generalized to all queue lifecycles. This Brighter-path run also faults a fresh
queue's DLQ. The earlier SDK-only topic reproduction establishes that Brighter is
unnecessary for that topic reproduction; this run alone does not provide an
independent SDK-only reproduction of the fresh-queue failure. The precise
SDK/emulator/backend interaction remains unproven. No speculative production fix
or mutation campaign was applied to a fixture with failing acceptance.

## Evidence and disposition

[Plan](plan.json), [configuration](Config.json), [results](results.json),
[per-case verification and historical identity comparison](verification.json),
[net9 details](net9.0-results.json), [net10 details](net10.0-results.json),
[runner](../run-asb-recovery.ps1) and [summary script](../summarize-asb-recovery.ps1)
retain exact case names, assertion errors/stacks, assembly hashes, process exit codes
and wall/monotonic timing. The sixteen adjacent per-case TRXs and their console logs
retain the full test observations; broker and SQL logs are saved with fixture
passwords redacted. There are no clock discontinuities or pre-stop OOM kills.

All four owned containers are stopped and retained, together with their two isolated
networks. See [final container inventory](containers-final.txt). Recorded pre-stop
states are running; exit 137 in the final inventory follows explicit Docker cleanup
after its ten-second stop deadline. Unrelated containers were not targeted.

[Post-run source verification](../source-verification-after-recovery.json) again
matches all 7,252 saved source/build inputs, with checkout CRLF conversion only.
The SQL Server acceptance results from the previous checkpoint remain unchanged.

**Production readiness remains blocked.** The .NET 10 reader-recovery sample passes,
but the .NET 9 matrix is incomplete and the emulator is not a reliably healthy
fixture. Ten named recovery mutations, full affected acceptance, native scheduler/
retry timing and settlement, and the lost-lock precondition/cleanup gate remain
open. Historical lifecycle findings, hosted ASB, final-source CI and historical
SQL Edge identity reconciliation also remain open. No unchanged broad rerun is queued.
