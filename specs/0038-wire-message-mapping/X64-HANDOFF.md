# Continue issue 4170 on the x64 machine

## Transfer checkpoint — 2026-11-11

The user authorized committing and pushing this checkpoint to the fork for work
on another computer. Use branch `wip/4170-x64-validation` on
`https://github.com/gabisonia/Brighter.git`. The earlier no-commit/no-push statements
below describe the validation run before this transfer authorization.

Start with [the final Docker report](review/x64-acceptance/docker-remaining/README.md)
and its raw-TRX-derived `acceptance-analysis.json`. SQL passes both targets. ASB
recovery/mutations/lost-lock acceptance, GCP .NET 10 shutdown replacement, Kafka
native close concurrency and final-source CI remain open. Kafka has 410/412 unique
passing identities per runtime across separate runs, not a green full suite.
All 44 remaining-matrix fixtures are stopped. Main production source is unchanged;
all 7,252 saved inputs match the validated parent snapshot
`2db6bcc9b53bdb1d388b667d8f8bbe6c05c0be33` allowing checkout line endings only.

Reviewed logs, TRX, crash reports and symbolicated stacks are included in this
checkpoint. Large `.core`/`.dmp` binaries remain on this computer; full core hashes
and ELF completeness checks are in `acceptance-analysis.json`. Absolute Windows
TEMP workspace and dump paths will not exist on the next computer. Do not claim
to inspect those raw cores from the transferred reports alone. Runners intentionally
refuse evidence overwrite and depend on a prepared isolated workspace; read them
before adapting paths or executing a discriminating follow-up.

Continue with the concrete remaining gates in the latest report. Do not repeat
unchanged broad runs merely seeking green. Preserve raw failing evidence and test
assertions. Do not rerun Redis, use paid services, rewrite history or push upstream.
Further commits/pushes require the user's instruction; this authorization covers
the transfer checkpoint.

Suggested next-machine prompt:

> Read AGENTS.md and specs/0038-wire-message-mapping/X64-HANDOFF.md on my fork's
> wip/4170-x64-validation branch. Continue from the final Docker report. Verify
> machine/source state, investigate the retained ASB/GCP/Kafka blockers using
> discriminating evidence, and preserve assertions. Do not rerun Redis, use paid
> services, commit, push or rewrite history without my instruction.

## Earlier validation history

**Latest local Docker checkpoint (2026-11-11):** [results and remaining gates](review/x64-acceptance/docker-remaining/README.md)
supersede the earlier next-run queue below. Expanded ASB acceptance has 745 passes,
12 failures and four existing skips per target; original recovery remains unstable
on both runtimes. Pub/Sub passes 459 cases on .NET 9 and 458 on .NET 10, with one
original async shutdown failure and 55 existing skips per target. Both passive Kafka
full suites abort in native consumer close after 223 passes; separate remainder
coverage is recorded in the latest report. Actual failing GCP traces and complete
Kafka cores are now available. Source remains on the fork at the verified HEAD;
no production changes, Redis rerun, paid services, commits or pushes. Production
readiness remains blocked; continue from the latest report, not the historical queue.

**Continuation completed on 2026-11-11:** see the
[native x64 checkpoint](review/x64-acceptance/README.md) before using the original
startup queue below. Host/source verification, both full SQL Server 2022 targets
and the bounded ASB comparison have now run at
`2db6bcc9b53bdb1d388b667d8f8bbe6c05c0be33`. SQL passes 396/four existing skips per
framework. ASB reproduces the SDK-only duplicate-sequence fault on native x64
under .NET 9; the .NET 10 sample passes. All owned fixtures are stopped. Historical
SQL Edge identity reconciliation and the recorded ASB/lifecycle/CI release gates
remain open. Do not restart these unchanged experiments merely from the original
handoff instructions. No new commit or push was made.

The user then requested further local Docker validation. The
[isolated Brighter reader-recovery follow-up](review/x64-acceptance/asb-recovery-isolated/README.md)
runs all eight cases with fresh entities per case: .NET 9 passes six/fails two;
.NET 10 passes eight. The two failures correlate with native duplicate-sequence
faults on distinct topic/queue entities. Four additional owned fixtures are stopped.
No source/assertion changes or mutations were applied; remaining release gates stay open.

This is a temporary validation snapshot on `gabisonia/Brighter`, branch
`wip/4170-x64-validation`. It is not a release or an upstream pull request.
The user explicitly authorized committing and pushing this transfer snapshot to
their fork. Earlier no-commit/no-push statements in the historical reports describe
those earlier checkpoints. Further commits, pushes, history rewrites or upstream
publication require a new user instruction. The transfer commits may be squashed
or reorganized after validation.

## Start here

1. Read the root `AGENTS.md`, then `completion-plan.md`, `tasks.md`,
   `release-review.md` and `local-review.md` in this directory.
2. Confirm the current branch and that the working tree is clean. Record `git rev-parse HEAD`
   with every new test result. Do not use `git reset` to resolve unexpected changes.
3. Run `sh specs/0038-wire-message-mapping/host-preflight.sh` from the repository root.
   It reads architecture, available resources, Docker state and .NET SDKs without
   installing software or starting containers. If using Windows, run the Linux
   commands in WSL2 with access to Docker's Linux engine.
4. Verify the 7,252 source/build inputs in
   `review/completion-source-manifest.json` before running acceptance. The snapshot
   describes the validated source, not the subsequent handoff documentation.
5. Proceed with the existing completion plan, starting with dedicated native x64
   SQL Server 2022 validation, then the bounded ASB emulator comparison.

## Validation order and constraints

- Use .NET 9 and .NET 10 for projects declaring both targets. Check repository SDK
  requirements and test fixture configuration before installing or running anything.
- SQL Server must run on native x86-64 hardware, using a dedicated local test
  database. Existing ARM64 SQL Edge results are 396 passes/four skips per framework.
  Run the complete MSSQL suite on each framework and compare test identities, not
  only totals. Retain TRX, SQL version and host/container architecture.
- ASB's ARM64-emulator/amd64-emulated-SQL combination faults during topic DLQ receive
  even with a standalone Azure SDK probe. Both cold framework samples have four
  topic failures/four queue passes. Empty-DLQ warmup fails on .NET 9 but passes on
  .NET 10, so it is not a reliable fix. See `review/asb-sdk-dlq/README.md`.
- The new ASB hypothesis is architecture/backend related: compare the same pinned
  emulator and SQL versions on native x64 with the same SDK-only scenarios. Use
  the amd64 emulator variant from the pinned multi-platform image. The historical
  Python runner hardcodes macOS paths, ARM64 and `caffeinate`; adapt these explicitly
  in a new runner, retaining original evidence. Do not run that runner unchanged.
- A healthy SDK-only result permits the original Brighter reader-recovery cases
  and outstanding mutations; it alone does not certify Brighter. Preserve all
  assertions and use fresh isolated entities between deliberate failure cases.
- Native retry/scheduler timing, duplicate detection and settlement, plus lost-lock
  cleanup acceptance, remain open. The lost-lock test is retained outside the
  compiled project because the emulator did not establish its required native
  `MessageLockLost` precondition. Do not silently skip it or weaken its contract.
- Historical ASB in-memory session-lock, GCP shutdown/replacement and Kafka-close
  findings remain open. Resume only with a discriminating hypothesis or correlated
  failure evidence from the table in `local-review.md`. Passing reruns do not explain
  them. No unchanged broad matrix solely to seek green.
- **Do not rerun Redis**, provision paid services, bypass quotas, or disturb unrelated
  containers/networks. Record and stop only fixtures owned by this validation.
- Final-source remote CI is still unrun. Publishing this WIP branch is not a CI pass.
  Do not open an upstream PR or change workflows merely to trigger CI without a
  separate user instruction. Reassess CI availability after the local acceptance work.

## Evidence and implementation state

Feature implementation and local requirement review are complete; production
readiness is not established. The latest production correction propagates current
trace context before ASB/GCP bulk mapping. Retained affected suites pass 540 ASB
local cases per framework and 459 GCP cases with 55 existing skips per framework.
Build/API evidence and other gateway matrices are recorded in the completion plan.
Do not treat this snapshot as a fresh all-green solution run.

The transfer retains feature source, tests, documentation and review artifacts,
including the specifically reviewed logs ignored by the general `*.log` rule.
Credentials, build outputs and machine-local `.current-gear` are not transferred.
The full staged whitespace check reports 455 pre-existing trailing-whitespace
findings: 432 in retained review artifacts and 23 in three newly tracked source
files. Their bytes are preserved to match the validated snapshot. Earlier
`git diff --check` results covered tracked changes and did not inspect these
previously untracked files. This is a recorded formatting cleanup item, not a
behavioral test failure; keep raw diagnostic evidence intact.
Absolute `/private/tmp` and `/Users/gabisonia` paths in historical evidence refer to
the original Mac. Their absence on the new machine is expected; reuse retained
repository artifacts and explicitly record anything that cannot be reconstructed.
Do not claim historical raw captures were inspected if only their reports remain.

The prior session used review-after implementation with authorization to follow
the completion plan without per-test approval pauses. Preserve TDD and meaningful
regression checks for any newly demonstrated defect. This handoff provides task
context for a new Codex session; a Git checkout does not transfer chat history.

## Prompt for the new Codex session

> Continue issue #4170 on this x64 machine. Read AGENTS.md and
> specs/0038-wire-message-mapping/X64-HANDOFF.md, then follow the saved completion
> plan. Start with host/source verification and native x64 SQL Server Docker
> acceptance, then the bounded ASB comparison. Preserve existing evidence and
> unrelated work; do not rerun Redis or use paid services. Do not commit, push,
> rewrite history or open an upstream PR without my instruction. Fix demonstrated
> defects and validate affected components. Do not claim production readiness
> while mandatory gates remain open.
