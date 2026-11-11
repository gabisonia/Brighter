# Remaining local Docker acceptance — 2026-11-11

**Not production-ready.** This checkpoint exercises the remaining ASB, Pub/Sub and
Kafka gates on native linux/amd64 Docker. It reuses the completed native SQL Server
acceptance and unchanged gateway/build/API evidence. It is not a new whole-repository
green run. Redis was deliberately excluded. No hosted or paid services, commits,
pushes, history rewrites or upstream PRs were used.

Source: fork `gabisonia/Brighter`, branch `wip/4170-x64-validation`, HEAD
`2db6bcc9b53bdb1d388b667d8f8bbe6c05c0be33`. The source manifest has 7,252 inputs.
Only isolated test-fixture routing and passive diagnostics differ; assertions are
unchanged. Retained approved lost-lock cases were admitted unchanged to the
temporary test project. Mutation source was restored byte-for-byte afterward.

## Results

| Acceptance | .NET 9 | .NET 10 | Disposition |
| --- | --- | --- | --- |
| Native SQL Server 2022, reused | 396 pass / 4 existing skips | 396 pass / 4 existing skips | All 400 current identities and outcomes match |
| ASB expanded inventory | 745 pass / 12 fail / 4 existing skips | 745 pass / 12 fail / 4 existing skips | 761 cases each; excludes original recovery eight, includes retained lost-lock two |
| Original ASB recovery, earlier isolated checkpoint | 6 pass / 2 fail | 8 pass | Later fresh .NET 10 baseline also fails; not reliably healthy |
| Pub/Sub messaging inventory | 459 pass / 55 existing skips | 458 pass / 1 fail / 55 existing skips | Same 514 identities |
| Kafka full passive run | Aborted after 223 passes | Aborted after 223 passes | Native close stall; zero assertion failures does not mean suite passed |
| Kafka methods not reached before abort | 187 pass | 187 pass | 410/412 unique identities pass per framework across separate runs; two concurrency rows remain explicitly blocked |

The authoritative case inventories, failure messages, core hashes and coverage
unions are in [acceptance-analysis.json](acceptance-analysis.json), reconstructed
from raw TRX. Existing skips are retained rather than counted as passes.

## What is left

1. **ASB recovery and settlement.** Ten failures per framework are emulator setup
   rejection: existing tests request three/four-day entity TTL, but the emulator
   permits at most one hour. Two retained lost-lock cases receive cancellation
   instead of the required native `ServiceBusException(MessageLockLost)`; their
   native precondition remains unmet. No assertion or TTL was weakened.
   Native scheduler/retry isolation (16), retry deduplication (2), broker renewal
   (1) and in-memory renewal (16) pass per framework. Passing separate custom-wire
   and default native retry tests does not establish their combined end-to-end path.
2. **ASB reader mutations.** Three of ten named .NET 10 mutations validate, with
   16 intended failures and healthy eight-case original/restored controls. Before
   mutation four, its fresh unmodified control fails one synchronous topic DLQ
   assertion, correlated with broker duplicate `S.SP-SequenceNumber:1` on that
   same fresh topic; the campaign stops without applying that mutation. Seven .NET 10
   mutations and all ten .NET 9 mutations remain blocked by fixture health.
   The earlier SDK probe reproduces duplicate sequence-number failures on native
   x64 too. Neither architecture nor a blanket .NET 10 workaround explains this.
   Historical in-memory session-lock failure is still unexplained by passing runs.
3. **GCP original async shutdown/replacement.** The original unsettled-message
   test fails on .NET 10: disposal completes, but the replacement returns an empty
   message instead of the expected application ID. Correlated native trace shows
   original-generation redelivery, a canceled 60-second extension and successful
   zero-deadline requests, followed by no replacement-generation native delivery.
   It does not establish server RPC execution order or a Brighter mapping defect.
   Next evidence must correlate stream creation/SDK task termination and backend
   deadline order. This is now an actual failing trace, separate from the known
   SDK-only ordered-nack emulator finding. See [shutdown analysis](gcp/shutdown-analysis.json).
4. **Kafka close lifecycle.** Both passive full suites stall in the existing
   commit/revoke concurrency class before diagnostics pause them. The native stack
   waits through `rd_kafka_op_req` / `rd_kafka_rebalance_protocol` in the SDK rebalance
   callback during consumer close. Exact-library symbolication and complete x64
   ELF cores are retained for both. Function names alone do not establish queue
   ownership or justify a production change. The full-context concurrency gate
   remains open even when the other 410 identities pass across separate runs.
5. **Release evidence.** Final-source remote CI has zero existing runs; no workflow
   was triggered. Historical SQL Edge raw inventories were not transferred, so its
   exact identity comparison remains unavailable. Resolve the lifecycle findings,
   native acceptance gaps and CI gate before claiming production readiness.

## Evidence and limitations

- [ASB](asb/), [mutation controls](asb-mutations/), [GCP](gcp/),
  [passive Kafka .NET 9](kafka-passive/net9.0/) and
  [.NET 10](kafka-passive-net10/net10.0/) retain raw TRX, native traces and service logs.
- [Source differences](isolated-source-differences.json), [image manifest](images/manifest.json),
  [workspace](workspace.json), [capacity](capacity-at-overlap.txt) and
  [CI read-only check](ci-readonly-check.json) record the execution boundary.
- Original Kafka diagnostic run paused a healthy process and produced a partial
  core; it is not the independent control. Passive runs reproduce before capture
  and write complete cores on Linux-local storage. Core paths are under Windows
  TEMP `brighter-4170-docker-native-captures`; large binaries remain local. SHA256,
  x64 ELF type and segment bounds are verified in the analysis.
- Two full-run PowerShell report writers became stuck in final JSON serialization
  after fixture cleanup. Only the verified writer processes were stopped. Raw TRX,
  crash reports and cores survived; their intermediate runner JSON is not the final
  acceptance authority. The remainder runner records a small summary instead.
- Fixtures overlapped after a capacity check (~10 GB Docker VM available memory).
  This is not an architecture-only controlled comparison; runtime patch versions
  also differ from the historical Mac. No OOM is attributed to these failures.
- Cleanup retains stopped owned containers/networks and raw failed evidence.
  `containers-final.jsonl` and `container-states-final.jsonl` record final states.

No production fix was inferred solely from emulator errors or blocked native stacks.
