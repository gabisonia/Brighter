# Native x64 acceptance checkpoint

**Latest local Docker checkpoint (2026-11-11):** [results and remaining gates](docker-remaining/README.md)
supersede the earlier next-run queue below. Expanded ASB acceptance has 745 passes,
12 failures and four existing skips per target; original recovery remains unstable
on both runtimes. Pub/Sub passes 459 cases on .NET 9 and 458 on .NET 10, with one
original async shutdown failure and 55 existing skips per target. Both passive Kafka
full suites abort in native consumer close after 223 passes; separate remainder
coverage is recorded in the latest report. Actual failing GCP traces and complete
Kafka cores are now available. Source remains on the fork at the verified HEAD;
no production changes, Redis rerun, paid services, commits or pushes. Production
readiness remains blocked; continue from the latest report, not the historical queue.

**Subsequent local Docker follow-up:** the
[isolated Brighter reader-recovery run](asb-recovery-isolated/README.md) executes
the original eight cases with fresh entities per case. .NET 9 passes six and fails
two (one topic and one queue); .NET 10 passes all eight. Both failures correlate
with broker duplicate-sequence faults. Four additional owned containers are stopped
and retained. This adds Brighter-path evidence beyond the SDK-only checkpoint
below; it does not close the remaining release gates.

2026-11-11. Source HEAD: `2db6bcc9b53bdb1d388b667d8f8bbe6c05c0be33`.
Branch: `wip/4170-x64-validation`, tracking the same branch on the user's
`gabisonia/Brighter` fork. [Remotes](remotes.txt) retain the distinction between
`origin` (fork) and `upstream` (BrighterCommand). No commit, push, history rewrite,
Redis execution, Azure credential check or paid service was performed.

Implementation remains complete; **production readiness remains blocked**.
Native SQL Server 2022 execution passes. The bounded ASB comparison still reproduces
the SDK-only topic/DLQ fault on native x64, so no Brighter recovery, mutation,
scheduler or lost-lock acceptance gate is closed by this checkpoint.

## Host and source verification

Windows x64 on an AMD Ryzen AI 9 HX PRO 470 uses Docker Desktop's native
`linux/amd64` engine, with 24 CPUs and 20,790,824,960 bytes of VM memory.
The Linux preflight reports `Linux x86_64`, about 595 GiB of free workspace disk
and 2.4 GiB of available VM memory at the initial check. Existing services were
inventoried before fixture creation and were not targeted by any stop command.

Only Docker Desktop's WSL2 distribution is installed. The supplied shell preflight
was run there with CRLF removed in memory, preserving the checked-in script.
That distribution rejects its internal Docker CLI and has no .NET installation;
the installed Windows CLI and Windows .NET hosts supplied those checks instead.
The CLI is outside this shell's PATH and required approved access to its installed
location. See [Docker version](docker-version.txt), [engine](docker-host.txt),
[Linux preflight](wsl-host-preflight.txt), [SDK 10](windows-dotnet.txt) and
[temporary SDK 9](net9-dotnet.txt).

All **7,252** source/build inputs match the saved SHA256 manifest in HEAD.
Every working input differs from the original LF bytes only by checkout CRLF
conversion (`core.autocrlf=true`). Both [initial verification](source-verification.json)
and [final verification](source-verification-final.json) record zero content
mismatches. The [verification script](verify-source.ps1) archives with
`core.autocrlf=false` and `core.eol=lf`; an ordinary Windows archive would itself
apply checkout conversion. No source line endings were rewritten.

## SQL Server 2022

The dedicated fixture uses the already cached image
`mcr.microsoft.com/mssql/server@sha256:cf8f0ab2ebe61feec2eed2629061a07d247f791c2a8ddda74a4ac9f06a291718`.
It reports SQL Server **2022 RTM-CU27-GDR, 16.0.4305.5, Developer Edition, X64**
on Ubuntu 22.04.5 LTS. Container `uname -m` reports `x86_64`.
The test host is Windows x64; SQL runs natively in the x64 Linux VM.

Existing cases use the constant localhost port 11433, database `BrighterTests`
and repository test password, while others accept environment configuration.
The owned container therefore binds only `127.0.0.1:11433`, and both paths use its
dedicated test database. Database-creation cases also create and delete their own
uniquely named databases inside this fixture. No application database is used.

| Target / runtime | Passed | Failed | Existing skips | Duration |
| --- | ---: | ---: | ---: | ---: |
| net9.0 / 9.0.17 | 396 | 0 | 4 | 87.17 s |
| net10.0 / 10.0.12 | 396 | 0 | 4 | 78.51 s |

All **400 exact test identities and outcomes match between frameworks**. The four
skips are the existing sync/async negative-ack redelivery cases. No tests or
assertions were changed. SDK 10.0.401 builds the declared targets; the temporary
SDK 9.0.315 muxer executes the net9 assembly without framework roll-forward.

The historical SQL Edge reports state 396 passes/four skips, but their raw TRXs
and per-case inventories were not transferred. Exact cross-environment historical
identity parity therefore remains **unverified**; matching totals and source
continuity are not claimed as a replacement for that comparison.

Evidence: [results](mssql-native/results.json),
[identity comparison and all cases](mssql-native/identity-comparison.json),
[net9 TRX](mssql-native/net9.0.trx), [net10 TRX](mssql-native/net10.0.trx),
[SQL version](mssql-native/sql-version.txt),
[container architecture](mssql-native/container-architecture.txt), and
[runner](run-mssql.ps1). Adjacent build/test logs are retained locally, including
logs ignored by the repository's general log rule. Existing analyzer/nullability
warnings appear in the first builds; both targets build successfully.

Three setup-only attempts are retained under `mssql/`, `mssql-acceptance/` and
`mssql-final/`. They exposed Windows process exit-code capture, native sqlcmd
quoting and an unsupported VSTest option. They executed no test cases and supply
no behavioral RED. The final runner corrects those issues and uses a 14-minute
test-session limit within a 15-minute outer process deadline.

## Bounded ASB comparison

Hypothesis: a native amd64 emulator with native amd64 SQL may avoid the fault
previously seen with ARM64 emulator/amd64-emulated SQL. One cold sample per target
uses fresh containers and a fresh isolated network, with the same pinned image
digests, unchanged configuration, locked Azure.Messaging.ServiceBus **7.21.0**
dependencies and unchanged SDK-only source. The probe is copied to a temporary
directory outside repository build-property inheritance. It has no Brighter dependency.

The emulator digest remains
`sha256:5a96d893b245031740f7d46e0fe5ff282d24b78c4b7d761dd57590f3f010a9b3`;
its amd64 variant and the SQL image above run without architecture emulation.
There is no macOS `caffeinate` dependency. Docker allocates isolated free subnets
without changing existing networks. Each sample has four topic cycles followed
by four queue controls, 60-second operation tokens and a 300-second process
deadline. No warmup variant or repeated green-seeking sample was run.

| Target / runtime | Topic passed / failed | Queue passed / failed | Duplicate-sequence broker fault |
| --- | ---: | ---: | --- |
| net9.0 / 9.0.17 | 0 / 4 | 4 / 0 | Present |
| net10.0 / 10.0.12 | 4 / 0 | 4 / 0 | Absent |

The .NET 9 first topic cycle receives null after successful dead-letter settlement.
The following three observe the preceding cycle's identity. The broker reports
duplicate `S.SP-SequenceNumber:1` and faults the subscription, matching the retained
SDK-only failure. These are four scenario failures on one faulted fixture, not four
independent root causes. Every queue control passes its full settlement/recovery
assertions. The .NET 10 cold sample passes all eight scenarios.

**ARM64/emulated SQL is not necessary to reproduce the fault.** Native x64 does
not reliably remove it. The .NET 10 success does not erase the .NET 9 failure or
establish a framework-specific cause. Runtime patches and test-host OS also differ
from the historical Mac samples (9.0.20/10.0.11); this is not a controlled claim
that architecture was the sole changed variable. The exact SDK/emulator/backend
cause remains unproven, and no production correction follows from these results.

Both probe processes finish in under three seconds, with no recorded clock
discontinuity. Neither SQL backend nor broker is OOM-killed or stopped before
its sample finishes. [Comparison](asb-sdk/comparison.json) records dependencies,
per-sample counts and broker-fault detection; [raw results](asb-sdk/results.json),
[net9 probe](asb-sdk/net9.0-probe.json), [net10 probe](asb-sdk/net10.0-probe.json),
[source provenance](asb-sdk/provenance.json), [emulator identity](asb-sdk/emulator-image.txt),
redacted broker/SQL logs and [adapted runner](run-asb.ps1) are retained together.

## Cleanup and remaining work

All **eight owned containers**, including setup attempts, are stopped and retained;
the two owned ASB networks are retained. See [final owned inventory](owned-containers-final.txt).
Exit 137 in this post-stop inventory follows the explicit ten-second Docker stop
deadline; the recorded pre-stop states are running with `OOMKilled=false`.
Unrelated services were not stopped or reconfigured. The final inventory no longer
lists the unrelated `gracious_bartik`/Ryuk test fixture seen initially; no command
in this work targeted those containers. Their lifecycle is outside this validation.

The requested SQL execution and bounded ASB comparison are complete. Recover the
historical SQL Edge case inventory to finish exact cross-environment identity
reconciliation. ASB still requires a healthy isolated fixture for original reader
recovery, ten named mutations, native retry/scheduler timing and settlement, and
the lost-lock precondition/cleanup acceptance. One successful SDK sample does not
certify those paths. Historical ASB session-lock, GCP shutdown/replacement and Kafka
close findings remain open, as do hosted ASB and final-source remote CI. No unchanged
local matrix, Redis run or credential polling is queued.
