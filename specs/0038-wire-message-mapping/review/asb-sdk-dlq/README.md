# Native SDK reproduction of the ASB Docker DLQ fault

Date: 2026-11-11. **Local cause isolation complete; native ASB acceptance remains
blocked.** This standalone executable references only Azure.Messaging.ServiceBus
7.21.0. Brighter is not required to reproduce the topic/DLQ fault seen in the
[original reader-recovery tests](../asb-reader-failure-recovery/README.md).

## Fixed experiment and results

Each framework uses a fresh emulator and SQL backend. Four topic cycles followed
by four queue controls send binary data, receive under PeekLock, dead-letter the
actual SDK-received object and verify the native DLQ identity, bytes and metadata.
Successful cycles then send, receive and complete a healthy delivery and verify
its removal with a bounded peek. Operations and the outer runner have deadlines.

| Variant | Framework | Topic | Queue | Broker duplicate-sequence fault |
| --- | --- | --- | --- | --- |
| Cold startup | .NET 9.0.20 | 4 failed | 4 passed | Present |
| Cold startup | .NET 10.0.11 | 4 failed | 4 passed | Present |
| Empty DLQ receive before scenarios | .NET 9.0.20 | 4 failed | 4 passed | Present |
| Empty DLQ receive before scenarios | .NET 10.0.11 | 4 passed | 4 passed | Absent |

In each failing sample, the first DLQ receive returns null after successful
dead-letter settlement. The next three topic cycles receive the preceding cycle's
ID. Broker logs report duplicate `S.SP-SequenceNumber:1` and an entity fault. Those
three identity failures are subsequent observations on the faulted fixture, not
three independent root causes. All queue controls complete their full assertions.

The follow-up first receives from each initially empty DLQ, asserts null and disposes
the warmup client/receivers before running the same scenarios. It tests whether
initialization before the first dead-letter prevents the fault. The .NET 9 failure
rejects this as a reliable workaround. The .NET 10 pass neither proves that warmup
caused success nor erases the other failures. No further identical run is queued.

## What this establishes

- Brighter mapping, receiver wrappers and lock-token-only settlement reconstruction
  are unnecessary for this reproduction: the probe settles the SDK-received object.
- The fault is not specific to .NET 10. The prior Brighter .NET 9 pass remains valid
  for that sample, but cannot establish a framework-specific cause.
- No Brighter correction follows from this evidence. The exact internal fault in
  the SDK/emulator/backend remains unproven. This does not resolve historical ASB
  session-lock, GCP shutdown or Kafka-close findings.
- The local emulator is unsuitable for completing the blocked DLQ acceptance and
  mutation checks in its current state. Use a healthy isolated broker fixture;
  preserve the original assertions and failure evidence.

The emulator runs natively on ARM64; SQL Server 2022 runs under amd64 emulation.
This environment cannot establish supported native-x64 SQL acceptance or exclude
the emulated backend as a contributing factor. Image digests, architecture, elapsed
wall/monotonic time and container states are recorded in both results files.
No clock discontinuity or OOM kill was recorded. All eight owned containers were
stopped and retained; unrelated services were untouched.

## Reproduction and retained evidence

The minimal application is [Program.cs](probe/Program.cs), with a pinned
[project](probe/Probe.csproj) and [lock file](probe/packages.lock.json).
Against an isolated fixture containing `wire-mapping-topic`/`wire-observer` and
`wire-mapping-queue` from [Config.json](Config.json), supply the connection through
`BrighterTestsASBConnectionString` without printing it. From this directory, copy
the standalone project outside the repository so it does not inherit Brighter's
central package management:

```sh
probe_dir=$(mktemp -d /tmp/asb-sdk-dlq.XXXXXX)
cp probe/Probe.csproj probe/Program.cs probe/packages.lock.json "$probe_dir/"
dotnet restore "$probe_dir/Probe.csproj" --locked-mode
dotnet build "$probe_dir/Probe.csproj" -c Release --no-restore
dotnet "$probe_dir/bin/Release/net9.0/Probe.dll" "$probe_dir/net9-result.json"
```

Use a fresh isolated fixture for the .NET 10 variant. The exit code is nonzero if
any scenario fails. Do not run against application entities: the probe receives,
dead-letters and deletes messages. The retained [runner](run.py) documents the exact
local Docker orchestration; its workstation path, one-shot names, explicit subnet
and macOS `caffeinate` dependency need adaptation on another host. It uses cached
images and refuses to overwrite an existing fixture network.

Baseline results: [summary](results.json), [net9](net9.0-probe.json),
[net10](net10.0-probe.json). Follow-up results: [summary](warmup/results.json),
[net9](warmup/net9.0-probe.json), [net10](warmup/net10.0-probe.json),
[modified probe](warmup/probe/Program.cs). Adjacent console and redacted broker logs
retain native IDs and failure stacks. No fixture password is included.

[Dependency verification](dependency-verification.json) confirms no Brighter
dependency. The initial sandbox build failed during setup and is retained separately;
the approved build completed both targets with zero warnings/errors. The warmup
build also completed with zero warnings/errors. Build setup failure is not behavioral
RED. [Source provenance](source-provenance.json) records the executed source hashes
and the MIT header added to retained source copies after execution; executable
statements are unchanged. No production or compiled test source changed in this work.

## Remaining acceptance

Complete the original reader-recovery cases and ten named mutations on a healthy
fixture, then the retry/scheduler native timing/settlement checks and the separate
lost-lock cleanup test whose native failure precondition is still unestablished.
Only an attributable assertion failure counts as mutation evidence. Follow the
[completion plan](../../completion-plan.md); a green SDK-only probe alone would not
certify the Brighter feature.
