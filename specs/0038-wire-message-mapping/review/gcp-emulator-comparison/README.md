# GCP ordered redelivery: emulator version comparison

Executed 2026-11-09. Pub/Sub emulator **0.8.37 reproduces the same native ordered
redelivery stall as 0.8.36** on both .NET 9 and .NET 10. Updating to the tested image
does not resolve this reproducer. The broader lifecycle release gate remains open.

## Fixed comparison

The existing [raw protocol executable](../gcp-ordered-nack-protocol-control/Program.cs)
and [project](../gcp-ordered-nack-protocol-control/ProtocolControl.csproj) are unchanged.
They use generated gRPC clients without Brighter or high-level SubscriberClient
handlers. The four existing combinations cross one/two streams with nack-first/
ack-second-first settlement order. A received marker establishes second-stream
readiness. Initial readiness, thirty-second redelivery observation and separate
five-second recovery bounds are unchanged. Each combination runs once per framework
and image, sixteen observations total; there is no retry-until-green run.

| Emulator / image architecture | .NET | Returned within 30 seconds | Timed out | Recovered after separate repeated ack |
| --- | --- | --- | --- | --- |
| 0.8.36 / amd64 | 9 | 3 | 1 | 1 |
| 0.8.36 / amd64 | 10 | 3 | 1 | 1 |
| 0.8.37 / arm64 | 9 | 3 | 1 | 1 |
| 0.8.37 / arm64 | 10 | 3 | 1 | 1 |

Every timeout is `addStream=True acknowledgeSecondFirst=True`. The nack RPC
completes, but the original message does not arrive within thirty seconds. Only
after recording that failure does the existing probe repeat the second message's
original acknowledgement. The first message then arrives on stream 1 with the same
native ID and body and a new ack ID. Observed time from repeated-ack success to
delivery is 14.076/30.203 ms on the baseline and 7.672/10.973 ms on the candidate.
Recovery does not convert a timeout into a pass; process exit zero means the probe
completed its observations, not that every redelivery succeeded.

The candidate is the official `592.0.0-emulators` arm64 manifest verified in the
preceding [candidate lookup](../gcp-disposal-result/emulator-candidate.json).
Image IDs, creation dates, architecture, JAR paths and SHA-256 hashes are retained
in [summary.json](summary.json). The pinned baseline image is amd64 on this ARM64
host; the candidate is native arm64. This is a comparison of those exact images,
not an experiment varying only JAR version. The failure reproduces on native
arm64 as well, so this observation does not require amd64 emulation.

## Build, timing and preservation

The unchanged executable builds successfully with zero warnings and zero errors,
using the repository's existing package resolution: Google.Cloud.PubSub.V1 3.38.0,
Grpc.Net.Client 2.71.0 and Google.Protobuf 3.31.1. Build outputs/intermediates are
external to the checkout:

```sh
dotnet build specs/0038-wire-message-mapping/review/gcp-ordered-nack-protocol-control/ProtocolControl.csproj -c Release -m:1 -nodeReuse:false -p:BaseOutputPath=/private/tmp/brighter-4170-gcp-emulator-comparison/bin/ -p:BaseIntermediateOutputPath=/private/tmp/brighter-4170-gcp-emulator-comparison/obj/
python3 /private/tmp/brighter-4170-gcp-emulator-comparison/run.py
python3 /private/tmp/brighter-4170-gcp-emulator-comparison/summarize.py
```

[run.py](run.py) creates four fresh isolated emulator containers, uses actual HTTP
readiness and runs the same binaries against each image. It inhibits idle sleep
only for its lifetime, compares wall and monotonic duration, retains every outcome
and stops owned fixtures in cleanup. All four runs have continuous clocks; all
four fixtures are verified exited, with their logs retained. The runner releases
its sleep inhibitor. Reusing its container names intentionally fails rather than
silently restarting an earlier fixture; a deliberate future rerun needs new names.

An initial image inspection attempted `python3` on the candidate's PATH and failed
before any probe ran (Docker exit 127). The image bundles Python at
`/google-cloud-sdk/platform/bundledpythonunix/bin/python3`; readiness and metadata
collection use that existing interpreter. No test or assertion was changed for
this setup correction.

[verification.json](verification.json) checks all 6,614 existing production/test C#
files plus the two build/package props and unchanged protocol source/project:
6,618 inputs remain byte-identical during the matrix. No production correction,
test edit, dependency/fixture update, hosted request, commit or push occurs. Redis
is not rerun. This diagnostic does not justify another full Brighter suite rerun:
no product/test input changed and the candidate still fails the isolated control.

Compact summaries, complete protocol traces, JAR metadata, stopped-container
records, binary hashes and the runner are retained here. Complete build artifacts,
source manifest, image metadata and broker logs remain in
`/private/tmp/brighter-4170-gcp-emulator-comparison/`.

## Release implication

The local [upstream reproduction report](../../gcp-ordered-nack-upstream-reproduction.md)
now covers both emulator versions. It remains unpublished. These observations
support a native emulator redelivery finding; they do not establish a cause for
every historical Brighter shutdown failure, replace hosted acceptance, or clear
the independent ASB/Kafka lifecycle and supported SQL Server validation gates.
