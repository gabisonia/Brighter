# Ordered Pub/Sub redelivery stalls in a native SDK control

Local report; not submitted or published. A subsequent raw gRPC control isolates a
stream-handoff stall in the pinned emulator without high-level SDK subscriber handling.
This report concerns absence of redelivery within thirty seconds, not demonstrated
permanent loss. A subsequent hosted SDK comparison recovers both messages in all
24 observations; its scope and environment differences are recorded below.

## Reproduction

The standalone [native control](review/gcp-ordered-nack-native-control/Program.cs)
has no Brighter production dependency. It uses native SDK publisher/subscriber
clients with their own connections and passive native RPC/StreamingPull observation.
It creates an ordered subscription, starts the subscriber, sequentially publishes
two messages with the same ordering key, receives/nacks the first, then immediately
receives and acknowledges subsequent deliveries until both IDs return or thirty
seconds elapse. It does not await the native nack RPC before receiving/acking the
second message. See [execution instructions and scope](review/gcp-ordered-nack-native-control/README.md).

Environment:

- Google.Cloud.PubSub.V1 3.38.0; Google.Api.Gax.Grpc 4.13.1.
- Linux arm64 .NET SDK containers `mcr.microsoft.com/dotnet/sdk:9.0` and `:10.0`.
- Fresh, readiness-checked Pub/Sub emulator per framework; emulator jar 0.8.36.
- Emulator image `gcr.io/google.com/cloudsdktool/cloud-sdk@sha256:3797a874fae6b75aa25260b5bd40fa0397c2f1102f18ae3ac7dc887732fbe449`.
- Subscription ack deadline 30 seconds, ordered delivery enabled, no retry policy.
- Subscriber flow-control message limit 1; SDK default client count left unset, or
  explicitly one/two clients. Reported processor count 11.
- Native ordering key `one-order`; distinct application IDs and binary bodies.

Run the approved project with an explicit local emulator and project:

```sh
dotnet build specs/0038-wire-message-mapping/review/gcp-ordered-nack-native-control/NativeControl.csproj -c Release
PUBSUB_EMULATOR_HOST=localhost:8085 GOOGLE_CLOUD_PROJECT=brighter-test dotnet run --project specs/0038-wire-message-mapping/review/gcp-ordered-nack-native-control/NativeControl.csproj -c Release -f net10.0 --no-build
```

The recorded execution used isolated container networking and external build/output
directories. The command above illustrates the project invocation; it is not a claim
that the control was rerun with a host-bound emulator. Setup and cleanup errors abort
the probe. Process exit zero alone does not mean successful redelivery: inspect every
RESULT line.

## Recorded results

Each framework completed all twelve fixed observations without setup/cleanup errors.
Eleven recovered both messages. The fourth default-client observation did not recover
the first message. All twelve observations completed a correlated native nack.

Both failing traces show two initial native messages, application Nack for the first,
application Ack for the second, successful native Acknowledge for the second ack ID,
successful deadline-zero ModifyAckDeadline for the first ack ID, and an overlapping
sixty-second extension. Only one native copy of the first message is observed.
The client-observed completion timestamps do not establish server processing order.

```text
RESULT round=3 clients=0 nativeNack=True firstReturned=False bothReturned=False nativeFirstDeliveries=1
```

This result occurs on both net9.0 and net10.0. The one/two-client arms recover both IDs
in this bounded sample; they do not establish a safe workaround. Concurrent framework
runs may affect timing, and four rounds do not establish a failure probability.

## What this establishes

The earlier Brighter diagnostic failure can occur without Brighter code participating.
The native control therefore narrows investigation to the upstream SDK/emulator path.
That SDK control alone did not establish which component was responsible. The raw
protocol comparison below now reproduces the stall without high-level subscriber
handling. Neither experiment establishes hosted Pub/Sub behaviour or justifies
increasing the original test timeout.

Google documents deferred processing of acknowledgements for later ordered messages
and affinity while messages remain outstanding. These are relevant to the state being
investigated; they do not establish the cause of this failure.
[Subscription properties](https://docs.cloud.google.com/pubsub/docs/subscription-properties),
[message ordering](https://docs.cloud.google.com/pubsub/docs/ordering).

Read-only bytecode inspection of this exact emulator shows deferred acknowledged
entries, a nack path that removes the earlier delivered entry, and stream selection
that combines hashing against current stream count with previous-owner outstanding
delivery checks. At that inspection stage, a stream-ownership stall was a hypothesis. Neither the
passive trace nor bytecode inspection observes the live server's internal state;
the subsequent protocol intervention below provides the behavioural comparison.

## Raw protocol comparison

The [approved protocol control](review/gcp-ordered-nack-protocol-control/README.md)
now executes on both frameworks. It uses generated gRPC service clients, no high-level
SubscriberClient handler, no automatic lease extension and no Brighter code. Two
messages are delivered on stream 0 before settling either. Adding stream 1 is verified
by an independent marker delivery/ack on that stream.

| Streams | Settlement order | net9.0 first redelivery within 30s | net10.0 first redelivery within 30s |
| --- | --- | --- | --- |
| Keep one | Nack first, then ack second | Yes | Yes |
| Keep one | Ack second, then nack first | Yes | Yes |
| Add second | Nack first, then ack second | Yes | Yes |
| Add second | Ack second, then nack first | No | No |

In the stalled arm, all target RPCs complete successfully. After recording the full
thirty-second failure, the control repeats only the second message's original ack.
The first message then returns on stream 1 with the same native ID/body and a new ack
ID, in 37.620 ms on net9.0 and 46.755 ms on net10.0. No new publication, retry of the
first nack, lease expiry, client restart or subscription reconfiguration is needed.
The sixty-second stream deadline has not expired at intervention (about 30.11 seconds
after native nack completion). Recovery is separate from the failed original bound.

The matrix shows that this emulator failure mode requires both the added stream and
the selected ack/nack order in these observations. Combined with the bytecode paths,
it supports deferred acknowledgement blocking stream ownership transfer. The live
server's internal state was not instrumented, and the result does not prove every
historical Brighter failure has the same cause. It does establish that high-level SDK
subscriber scheduling and lease-extension overlap are not necessary for the protocol
reproduction.

### Readiness correction and retained setup failures

The initial attempt completed the two single-stream arms on both frameworks but
aborted when the marker arrived on stream 0 before stream 1 was ready. That is a setup
error, not missing-redelivery evidence. The correction acknowledges an early marker
and observes subsequent markers until one arrives on stream 1, within the original
fifteen-second bound. It does not settle either target message early. Exact source
comparison limits the change to this readiness block; all settlement, observation,
recovery and cleanup code remains unchanged. Fresh emulators are used for the corrected
matrix. Both versions build without warnings/errors. Initial failed runners were
stopped after their unhandled error rather than waiting for crash-dump writing; their
logs/nonzero exits remain in initial-setup-error.

### Release implications

No Brighter production change, reduced default client count, longer observation bound,
SDK upgrade or test skip follows from this result. The earlier Brighter regression
failures remain visible. The native protocol reproduction is ready for upstream review,
but is not published. Hosted-service behaviour and unrelated release requirements are
still open.


## Hosted native SDK comparison (2026-10-29)

The approved [hosted control](review/gcp-ordered-nack-hosted-control/README.md)
ran its unchanged four-round × default/one/two-client matrix against
`brighter-pubsub-validation`, sequentially on both frameworks. Every observation
recovered both application IDs within the original thirty-second loop and recorded
successful native nack completion correlated to the first delivery. Native traces
show two copies of the first message, preserving its native ID, application ID,
binary body and ordering key, with a new acknowledgement ID on redelivery.

| Framework | Observations | Correlated native nacks | Both IDs recovered | Missing first ID |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 12 | 12 | 12 | 0 |
| net10.0 | 12 | 12 | 12 | 0 |

Execution used local macOS/Darwin 25.6.0 arm64, eleven reported processors,
Microsoft.NETCore.App 9.0.20/10.0.11 and unchanged Pub/Sub 3.38.0/GAX gRPC 4.13.1.
The emulator comparison ran in Linux containers, so this is not a controlled
single-variable comparison of server implementations. The raw protocol experiment
independently isolates the emulator handoff stall; these hosted observations did
not reproduce missing redelivery. Neither result establishes absence of intermittent
hosted failure or validates Brighter's hosted integration.

Both builds have zero warnings/errors; both processes exit zero, completing every
owned-resource delete. Read-only project listings independently verify that all
24 owned topics and 24 owned subscriptions are absent. Exactly 48 synthetic messages
were published. ADC was used without copying or printing credentials. No auth,
permission, quota, setup or cleanup error occurred. Traces retain eleven/net9 and
twelve/net10 client-cancelled lease-extension RPCs after the RESULT records during
subscriber shutdown; these do not replace the successfully completed nacks.

All 6,484 pre-existing src/tests C# files and the approved executable/project remain
byte-identical. No production change, mutation, SDK upgrade, timeout increase,
client-count reduction, skipped assertion, repeated-until-green run, commit or
publication is included. This standalone control does not rerun or supersede the
Brighter regression matrix or clear its failed restoration gate.

Evidence is in `/private/tmp/brighter-4170-gcp-ordered-nack-hosted-control/`:
source and review snapshots, build log, lock/dependency artifacts, runtime/environment
records, both full native logs and twelve-row results, `final-verification.json`,
read-only post-delete listings and `cleanup.json`.

The next [hosted Brighter adapter](review/gcp-ordered-nack-hosted-brighter/README.md)
is prepared through the test-first review workflow, unbuilt and unrun. It reuses the
existing six sync/async × client-count diagnostic assertions through public Brighter
factories, with hosted targeting shown in a mechanical diff. Its proposed twelve
observations across both frameworks use at most twelve resource pairs and twenty-four
synthetic messages in the same explicitly named project. Earlier SDK-only approval
does not execute this new adapter. Other GCP lifecycle, mapping, generated-pump and
broader release requirements remain open.

## Evidence and preservation

`/private/tmp/brighter-4170-gcp-ordered-nack-native-control/` contains build/dependency
artifacts, approved program/project snapshots, per-framework logs/JSON and isolated
failure traces, source hashes, emulator logs, readiness and cleanup records, and
`final-verification.json`. The preceding Brighter evidence and emulator bytecode are
under `/private/tmp/brighter-4170-gcp-ordered-nack-observation/`.

The approved native SDK control executes unchanged. All 6,484 existing production/test C# files
remain byte-identical. No dependency upgrade, default-client reduction, test weakening,
production change, commit or publication is included. Both owned emulators are removed.

The raw-protocol evidence is under
`/private/tmp/brighter-4170-gcp-ordered-nack-protocol-control/`: approved/executed
programs, readiness-only.patch, initial setup errors, all eight final observations,
per-framework failure/intervention traces, verified timing, native emulator logs,
build/dependency manifests, source hashes, cleanup.json and final-verification.json.
All 6,484 existing src/tests C# files remain unchanged. Both initial and both corrected
owned emulators are removed; no unrelated container is changed.


## Hosted Brighter comparison (2026-10-30)

The subsequent [Brighter adapter](review/gcp-ordered-nack-hosted-brighter/README.md)
passes six sync/async × SDK-default/one/two-client cases on each framework against the
same hosted validation project. All twelve cases preserve the existing assertions
and immediate nack/receive/ack sequence through public Brighter factories. Each has
a successful correlated native nack, first-message redelivery with a new ack ID and
both application IDs recovered. All twelve temporary resource pairs are deleted and
independently verified absent. Production/test sources and the approved adapter are
unchanged. Four obsolete-header compiler warnings are recorded; both builds succeed.

This bounded comparison provides evidence for the Brighter hosted path as well as the
preceding SDK-only control. It does not clear intermittent emulator regression/restoration
failures, establish their universal cause, or validate shutdown/replacement and buffered
redelivery. The [validation record](validation.md) retains the full results and limits;
evidence is in `/private/tmp/brighter-4170-gcp-ordered-nack-hosted-brighter/`.
No production correction or upstream publication is included.

## Emulator 0.8.37 comparison (2026-11-09)

The unchanged [raw protocol reproducer](review/gcp-emulator-comparison/README.md)
also reproduces the stall against official Google Cloud CLI 592.0.0's Pub/Sub
emulator 0.8.37. A fixed comparison runs the four combinations once per framework
and image against fresh 0.8.36 and 0.8.37 instances. In all four matrices, only
adding a stream and acknowledging the second message before nacking the first
prevents redelivery within thirty seconds. Repeating the second acknowledgement
after that recorded failure releases the first message with the same ID/body and
a fresh ack ID. The other twelve observations return within their original bound.

The candidate runs native arm64; the baseline pinned image is amd64 on the ARM64
host. This is not a JAR-only comparison, but it establishes reproduction on the
newer native arm64 image too. All clocks remain continuous, all four fixtures are
stopped and 6,618 source/build/protocol inputs remain unchanged during the matrix.
Exact images, JAR hashes and full protocol traces are retained in the review.
This report remains local and unpublished. Updating to the tested image does not
resolve this control or the separate historical shutdown release gate.
