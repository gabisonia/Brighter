# Native SDK control for ordered nack redelivery

Status: approved and executed unchanged. Native redelivery failure reproduced on both frameworks without Brighter; Subsequent raw protocol controls isolate an emulator failure mode; hosted behaviour remains unvalidated.

The approved six Brighter diagnostics now reproduce the historical failure with
native evidence. In the restored .NET 10 run and full .NET 9 run, the async/default
client case completes a deadline-zero RPC for the first message's observed ack ID,
but no further native copy of that message arrives within thirty seconds. The
second message is received and acknowledged. No production correction is established.

Review [the native control](Program.cs) and [its isolated project](NativeControl.csproj).
The project uses the repository-pinned Pub/Sub SDK and links only the two existing
passive observer helpers. It references no Brighter production assembly. SDK-owned
publisher/subscriber connections remain in use; an unbounded local channel and native
callback completion sources allow the same receive/nack/immediate receive/ack sequence.
The native bodies, application IDs, ordering key, flow-control limit, subscription
settings, first-receive bound and redelivery bound match the approved diagnostic.
This is an investigative control, not a substitute for its Brighter regression tests.

## Bounded execution

Run four fixed rounds of default, one and two native subscriber clients on each of
.NET 9 and .NET 10: twelve observations per framework. Zero means leave ClientCount
unset. Use fresh owned emulators with the same pinned image and explicit readiness;
keep all outputs, including unsuccessful recovery. Use a per-process hang limit of
twenty minutes. Each scenario has a ninety-second operation budget plus bounded
shutdown/deletion. Do not retry until green or change client count, SDK/image version,
timeouts, or nack/ack timing to obtain a desired outcome.

Each RESULT line records successful correlated native nack, whether the nacked ID
returned, whether both IDs returned, and how many raw first-message deliveries were
observed. Full native delivery/RPC traces remain available. Setup or cleanup errors
abort the probe and are distinguished from observed missing redelivery. A successful
process exit alone is not evidence of successful recovery; inspect every RESULT.

If the native control reproduces successful nack without redelivery, investigate the
SDK/emulator interaction without attributing the defect to Brighter. Passing controls
do not clear the existing failure. No production mutation is proposed: this executable
collects evidence outside Brighter and is not being claimed as a production guard.
The existing Brighter diagnostic's three mutations already produced eighteen intended
failures, but its restoration gate remains unresolved.

## Current hypothesis, not a demonstrated cause

Read-only inspection of the exact emulator 0.8.36 jar shows:

- Ordered acknowledgements may mark a later delivery acknowledged while retaining it
  behind an earlier unsettled delivery.
- Nacking the earlier delivery removes its delivered entry and requeues its message.
- Ordered stream selection hashes against the current stream count, then checks the
  previous stream owner and its outstanding delivery count before transferring ownership.

These branches suggest a possible stall when stream membership changes while a later
acknowledged delivery remains outstanding. Current traces do not expose server stream
ownership or internal processing order, so this is not established as the cause.
Overlapping lease-extension requests were also observed; their completion timestamps
alone do not establish server processing order. Bytecode and traces are saved under
`/private/tmp/brighter-4170-gcp-ordered-nack-observation/`.

## Approval boundary

The repository's [.agent_instructions/testing.md](../../../../.agent_instructions/testing.md)
requires "STOP after writing the test and ASK FOR APPROVAL". This is a new native
control experiment beyond the six approved Brighter cases; it is prepared for review
before execution. Approval covers building/running this concrete control, retaining
its native evidence and diagnosing the results. It does not authorize changing the
service contract, weakening the existing tests, upgrading dependencies, committing,
publishing or treating emulator evidence as hosted-service validation.

## Execution results

Both frameworks build with zero warnings/errors. Each records twelve observations,
eleven recoveries of both messages, and one missing first-message redelivery within
thirty seconds. All twelve correlated native nacks complete successfully. The failure
is round 3/default clients on both frameworks; the processor count is eleven. Only one
raw first-message delivery is observed in each failing scenario. No setup or cleanup
error occurs. The fixed round count is not extended and no outcomes are discarded.

The compiled manifests confirm no Brighter production dependency and the pinned SDK/
GAX versions. Program.cs and NativeControl.csproj remain byte-identical to approval.
All 6,484 existing production/test C# files remain unchanged. Both owned emulators are
removed. No commit/publication or hosted claim is made. This is an upstream-path
reproduction, not a successful redelivery validation or an established SDK/emulator fix.

See the [reproduction report](../../gcp-ordered-nack-upstream-reproduction.md) and
`/private/tmp/brighter-4170-gcp-ordered-nack-native-control/` for all logs and verification.
The subsequent [protocol control](../gcp-ordered-nack-protocol-control/README.md) now isolates the native emulator stall; see its execution record. Hosted behaviour remains unvalidated.
