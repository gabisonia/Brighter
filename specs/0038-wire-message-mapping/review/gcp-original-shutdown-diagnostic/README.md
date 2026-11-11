# Original stream shutdown cases: native input diagnosis

Review-after #4170. Observe the existing sync/async unsettled and buffered shutdown
cases through their original GcpStreamMessageGatewayProvider. Temporarily attach
the existing passive GcpStreamRpcObserver through the public StreamConfiguration
hook in the isolated workspace. Correlate each provider and stream build with
native deliveries, acknowledgement IDs and completed deadline-zero RPCs.

Preserve every assertion, operation, timeout, client count, factory and production
source. Build the diagnostic assembly and restore the provider byte-for-byte before
execution. Run the four existing cases once on each framework against separate
pinned local emulators. If those pass, run one complete available .NET 9 Pub/Sub
matrix with a fresh emulator to retain full-suite preconditions. Stop focused work
at a failure and investigate it; do not repeat until green.

This distinguishes a missing native redelivery from a delivery received by the SDK
but missing from the replacement channel. A successful nack alone is insufficient.
The async test's existing Task.WhenAny does not establish successful disposal;
native RPC failures must be retained rather than silently treated as success.
Passing runs do not resolve historical failures. Instrumentation can affect timing.

No production correction, dependency change, hosted resource, commit or push is
part of this diagnostic. Preserve logs/TRXs, wall/monotonic durations, source hashes
and owned-container shutdown evidence under
`/private/tmp/brighter-4170-gcp-original-shutdown-diagnostic/`.

## Result — 2026-11-09

All four focused cases pass on each framework. The fixed complete .NET 9 matrix
finishes in 559.83 monotonic seconds with **456 passes, one failure and 55 existing
skips**, retaining all 512 prior identities. Wall and monotonic durations differ
by 0.024 seconds. All four original shutdown cases pass in the full matrix too.

The [twelve shutdown observations](analysis.json) each correlate a successful native
deadline-zero RPC for the last original delivery with a second-generation delivery
of the same native/application ID and a fresh acknowledgement ID. Buffered cases
observe two first-generation deliveries before the replacement. These samples do
not reproduce or resolve the historical missing replacement delivery; the original
async disposal assertion still does not await the disposal task's result.

The full-run failure is the existing GcpOrderedNackRpcRecoveryTests async/default-
client case at line 190, outside the temporarily instrumented provider. Its own
[native trace](ordered-nack-failure-native.log) records eleven processors, initial
native IDs 414 and 417, and first-message acknowledgement suffix 427. The first
message's native nack completes at 19:52:15.4997897 UTC. Neither message returns at
native input before the test ends at 19:52:45.5889150 UTC. The second-message ack
and sixty-second extension complete almost simultaneously with that nack; client
timestamps do not establish server execution order. This is consistent with the
retained upstream ordered-redelivery finding, not evidence of a replacement-channel
shutdown defect or Brighter dropping an already-redelivered message.

The build has zero errors and 3,316 existing warnings. All 6,614 main production/test
source files are unchanged, and 974 relevant isolated inputs match main after the
provider's exact restoration. No test identities, bounds, defaults, assertions or
production code change. All three owned emulators are independently verified stopped;
unrelated services are untouched. No commit or push is performed. See
[verification](verification.json), [diagnostic patch](provider-observation.patch)
and per-run JSON/native traces in this directory. Complete TRXs/build/broker logs
remain in the evidence directory above. No further unguided repetition is planned.

The subsequent [disposal-result amendment](../gcp-disposal-result/README.md) fixes
the original async tests' missing await and proves it with a scoped disposal-fault
mutation. This strengthens observation of disposal errors; it does not retroactively
establish the cause of historical missing replacement deliveries.
