# Ordered nack: native protocol and stream-ownership control

Status: approved and executed with a readiness-only setup correction. The native emulator stall and separate recovery intervention reproduce on both frameworks.

The [native SDK control](../gcp-ordered-nack-native-control/README.md) reproduces
successful nack without redelivery within thirty seconds on both frameworks, with
no Brighter production dependency. It does not distinguish high-level SDK behaviour
from the emulator. This next control isolates native RPC order and stream membership.

Review [Program.cs](Program.cs) and [ProtocolControl.csproj](ProtocolControl.csproj).
It uses generated gRPC service clients directly, without SubscriberClient handlers,
automatic lease management, Brighter, intercepted responses, reflection or internal
emulator calls. All requests and responses reach the same pinned emulator.

## Four observations per framework

Each observation creates its own ordered subscription. A first raw streaming pull
receives two messages with one ordering key before any settlement occurs. The matrix
crosses retaining one stream versus adding a second stream, and completing the first
message's nack before the second message's ack versus completing that ack before the
nack. Run the four fixed combinations once on each of net9.0 and net10.0, eight total,
using fresh ready emulators and a ten-minute process hang limit. Retain all outcomes.

For the two-stream arms, a separate marker must actually arrive on stream 1 and be
acknowledged before proceeding. This establishes readiness through I/O, not a sleep.
The ordering keys `ordered-race-0` and `probe-marker-3` are selected to map to bucket 1
with two streams under the pinned emulator's Java string/Guava consistent hashing;
they map to bucket 0 with one stream. The marker verifies the assumed new-stream
assignment. An early marker on stream 0 is acknowledged and another is observed within the same fifteen-second readiness bound. Wrong message identity or failure to observe stream 1 is a setup error, not evidence of missing redelivery.

The native stream deadline is sixty seconds, matching the lease duration observed
in the failing SDK traces. The subscription deadline remains thirty seconds. Generous
raw flow-control limits avoid introducing a separate client-side queue limit. No
automatic lease extensions occur. Initial messages and the marker each have a
fifteen-second bound; redelivery is measured for thirty seconds. The whole operation
has a ninety-second budget plus bounded stream shutdown and resource cleanup.

If the first message does not return, preserve that RESULT, then repeat the second
message's original acknowledgement once and observe for five more seconds. Record
recovery separately. This intervention tests whether a deferred acknowledged delivery
is blocking progress; it never converts the original failure into a pass.

Expected discriminating pattern, not a required result: only adding a stream while
acking the second message before nacking the first prevents timely redelivery, and
repeating the second ack releases it. Different results must be retained and the
hypothesis reconsidered. Process exit zero alone is not evidence of redelivery.

This deliberately controls native RPC order in a separate experiment. The approved
Brighter and native SDK tests retain immediate nack/receive/ack sequencing. The control
does not prescribe serialized acknowledgements as a product fix or change their bounds.
No production mutation is proposed for this investigative executable.

## Approval boundary

[Repository testing instructions](../../../../.agent_instructions/testing.md) require
"STOP after writing the test and ASK FOR APPROVAL". This is a new protocol experiment,
prepared for review before execution. Approval covers this fixed matrix, evidence
collection, diagnosis and a local upstream reproduction report. It excludes publishing
that report, changing SDK/emulator versions, weakening existing tests, committing, or
treating emulator results as hosted-service validation.

## Execution outcome

Both frameworks complete all four combinations. Only adding a stream and completing
the second ack before the first nack prevents redelivery within thirty seconds. Repeating
the original second ack then releases the first message onto stream 1 within 37.620 ms
(net9.0) / 46.755 ms (net10.0), preserving native ID/body with a new delivery ack ID.
This is a separately recorded recovery, not a pass of the original bound.

The initial marker-on-stream-0 setup error is retained for each framework. The approved
source snapshot and readiness-only.patch document the correction; all settlement,
observation, recovery code and bounds remain unchanged. Initial and corrected builds
have zero warnings/errors. No production or compiled-test source changes occur. All
four owned emulators across the two attempts are removed; logs are preserved.

The [upstream report](../../gcp-ordered-nack-upstream-reproduction.md) and
`/private/tmp/brighter-4170-gcp-ordered-nack-protocol-control/` contain the complete
matrix and evidence. This establishes an emulator protocol reproduction, not hosted
behaviour or a Brighter correction. The [hosted control](../gcp-ordered-nack-hosted-control/README.md)
is prepared for separate review and remains unbuilt/unrun. Nothing is committed or published.
