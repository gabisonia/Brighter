# Ordered-nack lease-order control

Review-after #4170. The full generated-test native trace captures a successful
zero-deadline RPC concurrently with a 60-second extension for the same original
ack IDs. Client completion order cannot establish server application order.
This fixed raw-gRPC experiment distinguishes a post-nack extension from the already
reproduced emulator stream/ack stall without changing Brighter or its tests.

Reuse the existing raw protocol control with no Brighter/high-level SDK dependency,
automatic lease management, altered responses or server internals. Four cases per
framework cross one versus two proven-ready streams with a completed 60-second
extension before versus after the completed first-message nack. Always complete
the second-message ack first. Observe the first native ID for 30 seconds and retain
each result, including failures. No recovery intervention or repeat-until-green.
Existing stream readiness, bounds and cleanup remain unchanged. Eight observations
on fresh pinned emulators total; no hosted resources or product correction.

A one-stream failure only after extension would support that alternative; missing
redelivery confined to two-stream cases would support the existing emulator boundary.
These controlled sequences do not reproduce the exact concurrent server ordering
of the archived trace, so neither alone proves its internal cause.

## Outcome

All eight planned observations complete without setup/cleanup errors or clock
discontinuity. Both one-stream cases return the first native message within the
unchanged 30-second bound, whether the 60-second extension precedes or follows
the nack. Both two-stream cases fail that bound, on both .NET 9 and .NET 10.
Thus the controlled extension order does not independently reproduce the stall;
stream membership remains the discriminating factor in this control, consistent
with the earlier raw-protocol reproduction. No high-level SDK or Brighter code
participates. The original failures are retained as false RESULT values; process
exit zero means experiment completion, not that all redelivery cases passed.

This strengthens the emulator-boundary evidence, but does not reveal the internal
server processing order of the concurrently captured generated-test failure.
No production workaround, client-count change, longer assertion bound or test
exclusion is introduced. Both owned emulators are stopped; native logs are retained.
