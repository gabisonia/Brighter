# Generated ordered-nack RPC diagnosis

Review-after #4170. Observe the unchanged generated sync/async two-message ordered
nack cases once per framework (four observations), with the existing passive
GcpStreamRpcObserver attached through the provider's public StreamConfiguration
hook. No changes to test sequence, assertions, bounds, SDK client count or response
values. Use a fresh pinned emulator per framework and save raw native deliveries,
acknowledgements and deadline-zero RPC ordering. Instrument only the isolated test
provider, build, and restore it byte-for-byte before running its diagnostic binary.

This targets the current full-matrix failure rather than adding another differently
provisioned observation. Classify whether a missing application delivery is absent
already at native input, whether the exact native nack completed, and whether the
second-message ack precedes that nack. Passing observations do not erase retained
failures. No production correction is authorized by inference alone; no commits,
pushes or hosted resources are involved.

Both focused cases pass on both frameworks with passive observation, so the focused
sequence does not reproduce the current full-matrix finding. Preserve those four
passes without claiming resolution. Extend the diagnosis to one fixed complete
available .NET 9 Pub/Sub run with the already built diagnostic binary and a fresh
emulator. This retains the existing suite's preconditions and all test identities;
no further focused repetitions or changed assertions are included.

## Full-suite diagnostic result — 2026-11-09

The fixed .NET 9 run reproduces the exact generated async two-message failure:
456 passes, one failure, 55 existing skips; continuous wall/monotonic clocks.
The missing Brighter ID `01a1c1b2-1252-7896-9f1d-f6bf3bcf37a4` maps to
native message `2044`, original ack suffix `1950`; the second message is
`2045` / ack suffix `1951`. Both arrive at native input at 19:03:55.271 UTC.
The first message's deadline-zero RPC succeeds at 19:03:55.273201 UTC.
Neither message has another native delivery for this subscription before the
test fails at 19:04:25.383 UTC. Thus this observation does not show Brighter
dropping an already-redelivered message. See the retained native trace.

Do not overstate the cause: the second-message acknowledgement starts before
the first nack, but their completion order is reversed. A 60-second lease
extension for both original ack IDs is also concurrent and completes just
after the nack. Client completion timestamps do not prove server application
order. The prior raw-protocol control independently proves an emulator stall
without Brighter or automatic lease management, but this captured run alone
does not distinguish that stall from a lease-extension ordering race. Keep
the readiness finding open pending that distinction; no production fix or
test relaxation follows from this trace. Provider source was restored before
the diagnostic binary ran. The owned emulator is stopped with logs retained.

The [fixed lease-order control](../gcp-ordered-nack-lease-control/README.md) now
compares both completed RPC orders with one/two streams on both frameworks.
Only two-stream cases stall; neither one-stream order stalls. This supports the
independent emulator stream-membership finding rather than an extension-order-only
explanation. It still does not expose the exact archived server-side concurrent
order, and does not disposition separate shutdown/replacement findings.
