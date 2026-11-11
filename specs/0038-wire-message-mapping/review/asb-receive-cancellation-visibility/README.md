# Receive cancellation: initial visibility wait — validated

The approved test's immediate native peek can return null just after a successful
topic send. The first .NET 9 topic run fails at that setup assertion before calling
ReceiveAsync. The queue run reaches the intended RED: no cancellation exception is
thrown. Failed cases retain messages; the subsequent .NET 10 runs then fail source-ID
assertions against those leftovers. All original results are retained at
`/private/tmp/brighter-4170-asb-receive-cancellation/`.

The approved replacement test changes only the initial visibility observation:
poll native PeekMessageAsync from sequence zero up to 20 times, with 100 ms between
empty observations. Require a non-null delivery afterward and preserve the exact
source-ID assertion. A foreign message still fails immediately; the test does not
search past it or consume it. All cancellation, mapping, delivery-count, settlement
and recovery assertions remain unchanged. The original approved helper is unchanged.

This matches the bounded native-observation approach used elsewhere in this spec.
It tolerates delayed visibility without replacing native I/O or weakening the
cancellation behavior. Individual validation/mutation cases will also start with a
fresh fixture so an expected failure cannot contaminate later cases/frameworks.

The production pre-entry cancellation guard and XML documentation are implemented
from the queue RED. The amended test was approved, adopted unchanged and validated.
Both cases pass initially and after mutation restoration on both frameworks. Six
mutations produce 12 intended failures. Both full available suites pass 604 cases,
zero failures/skips, retaining all 602 previous outcomes. Original failure evidence
is preserved; the existing reader remains unchanged. All mutations are restored.

See the [parent batch](../asb-receive-cancellation-recovery/README.md) for scope and
remaining release limitations. Dedicated fixture containers are stopped. Nothing
committed or pushed.
