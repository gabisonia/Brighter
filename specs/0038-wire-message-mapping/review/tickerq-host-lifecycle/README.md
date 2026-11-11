# TickerQ baseline host lifecycle investigation

Review-after for #4170. Fixture teardown is validated and integrated. The separate
production timing correction is linked below; whole-feature readiness remains open.

The unchanged full scheduler baseline has 47 passes/one failure on .NET 9 and
43 passes/five failures on .NET 10. The failures include a handler already invoked
immediately after registration with a one-second delay. Diagnostic runs preserve
the assertion condition and record current/received request identities plus elapsed
time: two .NET 10 failures invoke the current request after 3.802 ms and 4.576 ms.
They are not merely stale dictionary entries. The diagnostic .NET 9 run also finds
early message publication. Original assertion sources are restored byte-for-byte.

`BaseTickerQFixture` starts a native scheduler but implements no fixture teardown.
The installed TickerQ in-memory provider uses static stores, so prior fixture hosts
can continue to operate on jobs registered by subsequent fixtures. An isolated
cleanup experiment stops the host at class teardown; .NET 9 retains all 48 passing
test outcomes, but synchronous service-provider disposal reports four class-cleanup
errors. Those errors prevent treating this experiment as a clean regression pass.
The asynchronous cleanup result is recorded below.

Source reference: installed TickerQ 10.4.0 records repository commit
`c6ed1e7daa90ab3f4c65b40319a153126a910093` in its NuGet metadata. The upstream
`TickerInMemoryPersistenceProvider` declares static stores and the scheduler uses
those shared jobs. This supports the lifecycle hypothesis, but does not by itself
prove the complete early-execution mechanism.

Evidence: `/private/tmp/brighter-4170-scheduler-wire-selection/TickerQ-*`.
No dependency versions, production timings or assertions have been relaxed.
Native selected-writer acceptance is now validated in the combined final package;
durable-restart acceptance remains open.

Asynchronous teardown removes all cleanup errors and yields 48 passing cases on
both frameworks. The expanded .NET 10 suite later reproduces one early-dispatch
failure, so teardown is retained as correct fixture ownership, not a complete timing
fix. The native one-second immediate-dispatch path and its Brighter correction are
tracked in [not-before dispatch](../tickerq-not-before/README.md).
