# TickerQ not-before dispatch

Review-after for #4170. Validated and integrated locally; no global readiness claim.
No commits or pushes.

The installed TickerQ registration path can immediately dispatch jobs scheduled
within one second of its current clock. The native function context retains the
actual scheduled time. See the pinned upstream
[registration path](https://github.com/arcenox-co/TickerQ/blob/c6ed1e7daa90ab3f4c65b40319a153126a910093/src/TickerQ.Utilities/Managers/TickerManager.cs#L139)
and the installed .NET 9
[scheduled-time context](https://github.com/arcenox-co/TickerQ/blob/0ef608b8327cb63167407b9af879da23922809b2/src/TickerQ.Utilities/Base/TickerFunctionContext.cs#L33).
This explains the earlier request/message baseline failures; host cleanup alone was
insufficient and is not recorded as their complete cause.

Four public native-job cases cover message/request jobs and sync/async registration
with a 750 ms delay. Both frameworks reproduce all four intended early-dispatch
failures. The initial async request probe used the sync-only fixture and timed out;
selecting the existing async request fixture corrects that setup without weakening
the timing assertion. Earlier diagnostics are retained.

Both job methods now await the native ScheduledFor time before invoking the command
processor. The wait observes native cancellation and rechecks the remaining time to
avoid dispatching early. Public constructor signatures, dependency versions and
serialized message/request contracts remain unchanged. All four focused cases pass
on each framework. The final combined package passes all 54 cases on each framework, preserving all
48 prior identities. Both selection/body mutations produce their four intended
failures, restore exactly and return to green. Every supported library target builds.
All six validated source/test files are integrated after comparing original bytes.

Evidence: `/private/tmp/brighter-4170-tickerq-not-before/` and
`/private/tmp/brighter-4170-scheduler-wire-selection/TickerQ-guard-*`.
Durable restart and broader scheduler/transport acceptance remain open.
