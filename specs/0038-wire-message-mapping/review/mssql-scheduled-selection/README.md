# SQL Server scheduled retry publication selection

Review-after for #4170; no commits or pushes. Validation and guarded local integration are complete.

Four native cases cover sync/async consumer scheduling with custom and default
writers. Real InMemoryScheduler callbacks dispatch through the command processor,
outbox and selected SQL producer. A registry alias differs from the SQL destination;
selection is captured at consumer creation. Native SQL observations require the
requested delay, exact destination, identity/type/body/tenant, dispatch traceparent,
empty outbox and an unchanged source message. An alternative registered publication
uses the opposite format to expose selector loss at the native payload assertion.

Configuration-only scaffolding leaves the initial production path unchanged.
After actual RED, wire the captured selector into a private scheduled message copy.
The named mutation omits the selector from that copy: all four native format
assertions fail, exact restoration returns all four to GREEN. Four actual pre-fix
format failures and four focused passes are recorded on each framework. An earlier
setup lacked the fallback publication type and timed out before mapping; those
attempts are retained separately and excluded from RED evidence. Complete regressions pass 396 cases/four existing skips per framework, retaining
all preceding 396 identities and adding four cases. All four library targets build
without warnings/errors. Public constructor signatures remain unchanged.

Evidence: `/private/tmp/brighter-4170-mssql-scheduled-selection/`.
