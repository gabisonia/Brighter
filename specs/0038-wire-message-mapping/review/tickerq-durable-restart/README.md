# TickerQ durable process restart

Review-after for #4170; no commits or pushes. Native acceptance passes on both declared test frameworks. Separate registration/worker processes share a real EF Core SQLite
file. The original process exits before dispatch; a new supported native host must
find and execute the persisted job and reach its persisted Done state.

Sync/async registration on .NET 9 and .NET 10 must retain the selected RabbitMQ
writer, logical destination, identity, type, tenant, body, fresh dispatch telemetry,
confirmation and empty outbox, with exactly one native message.

CHARACTERISE mutation: replace the deserialized message body inside
BrighterTickerQSchedulerJob before SendAsync. The probe must fail specifically at
native-payload. Restore exact production bytes, repeat restart checks and run
both complete 54-case TickerQ suites. Production dependencies remain unchanged.

Provider source: pinned TickerQ 9.3.0 and 10.4.0 NuGet repository commits;
AddOperationalStore/UseTickerQDbContext and the official console sample establish
the provider and host configuration. SQLite files are private acceptance artifacts.

Evidence: `/private/tmp/brighter-4170-tickerq-durable-restart/`.

## Validated result

Four baseline and four restored cases pass; both production corruption mutations
fail specifically at the native-payload assertion. The job reaches persisted Done
state after worker-process restart. Production source is restored exactly. Both
full suites pass 54 tests, matching the preceding certified TickerQ-guard-full
identities/outcomes. Earlier retained pre-correction runs are not that baseline.

Private acceptance dependencies are TickerQ.EntityFrameworkCore 9.3.0 with EF Core
SQLite 9.0.20 on .NET 9, and provider 10.4.0 with SQLite 10.0.12 on .NET 10. The
standalone probe uses public native host/provider APIs. Two compile setup errors
are retained separately and do not count as behavioral RED.

SQLite files remain in the private evidence directory; owned RabbitMQ resources
are cleaned. This proves process restart with this provider, not database-file
loss, broker crash or durable outbox recovery. Production dependencies are unchanged.
