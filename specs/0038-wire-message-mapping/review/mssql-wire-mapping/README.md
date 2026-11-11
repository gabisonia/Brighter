# SQL Server endpoint wire mapping

Status: initial integration validated on the native ARM64 repository fixture.
SQL Server 2022 environment finding open.
No commits or pushes.

Completion-plan stage 2 introduces MsSqlPublication.WireMessageWriter and subscription
reader/requeue-writer selections, with composable default JSON adapters. The existing
Publication-based factory contract remains valid. Payload mappings use strings because
SQL Server stores NVARCHAR payloads; row ID, topic, MessageType and SQL operations stay
outside the mapping. No table or outbox serialization changes are permitted.

SQL Server uses atomic read-and-delete. Acknowledgement and nack are existing no-ops;
there is no PostgreSQL-style visibility lease to preserve. The native tests must verify
that exact behavior, including unchanged row-lock/readpast selection. Reader failures
must become rejectable messages containing the raw payload and native row ID after
atomic removal. They must not invent a receipt or claim the row remains in the source.

The generic public MsSqlMessageQueue<T> remains compatible. An internal constructor can
accept serialization/deserialization strategies; its existing public constructor retains
its current System.Text.Json behavior. Tests exercise these strategies through production
producer/consumer factories, not the internal constructor.

First matrix: sync/async factories with complete and default-composed formats. Inspect
native Payload, Topic, MessageType and row ID; verify dispatch-time trace context and
unchanged caller body/bag. The consumer captures its mapping choices, reads custom
content, atomically removes exactly one row, and can immediately retry through its selected
writer. A retry inserts a new row with the same application identity. Native acknowledgement
must remain a no-op. Writer and reader failure/recovery cases and the existing complete
suite on both test frameworks are required before accepting the integration.

Named mutations: bypass selected producer writer, bypass selected consumer reader,
bypass selected immediate requeue writer, and propagate custom reader failure instead
of returning a rejectable message. Each must fail the matching native assertion and pass
after exact restoration. Generic queue default compatibility is covered by full regressions.

Positive-delay scheduling and configured rejection writers remain in the cross-gateway
scheduler/rejection workstreams and must be resolved before whole-issue readiness.

## Baseline and failure cases

The unchanged suite passes 368 cases/four existing skips on .NET 9. During the first
.NET 10 run, the native SQL Server container exited 139 (not OOM-killed). Its state,
server logs and the aborted runner logs are retained. Restarting the same database
restored native readiness and the complete unchanged .NET 10 suite passed 368 cases/
four skips. That recovery does not establish the root cause of the server crash.
The fixture is an amd64 SQL Server 2022 image on an ARM64 local host; an unverified
emulation cause is not claimed as a diagnosis. Retain this environment finding in
final release review and require stable native regression evidence.

Four selected-mapping cases and eight writer/reader failure cases pass on both frameworks. Writer
failures preserve zero native rows and recover on the same producer. Reader failures
return a rejectable message with native ID/raw payload after atomic removal. Rejection
remains a no-op without a forwarding channel; a subsequent read succeeds after recovery.

## Fixture diagnosis and validation boundary

Running the owned SQL Server and RocketMQ fixtures together exhausted the Docker VM's
available memory. SQL Server recorded FAIL_PAGE_ALLOCATION and inability to allocate
MsqlXactManager; the unchanged compiled baseline failed too. Only owned RocketMQ services
were stopped. Other user containers and Docker settings were left untouched.

With RocketMQ stopped, unchanged SQL Server 2022 baselines passed on both frameworks,
but the amd64 server again exited 139 after roughly five minutes. This reproduces the
crash independently of concurrent RocketMQ load; memory contention does not explain it
away. All three stopped/crashed fixture runs and native logs remain recorded in the
evidence directory. No intermittent pass is counted as a stable SQL Server 2022 result.

The repository's docker-compose-mssql.yaml uses Azure SQL Edge. Its native ARM64 image
passes both unchanged baselines (368 passes/four existing skips each), all twelve focused
cases on both frameworks, and all twenty intended mutation failures with exact restored
passes. The reader-null mutation initially exposed an incidental dereference failure;
an explicit NotNull assertion now reports the intended missing rejectable message.
The native application tests establish readiness; the ARM64 image lacks the sqlcmd binary
used by the initial fixture probe, so that probe's failure is not a database-start failure.

Evidence: `/private/tmp/brighter-4170-mssql-wire-mapping`. Preserve the SQL Server 2022
crash as a required environment finding in final release review. Stable repository-fixture
validation establishes mapping behavior, not resolution of that separate crash.

## Final integration result

Both full expanded suites pass 380 cases/four existing skips, preserving every one of
the 372 preceding identities and outcomes. All four gateway targets build with zero
warnings/errors. Source reconciliation confirms exactly the five intended existing
MsSql gateway files changed; schemas, outbox serialization and preceding source files
outside that set are unchanged. The native ARM64 server remained running throughout
the two baselines, mutation/restoration matrix and final regressions.

The completed integration adds three production types (publication and two default
adapters) and six test/helper files. The evidence's final-verification.json records
twelve focused cases per framework and twenty intended mutation failures. This closes
initial endpoint integration only; scheduled/rejection combinations, the SQL Server
2022 crash finding and final whole-issue validation remain open.

Microsoft's [SQL Server container support policy](https://learn.microsoft.com/sql/linux/install-upgrade/quickstart-install-docker)
requires Intel/AMD x86-64 Linux hosts and excludes emulation/translation environments.
This ARM64-host/amd64-image test setup is therefore outside that policy. This explains
the validation limitation, not the crash's root cause. Supported-host SQL Server 2022
coverage remains unavailable locally and must be reported separately at release review.
