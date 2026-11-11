# Hangfire durable process restart

Review-after for #4170. Durable native acceptance is validated. No commits or pushes. The probe uses the cached Hangfire.PostgreSql 1.20.4
provider solely as an acceptance fixture, with the repository's existing Npgsql
10.0.3 version explicitly selected. Production package versions are unchanged.

A registration process creates a persisted Hangfire job and exits with no worker
running. A separate container/process loads that existing job from PostgreSQL,
activates Brighter with a new command processor, and dispatches through the selected
RabbitMQ writer. Sync/async registration is checked on .NET 9 and .NET 10. The native
output must retain publication selection, identity, type, body and tenant, contain
fresh dispatch telemetry, confirm delivery exactly once and leave an empty outbox.

This is a CHARACTERISE package. Named production mutation: replace the deserialized
message body in BrighterHangfireSchedulerJob before dispatch. Registration and fresh
worker activation must still succeed, but the worker's `native-payload` assertion
must fail. Restore production bytes exactly, rerun the probe and run both complete
Hangfire project suites. No mutation is integrated.

Each case uses a unique database schema and broker resources. The runner preserves
state/results and failure diagnostics, and cleans only its own schemas and native
queues/exchanges. This proves app process restart for this provider, not broker or
database crash recovery or transactional durability of an in-memory outbox.

Sources are portable: pass `-p:BrighterRoot=/path/to/Brighter` when building elsewhere.
Evidence: `/private/tmp/brighter-4170-hangfire-durable-restart/`.

Four baseline and four restored restart cases pass across both frameworks and
registration modes. Both body-corruption mutations fail at `native-payload` with
normal exit code 1. Exact production bytes are restored. Both full Hangfire suites
pass all 54 cases. Every owned case schema and broker queue/exchange is cleaned.
The earlier missing storage-disposal cast, minimum provider dependency warning,
mutation-anchor typo and unhandled-exception exit hang are retained as harness
history, not accepted regression failures. The final probe catches failures and
exits normally; its runner stops an owned container if a phase times out.

Final evidence: `/private/tmp/brighter-4170-hangfire-durable-restart/final-verification.json`.
