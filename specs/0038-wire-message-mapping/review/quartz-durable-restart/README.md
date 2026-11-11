# Quartz durable process restart

Review-after for #4170. The standalone native acceptance probe is validated on .NET 9 and .NET 10. No commits or pushes. Production dependencies remain unchanged.

Registration and worker execution run in separate containers against PostgreSQL
JobStoreTX with string job-data properties and Quartz's System.Text.Json serializer.
The worker must find the persisted job before starting, execute the selected RabbitMQ
writer and preserve identity, type, payload, tenant, fresh dispatch telemetry,
confirmation and empty-outbox state. Sync/async registration is tested on both
frameworks. Each case owns its database schema and broker queue/exchange.

CHARACTERISE mutation: replace the deserialized callback body in QuartzBrighterJob.
The new worker must fail at the `native-payload` assertion, not job setup. Restore
production bytes exactly, repeat the restart checks and both full Quartz suites.

The schema is copied unchanged from the exact commit recorded by Quartz 3.22.4:
[PostgreSQL tables](https://github.com/quartznet/quartznet/blob/1194020e01a23bc19e65c3bdd602437a4b03cf13/database/tables/tables_postgres.sql).
Initialization first verifies `current_schema()` equals the generated owned schema;
its reset statements cannot target the shared public schema. Configuration follows
[Quartz 3.x System.Text.Json documentation](https://www.quartz-scheduler.net/documentation/quartz-3.x/packages/system-text-json.html).

Evidence: `/private/tmp/brighter-4170-quartz-durable-restart/`.

## Validated result

Four baseline and four restored restart cases pass across sync/async registration
and both frameworks. Both production body-corruption mutations fail at the native
payload assertion. Production bytes are restored exactly. Both complete Quartz
suites pass 53 tests, with matching identities and outcomes across frameworks.
Each owned PostgreSQL schema and RabbitMQ queue/exchange is cleaned up.

This proves application-process restart with the pinned PostgreSQL JobStoreTX
provider and serializer. It does not establish database/broker crash recovery,
durable outbox recovery or every Quartz persistence configuration. Dependencies
are private to this acceptance executable; production package versions are unchanged.
