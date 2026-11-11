# PostgreSQL rejection writer selection

Review-after for #4170. All twenty-four native cases are validated and integrated locally. No commits or pushes. Sync/async rejection and JSON/JSONB storage cover
selected dead-letter/invalid forwarding, writer throw/null and subsequent recovery.
Selection must be captured before the lazy producer is created. Native observations
require destination, identity, body, tenant and rejection metadata to be preserved.

The existing consumer deletes the source row in its rejection finally block even
when forwarding fails. These cases retain that behavior and verify recovery with a
fresh subsequent message. They do not promise recovery of the deleted original.

Configuration-only properties preceded RED. All 24 cases failed at the intended
native row/composed-content assertions, then passed after capturing the writers in
lazy producer creation. Full regression passes 341 cases with all 317 prior
identities/outcomes retained. All four library targets build. A main-workspace
locked restore, rebuild and native focused rerun also pass all 24 cases.
The initial undeclared-framework and missing-pump setup attempts remain separate.
Evidence: `/private/tmp/brighter-4170-postgres-rejection-selection/`.

The test project explicitly targets .NET 10 only; validation follows that declared
target. An attempted .NET 9 runner found no assembly and is retained as a setup
error, not a behavioral failure. Library builds cover their supported targets.
