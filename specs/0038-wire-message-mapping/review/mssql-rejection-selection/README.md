# SQL Server rejection writer selection

Review-after for #4170. Validated and guarded integration completed locally.
No commits or pushes.

Twelve native cases cover sync/async consumers, invalid/dead-letter destinations,
composed mapping and writer throw/null. Selection is captured when creating the
consumer, including before lazy rejection producer creation. Native SQL rows verify
routing, identity, payload and rejection metadata independently of the consumer.

The established atomic read-and-delete contract remains: a failed forwarding write
leaves no source or destination row and Reject returns true. Recovery means a fresh
subsequent message can be forwarded after the mapping recovers; it cannot recover
the already deleted original. Both routes are configured to expose wrong selection.

Configuration-only properties are added before the initial RED run, with no factory
or consumer wiring. Both frameworks produced twelve intended behavioral RED failures (unselected
default payload/row), then twelve GREEN cases after factory/consumer wiring.
Earlier missing shared-project and table setup failures are retained separately
and do not count as RED. Full regressions pass 392 cases/four existing skips on
each framework, preserving all prior 384 identities and adding twelve cases.
All four library targets build. Public constructor signatures remain unchanged.

Evidence: `/private/tmp/brighter-4170-mssql-rejection-selection/`.
