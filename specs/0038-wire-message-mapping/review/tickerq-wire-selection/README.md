# TickerQ serialized wire selection

Review-after for #4170; no commits or pushes. Validated and integrated locally.

Two sync/async cases use the actual TickerQ host and native RabbitMQ dispatch.
They require the serialized publication alias, logical topic, event type, tenant
and body to survive caller mutation after registration. Native publication must
use the selected custom writer and fresh dispatch telemetry, confirm the original
identity, clear the outbox and deliver exactly once to the bound queue.

Named production mutations remove the deserialized selector and replace the
serialized body. Each fails both cases at the corresponding assertion, with
exact restoration followed by passing focused cases. Full suites pass 54 cases on
each framework: all 48 original identities, two native mapping cases and four
[not-before timing cases](../tickerq-not-before/README.md). Fixture host teardown is tracked
separately in [the lifecycle investigation](../tickerq-host-lifecycle/README.md).

The existing in-memory persistence provider does not establish durable process
restart. That acceptance gate remains open. Evidence is collected under
`/private/tmp/brighter-4170-scheduler-wire-selection/TickerQ-*`.

Final evidence: `/private/tmp/brighter-4170-scheduler-wire-selection/TickerQ-final-verification.json`.
