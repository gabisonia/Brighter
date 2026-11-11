# Hangfire serialized wire selection

Review-after for #4170. The scoped package is validated and integrated locally; no commits or pushes.

Two sync/async cases run the real Hangfire background server with its in-memory
storage and a native RabbitMQ producer. The scheduled message selects a publication
alias distinct from its logical routing topic and retains its event type. Mutating
the caller's body, tenant and selector after registration must not change the
serialized job. The job must succeed, publish the original payload through the
custom writer, use a fresh producer trace, confirm the original identity and empty
the outbox. No broker or scheduler isolation mock is used.

Both focused cases pass on .NET 9 and .NET 10. Removing the deserialized publication
selector causes two intended confirmation assertion failures; replacing its body
causes two intended payload failures. Both mutations are exactly restored and the
two cases pass after each restoration. No production correction is currently needed.

The unchanged baseline passes 52 cases on each framework. The first full .NET 9
run with these cases retains those 52 passes, but both new cases time out awaiting
confirmation. Stored job-state diagnostics are being added to explain this full-suite
interaction. Focused passes and mutation results do not close that regression gate.

Evidence: `/private/tmp/brighter-4170-scheduler-wire-selection/Hangfire-*`.
The implementation and tests are being validated in the isolated checkout. The
project reference uses the existing RabbitMQ Async gateway and central package
versions; locked restore succeeds. This in-memory-storage package does not prove
durable process restart. That remains a separate acceptance requirement.

## Completed validation

The full-suite interaction is reproduced as a TypeInitializationException in
RmqMessageProducer caused by a disposed global LoggerFactory. The earlier DI factory
configuration test installed its provider-owned factory globally, then disposed the
provider without restoring the previous global reference. Its teardown now restores
that reference. Temporary diagnostic retry hooks are removed; normal resilience
settings and all acceptance assertions are retained.

Both complete suites now pass 54 cases, preserving all 52 prior identities/outcomes.
The two selection/body mutations again produce four intended failures and restore
exactly. The existing test cleanup, project reference and new test/helper are
integrated after original-byte checks. No scheduler production change was needed.
Durable process restart remains a separate open gate.
