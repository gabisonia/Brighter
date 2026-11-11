# Kafka outbox and confirmation-subscriber recovery

Status: approved and validated as characterisation. All six cases pass initially
and after mutation restoration on both frameworks without production changes.
Seven named mutations produce 40 intended assertion failures. Both complete Kafka
suites pass 323 cases with no failures/skips, retaining all 317 prior outcomes.
The approved test, reused helper, all 1,282 production files and dependencies remain
unchanged. No commit or push was made.

The [six cases](When_kafka_publication_or_confirmation_fails_should_preserve_pending_outbox_messages_and_recover.cs)
combine synchronous/asynchronous outbox clearing with a throwing writer, a null-returning
writer, or a throwing confirmation subscriber. They reuse the approved
[KafkaRecoverableWireWriter](../../../../tests/Paramore.Brighter.Kafka.Tests/TestDoubles/KafkaRecoverableWireWriter.cs).
The production producer comes from the public factory. A real
`OutboxProducerMediator<Message, CommittableTransaction>` uses an `InMemoryOutbox`
and the standard resilience policies. A native Kafka observer sees the records.
No mock, internal access, handler, logger registration or dependency is added.

For writer failures, a direct send first requires a surfaced error. Outbox dispatch
then exhausts its retries without publication. The original message must remain
pending with its binary body, identity, topic and tenant value intact. A healthy
message must still publish and only its outbox entry becomes dispatched. Repairing
the writer and clearing the original entry must publish its retained content and
mark it dispatched through the actual broker confirmation.

For subscriber failures, the selected writer succeeds. A subscriber registered before
the mediator observes the successful delivery and throws. Existing multicast semantics
skip the remaining subscribers for that confirmation; therefore the original outbox
entry remains pending even though its native record exists. A later healthy confirmation
must still run and dispatch its own entry. Repairing the subscriber and clearing the
original entry publishes again and completes dispatch. These cases explicitly expect
three native records, including two for the original message: this is recovery with
possible duplicate delivery, not an exactly-once claim. Sync cases throw from the sync
event; async cases return a faulted task through the async event.

Each native record must have the selected key, changed wire identity and encoded
binary body. Confirmations must keep source identity and topic. Bounded confirmation
signals and outbox-state polling precede assertions. Registry disposal flushes native
reports and drains callbacks before final counts and partition-watermark checks.
Unique one-partition topics are cleaned up even on failure. Source messages remain
unchanged. Persistent database outboxes and automatic sweepers remain separate work.

## Validation after approval

Copy the test unchanged into MessagingGateway; reuse the existing helper. Preserve
initial results on both frameworks. If initially green, CHARACTERISE using the named
mutations below. If a real failure occurs, preserve it, implement only this contract,
and prove the same mutations. Never weaken the approved assertions.

| Mutation | Cases | Intended assertion |
| --- | ---: | --- |
| Ignore the publication writer | 4 writer-failure cases | Mapping error is not null, line 131 |
| Mark dispatched after a resilience-wrapped send despite confirmation support | 6 | Initial pending entry is single, line 147 |
| Replace selected native bodies with original binary bodies | 6 | First native mapped body, line 164 for callback cases or 195 for writer cases |
| Replace selected native keys with source keys | 6 | First mapped native key, line 162 or 193 |
| Replace selected wire IDs with source IDs | 6 | First selected wire ID, line 163 or 194 |
| Confirm wire identity instead of source identity | 6 | First source confirmation ID, line 159 or 186 |
| Omit successful outbox marking in the mediator's async confirmation callback | 6 | Only the original message remains pending after healthy confirmation, line 189 |

Require 40 intended assertion failures across seven independent production mutations.
The premature-dispatch mutation covers both sync and non-bulk async dispatch. All
producer instances support async confirmations, so the omitted-mark mutation targets
the mediator's async callback even for sync clearing. Keep the real callbacks running;
the last mutation must reach the pending-state assertion after its bounded observation,
not fail on a missing confirmation. Do not accept compile errors, unrelated exceptions
or confirmation timeouts as mutation proof. Restore every mutated production byte.

Rebuild with normal optimization, rerun focused cases on both frameworks and both full
Kafka suites, expecting 323 passes each and retention of all 317 preceding outcomes.
Build every supported Kafka library target. If a correction changes Core or another
production project, also run that project's complete suites on both frameworks.
Verify source/dependency manifests, approved-file identity, warnings, links, whitespace,
HEAD and stash. Preserve evidence and stop task-owned services. No commit or push.

Broker failure/cancellation, rejection forwarding, factory isolation, malformed headers,
retry mapping, transactions/batches, concurrency and the [global #4170 gates](../../tasks.md)
remain incomplete.


Evidence is under `/private/tmp/brighter-4170-kafka-outbox-recovery/`. A mutation-runner
anchor initially matched bulk and non-bulk dispatch; it stopped before applying that
mutation. Narrowing the anchor to the reviewed non-bulk path allowed all intended
assertion failures to be proved. This harness correction changed no test assertion
and is not counted as mutation proof. The subsequent
[eight factory-isolation cases](../kafka-factory-isolation/README.md) are validated:
eight mutations produce 32 intended failures and both complete suites pass 331 cases
without production changes.


The .NET 10 full run had a 5-hour-31-minute wall-clock span but about 11.6 minutes
of summed reported test duration. All cases passed without a rerun. The
[validation record](../../validation.md#kafka-outbox-and-confirmation-subscriber-recovery)
and `long-run-timing.json` retain the timestamp discrepancy; its root cause is unverified.
