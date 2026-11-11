# Quartz scheduled writer failure and outbox recovery

Status: validated with the approved
[trace-chain amendment](../quartz-rmq-recovery-trace-chain/README.md). The original
draft below remains preserved; its direct-parent assertion failed on all 16 initial
executions. The amended test, with one equivalent xUnit overload cleanup, passes all eight cases on both pairs, three mutations
produce 24 intended failures, and all six full regressions pass. Production source
is unchanged. See the [validation record](../../validation.md#quartz-scheduled-writer-recovery-validated).

## Contract

The [eight cases](When_a_quartz_rmq_writer_fails_should_retain_the_message_until_outbox_recovery.cs)
cover message/request scheduling, sync/async dispatch, and a configured writer
that initially throws or returns null. They extend the validated native Quartz
path with actual mapping failure and explicit recovery through the public outbox
clearing API. The broker, Quartz job, command processor, scheduler handlers,
outbox mediator and native observation all remain real.

The [recoverable writer](QuartzRecoverableRmqWireWriter.cs) counts failed mapping
attempts and then delegates to the existing non-CloudEvents writer after recovery.
Its output uses AMQP headers and a plain text body. Both header/body formats are
already covered by the preceding successful-dispatch batch; failure happens before
either representation is produced.

Before Quartz starts, no writer attempt, publication or outstanding outbox entry
is allowed. After the scheduled callback completes with the writer still failing,
there must be a recorded failed attempt, no native delivery or successful publish
confirmation, and exactly one pending outbox entry with original ID, topic and
payload. Both original application objects must retain their exact serialized
representations.

Quartz completion is expected to succeed here: the outbox mediator deliberately
catches exhausted publication errors and retains the message for later clearing.
The earlier completion fix remains in place. This contract checks retention in
the in-memory outbox and does not introduce Quartz refiring or change its retry policy.
The existing direct SQS writer-failure tests use the same outbox recovery behavior.

After Quartz has shut down, recover the writer and clear the retained message
using `ClearOutbox`/`ClearOutboxAsync` under a fresh recovery activity. Independent
native observation must see one confirmed custom publication with the original
ID/payload, expected content type, persistence and SDK confirmation metadata.
Its traceparent must identify the recovery producer span, distinct from the
failed producer spans and schedule-time message parent. The outbox must settle,
the application objects must remain unchanged, and a second native read must find
no duplicate.

The cases reuse the preceding command, mapper, Quartz factory/listener and
non-CloudEvents writer without modification. No new request handler, dependency,
public production API, mock or generated fixture is introduced. Each case owns
its queue, exchange and Quartz scheduler. Shutdown precedes collaborator disposal
and native resource deletion. The fixture retains the validated one-second delay
and 45-second per-case cancellation budget.

## Named mutations

If initially GREEN, run these independent mutations on .NET 9 with test bytes
unchanged. Diagnostic JIT settings may preserve source locations; normal focused
and full suites retain default runtime settings.

| Mutation | Expected failures | Intended assertion |
| --- | ---: | --- |
| In PublishMessageAsync only, catch configured-writer exceptions or null results and substitute the default writer | 8 | Native queue remains empty before recovery |
| In the outbox mediator's sync and async send paths, mark the failed message dispatched despite mapping failure | 8 | Exactly one pending outbox entry before recovery |
| Move RabbitMQ producer propagation after native publication | 8 | Native traceparent equals the recovery producer span |

For the second mutation, add marking after the confirmation-capable sync
`ExecuteWithResiliencePipeline` call regardless of its result, and mark in the
async path when `sent` is false. Keep the actual failed publisher unchanged.
The three mutations should produce 24 intended assertion failures. Compilation
errors, timeouts or unrelated setup failures do not count. Preserve unexpected
evidence and return semantic test changes for review.

## Work after approval

Copy the test under `MessagingGateway` and the writer under `TestDoubles` in the
RabbitMQ Async test project. Run all eight cases on both framework/broker pairs,
fix only reproduced gaps, verify the three mutations and restore exact GREEN
source bytes. Rebuild all affected targets and rerun the focused cases with normal
runtime settings.

Run both full RabbitMQ Async suites with TLS/native-delay coverage. Retain all
548 passes/two existing skips and add eight passes (556 passes/two skips expected).
Run both full Quartz suites, retaining 53 passes each, and both full Core suites,
retaining 1,716 passes/seven existing skips. The Core runs cover restoration after
the outbox mutation. Use isolated task brokers and preserve HEAD and the saved
stash. No commit, push, publication or review-gear change is included.

AWS callback-pump settlement/recovery, other schedulers and gateways, scheduler-
backed consumer retries and final public configuration review remain separate
#4170 completion gates. Zero-delay timestamp rejection remains recorded separately.
