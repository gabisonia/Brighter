# Quartz recovery trace-chain assertion amendment

Status: approved and validated. The amended test, with one equivalent xUnit overload cleanup, passes all eight cases on both
framework/broker pairs. Three mutations produce 24 intended failures. Both full
RabbitMQ suites pass 556 cases/two existing skips; both Quartz suites retain 53
passes and both Core suites retain 1,716 passes/seven existing skips. The original
review draft and initial RED evidence remain preserved. Production is unchanged.
See the [validation record](../../validation.md#quartz-scheduled-writer-recovery-validated).

## Observed result

All eight original recovery cases fail on both .NET 9/RabbitMQ 4.2 and
.NET 10/RabbitMQ 4.3, at line 247: the assertion that the recovery activity directly
parents the publish span. Earlier assertions pass, including retained original
outbox payload after writer failure, successful recovery confirmation, native
custom output and equality between the wire traceparent and actual publish span.
Assertions following line 247 have not yet executed.

The test overlooked an existing public API behavior.
`OutboxProducerMediator.ClearOutbox` and `ClearOutboxAsync` create an outbox-clear
span beneath the supplied request-context span, then dispatch beneath that clear
span. The expected chain is recovery activity → outbox-clear span → publish span.
Changing production to remove the clear span would discard valid instrumentation.

## Exact amendment

The [amended test](When_a_quartz_rmq_writer_fails_should_retain_the_message_until_outbox_recovery.cs)
adds a separate queue for producer-kind spans with operation `clear`, clears it
before recovery, and replaces the direct-parent assertion with:

- Exactly one recovery clear span.
- Its trace ID and parent span ID identify the recovery activity.
- The publish span has the clear span's trace ID and parent span ID.

Native traceparent equality, distinction from failed publication spans and the
schedule-time parent, all failure/retention assertions, settlement, source
preservation and duplicate checks remain intact. The writer helper is unchanged.
No production code change is proposed.

## Work after approval

Replace only the compiled test with this amended version; retain the original
approved draft and initial failure evidence. Run all eight cases on both pairs.
Verify the three named mutations from the
[original review](../quartz-rmq-writer-recovery/README.md), using the amended
assertion line numbers, with 24 intended failures expected. Restore exact source
bytes, rebuild all affected targets and rerun focused checks under normal runtime
settings. Complete both full RabbitMQ (556 passes/two existing skips expected),
Quartz (53 passes expected), and Core (1,716 passes/seven existing skips expected)
suites. No commit, push, publication or review-gear change is included.

Initial results, source manifest and verified failures are retained under
`/private/tmp/brighter-4170-quartz-recovery/`. Both approved files and all 1,277
production source files remain unchanged. The three mutations and six full
regressions are validated after this amendment. Full #4170 acceptance remains open.
