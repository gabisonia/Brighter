# Kafka native close diagnostic under the full suite

Review-after #4170. The archived hang occurs in native assignment during consumer
close in KafkaMessageConsumerCommitRevokeConcurrency; twenty focused repetitions
did not reproduce it. Run one complete .NET 9 Kafka suite, preserving all 412 cases,
with native cgrp/consumer/protocol debug output enabled only for that existing
class's consumers. Record strategy/group/topic before construction. Do not change
assignment strategies, assertions, timeout configuration, consume/commit sequencing
or production code. Build the isolated diagnostic test assembly, then restore the
test source byte-for-byte before execution. Collect a native/managed hang dump on
five minutes of inactivity. Preserve native logs and all results, including failures.

One fixed diagnostic full run, not a repeat-until-green loop. A passing run narrows
observed preconditions but does not resolve the retained stall. Do not change SDK
versions or shutdown implementation without a reproduced cause. No commits/pushes.

## Result — 2026-11-09

The one planned .NET 9 full-suite run completes in 925.41 monotonic seconds:
412 passes, no failures/skips, with continuous clocks. Both existing commit/revoke
strategies pass. Native cgrp/consumer/protocol traces and the broker log are retained
under `/private/tmp/brighter-4170-kafka-native-close-diagnostic/`. Diagnostic test
source was restored before execution; no production or dependency changes occurred.
Both owned broker/schema fixtures are stopped.

This does not reproduce or resolve the archived native assignment/close stall.
Do not present this diagnostic pass as a fix or discard the original dump. Another
unguided green repetition would add no cause evidence; retain the finding for an
actual failing trace or a deterministic reproduction before changing shutdown.

## Pinned-source follow-up — 2026-11-09

The [2.15.1 managed rebalance callback](https://github.com/confluentinc/confluent-kafka-dotnet/blob/v2.15.1/src/Confluent.Kafka/Consumer.cs#L153-L299)
uses incremental assignment/unassignment for the cooperative protocol and ordinary
Assign/Unassign otherwise. Unassign calls Assign with a null partition list. Thus
the previously recorded `rd_kafka_assign` frame is consistent with the ordinary
assignment path; this is a source-based inference, not a recovered strategy label
from the failed run.

The [2.15.1 native implementation](https://github.com/confluentinc/librdkafka/blob/v2.15.1/src/rdkafka_subscription.c#L194-L254)
submits assignment to the group-operation queue and waits with RD_POLL_INFINITE.
This explains why a pending native assignment can hold Close without waiting on
Brighter's commit semaphore. It does not explain why the operation failed to finish.
A useful failing capture must include the native group-processing thread and queue
state, alongside the closing caller, strategy and group logs. Changing Brighter's
commit wait or adding an application timeout would not establish native cleanup.
No implementation or dependency change follows from this inspection.

## Evidence preservation and live capture — 2026-11-10

The original cancellation-recovery directory and raw failed stack are no longer
available locally; prior claims that they remain on disk are historical. The
[follow-up review](../kafka-live-capture/README.md) records this evidence gap,
preserves the four healthy consumer traces from this run's TRX, and validates
live native stderr plus nonterminating stack collection on the two existing
.NET 10 cases. Both pass; the captured healthy report includes 29 threads, nine
with librdkafka frames. No stalled queue state or historical root cause is
established. The full memory dump and native-symbol requirements remain explicit.
