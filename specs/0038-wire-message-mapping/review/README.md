# Wire mapping test reviews

The RabbitMQ fixture synchronization drafts are approved and validated.
The SQS constructor compatibility check is approved, implemented and validated.
The next review is [RabbitMQ publish-sequence ownership](rmq-confirmation-header/README.md).
Those drafts remain outside compiled test projects and await approval.

## Approved overlapping retry test review

## Keep each payload independent while retries overlap

- [Compiled test](../../../tests/Paramore.Brighter.RMQ.Async.Tests/MessagingGateway/When_rmq_retries_overlap_should_preserve_each_messages_mapping.cs)
- [Compiled composing writer helper](../../../tests/Paramore.Brighter.RMQ.Async.Tests/TestDoubles/CoordinatedTenantRmqWireMessageWriter.cs)

Two cases exercise sync and async public factories. Two consumers share one composing writer but use distinct queues and source messages. Sources differ in payload, ID, tenant, correlation ID, subject, persistence and handled count.

Both retry operations run concurrently on separate consumer channels. The composing writer invokes one shared default writer under a lock, renames the tenant header, then waits at a barrier outside the lock. Neither callback returns until both results exist. This makes overlapping output lifetime deterministic without relying on scheduler order. It does not claim to test simultaneous execution inside the default converter.

Native reads verify each queue receives its own body and metadata, generated unique retry ID, gateway retry controls and correct persistence. Both source messages and bags remain unchanged, both originals are settled and each queue has exactly one retry.

Named RED mutation: add a nullable `ReadOnlyMemory<byte>` instance field to `DefaultRmqWireMessageWriter`, initialize it from the first input body in `WriteMessage`, and use that cached body in subsequent outputs. Keep properties and headers freshly mapped. The helper's mapping lock makes first-body selection deterministic, and its barrier ensures both results are in flight. Each test case must fail at native body equality for one of the two queues. Do not alter the test or helper for the mutation.

Restore the default writer byte-for-byte, verify targeted GREEN on both frameworks, then run both complete RabbitMQ Async suites. Rebuild supported gateway targets if a production change is needed; otherwise preceding compatibility builds remain applicable.

The approved helper is in TestDoubles. It introduces no request type, mapper or public API; subscriptions use the existing `Command` type and public consumer factories. No logging initializer is needed. The two files in this review directory are byte-identical archived copies. Initial and restored targeted runs pass on both frameworks; the named mutation fails both cases at native body equality. Both new cases also pass in the initial full runs, which each have one separate failure. Ten focused repetitions per framework pass unchanged, and both unfiltered repeats pass with 488 passed and two existing skips. See the validation notes for both failures.

This checks overlapping retry outputs through default-writer composition. Other shared-header mutation cases, simultaneous converter execution, scheduler-backed retry, remaining reserved-header semantics and custom invalid forwarding remain separate work. The preceding delayed writer-recovery test is now in the compiled project. Nothing is committed or pushed.
