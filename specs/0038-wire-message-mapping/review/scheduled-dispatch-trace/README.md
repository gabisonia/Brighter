# Scheduled SQS dispatch trace

Status: approved and validated. The exact draft and approved test-only package reference are in the compiled project. Initial and restored cases pass on both frameworks; the named mutation fails both .NET 9 cases at wire-trace equality. Both full AWS V4 Release suites pass with 497 cases, zero failures and zero skips per framework.

[Test](When_a_scheduled_message_is_dispatched_should_map_the_current_producer_trace_context.cs)

Two cases use the public SQS producer factory and sync/async delayed sends. A 16-minute delay selects the scheduler branch. The real `InMemoryScheduler` uses `FakeTimeProvider`, then fires through `CommandProcessor`, `FireSchedulerMessageHandler`, the real outbox mediator and the configured SQS producer. A composed default writer retains tenant metadata. Native SQS observation independently verifies the final wire payload.

Before advancing time, there must be no publish span, outbox entry or native delivery, and the message must retain its scheduling traceparent. Advancing fake time triggers actual dispatch without a real 16-minute wait. The native CloudEvents traceparent must equal the completed publish span ID and differ from the scheduling span ID. Application identity, body, tenant metadata and the source bag survive; no outstanding outbox entry remains. The test does not prescribe which trace is chosen as the scheduled dispatch's parent.

The draft reuses the existing tenant writer and framework scheduler handler. No new request type, mapper or test double is introduced. Approval includes adding `<PackageReference Include="Microsoft.Extensions.TimeProvider.Testing" />` to the AWS V4 test project. Version 10.10.0 is already centrally managed and used by other repository tests. No production dependency or version change is proposed.

Named mutation: in `SqsMessageProducer.SendWithDelayAsync`, move the existing `BrighterTracer.WriteProducerEvent(...)` call from before writer/send execution to immediately after `sender.SendAsync(...)`. Both cases must fail at publish-span-ID versus native `traceparent` equality. A setup, pipeline, missing-span or timeout failure does not count as RED. Restore production bytes after the mutation.

The approved sequence is complete: dependency restore/build, exact draft copy, initial GREEN, intended mutation failures, byte-for-byte producer restoration, restored GREEN and both full Release suites. All 495 preceding test cases and outcomes are retained, with exactly two new passing cases. Logs, TRX files and original production/project snapshots are under `/private/tmp/brighter-4170-scheduled-trace/`. The task emulator is stopped. No lasting production change, commit or push was made.

This covers a scheduler that returns through Brighter's dispatch pipeline. AWS native scheduler targets with `UseMessageTopicAsTarget` remain a separate unresolved mapping contract; this test cannot establish their support.
