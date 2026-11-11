# Scheduled SNS dispatch trace

Status: approved and validated. The compiled test matches the approved draft. Initial and restored cases pass on both frameworks; the named mutation fails both .NET 9 cases at native traceparent equality. Both complete AWS V4 Release suites pass with 499 tests, zero failures and zero skips per framework.

[Test](When_a_scheduled_sns_message_is_dispatched_should_map_the_current_producer_trace_context.cs)

Two cases cover sync/async delayed sends through the public SNS producer factory. Unlike SQS, SNS uses its scheduler for every positive delay. The real `InMemoryScheduler` runs on `FakeTimeProvider` and returns through the command processor, framework scheduler handler and outbox mediator into the configured SNS producer. Native SNS-to-SQS envelopes independently expose the published content.

The message retains its original traceparent before fake time advances, with no publish span, outbox entry or native delivery. After scheduled dispatch, the serialized CloudEvents traceparent must equal the completed publish span ID and differ from the original scheduling span. The actual topic ARN, mapped topic, application ID, body, subject and composed tenant header must survive, with unchanged source content/bag and no outstanding outbox entry. Trace-parent selection between scheduling and dispatch is not prescribed.

This reuses the existing tenant writer, framework scheduler handler and the fake-time test dependency already approved for SQS. It introduces no new mapper, request type, test double or package reference.

Named mutation: in `SnsMessageProducer.SendWithDelayAsync`, move `BrighterTracer.WriteProducerEvent(Span, "aws_sns", message, _options)` from before mapping/publication to immediately after `publisher.PublishAsync(message)`. Both cases must fail at publish-span-ID versus native CloudEvents traceparent equality. Setup errors, missing spans/deliveries and timeouts do not count as RED. Restore the producer byte-for-byte afterward.

The approved sequence is complete: exact draft copy, initial GREEN, intended mutation failures, byte-for-byte producer restoration, restored GREEN and both complete Release suites. All 497 preceding cases and outcomes are retained, with exactly two new passing SNS cases. Artifacts are under `/private/tmp/brighter-4170-sns-scheduled-trace/`. The task emulator is stopped. No lasting production change, commit or push was made.

This covers the separate SNS dispatch implementation. AWS native scheduler targets with `UseMessageTopicAsTarget` bypass this execution path and remain outside the evidence established here.
