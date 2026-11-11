# Quartz scheduled dispatch through RabbitMQ wire mapping

Status: approved and validated. All eight cases pass on both frameworks; four
mutations produce 24 intended assertion failures. Both full RabbitMQ suites pass
548 cases with two existing skips, and both complete Quartz suites retain 53
passes. Five approved files are copied unchanged and the single approved test
reference is applied. No production correction was needed. See the
[validation record](../../validation.md#quartz-native-rabbitmq-wire-dispatch).

## Contract and public path

The [eight cases](When_quartz_dispatches_scheduled_work_should_use_the_configured_rmq_wire_writer.cs)
cover message/request scheduling, sync/async dispatch, and a writer that places
traceparent in native AMQP headers or the JSON body. They use an actual Quartz
scheduler, the production Quartz job and Brighter scheduler handlers, a real
command processor/outbox mediator, the public RabbitMQ producer factory, and an
independent native RabbitMQ client.

Message cases enter through `SendWithDelay`/`SendWithDelayAsync` with the Quartz
scheduler attached to the producer. The direct exchange does not enable native
delay, so the producer must use that scheduler. Request cases use the public
Quartz request scheduler with `RequestSchedulerType.Post`. A distinct
[command](QuartzWireCommand.cs) and [mapper](QuartzWireCommandMapper.cs) produce
plain application text. No new application handler is registered.

The [job factory](QuartzWireJobFactory.cs) supplies the real command processor;
the [listener](QuartzWireCompletionListener.cs) observes completion reported by
Quartz. Neither invokes the job manually. The existing
[non-CloudEvents writer](../../../../tests/Paramore.Brighter.RMQ.Async.Tests/TestDoubles/NonCloudEventsRmqWireMessageWriter.cs)
is reused unchanged.

While Quartz is stopped, the native queue, outstanding outbox and producer-span
collection must remain empty. Starting Quartz must produce one successful job
completion, one confirmed native publication, original application ID/payload,
the exact replacement content type/format, gateway persistence and SDK sequence
metadata. Independent native observation checks that traceparent equals the
actual producer span, rather than the supplied schedule-time message parent.
The application objects' complete serialized representations must remain unchanged
before and after firing, the outbox must settle, and a second native read must
find no duplicate. The request itself carries no schedule-time trace context;
its assertion establishes that the writer sees the dispatch producer span.

This batch specifically validates writer selection, payload/identity round trip,
dispatch-time traceparent and native controls. The earlier telemetry batches
retain the evidence for nonempty tracestate/baggage and consumer handlers.
This batch does not assert Quartz database-store persistence, callback failure
recovery or scheduler-backed consumer retries.

Every case owns a uniquely named exchange, durable classic queue and scheduler.
The scheduler shuts down before collaborators are disposed or native resources
deleted. Waits use a 45-second cancellation budget; scheduling uses the validated
one-second delay. No shared static processor, mock or generated pump is used.

## Test dependency

The [reference patch](project-reference.patch) adds the existing Quartz scheduler
project to the RabbitMQ Async test project's references. It does not change a
production package dependency or package version. After approval, copy the test
under `MessagingGateway` and its four helpers under `TestDoubles`, apply this
single reference addition, and restore test assets.

## Named mutations

If initially GREEN, establish behavioral RED using these independent temporary
production changes, with reviewed test bytes unchanged:

| Mutation | Expected failures | Intended assertion |
| --- | ---: | --- |
| Ignore the publication-selected writer when constructing the RabbitMQ publisher | 8 | Native legacy-id header must exist |
| Move RabbitMQ producer propagation after native publication | 8 | Native traceparent equals actual producer span |
| After deserializing FireSchedulerMessage, replace its contained message body with `corrupted`, preserving its header | 4 message cases | Original native application payload |
| After deserializing FireSchedulerRequest, replace `scheduled native payload` with `corrupted` in RequestData | 4 request cases | Original native application payload |

The four mutations should produce 24 intended assertion failures on .NET 9.
Compile failures, scheduling exceptions, job errors and timeouts are not valid
behavioral RED for these assertions. Restore exact GREEN production bytes after
each mutation. Preserve unexpected evidence and return semantic test changes for
review before executing them.

## Work after approval

Run the eight cases on .NET 9/RabbitMQ 4.2 and .NET 10/RabbitMQ 4.3, using the
existing isolated task brokers/runners. Validate the mutations and restored
focused cases. Build RabbitMQ Async, Quartz and ServiceActivator on all supported
targets. Run both complete RabbitMQ Async suites, retaining all 540 passes/two
existing skips and adding eight passes (548 passed/two skipped expected). Run both
complete Quartz suites, retaining all 53 passes. Include full Core regressions if
shared Core production code changes. Preserve HEAD and the saved stash.

Other scheduler integrations, remaining gateway mappings, callback failure
recovery and the final configuration review remain completion gates for #4170.
The separate zero-delay timestamp rejection remains recorded in the scheduler
audit. No commit, push, publication or review-gear change is included.
