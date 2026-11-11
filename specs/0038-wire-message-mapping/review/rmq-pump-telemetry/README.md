# RabbitMQ non-CloudEvents telemetry through application handlers

Status: approved and validated. All four cases pass on .NET 9 and .NET 10;
seven mutations produce 24 intended assertion failures. Both full RabbitMQ suites
pass 540 cases with two existing skips, and both Core suites pass 1,716 with seven
existing skips. No production correction was needed.

The original review drafts are retained. The compiled test qualifies two pump
constructor type names to resolve sibling-namespace collisions. Two mapper helpers
annotate the fixture-supplied publication topic as non-null; these annotations do
not change emitted behavior or assertions. The other six helpers are unchanged.
Initial compiler failure and warning evidence is retained, followed by successful
builds and final focused checks. See the [validation record](../../validation.md#rabbitmq-non-cloudevents-telemetry-through-application-pumps).

## Contract and public path

Four cases cover sync/Reactor and async/Proactor, with traceparent, tracestate and
baggage in custom AMQP headers or JSON body. Each runs on .NET 9 and .NET 10.

The [integration test](When_dispatching_rmq_non_cloud_events_telemetry_should_reach_the_application_handler.cs)
uses the public RabbitMQ producer factory, real outbox mediator, native broker
connection, public channel factory and actual service-activator pumps. A unique
exchange and durable classic queue isolate each case. The standard localhost
broker URI follows the existing suite; run in the task-owned broker container's
network namespace so unrelated host port 5672 is untouched.

The message is deposited under one trace and baggage and dispatched under another.
The test waits for a native publish confirmation and outbox settlement, with a
bounded cancellation token. It independently reads native bytes/properties using
BasicGet without acknowledgement. It verifies original identity, persistence,
confirmation ownership, custom content, absence of CloudEvents headers and the
actual producer span's traceparent, `vendor=dispatch` and `stage=dispatch`. Gateway
publish-sequence metadata is excluded only from the custom application-header
count and remains separately asserted.

The native receipt is nacked for requeue, then a Reactor or Proactor uses the
subscription-selected reader and application mapper to dispatch to a registered
handler. The pump starts with unrelated ambient telemetry. Its process span must
parent to the producer, and the handler must see the same trace, tracestate,
baggage, identity and payload. Handler completion signals a local quit; the test
joins the pump and verifies one invocation. It closes the consumer channel before
checking the queue is empty, so an unacknowledged delivery cannot hide behind an
open channel. Cleanup also stops/joins the pump before deleting broker resources.
No internal parser, manual tracer extraction or generated conformance pump is used.

## Collaborators

- [Wire writer](NonCloudEventsRmqWireMessageWriter.cs) creates fresh native content and application headers.
- [Wire reader](NonCloudEventsRmqWireMessageReader.cs) restores telemetry and payload from those locations; the gateway retains delivery ownership.
- [Sync command](RmqPumpTelemetrySyncCommand.cs), [mapper](RmqPumpTelemetrySyncMapper.cs) and [handler](RmqPumpTelemetrySyncHandler.cs).
- [Async command](RmqPumpTelemetryAsyncCommand.cs), [mapper](RmqPumpTelemetryAsyncMapper.cs) and [handler](RmqPumpTelemetryAsyncHandler.cs).

Each new handler has a distinct request type. Registration is explicit and results
are observed through per-test callbacks, without shared static counters. Existing
quick handler factories provide the application handlers. This test project uses
standard logging and has no generic-dispatch `Initializer.cs` stub to extend.
There is no new dependency, public API or generated-test collection change.

## Named mutations

Initial GREEN is possible. Keep all test/helper bytes unchanged and apply these
temporary production changes independently on .NET 9/RabbitMQ 4.2:

| Mutation | Expected failures | Intended assertion |
| --- | ---: | --- |
| Ignore the publication-selected RabbitMQ writer | 4 | Native `legacy-id` header must exist |
| Move producer context propagation after native publish | 4 | Native traceparent equals actual producer span |
| Clear the configured reader result's traceparent before returning from consumer | 4 | Process trace equals producer trace |
| Clear that result's tracestate | 4 | Process tracestate equals `vendor=dispatch` |
| Omit consumer context propagation in Reactor | 2 sync cases | Handler baggage equals `dispatch` |
| Omit consumer context propagation in Proactor | 2 async cases | Handler baggage equals `dispatch` |
| Omit the BasicAck in consumer `AcknowledgeAsync` | 4 | Queue is empty after the consumer closes |

Seven mutations should yield 24 intended assertion failures. Compilation/setup
errors, parsing exceptions and timeouts do not count as behavioral RED. Restore
exact production bytes after every mutation and rebuild restored focused GREEN
on both frameworks/broker versions.

## Work after approval

Copy the integration test under `MessagingGateway` and the other eight files under
`TestDoubles` in the RabbitMQ Async test project. Run the four cases on both
frameworks; fix only behavior gaps reproduced by this contract. Preserve failures
and return semantic test changes for review before executing them.

Validate all seven mutations, restored focused cases, all supported target builds
for RabbitMQ Async and ServiceActivator, both complete RabbitMQ Async suites and
both complete Core suites. Use the existing isolated task brokers/runners: .NET 9
with RabbitMQ 4.2 and .NET 10 with RabbitMQ 4.3, including the suite's TLS/native-delay
configuration. Retain every preceding test name/outcome: 540 RabbitMQ passes and
two existing skips per framework are expected (536 plus four), and Core retains
1,716 passes/seven skips. Do not change the RMQ Sync defaults to suit 4.3.

These are local broker results, not remote CI or a production deployment. This
batch establishes the ordinary RabbitMQ publish/consume telemetry path. Scheduler
callbacks, scheduler-backed retry mapping/failure recovery, other transports and
final configuration review remain separate completion work. No commit, push or
review-gear change is included.
