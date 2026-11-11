# Non-CloudEvents SQS telemetry round trip

Status: approved and executed. Initial and restored focused runs pass all four
cases on .NET 9 and .NET 10. Each of the three named mutations fails all four
.NET 9 cases at its intended assertion. Production is restored byte-for-byte.
Both complete AWS V4 Release suites pass with 503 cases and zero failures/skips
per framework. All 499 preceding cases and outcomes are retained. This approved
telemetry increment is complete; full #4170 implementation remains open.

The compiled files match the reviewed drafts except for two nullable dictionary
value annotations corrected after the first build (`string` to `string?` for
OpenTelemetry baggage). The review copies include the same annotation correction;
no setup values, behavior or assertions were changed. The original approved test
is preserved with the run artifacts.

## Concrete behavior

[Four-case test](When_dispatching_non_cloud_events_telemetry_should_restore_the_consumer_context.cs)
uses sync/async factory and dispatch paths with two legacy formats: telemetry in
custom native attributes, or telemetry in a JSON body alongside the payload.

[Writer](NonCloudEventsSqsWireMessageWriter.cs) and
[reader](NonCloudEventsSqsWireMessageReader.cs) implement the public contracts.
They do not compose CloudEvents defaults. No new request/handler/mapper type,
closed logging generic, project reference or dependency is required.

The real producer factory and outbox mediator send through SQS. The native SDK
independently observes application identity, the absence of CloudEvents metadata,
and all three telemetry fields. Deposit telemetry is deliberately different from
dispatch telemetry. The native traceparent must equal the actual completed publish
span, not the deposit span; tracestate and baggage must contain dispatch values.

The test releases the observed native delivery, then receives the same message
through the real consumer factory and configured reader. It checks payload,
application identity, receipt metadata and all restored telemetry fields. It then
uses the same public `BrighterTracer` consumer-context/process-span operations
used by the pumps, under an unrelated ambient trace and baggage. The process span
must use the remote publish trace/parent and restored tracestate; ambient baggage
must contain the received value. Acknowledgement is checked by native observation
after the queue visibility interval.

This tests gateway-to-tracer interoperability. It does not run a service-activator
pump or application handler; that wiring is separate work on the full completion
checklist. Native observation itself adds one delivery attempt, so the test does
not prescribe a first-delivery count. It does not change the source body. It uses
the repository's native SDK/emulator integration pattern, not mocked gateway I/O.

## Named mutations for behavioral RED

The existing APIs may already satisfy this scenario. On initial GREEN, use each
of these reviewed production mutations separately, without modifying test code:

1. In `SqsMessageProducer.SendWithDelayAsync`, move `WriteProducerEvent` from
   before the sender call to immediately after `sender.SendAsync`. The intended
   failure is actual publish-span ID versus native custom traceparent equality.
2. In `SqsMessageConsumer.CreateMessage`, after successful custom mapping and
   before returning the message, set `message.Header.TraceState = null`.
   The intended failure is native tracestate versus received header equality.
3. At the same consumer boundary, replace `message.Header.Baggage` with a new
   empty `Paramore.Brighter.Observability.Baggage`. The intended failure is native
   baggage versus received header equality.

Each mutation must fail all four cases at its specified assertion. Compilation
errors, setup errors, absent deliveries and timeouts do not count as RED. Restore
production from byte snapshots after each mutation; never use git reset/restore
on the existing WIP. If initial behavior fails, diagnose and correct it only
after approval, retaining the intended acceptance assertions.

## Execution record

The files are copied into the AWS V4 project's MessagingGateway and TestDoubles
directories. Focused initial/restored runs pass on both frameworks. The three
mutations fail at native traceparent (line 177), received tracestate (line 205)
and received baggage (line 206), respectively. There are no lasting production
changes or additional package references.

The full suites ran against Floci 1.5.19 with isolated account namespaces and
sleep inhibition: 7 min 46 sec on .NET 9 and 7 min 48 sec on .NET 10. The emulator
is stopped. Results and preservation checks are recorded in validation.md.
Artifacts are under `/private/tmp/brighter-4170-non-cloud-telemetry/`.

No commit, push, publication, change of review gear or global completion claim is
authorized by this test review.
