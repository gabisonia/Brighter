# Non-CloudEvents telemetry through real AWS message pumps

Status: approved and validated. All eight mutations, restored focused checks,
four full AWS suites and both full Core suites pass their required checks.
All fourteen C# files were copied unchanged into their test projects. This batch
extends the existing approved native-wire telemetry cases into real application
dispatch. It proposes no new public API or dependency.

## Contract and cases

There are four cases per SDK, eight total: Reactor/Proactor × telemetry in custom
native attributes/JSON body. Each runs on .NET 9 and .NET 10.

- [SDK V3 integration test](V3/When_dispatching_non_cloud_events_telemetry_should_reach_the_application_handler.cs)
- [SDK V4 integration test](V4/When_dispatching_non_cloud_events_telemetry_should_reach_the_application_handler.cs)

The first half retains independent native inspection of the outgoing SQS message.
A real outbox holds a message created under deposit telemetry. Clearing it under
a different activity and baggage must serialize the actual producer context in
the custom wire locations, with no CloudEvents envelope. The native assertions
check traceparent, tracestate, baggage, identity and payload independently of the
custom reader.

After releasing that native receipt, a real `ChannelFactory` creates the input
channel with its configured reader. A real Reactor or Proactor runs on a dedicated
thread, unwraps the message through the application mapper, and dispatches through
a real `CommandProcessor` to a registered application handler. The test does not
call tracer propagation or span creation to emulate consumer behavior.

The pump starts with unrelated ambient tracing and baggage. Its completed process
span must parent to the actual producer span. The handler must observe the same
trace, `vendor=dispatch` tracestate, `stage=dispatch` baggage and original identity
and payload. Completion is signaled by the handler. A local quit message stops
the pump, and the test awaits its exit before verifying one invocation and native
source settlement after visibility expires. Cleanup also stops and joins the pump
before deleting the queue.

## Test collaborators

Each SDK reuses its approved `NonCloudEventsSqsWireMessageWriter` and reader.
There are six new helpers per SDK: distinct `PumpTelemetrySyncCommand` and
`PumpTelemetryAsyncCommand` types, each with its own application mapper and
handler. They use explicit mapper/subscriber registration, not assembly scanning.
Handlers observe the actual request context and ambient baggage using a per-test
callback; there is no shared static result state. The existing handler factories
supply these application handlers to the real command processor.

Each helper has its own file. These AWS test projects have no logging
`Initializer.cs` or type-dispatching logger stub: they use
`ApplicationLogging.LoggerFactory`, whose standard `CreateLogger<T>` supports the
new closed generic types. No stub registrations or global logging replacement are
needed. The tests do not use the generated conformance pump or its static counters.

## Named mutations

Initial GREEN is possible because the preceding gateway work already supplies
these fields. The following independently applied mutations must establish RED:

| Production mutation | Cases | Intended assertion failure |
| --- | ---: | --- |
| In each SDK, move producer context propagation after SQS serialization/send | Four per SDK | Native traceparent differs from the actual producer span |
| In each SDK, clear the configured reader result's `TraceParent` before returning it from the consumer | Four per SDK | Pump process trace differs from the producer trace |
| In each SDK, clear the configured reader result's `TraceState` before returning it from the consumer | Four per SDK | Pump process tracestate differs from `vendor=dispatch` |
| Omit `PropagateConsumerContext(message)` in Reactor | Two per SDK | Handler baggage differs from `dispatch` |
| Omit `PropagateConsumerContext(message)` in Proactor | Two per SDK | Handler baggage differs from `dispatch` |

There are eight source mutations and 32 expected assertion failures when run on
.NET 9 across both SDKs. Tests and custom test converters remain unchanged.
Compilation failures, handler timeouts and unrelated exceptions do not count as
behavioral RED. Restore production bytes exactly after every mutation.

## Work after approval

Copy the exact drafts, run all cases on both frameworks, and correct only gaps
reproduced by the reviewed contract. Verify the eight mutations and restored
focused GREEN. Build both gateways and ServiceActivator on all supported targets.
Run all four complete AWS Release suites and both Core suites, which contain the
pump regression tests. Compare the prior 603 AWS cases with four additions per
SDK/framework: 607 passing cases expected per AWS suite. Preserve the existing
Core skips and report actual counts. If a test needs a semantic correction, keep
its failure evidence and return the proposed correction for review.

Document native/emulator limits and the real pump/handler evidence separately.
This batch covers SQS in both SDKs. SNS/RabbitMQ telemetry, native AWS scheduling,
scheduler-backed retries, other transports and the public configuration review
remain open for full #4170 completion. No commit, push or review-gear change is
included.

## Results

All initial and restored focused runs pass four cases per SDK/framework. Eight
mutations produce 32 intended assertion failures. All production bytes are
restored; no production change is needed. The temporary Reactor mutation required
a syntax correction to retain its unbraced if's body; the compile failure is not
counted as behavioral RED. Approved tests and helpers remain byte-identical.

All four complete AWS Release suites pass 607 cases with no skips or failures,
retaining every preceding 603 case outcome. Both complete Core suites pass 1,716
cases with seven existing skips and no failures. Both gateways and ServiceActivator
build on all supported targets. [Validation details](../../validation.md#sqs-non-cloudevents-telemetry-through-real-application-pumps)
record artifacts, exact mutation assertions, run durations and emulator limits.

The next [scheduler destination-selection draft](../aws-scheduler-destinations/README.md)
contains sixteen cases and local fixture changes for review. It remains uncompiled
and unrun. Full #4170 implementation remains open; no commits or pushes were made.
