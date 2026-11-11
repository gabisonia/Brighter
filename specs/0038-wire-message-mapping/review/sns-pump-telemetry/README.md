# SNS non-CloudEvents telemetry through application handlers

Status: approved and validated. All six files are copied unchanged into their
test projects. Initial/restored focused runs, all ten mutations, all-target builds
and all six full regressions are complete. No production correction was needed.

## Contract

An SNS business publication must be able to relocate the actual dispatch-time
traceparent, tracestate and baggage to custom native attributes or JSON body.
After raw or enveloped SNS-to-SQS delivery, a subscription-selected reader must restore those
fields before a real Reactor/Proactor dispatches to the application handler.
The handler must retain identity, payload, producer trace lineage, tracestate and
baggage; the pump must acknowledge the source delivery after success.

Eight cases per SDK cover sync/Reactor and async/Proactor, telemetry in
attributes and in the body, and raw/enveloped delivery. Sixteen distinct cases
run on .NET 9 and .NET 10.

- [V3 integration test](V3/When_dispatching_sns_non_cloud_events_telemetry_should_reach_the_application_handler.cs)
- [V4 integration test](V4/When_dispatching_sns_non_cloud_events_telemetry_should_reach_the_application_handler.cs)
- [V3 SNS writer](V3/NonCloudEventsSnsWireMessageWriter.cs)
- [V4 SNS writer](V4/NonCloudEventsSnsWireMessageWriter.cs)
- [V3 SNS delivery reader](V3/NonCloudEventsSnsDeliveryWireMessageReader.cs)
- [V4 SNS delivery reader](V4/NonCloudEventsSnsDeliveryWireMessageReader.cs)

The tests use public producer factories, a real outbox mediator, native SNS and
SQS clients, public channel factories and actual service-activator pumps. The SNS
topic, SQS queue, subscription and source-restricted queue policy are created
per case. Native content is inspected independently of the custom reader before
releasing that receipt for the pump. No tracer extraction is manually invoked.

A message deposited under one trace is dispatched under a different trace and
baggage. Native observation checks the actual producer span, payload and custom
attributes, with no CloudEvents attribute. The pump starts under unrelated ambient
context; its process span must parent to the producer. The registered handler
must see the same trace, `vendor=dispatch` and `stage=dispatch`. After the handler
signals completion, the test queues a quit message, joins the pump, checks one
invocation and verifies an empty queue after visibility expires. Cleanup joins
the pump before reaping the topic and queue.

Each SDK composes its existing SQS reader and `PumpTelemetrySyncCommand`/
`PumpTelemetryAsyncCommand`, mappers and handlers, with per-test observation
callbacks and explicit registration. There are no new handler/request types or
shared static counters. The new SNS content writer creates fresh native attributes on each call. A new
SNS delivery reader delegates raw content to the existing SQS reader; for
enveloped content it creates a fresh native content representation from the
inner message/attributes before delegating. It leaves the original delivery
untouched. The actual consumer retains the original receipt for settlement. No generated conformance
pump is used; no logging-stub registration or dependency change is needed.

## Named mutations

Initial GREEN is possible. Apply each temporary mutation to production only,
keeping all reviewed test/helper bytes unchanged:

| Mutation | Cases failing | Intended assertion |
| --- | ---: | --- |
| Ignore the publication-selected SNS writer, once per SDK | 8 per SDK | Native `legacy-id` attribute must exist |
| Move SNS producer trace propagation after serialization/send, once per SDK | 8 per SDK | Native traceparent equals the actual producer span |
| Clear the configured reader result's traceparent in the SQS consumer, once per SDK | 8 per SDK | Process trace equals producer trace |
| Clear the configured reader result's tracestate in the SQS consumer, once per SDK | 8 per SDK | Process tracestate equals `vendor=dispatch` |
| Omit consumer context propagation in Reactor | 4 per SDK | Handler baggage equals `dispatch` |
| Omit consumer context propagation in Proactor | 4 per SDK | Handler baggage equals `dispatch` |

Ten mutations should produce eighty intended assertion failures on .NET 9 across
both SDKs. Compile errors, parsing errors and handler timeouts are not behavioral
RED. Restore exact production bytes after each mutation, then rebuild and run
focused GREEN on both frameworks.

## Work after approval

Copy the six exact files to the respective AWS test projects: integration tests
under `MessagingGateway`, writers and readers under `TestDoubles`. Run initial focused checks;
fix only behavior failures established by this contract. Preserve all failed
runs. Return any semantic test correction for review before executing it.

Validate the named mutations, restored focused cases, all-target builds of both
AWS gateways and ServiceActivator, all four complete AWS suites and both Core
suites. Retain every previous case outcome: 619 AWS passes per SDK/framework are
expected (611 existing plus eight), and Core's existing 1,716 passes/seven skips.
Compare exact names/outcomes, not counts alone. Use local Floci 2.3.0; no real AWS
IAM enforcement or remote CI pass is claimed.

This batch covers SNS publishing with raw and enveloped SQS subscriber delivery.
RabbitMQ telemetry, AWS callback failure recovery,
scheduler-backed retries, other scheduler/transport integrations and final public
configuration review remain separate completion work. No commit, push or review
gear change is authorized by this batch.


## Results

All 32 initial and restored SDK/framework executions pass. Ten named mutations
produce eighty intended assertion failures; all production source bytes are
restored exactly, and test/helper bytes remain identical to the reviewed files.
All four full AWS suites pass 619 cases with zero failures/skips, retaining the
previous 611 names/outcomes plus eight additions. Both Core suites retain exactly
1,716 passes and seven existing skips. Gateways and ServiceActivator build on all
supported targets. [Validation details](../../validation.md#sns-non-cloudevents-telemetry-through-raw-and-enveloped-application-pumps)
record exact assertions, warnings, durations, artifacts and emulator limits.

The next [RabbitMQ pump telemetry batch](../rmq-pump-telemetry/README.md) is prepared
outside compiled projects for review. Full #4170 implementation remains open.
No commits or pushes were made.
