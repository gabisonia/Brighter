# ASB scheduled custom writer — validated

Status: approved and validated. All four native cases pass on .NET 9 and .NET 10,
initially and after mutation restoration. Six mutations produce 24 intended assertion
failures. Both full available suites pass 594 cases, zero failures/skips, retaining
all 590 prior case/outcome pairs. No production correction was needed.

## Approved contract

Queue/topic × sync/async cases call the public producer's SendWithDelay or
SendWithDelayAsync through the registry. Each publication selects the existing
approved AsbExternalWireWriter and a five-minute TTL. The requested delay is five
seconds. The writer and existing tests remain unchanged.

A native SDK receiver must observe the custom ID, binary body, subject, correlation,
content type and trace/baggage properties, without default CloudEvents metadata.
The publication TTL overrides the writer's one-minute TTL. The writer runs exactly
once and the original source identity, route, body and metadata remain unchanged.

The native ScheduledEnqueueTime must fall between the local start time plus delay
and the local submission-completion time plus delay, allowing one second at either
end for timestamp precision. Capture receive completion immediately after the native
receive and require it to be no earlier than start plus delay minus one second.
The native timestamp check catches immediate-send bypass even if local scheduling
pauses obscure the elapsed-time check. This is a bounded timing assertion, not a
delivery-latency SLA or a claim of precision finer than one second.

Use isolated native destinations and a 30-second receive wait. Async operations use
a 60-second cancellation bound; sync operations also rely on the external runner
timeout. Complete the observed native message in a finally block. No mocked broker
I/O, new helper, handler or logging registration is introduced.

This directly exercises native producer scheduling. It does not validate the separate
Azure scheduler package, cancellation, scheduled writer failure recovery, default
writer scheduling, outbox scheduling or cancellation of a scheduled message.

## Named production mutations

1. Ignore the configured writer: custom native identity or writer count fails.
2. Send immediately instead of calling native scheduling: scheduled timestamp fails.
3. Call native scheduling with the current time instead of adding the delay:
   scheduled timestamp fails.
4. Omit the publication TTL override: native TTL remains the writer's one minute
   and fails the five-minute assertion.
5. Invoke the selected writer twice before scheduling: invocation count fails.
6. Replace the mapped native body with the original source bytes before scheduling:
   native binary-body assertion fails.

## Validation evidence

Each mutation produces four failures at its intended assertion. Both immediate-send
bypass and scheduling without adding the delay fail at ScheduledEnqueueTime bounds;
no timing assertion was weakened. Native scheduling is demonstrated in this emulator
for these cases, with the stated one-second timing tolerance and bounded receive.

All mutations are restored. All 1,171 non-generated production C# files match their
pre-batch hashes. The approved test matches its compiled copy byte-for-byte; the
reused helper is unchanged. All library targets build. Both full available suites
pass 594 cases on their first runs, zero failures/skips, with exact retention of all
590 previous outcomes. Session-lock instability remains unresolved despite passing here.

Evidence: `/private/tmp/brighter-4170-asb-scheduled-writer/` contains initial/restored
matrices, mutations.json, full regression TRXs, exact baseline comparisons and source
verification. Dedicated fixture containers are stopped after validation. Nothing
committed or pushed.

Cancellation, session-lock instability, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader release gates remain open. No commits or pushes requested.
