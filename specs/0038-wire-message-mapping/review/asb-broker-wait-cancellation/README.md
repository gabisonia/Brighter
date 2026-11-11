# ASB cancellation during an empty receive — validated

Status: approved, implemented and validated. Both cases pass on .NET 9 and .NET 10.
Six mutations produce 12 intended assertion failures. Both full available suites
pass 612 cases, zero failures/skips, retaining all 610 prior outcomes. Abandonment-
failure validation remains open because its native failure precondition was not met.

## Approved contract

Two queue/topic cases use isolated empty native entities and the existing
AsbExternalWireReader unchanged. A short successful empty receive warms the consumer
and receiver. Then an initially active caller token is passed to a ten-second receive.
After 250 milliseconds the test records whether the call is still pending, starts
a stopwatch, and cancels the token. It awaits the original receive task directly,
recording its exception and elapsed cancellation time.

The call must have been pending at cancellation, must propagate an
OperationCanceledException directly, and must finish less than five seconds after
cancellation was requested. It must not invoke the reader on an empty entity.
Returning successful emptiness, wrapping cancellation in ChannelFailureException,
or waiting out the original ten-second receive is not the proposed behavior.

Only after the cancelled receive has finished does the test publish a native
message with a distinct ID and binary body. Bounded peek confirms visibility. The
same consumer with an active token must receive it, invoking the configured reader
once, retaining custom identity/body/headers and correct native lock/sequence
metadata. Native delivery count must be one. Acknowledgement must remove it, verified
with bounded native peek from the original sequence.

## Evidence limits and implementation boundaries

The warm-up and pending-task observation exercise cancellation after entry on an
established consumer. They do not directly instrument the instant an AMQP request
reaches the broker. The five-second threshold gives scheduling allowance while
remaining well below the requested ten-second wait. A prematurely completed receive,
foreign fixture message, setup error or outer-timeout cancellation must be reported
separately; do not silently retry or count it as a cancellation defect.

The test awaits the receive rather than abandoning it through Task.WhenAny or an
assertion-timeout wrapper. Do not implement cancellation by merely detaching a
still-running broker receive: production must cancel or otherwise safely terminate
that operation. Recovery tests observable delivery behavior, not the absence of
every possible internal background task. A non-returning native operation remains
subject to the external test runner's hang diagnostics.

No receiver substitutes, private reflection, production test hooks or new helper
classes are proposed. ASBTestCommand only supplies subscription configuration;
no handlers or closed-generic logging registrations are added. Preserve existing
public receiver-wrapper implementers and the synchronous receive path. A new
required interface member would be a compatibility change requiring separate
review, not an implicit part of this test approval.

## Named production mutations

1. Ignore caller cancellation in the native receive, checking it only after the
   ten-second wait: the elapsed-time assertion must fail (or the direct cancellation
   assertion if the ignored token returns successful emptiness).
2. Wrap native caller cancellation in ChannelFailureException: the direct
   OperationCanceledException assertion fails.
3. Return successful empty results when native caller cancellation is observed:
   the expected exception assertion fails.
4. Clear the configured reader during cancellation handling: recovery reader count
   or mapped identity fails.
5. Replace recovered custom body with native bytes: mapped-body assertion fails.
6. Omit native completion during acknowledgement: final peek remains nonempty.

Only intended assertion failures count as mutation evidence; retain setup, build
and hang diagnostics separately. Restore every mutation. If initially green, use
these mutations to establish RED without altering the approved test. Build all
library targets, run focused cases on both frameworks and both full available
suites, comparing all 610 baseline outcomes. Confirm approved/adopted source equality
and unchanged reused reader. Preserve unrelated workspace changes. No commits or pushes.

## Remaining gates

This batch covers
an empty non-session receive after warm-up; arrival/cancellation races, provisioning,
session acquisition, settlement cancellation and abandonment failures remain open.
Session-lock instability, eight DLQ-blocked cases, 52 unavailable hosted classes
and broader transport/release gates remain unresolved. No readiness claim is made.

## Implementation and validation evidence

All four initial executions return successful emptiness after roughly ten seconds,
failing the direct cancellation assertion at line 95. The correction adds a
cancellation-aware overload to the internal native ServiceBusReceiverWrapper and
passes the caller token to the SDK ReceiveMessagesAsync operation. Its existing
two-argument method delegates with CancellationToken.None. AzureServiceBusConsumer
selects the native overload and catches caller-triggered OperationCanceledException
before generic channel-failure handling, resetting the receiver and rethrowing it.
The receive XML exception documentation now includes waiting on the native receiver.

The public IServiceBusReceiverWrapper interface and its existing signatures are
unchanged. Other wrapper implementations continue using their existing receive
method; this does not add in-flight cancellation support to arbitrary external
wrappers. No detached receive tasks or public interface/dependency additions are
introduced. Only AzureServiceBusConsumer and the internal ServiceBusReceiverWrapper
change among 1,171 non-generated production C# files.

All four corrected and all four restored executions pass with the unchanged approved
test and unchanged external reader. Six mutations fail both cases on .NET 9 at their
intended assertions: ignoring native cancellation but checking afterward fails the
elapsed bound (line 96), wrapped/suppressed cancellation fails line 95, lost reader
fails line 115, replaced custom body fails line 120, omitted completion fails line
139. All 12 failures are intended xUnit assertions; all mutations are restored.

All library targets build. Both full available suites pass 612 cases on their first
runs, zero failures/skips, retaining exactly all 610 prior case/outcome pairs.
Evidence: `/private/tmp/brighter-4170-asb-wait-cancellation/` contains original RED,
corrected/restored TRXs, mutations.json, regression TRXs, exact baseline comparisons,
source verification and production snapshots. Nothing committed or pushed.

The isolated emulator containers and network were removed after validation.
