# ASB cancellation during session acquisition — validated

Status: approved, adopted and validated. Both full available suites pass 620 cases
on .NET 9 and .NET 10, retaining all 618 prior outcomes. No commits or pushes.

## Approved contract

Two native queue/topic cases use empty session-enabled entities. ReceiveAsync starts
with an active caller token and RequireSession=true. After 250 milliseconds the test
records whether acquisition is still pending, starts a stopwatch and cancels the
token. It awaits the original receive task directly. The receive must have been
pending, must directly propagate OperationCanceledException, and must finish within
five seconds of cancellation. The reader must not run on the empty entity.

Only after the cancelled call finishes does the test publish a native message with
a new SessionId, unique message ID and binary body. Bounded native peek establishes
visibility. The same consumer with an active token must acquire the session and
receive that message. Check native SessionId, body, delivery count one and sequence,
custom mapped identity/body/headers, and current gateway lock/sequence metadata.
The reader must run exactly once. Acknowledgement must remove the delivery, verified
by bounded native peek from the original sequence.

## Native fixture and bounded failure

AsbBoundedSessionClientProvider is a real SDK client configuration, not a receiver
substitute: TryTimeout is ten seconds and MaxRetries is zero. This bounds failed
session acquisition without replacing or detaching native I/O. The helper uses the
existing public ServiceBusClientProvider extension point. Its connection string
comes from the normal test environment and must never be logged. This fixture
requires connection-string configuration; it does not silently substitute another
authentication scheme. AsbExternalWireReader is reused unchanged.

Merge the adjacent session-entities.json fragment into the isolated emulator's
existing namespace configuration, retaining all baseline entities. It supplies
wire-session-queue and wire-session-topic/session-observer with RequiresSession=true
and one-minute locks. Tests also accept BrighterTestsASBSessionQueue,
BrighterTestsASBSessionTopic and BrighterTestsASBSessionSubscription overrides.
Focused cases use fresh isolated fixtures; include the session entities for full
available regression runs. Do not enable sessions on the existing non-session entities.

No native message exists when cancellation is requested. There is no warm-up receive,
because that would itself require session acquisition. Pending public-call observation
does not identify the exact instant the broker receives the AMQP request; it tests
cancellation during the session-acquisition path, potentially including connection
setup. The five-second threshold allows scheduling overhead while remaining below
the configured ten-second native timeout. A prematurely completed receive, unsupported
session entity, setup failure, or timeout from the outer test bound is not usable
cancellation evidence. Preserve it and review any amendment rather than retrying
until green or weakening the contract.

The test awaits the original receive. Do not implement cancellation by detaching an
ongoing AcceptNextSessionAsync operation that could acquire a later session. Recovery
checks observable session acquisition and delivery, not every internal background
task. There are no production test hooks, private reflection, mocked receivers,
new handlers or closed-generic logging registrations; ASBTestCommand is configuration.

## Named production mutations

1. Drop the token before native session acceptance: the direct cancellation or
   elapsed-time assertion fails after the native timeout.
2. Swallow caller cancellation in session-provider handling and return no receiver:
   the expected direct cancellation assertion fails.
3. Wrap caller cancellation in ChannelFailureException: the direct cancellation
   assertion fails.
4. Clear the configured reader after cancelled acquisition: recovery reader count
   or mapped identity fails.
5. Replace recovered custom body with native bytes: the custom-body assertion fails.
6. Omit native completion during acknowledgement: the final native peek is nonempty.

If initially green, use the named mutations to establish RED. Only intended assertion
failures count; preserve setup/build/timeout diagnostics separately. Restore every
mutation and confirm adopted sources match the approved test/helper. Build all library
targets, run focused cases on both frameworks and both full available suites, retaining
all 618 prior outcomes. Preserve unrelated workspace edits. No commits or pushes.

## Approval scope and remaining gates

The adjacent test, native-client helper and entity fragment were approved before
adoption, execution, correction, mutations and regressions. Public receiver/provider
contracts and consumer subclass extension points are preserved; no new required
interface member, replacement abstract signature or dependency was introduced.

This does not resolve session-lock renewal instability or validate session contention,
arrival/cancellation races, provisioning, in-flight settlement cancellation, DLQ
reading or abandonment-failure recovery. Eight DLQ-blocked cases, 52 unavailable
hosted classes and broader transport/release gates remain open. Overall production
readiness is not established.

## Results

The [approved queue/topic batch](review/asb-session-acquisition-cancellation/README.md)
reproduces ignored cancellation on both frameworks: an empty session acquisition
waits for the native timeout and returns without the expected cancellation exception.
The native provider now forwards the caller token to AcceptNextSessionAsync. Queue
and topic consumers propagate caller cancellation directly. The same consumer can
subsequently acquire a newly published session, preserve custom mapping and native
session/lock metadata, and acknowledge the delivery.

A scoped AsyncLocal carries the token through the existing virtual acquisition hook
and restores its previous value in finally. This preserves existing subclass dispatch
and the public provider/receiver interfaces. Existing external providers retain their
original call path; arbitrary external-provider cancellation is not established.
The native forwarding consumer uses the same token path, but has no dedicated
forwarding-session test in this batch. Five production files change among 1,171
non-generated C# files: the base, queue, topic and forwarding consumers, and the
internal ServiceBusReceiverProvider. No dependency changes.

The approved native-client helper bounds SDK acquisition to ten seconds with no
retries. Empty session-enabled entities exercise pending acquisition, potentially
including connection setup; the test does not locate the precise AMQP request instant.
Both cases require direct cancellation within five seconds and same-consumer recovery.
All four initial cases fail the intended cancellation assertion. Six named mutations
on .NET 9 produce twelve intended xUnit assertion failures. All mutations are restored;
approved test/helper sources, entity fragment and reused reader are unchanged.

All library targets build. Restored focused cases and both full available suites pass
on their first runs: **620 cases, zero failures/skips per framework**, retaining all
618 prior outcomes. Evidence: `/private/tmp/brighter-4170-asb-session-cancellation/`.
Nothing committed or pushed.

Session contention/renewal, receive-arrival races, provisioning, in-flight settlement
cancellation and abandonment-failure recovery remain unvalidated. Session-lock renewal
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. This batch does not establish production readiness.

The isolated emulator containers and network were removed after validation.
