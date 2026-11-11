# ASB negative acknowledgement cancelled before entry — validated

Status: approved, implemented and validated. Both cases pass on .NET 9 and .NET 10.
Five mutations produce ten intended assertion failures. Both full available suites
pass 616 cases, zero failures/skips, retaining all 614 prior outcomes.

## Approved contract

Two native queue/topic cases publish a distinct binary message, establish visibility
by bounded peek, and receive through the configured AsbExternalWireReader. The test
checks custom mapping, source bytes, delivery count one and native lock/sequence.

NackAsync with an already-cancelled token must directly throw
OperationCanceledException without abandoning the delivery. The native ID, sequence
and body must remain visible to peek, and the mapped message and its native metadata
must remain unchanged with only one reader invocation. A separate native receiver
then attempts a one-second receive and must obtain nothing while the original lock
is still live. Peek alone cannot distinguish a locked message from an abandoned one.

Retry NackAsync with an active token on the same consumer and mapped Message.
Receive the redelivery on that consumer before the original lock deadline, with
native delivery count two, a fresh lock token, unchanged native ID/sequence/body,
preserved custom mapping and current gateway lock/sequence metadata. Exactly two
reader invocations occur overall. Acknowledge the redelivery and confirm native
removal using bounded peek from the original sequence.

## Scope and limits

This covers pre-entry cancellation only, not cancellation after native abandonment
starts, transport failures, expired locks or sessions. The one-second competing
receive is a bounded availability observation, not an absolute proof of native lock
state. The named late-cancellation mutation must demonstrate that this observation
detects premature abandonment in the actual fixture. Do not count unrelated failures
or silently retry if it does not; preserve evidence and review an amendment.

Both observer completion and healthy redelivery must precede the recorded original
lock deadline. Otherwise natural expiry could mimic successful abandonment. Fixture
entities use one-minute locks. Unexpected initial IDs, timeouts, expired locks or
an incomplete setup are not cancellation evidence. Failed mutation cases use fresh
isolated fixtures so a competing receive cannot contaminate later cases.

The observer is a real native receiver, with no substituted broker I/O. No new helper,
reflection or production test hook is introduced. AsbExternalWireReader is reused
unchanged. ASBTestCommand supplies subscription configuration only; no handler or
logging registrations are added. Existing NackAsync handling of native settlement
errors is not established or changed by this contract.

## Named production mutations

1. Remove the NackAsync entry guard: expected cancellation exception fails.
2. Return success for an already-cancelled token without abandonment: expected
   cancellation exception fails.
3. Check cancellation only after native abandonment: the competing receive acquires
   the delivery and its Assert.Null fails before the healthy retry.
4. Omit native abandonment during the active-token retry: the consumer's redelivery
   Assert.Single fails before original lock expiry.
5. Omit native completion of the redelivery: the final peek remains nonempty.

If initially green, use these mutations to establish RED. Only intended assertion
failures count; preserve setup/build failures separately and restore every mutation.
Adopt the approved test unchanged and verify the reused reader is unchanged. Build
all library targets, run focused cases on both frameworks and both full available
suites, comparing all 614 prior outcomes. Preserve unrelated workspace edits and
public receiver-wrapper compatibility. No commits or pushes.

## Remaining gates

In-flight
settlement cancellation, reject cancellation, receive-arrival races, provisioning,
session acquisition, abandonment-failure recovery, session-lock instability, eight
DLQ-blocked cases, 52 unavailable hosted classes and broader transport/release gates
remain open. This batch does not establish overall release readiness.

## Implementation and validation evidence

All four initial executions fail at line 116 because no cancellation exception is
thrown. NackAsync now checks its token at entry before accessing the channel,
receiver or native lock, and XML documentation describes the exception. Its existing
handling of native settlement errors remains unchanged. All four corrected and four
restored executions pass with the approved test and reused reader unchanged.

Five mutations fail both queue/topic cases on .NET 9 at their intended xUnit
assertions: removed guard and successful early return at line 116, cancellation
checked after abandonment at line 137, omitted active-token abandonment at line
141, and omitted completion at line 170. In the late-cancellation mutation, the
competing native receiver acquires the prematurely released delivery in both cases.
This establishes the bounded observer's sensitivity in the tested fixture; it is
not an unbounded lock-state guarantee. All ten intended failures are recorded and
all mutations are restored.

All library targets build. Both full available suites pass 616 cases on their first
runs, zero failures/skips, retaining exactly all 614 prior case/outcome pairs.
Only AzureServiceBusConsumer changes among 1,171 non-generated production C# files:
the NackAsync entry guard and XML exception documentation. No public signatures,
receiver-wrapper contracts or dependencies change. Evidence:
`/private/tmp/brighter-4170-asb-nack-cancellation/` contains original RED,
corrected/restored TRXs, mutations.json, regression TRXs, exact baseline comparisons
and source verification. Nothing committed or pushed.

The isolated emulator containers and network were removed after validation.
