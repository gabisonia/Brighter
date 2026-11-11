# ASB acknowledgement cancelled before entry — validated

Status: approved, implemented and validated. Both cases pass on .NET 9 and .NET 10.
Four mutations produce eight intended assertion failures. Both full available suites
pass 614 cases, zero failures/skips, retaining all 612 prior outcomes.

## Approved contract

Two native queue/topic cases publish one distinct binary message, confirm visibility
with bounded peek, and receive it through the configured AsbExternalWireReader.
The mapping, body, headers, native delivery count one, lock token and sequence are
checked before settlement.

Calling AcknowledgeAsync with an already-cancelled token must directly throw
OperationCanceledException. It must not complete the native delivery. A subsequent
peek from the original sequence must still see the same native ID, sequence and
body. The configured reader must still have run only once, and the mapped message
identity/body and native lock/sequence metadata must remain unchanged.

Before the original lock expires, retry AcknowledgeAsync on the same consumer with
the same mapped Message instance and an active token. It must complete successfully
without another receive or mapping. A bounded native peek must confirm removal.
This verifies retry using the original delivery, not redelivery after lock expiry.

## Scope and limits

This deliberately covers cancellation before acknowledgement entry. It does not
establish cancellation after native settlement starts, nor guarantee rollback when
the broker may already have committed a settlement. Nack/reject cancellation,
settlement transport failures, expired locks and sessions remain separate behaviors.

No new helper, receiver substitute, private reflection or production test hook is
introduced. AsbExternalWireReader is reused unchanged. ASBTestCommand supplies
subscription configuration only; no handler or logging registration is added.
Initial visibility uses the existing bounded peek pattern and rejects unexpected
fixture IDs. A setup failure or naturally expired lock is not cancellation evidence.

## Named production mutations

1. Remove the entry cancellation guard: expected OperationCanceledException fails.
2. Return successful acknowledgement for an already-cancelled token without native
   completion: expected OperationCanceledException fails.
3. Check cancellation only after native completion: the retained native-delivery
   assertion fails, despite the caller receiving OperationCanceledException.
4. Omit native completion during the active-token retry: the final peek is nonempty.

If initially green, use these named mutations to establish RED. Only failures at
the intended assertions count. Preserve setup/build failures separately and restore
every mutation. Adopt the approved test unchanged and verify the reused reader is
unchanged. Build all library targets, run both focused cases on .NET 9 and .NET 10,
and both full available suites, comparing all 612 prior outcomes. Preserve unrelated
workspace edits and existing public receiver-wrapper compatibility. No commits or pushes.

## Remaining gates

In-flight
settlement cancellation, nack/reject cancellation, receive-arrival races, provisioning,
session acquisition, abandonment-failure recovery, session-lock instability, eight
DLQ-blocked cases, 52 unavailable hosted classes and broader transport/release gates
remain open. This batch does not establish overall release readiness.

## Implementation and validation evidence

All four initial executions fail at line 114 because no cancellation exception is
thrown. AcknowledgeAsync now checks its token at entry, before accessing the channel
or receiver. XML documentation describes the pre-entry cancellation exception.
No native settlement has begun when this guard throws, so the same mapped delivery
can be acknowledged with an active token without receiving or mapping it again.

All four corrected and all four restored executions pass with the approved test and
reused reader unchanged. Four mutations fail both cases on .NET 9 at their intended
assertions: removed entry guard and successful early return at line 114, checking
cancellation after completion at the retained-delivery assertion on line 120, and
omitted active-token completion at the final peek on line 144. All eight failures
are intended xUnit assertions. All mutations are restored.

All library targets build. Both full available suites pass 614 cases on their first
runs, zero failures/skips, retaining exactly all 612 prior case/outcome pairs.
Only AzureServiceBusConsumer changes among 1,171 non-generated production C# files.
The change is the entry guard and XML exception documentation. No public signatures,
receiver-wrapper contracts or dependencies change. Evidence:
`/private/tmp/brighter-4170-asb-ack-cancellation/` contains original RED,
corrected/restored TRXs, mutations.json, regression TRXs, exact baseline comparisons
and source verification. Nothing committed or pushed.

The isolated emulator containers and network were removed after validation.
