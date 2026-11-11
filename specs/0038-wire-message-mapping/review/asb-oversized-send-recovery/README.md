# ASB oversized send failure and recovery — validated with regression instability

Status: approved and executed. Both native cases pass on .NET 9 and .NET 10 initially
and after restoration. Six mutations produce 12 intended assertion failures. No
production change was needed; the compiled test matches the approved review copy.
Final full suites pass 567 cases each, but .NET 10 required a repeat after a recurring
session-lock renewal failure. That unresolved instability remains a readiness issue.

## Contract

Two native cases, queue and topic. Public bulk producer creation maps an oversized
binary source into the single-message fallback using the existing approved custom
writer. The source exceeds negotiated native batch capacity by 1 KiB (bounded native
capacity 16 KiB–16 MiB). A direct native SDK send of the mapped body must first produce
ServiceBusException with MessageSizeExceeded. This is an explicit environment precondition:
if the namespace accepts the payload, the test fails and the fixture needs investigation;
do not silently skip or pretend that rejection was exercised.

Sending the fallback through IAmABulkMessageProducerAsync must expose a non-transient
MessageSizeExceeded failure. The same producer then creates and sends a healthy batch.
An independent native receiver checks custom identity, exact binary body, metadata and
publication TTL, then settles the delivery. Original source IDs, payloads and selected
metadata remain intact, and the writer is called exactly twice across the two sources.
Failed fallback content and its original bookkeeping ID remain intact.

The installed Azure.Messaging.ServiceBus 7.21.0 XML documentation for SendMessageAsync
identifies ServiceBusException/MessageSizeExceeded for oversized messages. Neither the
test nor this contract asserts whether enforcement occurs locally in the SDK or at the
broker. This establishes native send rejection and subsequent explicit caller recovery,
not confirmed on-wire broker rejection, automatic retry, outbox recovery or exactly-once
semantics. Successful oversized delivery needs separate hosted coverage.

## Named characterisation mutations

1. Swallow the fallback send exception and return success: ThrowsAsync fails.
2. Replace the failure with a generic exception: expected exception type fails.
3. Change the failure reason to ServiceBusy: MessageSizeExceeded assertion fails.
4. Lose custom writer selection for the healthy batch: writer count or native mapping fails.
5. Remap the fallback during send: exact writer invocation count fails.
6. Clear the failed source body during mapping: source preservation assertion fails.

Mutations apply only to production code. The native SDK control remains independent.
A compile failure or unrelated broker error does not count as mutation evidence.
Keep the approved test unchanged; restore every mutation before final validation.

## Validation results

Both cases pass initially and after restoration on both frameworks. Each of the six
mutations fails both cases at its intended assertion on .NET 9. All production source
hashes match the baseline after restoration. All library targets build. The native
control confirms size rejection in this fixture; successful oversized delivery and
whether enforcement occurred locally or remotely are not established.

.NET 9 passes all 567 available cases on its first final run. The first .NET 10 full
run passes 566 and fails one existing session-lock case (queue, async, buffer size 1).
A focused unchanged rerun passes 15/16 and fails a different session-lock case (topic,
sync, buffer size 1). Both throw MessageLockLost in the in-memory receiver during
completion. A subsequent unchanged full .NET 10 run passes 567. Exact comparisons
retain all 565 previous case/outcome pairs on the successful final runs. The two
failures remain evidence of unresolved instability, not erased by the passing repeat.

Those tests use two-second in-memory locks and real-time delays. Timing sensitivity
is a hypothesis, not a confirmed root cause. No timing, test, helper or production
changes were made to obtain the repeat. Native send recovery coverage is complete
within this batch; the session-lock issue remains a separate readiness gate.

Eight DLQ-blocked cases and 52 hosted classes remain unrun/excluded. No new helpers,
test-only transports, request mappings or handlers. Evidence:
`/private/tmp/brighter-4170-asb-oversized-send-recovery/`, including all TRXs,
mutations.json, lock-renewal-instability.json, both full .NET 10 runs, focused-repeat
evidence, baseline comparisons and source-verification.json. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.
