# RabbitMQ fixture synchronization review

Status: approved and validated. The compiled files match these four drafts exactly.

These changes address two synchronization gaps found while investigating the
initial overlapping-retry full-suite failures. Passing unchanged repeats do not
establish either failure's cause.

## Delivery-tag collision

- [Test draft](When_sends_collide_on_a_delivery_tag_should_not_confirm_either_message.cs)
- [Channel helper draft](GatedTagReadRmqChannel.cs)
- [Producer helper draft](GatedTagReadRmqProducer.cs)

The existing fixture holds sequence-number reads until both callers arrive.
After that gate opens, one caller can register, publish and receive confirmation
before the other registers. The intended pending-record collision is therefore
not guaranteed. Concurrent first use can also race channel-wrapper installation.

The draft prepares the real channel before concurrent sends. It retains the
read gate and adds a gate before native publication. Both pending records must
exist before either native publish starts. Both gates fail on timeout. The test
asserts that two equal sequence numbers were read, then retains the assertion
that neither message was confirmed. All I/O still reaches the real broker.

Named mutation: in `RmqMessageProducer.RecordPendingConfirmation`, replace
collision removal with `_pendingConfirmations[deliveryTag] = confirmation`.
The test must fail at `Assert.Empty(_confirmed)`, not at a gate or tag assertion.
Restore the entire production file from its pre-mutation byte snapshot.

## Original-ID retry observation

- [Test draft](When_rmq_requeue_original_id_headers_differ_in_case_should_preserve_one_source_identity.cs)

The current fixture reads once immediately after consumer disposal. A retry
prefetched by that consumer may not yet be visible to the independent reader.
This is a hypothesis about the observed failure, not a demonstrated root cause.

The draft polls the native queue every 25 ms for at most five seconds, within the
existing overall cancellation deadline. It reports empty-read count, elapsed
time and redelivery status in the test output. All existing assertions remain:
native payload, application properties, persistence, generated identity, exactly
one original-ID header, gateway retry controls, source immutability and no extra
message.

Two named mutations validate the retained guarantees:

1. Omit case-insensitive original-ID cleanup in
   `RmqMessagePublisher.AddOriginalMessageIdOnRepublish`. All four cases must
   fail at the single-original-ID assertion.
2. Omit only the native publish call in `RequeueMessageAsync`, allowing the
   consumer's normal successful-return and acknowledgement path to run. The
   test must fail because the retry is absent after bounded observation.
   A returned original must instead fail its payload or identity assertion.

Restore the publisher byte-for-byte after each mutation. Polling must never
convert missing, duplicate or incorrectly mapped delivery into a pass.

## Validation after approval

Apply the four exact drafts to their existing locations. Build both test target
frameworks, run the five cases, prove each named mutation and restore GREEN.
Repeat focused executions on .NET 9 / RabbitMQ 4.2 and .NET 10 / RabbitMQ 4.3,
then run both unfiltered affected suites. Retain initial failures and all output.

Inspect observation diagnostics to determine whether delayed visibility actually
occurred. A passing run with zero empty reads does not confirm that hypothesis.
Any recurring failure needs investigation before the readiness item is closed.
No production change is proposed by this review, and nothing is committed or pushed.

## Results

The five initial cases pass on both frameworks. The collision mutation fails at
the empty-confirmation assertion. All four original-ID cases fail at the
single-header assertion when cleanup is omitted. All four fail at retry presence
after five seconds when retry publication is omitted.

Both production files were restored byte-for-byte before regression. Ten fresh
focused runs per framework pass all 140 executions, including overlapping retry
coverage. Each full suite passes 488 tests with two existing skips and no failures.
Their test names and outcomes match the previous complete passing suites.

All 96 successful original-ID executions across initial, focused and full runs
report zero empty reads and a redelivered retry. The five-second wait did not
activate in a successful case. The original timing failure therefore remains
unconfirmed; the omitted-publication mutation establishes that polling does not
hide a lost retry. Full evidence is in [validation.md](../../validation.md).
