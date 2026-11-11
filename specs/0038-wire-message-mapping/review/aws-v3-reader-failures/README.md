# AWS SDK V3 reader failures and envelope compatibility

Status: approved and implemented locally. All 36 focused cases pass on both
frameworks, and all seven named mutations fail at their intended assertions.
Both complete SDK V3 Release suites pass 464 cases, with zero failures or skips.

The first SDK V3 batch exposed readers but let a custom reader's exception or
null result abort reception. Default parser failures also lacked the payload
and settlement preservation implemented in SDK V4. This batch closes those
gaps while protecting valid SNS-envelope behavior.

## Reviewable tests

All tests are adapted from the validated SDK V4 counterparts with only the
test/gateway namespace substitutions. They use native SDK V3 clients, existing
isolated emulator accounts and resource cleanup, and public gateway factories.
There are 36 cases across 12 files:

| Behavior | Cases | Draft |
| --- | ---: | --- |
| Throwing reader becomes a rejectable delivery | 2 | [Throwing reader](MessagingGateway/When_a_custom_wire_reader_throws_should_return_a_rejectable_failure_message.cs) |
| Null reader preserves native body, ID and invalid-queue forwarding | 2 | [Null reader](MessagingGateway/When_a_wire_reader_returns_null_should_preserve_rejection_and_native_payload.cs) |
| Throwing reader preserves forwarded body and source settlement | 2 | [Invalid forwarding](MessagingGateway/When_rejecting_a_wire_reader_failure_should_preserve_native_payload_in_invalid_queue.cs) |
| Throwing/null reader can be requeued with stable identity, fresh receipt and incremented count | 4 | [Requeue](MessagingGateway/When_requeuing_a_wire_reader_failure_should_preserve_payload_and_delivery_identity.cs) |
| Raw parsing failure preserves parsed application ID/topic and native body | 4 | [Raw parser](MessagingGateway/When_raw_message_parsing_fails_should_preserve_application_identity_and_native_payload.cs) |
| Malformed envelope preserves native body, fallback ID and rejection | 4 | [Envelope parser](MessagingGateway/When_default_envelope_parsing_fails_should_preserve_rejection_and_native_payload.cs) |
| Mixed batch retains good and throwing-reader deliveries | 2 | [Throwing batch](MessagingGateway/When_a_batch_contains_a_wire_reader_failure_should_preserve_every_delivery.cs) |
| Mixed batch retains good and null-reader deliveries | 2 | [Null batch](MessagingGateway/When_a_batch_contains_a_null_wire_reader_result_should_preserve_every_delivery.cs) |
| Mixed batch retains raw-parser failures and good deliveries | 4 | [Raw batch](MessagingGateway/When_a_batch_contains_a_default_parser_failure_should_preserve_every_delivery.cs) |
| Mixed batch retains malformed envelopes and good deliveries | 4 | [Envelope batch](MessagingGateway/When_a_batch_contains_a_malformed_envelope_should_preserve_every_delivery.cs) |
| Successfully parsed rejection envelope keeps its inner payload and metadata | 4 | [Valid rejection](MessagingGateway/When_reading_a_valid_rejection_envelope_should_preserve_parsed_payload_and_metadata.cs) |
| Composed envelope reader keeps inner content and acknowledgement | 2 | [Envelope composition](MessagingGateway/When_composing_the_default_envelope_reader_should_preserve_inner_message_and_acknowledgement.cs) |

Each row covers sync and async factories. Four-case rows additionally vary
explicit/default reader selection, or throwing/null results for requeue.
Three companion readers in `TestDoubles/` throw, return null, or return null only
when a required tenant header is absent. Existing legacy/default-composition
helpers are reused. No new package, request type or logging generic is needed.

## Implementation contract

Handle each native delivery independently. Preserve native receipt and delivery
count on custom-reader failure, use its native ID when no application ID has
been parsed, retain the exact native body, and return an unacceptable message
through the existing rejection/requeue paths. Never silently fall back to the
default parser for custom-reader failures.

Enrich failed default parsing with the same native payload and settlement data,
retaining any application identity/topic already parsed. Do not replace a valid
parsed rejection envelope with its outer JSON. Keep legacy public constructors,
normal default wire parsing and gateway-owned settlement operations intact.

This batch verifies existing default invalid-queue serialization. It does not
define custom writer selection for invalid forwarding or scheduler-backed retry;
those remain explicit full-scope tasks.

## RED and mutation checks

After approval, copy the exact drafts into the SDK V3 project and run them before
implementation. Record actual behavioral failures; compilation/setup failures
are not RED evidence. Envelope composition and valid rejection may already pass;
characterise them using the named mutations below without weakening the tests.

After implementing the missing behavior, apply each mutation separately on
.NET 9, inspect the intended assertion, restore the exact implementation bytes,
then confirm all 36 cases pass on both frameworks.

| Named production mutation | Selected cases and intended failure |
| --- | --- |
| Empty the body of a custom-reader failure before returning it | Throwing invalid forwarding and null-reader tests: native body equality, 4 cases |
| Bypass default-parser failure enrichment and return its original failure object | Raw and malformed-envelope parser tests: native body equality, 8 cases |
| Always use native ID when enriching a default-parser failure | Raw-parser tests: application ID equality, 4 cases |
| Remove the receipt-present guard that preserves successfully parsed rejection envelopes | Valid-rejection tests: inner payload equality, 4 cases |
| For successful envelope parsing, overwrite parsed body with the whole native envelope | Envelope-composition tests: inner payload equality, 2 cases |
| Return an empty received batch when any mapped delivery is unacceptable | All four mixed-batch classes: expected three delivered messages, 12 cases |
| Return false from requeue for an unacceptable message before changing visibility | Requeue tests: successful requeue assertion, 4 cases |

An exception from test setup, timeout or missing native delivery is not the
intended mutation result. Restore snapshots rather than HEAD to retain local WIP.

## Validation after approval

Build the SDK V3 gateway for all four supported targets, run focused cases on
.NET 9 and .NET 10, validate the seven mutations, and run both complete SDK V3
Release suites after restoring production. Preserve all previous test cases.
Recheck affected projects if shared code must change; none is expected.

This is another step toward full #4170 implementation. SDK V3 concurrency,
endpoint isolation, FIFO, writer failure recovery, dispatch telemetry and the
global scheduler/forwarding work remain required. No commit, push or review-gear
change is authorized by approving this batch.

## Local validation

Artifacts: `/private/tmp/brighter-4170-aws-v3-reader-failures/`.

| Check | Result |
| --- | --- |
| Initial .NET 9 behavior | 30 expected failures, 6 valid-envelope cases already passing |
| Initial GREEN, .NET 9 / .NET 10 | 36 passed / 36 passed |
| Empty custom-reader failure body | 4 failures at native body equality |
| Bypass default-parser failure enrichment | 8 failures at native body equality |
| Replace parsed application ID with native ID | 4 failures at application-ID equality |
| Discard valid rejection envelope | 4 failures at inner-body equality |
| Replace parsed body with complete envelope | 2 failures at inner-body equality |
| Discard mixed batch | 12 failures at delivery count, expected 3 and actual 0 |
| Refuse failure requeue | 4 failures at successful requeue assertion |
| Restored GREEN, .NET 9 / .NET 10 | 36 passed / 36 passed |
| Gateway Release targets | netstandard2.0, net8.0, net9.0 and net10.0 build |
| Complete Release, .NET 9 | 464 passed, zero failures/skips; 7m36s |
| Complete Release, .NET 10 | 464 passed, zero failures/skips; 7m35s |

Initial failures came from custom-reader exceptions/nulls escaping reception or
the default parser losing the native failure body. Requeue cases failed while
receiving the deliberately invalid delivery. No initial failure was attributed
to emulator setup. All 12 mixed-batch cases on both initial GREEN runs received
their three deliveries together.

Two attempts to compile the bypass-enrichment mutation failed on the obsolete
message-type diagnostic, including one where the existing restore directive
overrode file-level suppression. Both attempts restored the original source.
The corrected temporary mutation retained suppression past that directive and
then produced all eight intended assertion failures. These compilation attempts
are retained as logs and are not counted as behavioral RED.

Only `SqsMessageConsumer.cs` and `DefaultSqsWireMessageReader.cs` changed in
production during this batch. Every mutation was restored to its exact GREEN
snapshot before rebuilding; all other production files match their pre-batch
hashes. Test copies match the approved drafts byte-for-byte.

The complete test-name/outcome multisets retain every preceding 428 case and add
exactly 36 passing cases on each framework. `verified-results.json` records the
comparison and source checks. The emulator is stopped; HEAD and the saved stash
are unchanged. Nothing was committed or pushed.

The subsequent [39-case lifecycle and compatibility batch](../aws-v3-lifecycle/README.md)
has been approved and integrated; its review records its separate validation.

Separately, source review identified loss of stamped count/rejection metadata
when a routed rejection copy fails default parsing in both AWS SDK variants.
This batch covers valid rejection envelopes and first-delivery parser failures,
not malformed routed copies. That edge is recorded in the full completion tasks
and requires reviewed tests; it has not been reproduced at runtime here.
