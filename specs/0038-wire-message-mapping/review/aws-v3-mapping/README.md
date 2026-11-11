# AWS SDK V3 mapping: first implementation batch

Status: approved, implemented and validated locally. The exact drafts are now in
the SDK V3 test project. Focused tests, all six mutations and both complete
regression suites pass their required checks.

This batch implements the absent SDK V3 endpoint extension points. It is part of
the restored full scope, not a new reduction of it. Failure edges, FIFO controls,
SNS envelope composition, isolation/concurrency, non-CloudEvents telemetry and
scheduler/forwarding integration remain required follow-ups before V3 parity is
marked complete.

## Reviewable tests

Each file has sync and async cases, for 12 cases total. They use native AWS SDK V3
clients and the existing isolated emulator/resource reaper. Each is adapted from
the validated SDK V4 counterpart with only test/gateway namespace changes.

| Behavior | Test |
| --- | --- |
| SQS replacement writer sends only selected native attributes without source mutation | [Replacement writer](MessagingGateway/When_sending_with_a_custom_wire_writer_should_use_its_native_attributes.cs) |
| SQS default writer composition preserves ordinary attributes and source bag | [Default writer composition](MessagingGateway/When_customizing_the_default_wire_writer_should_preserve_other_attributes.cs) |
| SQS replacement reader accepts legacy metadata and preserves receipt/acknowledgement | [Legacy reader](MessagingGateway/When_receiving_a_legacy_wire_message_should_map_headers_and_preserve_acknowledgement.cs) |
| SQS default reader composition changes selected bag fields and retains parsed content | [Default reader composition](MessagingGateway/When_customizing_the_default_wire_reader_should_preserve_other_fields.cs) |
| SNS replacement writer controls body, subject and native attributes through independent SNS-to-SQS observation | [SNS replacement writer](MessagingGateway/When_publishing_with_a_custom_sns_wire_writer_should_use_its_body_subject_and_attributes.cs) |
| SNS default writer composition retains normal content and metadata without mutating source handled-count state | [SNS default composition](MessagingGateway/When_customizing_the_default_sns_wire_writer_should_preserve_other_attributes.cs) |

The six companion classes in `TestDoubles/` implement real custom mappings. They
are not mocks of gateways or SDK I/O. Existing native helpers are reused; there
are no new request types, closed logging generics or package dependencies.

## Proposed production contract

Add SDK V3-specific `SqsWireMessage` and `SnsWireMessage` carriers, closed over the
V3 SDK attribute types, plus reusable default writers and a default SQS reader.
Use the already established shared core interfaces. Expose writer properties on
SQS/SNS publications and a reader property on SQS subscriptions; carry them through
the public production factories. Keep destination, FIFO controls and delivery
metadata owned by the gateways.

Preserve existing public constructor signatures and default wire fields. Use an
internal reader-taking consumer constructor, avoiding the source ambiguity found
and fixed in V4. Default writer composition must copy the serialized bag rather
than stamping handled count onto the source. Do not copy unrelated V4 code over
V3 files; preserve SDK-specific APIs and existing behavior through extraction.

## Named mutations after implementation

Missing API compilation failures are expected initially and do not count as
behavioral RED. After the APIs compile and the tests pass, run each mutation
separately against the corresponding two-case test, restoring production bytes
between mutations:

| Mutation in SDK V3 production | Intended failure |
| --- | --- |
| Do not pass the configured writer from `SqsMessageProducer` into the sender | Replacement writer: native attribute count expected one |
| In the extracted default SQS writer, restore the old source-bag handled-count assignment before serialization | Default composition: unchanged source bag expected one entry |
| Ignore the subscription reader in `SqsMessageConsumerFactory` | Legacy reader: application ID expected `legacy-order-17` |
| In `DefaultSqsWireMessageReader.ReadMessage`, replace a successfully parsed body's content with an empty body | Default reader composition: payload equality |
| Do not pass the configured writer from `SnsMessageProducer` into the publisher | SNS replacement: native envelope message expected `legacy:` content |
| In the extracted default SNS writer, restore the old source-bag handled-count assignment before serialization | SNS composition: unchanged source bag expected one entry |

Setup failures, compilation errors, missing messages and timeouts are not the
intended RED. Do not change the tests to accommodate a mutation. Restore each
mutation from a snapshot of the implementation, not HEAD, to preserve local WIP.

## Approved work to perform only after review

1. Copy these exact drafts into the SDK V3 test project and establish the missing
   API state. Implement only the behaviors covered by this batch.
2. Build the gateway's supported targets and run all 12 cases on .NET 9 and .NET 10.
3. Verify the six named mutations, restore production bytes and confirm GREEN.
4. Run both complete SDK V3 Release suites, preserving prior cases and outcomes.
   Recheck the V4 suite if shared core code changes; this batch is designed to
   reuse core contracts without modifying them.
5. Record evidence and any remaining parity gaps. Keep the global rollout row
   open until its complete behavior and validation matrix is satisfied.

No commit, push or change to review gear is authorized. Full implementation of
#4170 remains the goal after this batch.

## Local evidence

Artifacts: `/private/tmp/brighter-4170-aws-v3-mapping/`.

- Initial compilation exposed the expected missing wire API types. This is not
  counted as behavioral RED.
- Initial and restored focused runs: 12 passed, zero failed/skipped on each of
  .NET 9 and .NET 10.
- Gateway Release build succeeds for netstandard2.0, net8.0, net9.0 and net10.0.
- Each named mutation produces two intended assertion failures on .NET 9:
  ignored SQS writer at line 99, source SQS bag mutation at line 111, ignored
  reader at line 118, lost parsed body at line 147, ignored SNS writer at
  line 131, and source SNS bag mutation at line 153 of their respective tests.
- Every mutation is restored from the exact green source snapshot before the
  next mutation. The test assembly was rebuilt before restored/full test runs.
- Complete SDK V3 Release suites: 428 passed, zero failed/skipped on each
  framework (7m18s on .NET 9; 7m28s on .NET 10).
- All 413 prior discovery entries are retained. One pre-existing member-data
  theory expands into four runtime cases: 416 existing executions plus the 12
  additions account for all 428 results. `verified-results.json` records the
  exact name comparison. Discovery alone is not evidence of prior passing
  outcomes.
- All green SDK V3 source snapshots match after testing. All pre-existing
  production files outside SDK V3 match their pre-batch hashes. Approved test
  drafts match the compiled copies byte-for-byte.
- HEAD and the named stash remain unchanged; `git diff --check` passes. The
  local emulator was stopped after the runs.

The implementation extracts default serialization from SDK V3 itself and keeps
the existing public sender, publisher and consumer constructor signatures.
The new reader-taking consumer constructor is internal. Default writers serialize
a copied, filtered bag rather than adding handled count to the source bag.

Custom-reader exception/null handling was incomplete at this checkpoint.
The subsequent [36-case reader failure and envelope batch](../aws-v3-reader-failures/README.md)
is now approved, implemented and validated with 464 passing full-suite cases per
framework. The global rollout row remains open for the remaining lifecycle and
integration work.
