# AWS SDK V3 mapping lifecycle and compatibility

Status: approved and integrated. All 39 cases pass initially and after mutation
restoration on both frameworks. All 15 named mutations fail at their intended
assertions. Both complete SDK V3 regression suites pass all 503 cases.

## Reviewable cases

The 14 test files contain 39 cases adapted from the validated SDK V4 tests.
Only the test and gateway namespaces change. Native SDK V3 observations remain
independent of the configured converters.

| Behavior | Cases | Draft |
| --- | ---: | --- |
| Reused SQS default writer retains each message's content and source bag | 2 | [Writer reuse](MessagingGateway/When_reusing_a_default_wire_writer_should_preserve_each_messages_content.cs) |
| SQS publications keep independent default/composed mappings in both creation orders | 4 | [SQS isolation](MessagingGateway/When_publications_select_different_wire_writers_should_keep_their_mapping_isolated.cs) |
| Overlapping SQS sends retain independent native attributes | 2 | [SQS overlap](MessagingGateway/When_sends_overlap_should_preserve_each_messages_wire_attributes.cs) |
| SNS publications keep independent default/composed mappings in both creation orders | 4 | [SNS isolation](MessagingGateway/When_sns_publications_select_different_wire_writers_should_keep_their_mapping_isolated.cs) |
| Overlapping SNS sends retain independent native attributes | 2 | [SNS overlap](MessagingGateway/When_sns_sends_overlap_should_preserve_each_messages_wire_attributes.cs) |
| Subscriptions keep their selected readers in both creation orders | 4 | [Reader isolation](MessagingGateway/When_subscriptions_select_different_wire_readers_should_keep_their_mapping_isolated.cs) |
| Shared default reader preserves outstanding raw/enveloped results and receipts | 4 | [Reader reuse](MessagingGateway/When_consumers_share_a_default_wire_reader_should_preserve_each_delivery.cs) |
| Custom SNS writing retains gateway-owned FIFO group/deduplication controls | 4 | [FIFO](MessagingGateway/When_publishing_with_a_custom_sns_wire_writer_should_preserve_fifo_controls.cs) |
| Failed custom writing leaves the outbox entry pending until recovery | 2 | [Writer recovery](MessagingGateway/When_a_wire_writer_fails_should_keep_the_outbox_message_pending_until_recovery.cs) |
| SQS outbox clearing maps the current producer trace | 2 | [Outbox trace](MessagingGateway/When_clearing_the_outbox_should_map_the_dispatch_trace_context.cs) |
| In-memory scheduled SQS dispatch maps the current producer trace | 2 | [SQS scheduling](MessagingGateway/When_a_scheduled_message_is_dispatched_should_map_the_current_producer_trace_context.cs) |
| In-memory scheduled SNS dispatch maps the current producer trace | 2 | [SNS scheduling](MessagingGateway/When_a_scheduled_sns_message_is_dispatched_should_map_the_current_producer_trace_context.cs) |
| Non-CloudEvents attributes/body carry dispatch telemetry through native reading and public consumer tracing | 4 | [Telemetry interoperability](MessagingGateway/When_dispatching_non_cloud_events_telemetry_should_restore_the_consumer_context.cs) |
| Existing three-argument SQS construction retains one applicable public overload | 1 | [Constructor compatibility](MessagingGateway/When_selecting_the_legacy_sqs_constructor_should_have_one_applicable_overload.cs) |

All applicable cases exercise sync and async operations. Five test doubles
provide two overlap coordinators, one recoverable writer, and a paired
non-CloudEvents writer/reader. Existing helpers supply the remaining mappings.
No new request type or logging generic is introduced.

The proposed [test-project file](Paramore.Brighter.AWS.Tests.csproj) adds only
`Microsoft.Extensions.TimeProvider.Testing`, already centrally pinned to
10.10.0. It supplies the same fake clock used by the validated V4 in-memory
scheduler tests. No production dependency or central version changes.

## Named mutations

Run each independently on .NET 9 after initial focused runs, restoring exact
production snapshots between mutations. Tests must fail at the specified
assertion; compilation failures, setup failures, missing messages and timeouts
are not evidence. Keep temporary obsolete-API diagnostic suppression confined
to the mutation, where required.

| Mutation | Selected behavior and intended failure |
| --- | --- |
| Cache and return the first `SqsWireMessage` in the default SQS writer | Writer reuse: distinct native application-ID count, 2 cases |
| Cache the first resolved writer in a static field of `SqsMessageSender` while preserving each sender's destination | SQS isolation: mapped topic equality, 4 cases |
| Clear and refill one shared instance attribute dictionary in the default SQS writer | SQS overlap: distinct native application-ID count, 2 cases |
| Cache the first resolved writer in a static field of `SnsMessagePublisher` while preserving each publisher's destination | SNS isolation: mapped topic equality, 4 cases |
| Clear and refill one shared instance attribute dictionary in the default SNS writer | SNS overlap: distinct native application-ID count, 2 cases |
| Cache the first subscription reader selection, including null, in the consumer factory and reuse it for subsequent consumers | Reader isolation: tenant-key presence, 4 cases |
| Cache and return the first parsed `Message` in the default reader | Reader reuse: distinct application-ID count, 4 cases |
| Replace the SNS request's FIFO group ID with `incorrect-group` | FIFO: native group-ID equality, 4 cases |
| Replace the SNS request's FIFO deduplication ID with `incorrect-deduplication` | FIFO: native deduplication-ID equality, 4 cases |
| Catch the custom writer's `FormatException` in the SQS sender and silently use its default writer | Writer recovery: pending outbox entry preservation, 2 cases |
| Move SQS `WriteProducerEvent` to immediately after `sender.SendAsync` | Outbox trace, scheduled SQS trace and non-CloudEvents telemetry: actual publish-span versus native traceparent equality, 8 cases |
| Move SNS `WriteProducerEvent` to immediately after `publisher.PublishAsync` | Scheduled SNS trace: actual publish-span versus native traceparent equality, 2 cases |
| Clear custom-read `TraceState` before the consumer returns its message | Non-CloudEvents telemetry: received tracestate equality, 4 cases |
| Replace custom-read `Baggage` with an empty baggage object before return | Non-CloudEvents telemetry: received baggage equality, 4 cases |
| Make the new reader-taking SQS consumer constructor public | Legacy compatibility: one applicable public constructor, 1 case |

For both static writer-cache mutations, run every theory case in a fresh test
process so each creation order starts with an empty cache. The reader-factory
cache is an instance field and must preserve null as a selected value.

## Approved sequence after review

1. Copy the exact drafts and the proposed project file, restore the test-only
   dependency, and run all 39 cases on .NET 9 and .NET 10.
2. If a behavior already passes, use its named mutation to establish the guard.
   Fix actual missing behavior within this contract if initial tests expose it.
3. Verify all 15 mutations, restore source bytes, rebuild all gateway targets
   and confirm focused GREEN on both frameworks.
4. Run both complete SDK V3 Release suites with all preceding cases retained.
   Review any new warnings or cross-project changes before declaring completion.
5. Update the compatibility/validation matrix with actual results and limitations.

These scenarios cover the corresponding SDK V4 ordinary gateway paths. They do
not complete native AWS scheduler targets, custom invalid-forwarding selection,
SNS/RabbitMQ non-CloudEvents pump integration, other gateways or the public
configuration usability review. The telemetry cases use public tracer operations
and do not execute a service-activator pump/handler. The full issue remains open.

No commit, push or review-gear change is included.

## Local evidence

Artifacts: `/private/tmp/brighter-4170-aws-v3-lifecycle/`.

- The 14 tests, five helpers and test-project file match the approved drafts.
  The only project change adds the centrally pinned test clock dependency.
- Initial and restored focused runs: 39 passed, zero failed/skipped on each of
  .NET 9 and .NET 10.
- All 15 named mutations fail at the specified assertion lines. This requires
  21 isolated test executions and produces 51 intended failures, because each
  static publication-cache theory case runs in its own fresh process.
- The mutation runner verifies failure type, case count and assertion location.
  All mutations compile and execute without a harness correction.
- All production source files match their pre-batch hashes after restoration.
  No lasting production fix was needed for these characterisations.
- Restored gateway builds cover netstandard2.0, net8.0, net9.0 and net10.0.
- Complete SDK V3 Release: 503 passed, zero failed/skipped on .NET 9 (7m21s)
  and .NET 10 (7m16s).
- Exact test-name/outcome comparisons retain all 464 preceding cases and add
  exactly the 39 approved cases on each framework. `verified-results.json`
  records these checks, source restoration and exact draft comparisons.

The next [malformed rejection-copy count batch](../aws-malformed-rejection-count/README.md)
contains 32 cases across both SDK versions. It has since been approved and
integrated; its review records the subsequent validation.
