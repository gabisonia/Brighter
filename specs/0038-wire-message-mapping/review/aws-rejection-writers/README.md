# Explicit AWS rejection writer selection

Status: approved and integrated. All 72 cases pass on both frameworks, and all
ten mutations fail at their intended assertions. Temporary mutations are restored
and all restored focused checks pass. All four full regressions pass 555 cases
each, with zero failures/skips.

## Contract

Add two optional properties to each SDK's `SqsSubscription`:

- `InvalidMessageWireMessageWriter`: `IAmAWireMessageWriter<SqsWireMessage>?`
- `DeadLetterWireMessageWriter`: `IAmAWireMessageWriter<SqsWireMessage>?`

The public consumer factory captures both selections for its consumer. Each
lazy rejection producer passes the corresponding writer to its publication.
Existing public constructors and omitted configuration retain the default mapping.
A reader cannot infer the outgoing mapping: these selections are explicit and
independent of `WireMessageReader` and normal producer publications.

Existing route rules remain authoritative. Unacceptable messages use the invalid
route when configured and otherwise fall back to the dead-letter route. Delivery
errors use the dead-letter route. Fallback selects the dead-letter writer even if
an invalid-message writer is supplied without an invalid routing key. A writer
selects content, not the native destination queue.

The writer sees the message after rejection metadata is refreshed and the route
is selected. A replacement may encode the payload, identity, handled count and
rejection metadata in its own body. Composition retains default serialization
and adds native attributes. This does not infer a reverse mapping or impose the
default metadata encoding on a replacement writer.

Throwing and null-returning writers must not delete the source or publish a
fallback/default copy. Preserve the existing SQS rejection contract: `Reject`
returns true after handling the failure and releasing the native delivery for
redelivery. Once the writer recovers, a fresh delivery through the same consumer
can be forwarded and the source settled. Do not claim atomic publish/delete or
absence of arbitrarily late duplicates.

## Reviewable tests

There are 36 cases per SDK, 72 total: sync/async × three routes ×
replacement/composition × successful/throwing/null-returning mapping.

| SDK | Test | Helper |
| --- | --- | --- |
| V3 | [Routing and source preservation](V3/When_rejecting_with_a_configured_wire_writer_should_preserve_routing_and_source_delivery.cs) | [Rejection writer](V3/RejectionProfileSqsWireMessageWriter.cs) |
| V4 | [Routing and source preservation](V4/When_rejecting_with_a_configured_wire_writer_should_preserve_routing_and_source_delivery.cs) | [Rejection writer](V4/RejectionProfileSqsWireMessageWriter.cs) |

`route`: 0 invalid-message, 1 dead-letter, 2 invalid-message fallback to dead-letter.
`failureMode`: 0 success, 1 exception, 2 null. Both writers use different profile
markers. The source is published with independent native SDK input, and both
possible destinations are independently created and observed using the SDK.
No production mapper creates the fixture or interprets the outgoing result.

Every successful/recovered copy must have the selected marker, application ID,
exact original payload, count three, original topic/type, reason, description and
rejection timestamp. The test sets the received handled count to three to model
pump bookkeeping before rejection. Timestamp checks use the native ISO value and
a bounded UTC interval, avoiding assumptions about the default bag parser's CLR
representation. Composed output additionally retains default body/ID/count and
omits the local receipt handle from its bag.

Failure cases observe the original native delivery, verify body/identity/topic,
confirm both destination queues are empty, recover the same selected writer and
receive again with a new receipt. All cases check the other route is empty and
source settlement after shortening visibility and waiting for its expiry.

The helper is a mapping strategy, not a broker mock. There are no new request
classes or logging generics, and no dependency changes. Existing full suites
retain omitted-configuration and constructor compatibility coverage. Cross-consumer
selection isolation, FIFO rejection, scheduler dispatch and real pump telemetry
remain separate requirements; these tests do not claim those paths.

## Approved work

After approval, copy the exact tests/helpers to the respective projects. The two
missing properties initially prevent compilation; that alone is not behavioral
RED. Add the minimum public configuration and factory/producer wiring, run the
cases, and then apply all named mutations below independently on .NET 9.

| Mutation in each SDK | Selected cases | Required failure |
| --- | --- | --- |
| Resolve both lazy rejection publications with a null writer | 12 `failureMode: 0` cases | Missing native `rejection-profile` at the explicit presence assertion |
| Swap invalid and dead-letter writer selections without changing destinations | 12 `failureMode: 0` cases | Wrong native profile value |
| Delete the source in the rejection failure catch before attempting release | 24 `failureMode: 1/2` cases | Missing original native delivery after failed mapping |
| Set refreshed `OriginalTopic` to `incorrect-original-topic` before publication | 12 `failureMode: 0` cases | Native metadata original-topic equality |
| Set the message handled count to zero before rejection publication | 12 `failureMode: 0` cases | Native metadata count equality, expected three |

These are ten mutations overall. Compilation, unrelated exceptions and timeouts
are not evidence. Restore each mutation from the current source snapshot, never
from HEAD. Rebuild both gateways on netstandard2.0, net8.0, net9.0 and net10.0;
verify focused GREEN on both test frameworks, and run all four full Release suites.
Record exact previous-case/outcome retention and test additions.

This batch closes explicit AWS rejection publication selection and basic recovery.
The global feature remains incomplete until the remaining checklist is satisfied.
No commit, push or review-gear change is included.

## Local evidence

Artifacts: `/private/tmp/brighter-4170-rejection-writers/`.

- Initial builds fail only on the two missing subscription properties. These
  compilation failures are not counted as behavioral RED.
- Both SDKs add those nullable properties, capture them in the internal consumer
  constructor through the factory, and assign them to the matching lazy producer
  publications. Existing public constructor text is unchanged.
- Initial focused runs pass all 36 cases per SDK on each framework. Approved
  tests and helpers are copied without changes.
- All ten mutations compile and fail at the exact intended assertions: profile
  presence at line 219, profile value at line 220, original-topic equality at
  line 227, count equality at line 226, and original source presence at line 166.
  There are 144 intended assertion failures, with no harness correction required.
- Every mutation is restored from the current source snapshot. Both gateways
  build for netstandard2.0, net8.0, net9.0 and net10.0; both test projects rebuild.
- Only the six intended subscription/factory/consumer production files differ
  from the pre-batch hash manifest. No dependency or shared core change.
- Restored focused checks pass all 36 cases per SDK/framework, zero failures/skips.
- Full SDK V3 Release: 555 passed, zero failures/skips on .NET 9 (7m33s)
  and .NET 10 (7m9s).
- Full SDK V4 Release: 555 passed, zero failures/skips on .NET 9 (7m36s)
  and .NET 10 (7m21s).
- Exact test-name/outcome comparisons retain all preceding 519 cases and add
  exactly 36 passing cases per suite. `verified-results.json` records the full
  comparison, source restoration and approved draft checks.

The subsequent [96-case FIFO/isolation batch](../aws-rejection-controls/README.md)
is approved and validated: all ten mutations fail as intended, all restored
focused checks pass, and all four full suites pass 603 cases. It requires no
production change.
