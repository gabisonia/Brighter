# AWS malformed rejection-copy count preservation

Status: the batch and timestamp assertion correction are approved and integrated.
All 32 cases pass on both frameworks. All six named mutations fail at their
intended assertions, and the source is restored. All four full regressions pass
519 cases each, with zero failures/skips.

## Reviewable contract

Before this fix, both SDK variants' parser catch paths returned a fresh failure message
without the parsed bag or handled count. The default wire reader then reconstructs
the failure without the rejection marker used by `DeliveryCount.Resolve`.
Consequently, malformed routed rejection copies lost their stamped count.

The proposed tests send native messages with a deliberately invalid content type
but readable application ID, topic, bag and count. They exercise:

- SDK V3 and SDK V4.
- Sync and async consumer factories.
- Implicit default reading and an explicitly configured default reader.
- Raw SQS attributes and SNS-envelope attributes.
- Routed rejection copies and ordinary messages carrying the same count of seven.

There are 16 cases per SDK, 32 total:

- [SDK V3 draft](V3/When_default_parsing_fails_should_preserve_rejection_metadata_and_count_precedence.cs)
- [SDK V4 draft](V4/When_default_parsing_fails_should_preserve_rejection_metadata_and_count_precedence.cs)

The native input carries count seven in both the raw bag and legacy envelope
attribute locations. Routed-copy cases also contain all five existing rejection
metadata keys. Ordinary control cases omit all five keys. No production mapper
creates the fixture, so the input independently specifies the native contract.

After failed parsing, the tests require unacceptable message type, exact native
body, parsed application ID/topic and a usable receipt. Routed copies retain
count seven and all rejection metadata. Ordinary messages use the broker's
normalized count: zero on first delivery and one after immediate requeue.
Requeued routed copies keep seven. Both deliveries have independent receipts;
the first result retains its original count and metadata. Native observation
after acknowledgement verifies deletion.

This specifies recovery of readable delivery metadata when another field fails
parsing. It does not invent a count or rejection marker for unreadable JSON/bags,
change successful wire formats, or configure custom invalid-queue writers.

## Implementation boundary

After reproducing the failure, preserve readable count and rejection metadata
through raw/envelope parser failure and default-reader reconstruction in both
SDK packages. Retain the established precedence in `DeliveryCount.Resolve`:
routed copies use their stamped count; ordinary deliveries use broker count.
Keep payload, identity, valid-envelope behavior and settlement intact.
Recovered bag fields must not supply settlement handles; receipt metadata must
come from the current native delivery.

No shared core contract, public constructor or package dependency change is
expected. Any implementation must preserve malformed-payload isolation and must
not allow recovery parsing itself to throw and abort reception.

## RED and mutation plan

Run both SDK drafts before implementation. Expected initial failures are the
eight routed-copy cases per SDK at count equality (seven expected). Ordinary
control cases may already pass and must be characterised with the named mutation.
Record actual results; setup/compilation errors do not establish behavioral RED.

After the fix, apply these mutations separately for each SDK on .NET 9:

| Mutation | Selected cases | Intended failure |
| --- | --- | --- |
| At the consumer's final mapping boundary, force recovered routed-copy handled count to zero, covering both implicit and configured readers | 8 routed-copy cases per SDK | First received count, expected seven |
| At the same boundary, force ordinary failure count to seven instead of using broker count, covering both reader branches | 8 ordinary cases per SDK | First received count, expected zero |
| In the default reader's reconstructed failure, remove only `OriginalTopic` from the recovered bag, retaining reason/count | 8 routed-copy cases per SDK | Original-topic metadata key presence |

Temporary mutations must be restored byte-for-byte from the current source,
never from HEAD. Do not weaken the tests or count an unrelated failure as RED.

## Validation after approval

Copy the exact drafts into their respective test projects. Reproduce the gap,
implement the minimal fix, run all cases on .NET 9 and .NET 10, and validate the
three named mutations in each SDK. Rebuild all supported gateway targets after
restoration. Run all four complete Release suites: V3/V4 on both test frameworks,
retaining every preceding test and outcome.

Use the existing native emulator and isolated account/resource helpers. No new
request types, test doubles, logging generics or dependencies are needed.

This closes one recorded AWS delivery-control edge. The remaining global
gateways, native scheduler mapping, forwarding selection and configuration
usability work remain required for full #4170 completion. No commit, push or
review-gear change is included.

## Approved timestamp assertion correction

The first post-fix runs pass the count assertions, but all routed-copy cases then
fail at timestamp equality. The existing shared bag JSON converter infers a
`DateTime` from ISO timestamp strings. In this environment the input
`2026-10-01T12:00:00.0000000+00:00` is represented as the same instant at
`2026-10-01T14:00:00.0000000+02:00`. Changing that existing successful-parser
behavior is outside this fix.

Only `AssertMetadata` changes in the two review drafts: timestamp values must
be `DateTime` and must equal the exact expected UTC instant after normalization.
The other four rejection metadata values still require exact equality. Key
presence, count precedence, payload, identity, receipts, redelivery independence
and settlement assertions are unchanged. The three reviewed mutations per SDK
are unchanged; the metadata-presence assertion remains on line 206.

The correction was approved and copied exactly into both test projects.
Shared JSON serialization is unchanged.

## Work completed before the assertion correction

- Initial .NET 9 runs: eight routed-copy count failures and eight passing ordinary
  controls per SDK. All failures expect seven but receive zero at line 142.
- Six production files change: the raw parser, envelope parser and default wire
  reader in each SDK. Parsed bag/count are retained through failure. Parser
  failures remove recovered receipt handles; the default reader supplies the
  current native receipt and resolves count using the retained rejection marker.
  No new parsing happens in the catch path.
- The first post-fix runs on both frameworks produce eight timestamp assertion
  failures and eight ordinary-control passes per SDK. All count assertions
  preceding timestamp checks pass. These are not successful focused runs.
- Four count mutations produce eight intended count assertion failures each.
  SDK V4 required a harness indentation correction before its mutations could
  be applied. That harness error is not counted as behavioral RED.
- All mutation source bytes are restored. Both gateway packages build for
  netstandard2.0, net8.0, net9.0 and net10.0.
- The two metadata mutations, complete focused GREEN and four full regression
  suites remain outstanding. Nothing is committed or pushed.

Logs, TRX files, original/current source snapshots, the mutation runner and
`post-fix-review.json` are in `/private/tmp/brighter-4170-malformed-rejection-count/`.

## Validation after the correction

- Corrected and restored focused runs each pass all 16 cases per SDK on each
  framework, with zero failures/skips.
- The two metadata mutations each fail all eight routed-copy cases at the
  original-topic presence assertion, line 206. Together with the four count
  mutations, all six named mutations produce 48 intended failures.
- Every temporary source mutation is restored byte-for-byte. Both gateways
  rebuild on all four targets, and both test projects rebuild on both frameworks.
- Full SDK V3 Release: 519 passed, zero failures/skips on .NET 9 (7m24s)
  and .NET 10 (7m16s).
- Full SDK V4 Release: 519 passed, zero failures/skips on .NET 9 (7m18s)
  and .NET 10 (7m17s).
- Exact test-name/outcome comparisons retain every preceding 503 case and add
  exactly sixteen passing cases per suite. `verified-results.json` confirms
  source restoration and exact approved test copies; only the six intended
  production files differ from the pre-batch hash manifest.

The next [explicit rejection-writer batch](../aws-rejection-writers/README.md)
has since been approved and integrated; its review records the subsequent
validation.
