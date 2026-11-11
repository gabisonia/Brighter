# AWS rejection writer isolation and FIFO delivery controls

Status: approved and validated. All focused cases, ten named mutations and
four full regressions pass their required checks. No production change is needed.
The four exact reviewed test files have been copied into the compiled projects. They reuse the approved `RejectionProfileSqsWireMessageWriter` helper.
There are no new request types, logging generics or dependencies.

## Contract and cases

Retain the two explicit rejection-writer settings from the preceding batch.
This batch tests two remaining properties of that configuration without adding
another public API:

1. Each consumer retains its own selected writers, including the default/null
   selection, when one factory creates differently configured consumers.
2. FIFO rejection forwarding retains the original native message group and
   deduplication ID. Throwing/null mappings preserve the source for recovery,
   just as with standard queues. The gateway owns these native controls.

There are 48 cases per SDK, 96 total:

| Behavior | Cases per SDK | V3 | V4 |
| --- | ---: | --- | --- |
| Subscription isolation: sync/async × default/custom creation order × invalid/dead-letter/fallback route | 12 | [V3 isolation](V3/When_subscriptions_select_rejection_writers_should_keep_each_consumers_mapping_isolated.cs) | [V4 isolation](V4/When_subscriptions_select_rejection_writers_should_keep_each_consumers_mapping_isolated.cs) |
| FIFO: sync/async × three routes × replacement/composition × success/throw/null | 36 | [V3 FIFO](V3/When_rejecting_fifo_with_a_configured_wire_writer_should_preserve_native_controls_and_recovery.cs) | [V4 FIFO](V4/When_rejecting_fifo_with_a_configured_wire_writer_should_preserve_native_controls_and_recovery.cs) |

Isolation cases create both consumers through the same factory before either
rejects a message. Their source queues, IDs and payloads are distinct. Native
observation requires a profile marker only for the configured consumer, the
correct route-specific profile, independent payload/identity/original-topic,
count three, an empty unselected route and settlement of both sources.
The two creation orders make null/default selection observable as well as custom
selection. These are sequential consumer operations; they do not claim concurrent
calls on a single consumer or safe application-written mutable converters.

FIFO cases adapt the preceding approved standard-queue cases. All queues have
`.fifo` names and native `FifoQueue=true`, with content-based deduplication disabled.
Native input supplies explicit group and deduplication IDs, and the subscription
uses matching FIFO attributes. The forwarded native system attributes must retain
both supplied IDs. The previous payload, rejection metadata, explicit writer
selection, throwing/null recovery and source-settlement assertions remain intact.

No production mapper creates the fixture or interprets the outgoing result.
No test depends on broker redrive to trigger forwarding: it calls public consumer
rejection after modeling the pump's handled-count bookkeeping. Native broker
redrive remains outside these writer settings.

## Named mutations

If the cases pass initially, apply the following independently in each SDK on
.NET 9. They are characterization tests, so initial GREEN alone is insufficient.

| Mutation | Selected cases per SDK | Intended assertion failure |
| --- | ---: | --- |
| Cache the first invalid/dead-letter writer selections, including null, in the consumer factory and reuse them for subsequent consumers | 12 isolation cases | Per-consumer native profile-marker presence |
| Resolve the lazy rejection publications with null writers | 12 FIFO `failureMode: 0` cases | Native profile-marker presence |
| Set the SQS send request's FIFO group to `incorrect-group` | 12 FIFO `failureMode: 0` cases | Native group equality |
| Set the SQS send request's FIFO deduplication ID to `incorrect-deduplication` | 12 FIFO `failureMode: 0` cases | Native deduplication-ID equality |
| Delete the source in the rejection failure catch before releasing it | 24 FIFO `failureMode: 1/2` cases | Original native delivery presence after failed mapping |

The factory cache is an instance field, not static. Each theory case uses a fresh
factory, and the cached null selection must be distinguished from not-yet-set.
There are ten mutations overall, expected to produce 144 assertion failures.
Compilation/setup failures and unrelated exceptions are not behavioral evidence.
Restore temporary source changes byte-for-byte from the current snapshots.

## Approved validation work

Copy the exact drafts, run all cases on both frameworks, and fix any reproduced
contract gap without changing public constructors or default formats. Verify all
ten mutations, rebuild both gateways on all four supported targets and restore
focused GREEN. Run all four complete SDK V3/V4 Release suites on .NET 9/10 and
compare exact preceding-case outcomes plus the 48 additions per suite.

Document native/emulator limitations and any newly discovered issue separately.
Remaining scheduler dispatch, RabbitMQ rejection selection, other transports,
converter concurrency and real pump telemetry work remain required for full
#4170 completion. No commit, push or review-gear change is included.

## Mutation evidence

All 48 cases per SDK pass initially on both .NET 9 and .NET 10. No production
change is needed for this batch. Each named mutation compiles and fails at its
intended assertion in both SDKs, without test or harness corrections:

| Mutation | Failures per SDK | Assertion line |
| --- | ---: | ---: |
| Factory caches first selected writer pair, including null | 12 | 143, profile presence |
| Lazy rejection publications ignore selected writers | 12 | 236, profile presence |
| Incorrect FIFO message group | 12 | 238, group equality |
| Incorrect FIFO deduplication ID | 12 | 239, deduplication equality |
| Delete the source after mapping failure | 24 | 182, native source presence |

All ten mutations produce 144 intended failures. Source files are restored
byte-for-byte from the pre-mutation snapshots. Both gateways rebuild successfully
on netstandard2.0, net8.0, net9.0 and net10.0, and both test projects rebuild on
.NET 9 and .NET 10. Artifacts are in
`/private/tmp/brighter-4170-rejection-controls/`.

## Restored regression results

| Check | Result |
| --- | --- |
| Initial and restored focused runs, both SDKs × both frameworks | 48 passed per run, zero failures/skips |
| Full SDK V3 Release, net9.0 | 603 passed, zero failures/skips; 7 m 44 s |
| Full SDK V3 Release, net10.0 | 603 passed, zero failures/skips; 7 m 52 s |
| Full SDK V4 Release, net9.0 | 603 passed, zero failures/skips; 7 m 16 s |
| Full SDK V4 Release, net10.0 | 603 passed, zero failures/skips; 7 m 45 s |

The final verifier compares exact test-name/outcome multisets: every preceding
555-case run is retained, with exactly 48 passing additions per suite. All 1,164
pre-batch production source hashes match. All four compiled test files match the
approved drafts byte-for-byte. Initial builds reported existing warnings but none
for the new files; final incremental builds have zero warnings/errors.

The emulator is Floci 1.5.19 with per-process account isolation and idle-sleep
inhibition. These results do not claim real AWS or remote CI validation. FIFO
checks observe native system attributes; they do not model every managed-service
ordering, deduplication-window or failure mode. Isolation cases are sequential.
Publication and source deletion remain separate operations.

Artifacts include initial/restored/full TRX and logs, build and mutation logs,
source snapshots, hash manifest, runners and `verified-results.json` under
`/private/tmp/brighter-4170-rejection-controls/`. Core and RabbitMQ production
code did not change and those suites were not repeated for this batch.

The subsequent [24 RabbitMQ invalid-writer cases](../rmq-invalid-writer/README.md)
are implemented and validated with 536 passing full-suite cases and two existing
skips per framework. Full #4170 completion still requires
scheduler paths, remaining transports, real pump telemetry and the final public
configuration review. No commit or push was made; the saved stash is preserved.
