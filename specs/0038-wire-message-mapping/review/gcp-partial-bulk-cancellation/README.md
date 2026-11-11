# GCP bulk cancellation after confirmed progress

Status: approved test adopted unchanged and characterization completed; no production change. Historical streaming lifecycle findings remain open.

## Behavior

The preceding cancellation batch covers immediate sends and a one-message bulk.
It does not establish what happens after a bulk has already published a prefix.
The current per-message guards appear to support this case, so this is an expected
characterization batch with named production mutations, not a claimed new defect.

Review [the two cases](When_a_gcp_bulk_send_is_cancelled_after_progress_should_preserve_the_prefix_and_retry_the_remainder.cs).
They reuse the existing GcpCancellationWireWriter and GcpPurgeFaultInterceptor unchanged.
Each case sends three distinct sources through the public producer factory, one native
client and a real ordered subscription. The observer records the first successful
Publish RPC, then either cancels immediately or enables cancellation inside the next
configured mapping. No request or response is replaced and no native failure is injected.

The bulk must return OperationCanceledException with the caller's token. The first
case must visit only the first writer input; the second must visit exactly the first
two. Exactly one attempted/completed native Publish must contain the first source's
custom identity. Independent native pull verifies its payload, attributes, ordering
and receipt before acknowledgement. These checks establish real partial progress,
not an assumed callback or elapsed-time precondition.

The caller explicitly creates a new batch containing only the two remaining sources
and retries through the same producer with an active token. Both must use the same
configured writer, publish successfully and be observed/acknowledged natively in
order. Exactly three successful publication RPCs and three distinct native IDs are
expected across the scenario. Every source retains its ID, bytes, topic, partition
and application bag. The first message is acknowledged before observing the remainder
to respect ordered delivery. Operations and cleanup retain the existing bounds and
execution requires a process/hang timeout.

The selected remainder is known to this deterministic test. This does not introduce
a production progress API, tell a general caller which messages reached the broker,
or make retrying a whole batch safe from duplicates. Lost publish responses, remote
retries, in-flight cancellation, writer exceptions and concurrent batches remain separate.

## Approved execution scope

Adopt only this test and run both cases on net9.0/net10.0. The existing helpers must
stay unchanged. Expected initial GREEN needs the named mutations below to establish
RED; a setup/compile error or unrelated failure is not accepted. If a real behavior
fails before mutation, investigate it and make only the minimum supported correction.

| Named production mutation | Intended failure |
| --- | --- |
| Move the bulk per-message entry check outside the foreach, preserving the post-mapping check | The after-publication case visits the second mapping: initial writer-count assertion fails; the during-second-mapping case passes |
| Remove only the bulk post-mapping check | The during-second-mapping case publishes the second message: attempted-publication count is two instead of one; the after-publication case passes |
| Throw cancellation with a default token in the bulk guards | Both cases fail cancellation-token equality |
| Omit gateway ordering enforcement | Both cases fail independently observed ordering on the confirmed first message |

Expected total: six intended assertion failures. Restore the exact production source
between mutations and confirm both cases green after each. Finish with both restored
focused suites, all available Pub/Sub regressions and all four library targets.
GcpNativeWireWriter/Requires traits already select emulator execution and exclude
hosted CI. No new request type, helper, pump or logging registration is needed.

## Review boundary

The repository's [.agent_instructions/testing.md](../../../../.agent_instructions/testing.md)
requires "STOP after writing the test and ASK FOR APPROVAL". Approval covers adoption,
execution, any minimum correction supported by an initial behavioral RED, the four
named mutations and complete available regressions. No commit or publication is
included. Other batch, concurrency, scheduler/rejection, hosted and gateway rollout
requirements and the historical streaming lifecycle findings remain open.

## GCP cancellation after partial bulk progress characterized

The [approved two cases](README.md) are adopted
unchanged. Both pass initially and after exact mutation restoration on net9.0 and
net10.0 without a production change. A real first Publish response triggers either
immediate cancellation or cancellation during the second mapping. The tests verify
the caller token, exactly one native publication, the expected writer inputs and
unchanged sources. Independent native pull/ack observes the confirmed prefix and
both explicitly selected remainder messages retried through the same producer.

Four named production mutations produce **six intended assertion failures**: moving
the entry check outside the loop visits an unwanted second mapping (one failure);
removing the post-mapping check publishes an unwanted second message (one failure);
default-token cancellation loses token identity (two failures); omitting gateway
ordering enforcement changes independently observed native ordering (two failures).
Each mutation is restored exactly and followed by two passing net10.0 cases. Both
final focused suites pass two cases. No setup or compile failure is counted as RED.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 407 | 1 | 55 | 463 |
| net10.0 | 406 | 2 | 55 | 463 |

All 461 prior case identities remain, with exactly two new passing cases per framework.
Outcome changes are recorded in final-verification.json. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

These ordered-nack failures are the missing nacked-message ID at the first final
assertion: Reactor line 152 and Proactor line 158. The previously failing async
unsettled-disposal and sync/two-client factory replacement cases pass in these runs;
this does not establish a fix.

The prior ordered-nack, unsettled/buffered disposal and sync/two-client factory
replacement findings remain open. This characterization changes no streaming code
and establishes no lifecycle fix. The known remainder belongs to this deterministic
fixture: no progress API or duplicate-free whole-batch retry guarantee is introduced.

All 6,479 pre-existing src/tests C# files remain byte-identical. The one added compiled
test matches its approved review copy; both reused helpers are unchanged. All four
library targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero warnings/errors.
Test-project builds retain existing warnings. Both full runs wait for fresh native
emulator readiness. Existing CI traits include these tests in emulator execution
and exclude hosted runs. No hosted, Firestore, Spanner or remote-CI result is claimed.
All three batch-owned emulators are removed; unrelated containers are untouched.
Nothing is committed or published.

Evidence: `/private/tmp/brighter-4170-gcp-partial-bulk-cancellation/` contains
initial/restored, mutation and full TRXs/JSON; emulator readiness and logs;
source hashes and exact snapshots; all-target build; cleanup.json and
final-verification.json.

The next [partial-bulk writer-failure draft](../gcp-partial-bulk-writer-failures/README.md)
is outside compiled projects, unbuilt/unrun pending review. It covers configured
writer throw/null after confirmed progress. Other partial-batch and concurrency
paths, in-flight cancellation, scheduler/rejection mapping, historical lifecycle
findings, hosted checks and five remaining gateway integrations still prevent
feature-wide production readiness.
