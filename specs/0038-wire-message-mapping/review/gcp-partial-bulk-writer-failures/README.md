# GCP bulk writer failure after confirmed progress

Status: approved test adopted unchanged and characterization completed; no production change. Historical streaming lifecycle findings remain open.

## Behavior

The earlier writer-failure cases fail before any publication. The partial-bulk
cancellation cases establish a confirmed prefix, but cancellation guards do not
exercise a writer throwing or returning null after progress.

Review [the two cases](When_a_gcp_bulk_writer_fails_after_progress_should_preserve_the_prefix_and_retry_the_remainder.cs).
They reuse GcpRecoverableWireWriter and GcpPurgeFaultInterceptor unchanged. Both
create a real ordered subscription and a public factory producer with one native
client, then submit three distinct sources. A passive observer enables writer
failure only after the first native Publish RPC succeeds. The second mapping
either throws the writer's original exception or returns null. No native request
or response is replaced, and no broker fault is injected.

The bulk must stop with the original exception instance or the documented null
result error. Exactly two source objects must have reached the writer; the third
must remain unvisited. Exactly one attempted and completed publication must
contain the first source. Independent native pull verifies its custom payload,
attributes, ordering and receipt before acknowledgement. All three source IDs,
bytes, destinations, partitions and application bags must remain unchanged.

The caller disables the writer failure and explicitly creates a new batch from
the two remaining sources. The same producer must map and publish both in order.
Independent pull/ack verifies their complete content and three distinct native
IDs across the scenario; the publication ledger must contain exactly three calls.
The fixture knows this remainder deterministically. This adds no progress API and
does not establish that retrying an arbitrary failed whole batch avoids duplicates.

## Approved execution scope

Adopt only the test and run both cases on net9.0/net10.0. Existing helpers stay
unchanged. The behavior is expected to pass initially; validate these named
production-only mutations, restoring exact source and confirming green after each:

| Mutation | Required assertion failure |
| --- | --- |
| Remove the CreateWireMessage null-result guard | Null case gets NullReferenceException instead of InvalidOperationException; throwing case passes |
| Catch the configured writer's InvalidOperationException and throw a new InvalidOperationException with the same message | Throwing case fails exception-instance identity; null case passes |
| Catch mapping exceptions inside the bulk loop, remember the first, continue visiting remaining sources, then rethrow the first after the loop | Both cases fail writer-input count: three instead of two |
| Omit gateway ordering enforcement | Both cases fail native ordering on the independently received first message |

Expected total: six intended assertion failures. A build/setup failure is not RED.
The deferred-exception mutation must surround only CreateWireMessage, keep native
publication outside that catch, skip publication for failed mappings and preserve
the first exception object. The exception-replacement mutation must surround only
the writer invocation, leaving the null guard outside its catch.

If the original behavior genuinely fails, make only the minimum supported
correction. Finish with both restored focused suites, full available Pub/Sub
matrices and all four library targets. Preserve prior case identities and report
all failures and skips. Existing emulator traits already select this coverage;
no new helper, request type, pump, logging registration or CI filter is needed.

## Review boundary

The repository's [.agent_instructions/testing.md](../../../../.agent_instructions/testing.md)
requires "STOP after writing the test and ASK FOR APPROVAL". Approval covers the
concrete test, minimum correction if supported, four mutations and regressions.
No commit or publication is included. Streaming lifecycle findings, concurrent
operations, ambiguous broker outcomes, scheduler/rejection mapping, hosted checks
and other gateway integrations remain separate release requirements.

## GCP writer failure after partial bulk progress characterized

The [approved two cases](README.md) are adopted
unchanged. Both pass initially and after mutation restoration on net9.0/net10.0
without a production change. After a confirmed first Publish, the second mapping
either throws the writer's original exception or returns null. The bulk preserves
the original exception instance or documented null-result error, visits only the
first two source objects and publishes only the first. Independent native pull/ack
verifies the prefix, then the explicitly selected two-message remainder retried
through the same producer. Source IDs, bytes, topics, partitions and bags stay intact.

Four production-only mutations produce **six intended assertion failures**: removing
the null guard produces NullReferenceException (one); replacing the writer exception
loses instance identity (one); deferring mapping failure until after the loop visits
the third source (two); omitting gateway ordering enforcement changes independently
observed native ordering (two). Each mutation is restored exactly and followed by
two passing net10.0 cases. Both final focused suites pass two. No build/setup failure
is counted as behavioral RED.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 409 | 1 | 55 | 465 |
| net10.0 | 409 | 1 | 55 | 465 |

All 463 prior case identities remain, with exactly two new passing cases per framework.
Outcome changes are recorded in final-verification.json. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.Stream.GcpStreamDisposeWithUnsettledMessageTests.When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_should_return_promptly — Assert.Equal() Failure: Values differ
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set

The net9.0 failure is the previously observed empty replacement receive in the
synchronous unsettled-disposal case at line 82. The net10.0 failure is the missing
nacked-message ID at the first final assertion in generated Reactor line 152.
The factory diagnostic cases pass in these runs; that does not establish a fix.

Historical ordered-nack, unsettled/buffered disposal and sync/two-client factory
replacement findings remain open. No streaming code changes in this characterization,
and it establishes no lifecycle fix. The known remainder is specific to the fixture;
there is no general progress API or duplicate-free whole-batch retry guarantee.

All 6,480 pre-existing src/tests C# files remain byte-identical. The sole added compiled
test matches its approved review copy; both reused helpers remain unchanged. All four
library targets (netstandard2.0/net8.0/net9.0/net10.0) build without warnings or errors.
Test-project builds retain warnings. Both full runs wait for fresh native emulator
readiness. Existing CI traits select emulator execution and exclude hosted execution;
no hosted, Firestore, Spanner or remote-CI result is claimed. All three batch-owned
emulators are removed; unrelated containers remain untouched. Nothing is committed
or published.

Evidence: `/private/tmp/brighter-4170-gcp-partial-bulk-writer-failures/` contains
initial/restored, mutation and full TRXs/JSON, emulator readiness/logs, source hashes
and exact snapshots, all-target build, cleanup.json and final-verification.json.

The next [factory native shutdown observation amendment](../gcp-factory-native-shutdown-observation/README.md)
is outside compiled projects, unbuilt/unrun pending review. It adds native nack/ack
RPC evidence to the existing eight factory cases through the public SDK settings
hook, retaining native connection ownership, case identities, assertions and bounds.
Other batch/concurrency paths, scheduler/rejection mapping, hosted checks, historical
lifecycle findings and five remaining gateway integrations still block feature-wide
production readiness.
