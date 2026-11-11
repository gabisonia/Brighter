# Redis rejection writers

Status: approved, implemented and validated on 2026-10-30.

Review [the twenty cases](When_redis_rejection_selects_a_writer_should_preserve_native_forwarding_and_recover_from_mapping_failure.cs)
and [the recording/recoverable writer](RedisRejectionWireWriter.cs).

## Contract and behavior

RedisSubscription.InvalidMessageWireMessageWriter and DeadLetterWireMessageWriter
will independently select IAmAWireMessageWriter<string> mappings for Brighter-managed
invalid-message and dead-letter forwarding. Null selection retains the legacy
envelope. Both factories capture the selections per consumer. Existing public
constructor signatures remain compatible; retry and publication writer selections
are independent.

Twenty cases cover sync/async × invalid/dead-letter route ×
legacy/custom/composed/throw/null. Both destinations are configured. Unacceptable
routes to the invalid channel; DeliveryError routes to the dead-letter channel.
The unused destination writer and a retry writer are decoys that must never run.

The chosen writer sees the destination topic and refreshed rejection metadata:
original application topic, rejection reason/description and timestamp. Successful
rejection retains application ID/body/marker, publishes exactly one native ID on
only the selected destination with configured expiry, and releases the source's
in-flight gate. Native observation verifies the exact legacy/custom/composed stored
string, counter and queue, and absence of publication to the other destination.
Stored source payloads remain unchanged and the next source ID stays queued until
successful rejection.

A mapping exception must propagate as the original instance; null must throw
InvalidOperationException. Before native publication, failure must restore the source
application topic, retain its in-flight entry and leave both destination counters,
payloads and queues untouched. A second Reject on the same message/consumer must
succeed, then the consumer must receive and acknowledge its next source delivery.
Refreshed rejection metadata may remain on the source after mapping failure.

The correction must distinguish mapping failures from existing native publication
error handling. Preserve the existing native-error behavior, missing-channel behavior
and invalid-to-DLQ fallback; these cases do not establish their wider recovery
contracts. Do not claim atomic forwarding, crash redelivery, concurrent consumer
safety, cancellation, missing-channel/fallback writer selection or arbitrary
multiline/binary round trips. All payloads here are single-line Unicode.

## Results

Both frameworks first reported the expected missing properties (CS0117). Property-only
scaffolding then produced sixteen intended behavioral failures: eight wrong native
strings and eight missing throw/null errors, with four legacy passes per framework.
Compilation failures were recorded separately from behavioral RED.

Optimized payload failures initially omitted assertion locations. Incremental
diagnostic builds and JIT settings still gave incomplete/preceding sequence points;
their location-verification checks were rejected and retained. A clean diagnostic
rebuild then verified all sixteen intended assertion lines on both frameworks.
No test, helper, fixture, assertion or time bound changed.

Both factories now capture independent invalid/dead-letter writers. Mapping is
separate from shared native publication: mapping errors restore the source topic
and retain its in-flight entry, while native publication errors retain their existing
logged/true-result behavior and release the entry. Internal mapped-send methods
reuse the ordinary producer's native publication code without invoking mapping twice.
Public constructors are unchanged; the two selection properties and rejection methods
document the contract.

All twenty approved cases pass on both frameworks. Four diagnostic .NET 10 mutations
produced all 64 intended failures at the reviewed assertion lines, with expected
unaffected cases passing. Every byte-exact restoration returned twenty passes. A
clean optimized rebuild also passed twenty focused cases per framework; those focused
runs retained diagnostic JIT settings. The full suites below used ordinary JIT.

Both complete optimized suites ran once, unfiltered, on fresh brokers: 158 passed,
zero failed, four existing skips each. All 142 preceding identities/outcomes are
retained with twenty new passes. All four library targets built with zero warnings/errors.
Initial, diagnostic, green and final optimized dependency builds each reported 604
existing warnings, none in new/changed files. Hosted CI was not run.

Of 6,519 preceding C# files, 6,515 remain byte-identical. Only consumer, factory,
producer and subscription changed; the two compiled test/helper additions match
approved snapshots. Dependency, lockfile and workflow hashes are unchanged. All three
owned brokers were removed after logs were saved; unrelated containers were untouched.
TRX, logs, diagnostic findings, mutation snapshots, scoped source diff, verification
and cleanup evidence are retained in `/private/tmp/brighter-4170-redis-rejection-writers/`.

The next [rejection-boundary draft](../redis-rejection-boundaries/README.md) covers
twelve fallback, missing-destination and native-error cases. It reuses the unchanged
helper and remains outside the compiled suite, unbuilt/unrun.

## Approved execution plan (completed)

Adopt the test/helper unchanged. Record missing-property compile errors separately
from behavioral RED. Add only the two nullable public auto-properties with XML
contract documentation, leaving selection unused. Both frameworks should then
show sixteen intended behavioral failures (eight custom/composed payload failures
and eight missing throw/null errors), with four legacy cases passing.

Implement minimum factory/consumer/producer wiring and mapping-failure handling.
Mapping and null validation must precede native publication; retain the source's
topic and in-flight entry on mapping failure, while preserving the separate existing
native publication error handling. Internal helpers/overloads may separate mapping
from native send without widening public constructors. Require twenty passes on
both frameworks with approved tests and bounds unchanged.

Apply four independent production-only .NET 10 mutations:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Ignore both selected rejection writers and use legacy encoding | Exact native string in custom/composed modes (8); throw/null error presence (8) | 16 |
| Remove the source in-flight entry before invoking rejection mapping | Receive must remain blocked after throw/null failure | 8 |
| Publish the successfully mapped rejection to the other configured destination | Selected destination native counter must equal one | 20 |
| Store an empty native string instead of the successful mapped result | Exact native destination string | 20 |

Require 64 intended assertion failures. The first mutation's four legacy cases and
the second mutation's twelve non-failing modes must pass. The wrong-destination
mutation must leave mapping and its errors intact and only alter native routing
after successful conversion. Setup/compiler/timeout failures do not count.
Restore production bytes exactly after each mutation and require twenty passes.

Finish with focused passes on both frameworks, all four library targets built, and
both full suites preserving all 142 preceding identities/outcomes plus twenty new
passes. Expected complete matrices are 158 passes/four existing skips each.
Run full suites once on separate fresh ready brokers; retain and investigate
failures without retrying behavioral regressions until green.

Use the pinned Redis image, native Docker SDK runners, no host ports, unique keys,
45-second async tokens and one-second receives. External process/hang limits bound
sync/native calls and disposal. Save logs and remove only owned containers.
No dependency/workflow changes or hosted services are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
twenty cases, minimum public-property compilation scaffolding, evidenced mapping and
native-send separation, factory/consumer/producer changes and documentation,
64 reviewed mutation failures with exact restorations, native Docker validation,
full affected regressions, four library builds, owned cleanup and local reports.
Hosted calls, commits, deployment and publication are excluded. The compiled test/helper
match the approved draft. Further boundary and lifecycle tests require their own review.
