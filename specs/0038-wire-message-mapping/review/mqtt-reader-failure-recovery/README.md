# MQTT reader failure recovery

Status: approved, adopted unchanged and validated. Scoped characterization complete; broader lifecycle/release gates remain open.

Review [the four cases](When_an_mqtt_reader_fails_should_drop_the_failed_mapping_and_receive_the_next_delivery.cs)
and [the recoverable reader](MqttRecoverableWireReader.cs).

## Behavior

Both public factory modes receive two native QoS 1 publications on an isolated
Docker broker. The configured reader either throws or returns null for the first
payload, then composes the default reader for the second. The first payload is
valid default JSON: silently falling back to default decoding must expose the
wrong message ID, rather than accidentally satisfying the test.

The test waits until the reader has seen the failed delivery before publishing
the good one. The same consumer must return the good ID, exact binary body,
application topic and marker, with exactly two reader calls. It must then return
the no-message sentinel during a bounded 250 ms empty observation. This checks
continued consumption and absence of a buffered failed mapping; it does not prove
unlimited absence of protocol duplicates.

The expected failure policy follows MQTT's existing log-and-drop handling for
unreadable default payloads. A custom throw/null must not enqueue a null, an empty
placeholder or a default-decoded substitute. It must not prevent subsequent valid
deliveries. MQTTnet owns protocol acknowledgement; this batch adds no manual
settlement or dead-letter/rejection guarantee. Failure forwarding, arbitrary
non-JSON content, default-reader failure composition, cancellation, concurrency
and reconnect scenarios remain separate.

## Approved execution

After approval, adopt the test/helper unchanged and run all four cases on .NET 9
and .NET 10. Retain any initial failures and diagnose their exact cause. The current
callback excludes null results but does not itself contain configured-reader
exceptions. Do not assume how the SDK handles them: observe the native run. Add
only the minimum boundary handling needed for recovery. If cases already pass,
establish their value using the named mutations below without inventing a fix.

Independently apply each production-only mutation on .NET 10:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Latch a per-consumer fault when a configured reader throws, then drop all subsequent mappings for that consumer | Good message ID equality in throwing cases | 2 |
| Replace a configured-reader throw/null result with a fresh empty Message | Good message ID equality in all cases | 4 |
| Fall back to default decoding when the configured reader throws or returns null | Good message ID equality in all cases | 4 |
| Replace successfully mapped bodies with empty bytes before buffering | Exact good body equality in all cases | 4 |

The first mutation must contain the original exception, set the fault flag and
leave later reads without a mapped delivery. The placeholder/fallback mutations
must handle both throw and null outcomes. Fourteen intended assertion failures are
expected; native setup failures or exceptions before the assertions do not count.
Restore production bytes exactly after each mutant and require four passing cases.
Never change assertions or time bounds to obtain a desired result.

Finish with focused passes on both frameworks, all three MQTT library targets
built, and both complete regression suites on fresh, readiness-checked brokers.
Retain all 95 preceding identities/outcomes plus four new cases and four existing
skips. Run each full suite once; preserve failures rather than retrying until green.

Use the repository Mosquitto image, recording its digest/platform, with unique
prefixes/client IDs and no host port bindings. Operation tokens are 45 seconds;
failed-delivery readiness and receives are bounded to ten seconds. External
process/hang limits bound synchronous calls and disposal. Save logs and remove
only owned containers. Existing MQTT CI selection includes the cases; no package
or workflow change is expected. The existing MyCommand supplies subscription
metadata only; no handler, mapper or logging registration is added.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
four cases, an evidenced minimum correction, fourteen mutation failures with exact
restorations, native Docker validation, affected regressions/builds, owned cleanup
and local reports. Hosted calls, commits, deployment and publication are excluded.
The approved execution is complete; no production correction was needed.

## Results

All four initial and final focused cases pass on both frameworks without production
changes. Four named mutations produce fourteen intended assertion failures; every
exact restoration passes all four. Both complete suites pass 95 cases with four
unchanged skips, retaining all 95 preceding identities/outcomes. All three library
targets build cleanly; neither new file adds a warning.

All 6,499 prior C# files and the approved test/helper bytes remain unchanged. Three
owned brokers are removed. The observed SDK dispatch continues after the configured
InvalidOperationException; this does not add or prove a Brighter exception boundary,
log capture, manual settlement or unlimited absence of duplicates. See
[validation](../../validation.md) and `/private/tmp/brighter-4170-mqtt-reader-failure-recovery/`.
The next [writer failure-recovery draft](../mqtt-writer-failure-recovery/README.md)
remains unbuilt/unrun outside the compiled suite pending review approval.
