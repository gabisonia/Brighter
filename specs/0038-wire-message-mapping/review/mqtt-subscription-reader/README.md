# MQTT subscription reader

Status: approved, implemented and validated, with the two fixture corrections recorded below. Successful selection is complete; failure/lifecycle and broader release gates remain open.

Review [the six cases](When_an_mqtt_subscription_selects_a_reader_should_preserve_mapping_and_native_content.cs)
and [the reader](MqttProbeWireReader.cs). The publication writer increment is complete;
this batch adds successful receive mapping through both public consumer factories.

## Contract and behavior

Propose an optional MqttSubscription.WireMessageReader of type
IAmAWireMessageReader<MqttApplicationMessage> and a reusable, stateless public
DefaultMqttWireMessageReader. Preserve public consumer/factory constructor signatures;
an internal constructor may carry the subscription's immutable reader selection.
The native MQTT application message separates content from acknowledgement operations,
so no new carrier is needed. Mapping remains synchronous inside the SDK callback.

Both factory modes receive native QoS 1 publications through isolated Docker
Mosquitto. Six cases cover unconfigured default decoding, a custom reader using the
native bytes as its application body, and composition of the default JSON reader.
The custom and composed modes prefix different bodies and replace the application
topic. Assertions cover exact body bytes, application identity/topic, marker bags,
one configured invocation and unchanged native payload during mapping. The helper
also records the received native topic and QoS. Every publication contains valid
default JSON, so ignoring the configured reader reaches a precise payload assertion.
This does not establish arbitrary non-JSON handling or concurrency.

The default adapter reuses MqttMessageCreator's JSON rules. The consumer must retain
the existing log-and-drop behavior for unreadable default payloads; the shared reader
contract must not introduce null messages into the buffer. Existing malformed-input
regressions remain required. Custom-reader throw/null recovery, rejectable failure
mapping and broader failure-policy decisions remain separate work.

The MQTT SDK currently acknowledges protocol deliveries independently of Brighter's
public acknowledgement methods. These tests make no manual-settlement, redelivery,
rejection or retained-message claim.

## Approved execution

Adopt the test/helper unchanged after approval. Add only the public property and
default adapter needed to compile, then observe four custom/composed payload failures
before wiring factory selection and mapping. Compilation failure is not behavioral
RED. Preserve the writer increment and existing public entry points.

Run six focused cases on .NET 9 and .NET 10. Independently apply these production-only
mutations on .NET 10 after green:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Ignore configured readers and use default decoding | Custom/composed exact body equality | 4 |
| Replace the default decoded body with empty bytes | Legacy/composed exact body equality | 4 |
| Replace every mapped application topic with the native broker topic | Exact application topic equality in all modes | 6 |
| Invoke a configured reader twice and return the second result | Configured invocation count | 4 |

Restore implemented production bytes exactly after each mutation and require six
passes. Eighteen intended assertion failures are expected; unrelated timeout/setup
or compilation failures do not count. End with both focused frameworks green, all
three MQTT library targets built, and full suites retaining all 89 preceding
identities/outcomes plus six new cases. Preserve four existing skips. Run each full
suite once on a fresh, readiness-checked broker and report failures without retries.

Use the repository's Mosquitto image, recording its digest and platform. Keep unique
topics/client IDs and no host port bindings. The operation token is 45 seconds,
receive waits are ten seconds, and external process/hang limits bound synchronous
operations/disposal. Save broker logs and remove only owned containers. Existing
MQTT CI selection covers the cases; no package or workflow change is expected.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
six cases, the scoped implementation, eighteen mutation failures/restorations,
native Docker testing, affected regressions/builds, owned cleanup and local reports.
Hosted calls, commits, deployment and publication are excluded. The approved execution is complete.

## Results and fixture correction

The initial draft omitted the base subscription's required pump type and request
type. Two .NET 9 setup runs each fail six cases before receiving. The test now supplies
Reactor/Proactor according to useAsync and the existing MyCommand request type.
These are the only test changes; all assertions/bounds and the helper are unchanged.
Original and corrected snapshots and setup failures are preserved in the evidence.

Each corrected baseline passes two legacy cases and fails four custom/composed body
assertions. The implementation passes all six on both frameworks. Four mutations
produce eighteen intended assertion failures and exact restorations return six passes.
Both complete suites pass 91 cases with four unchanged skips, retaining all 89 prior
outcomes. All three library targets build cleanly; new/changed files add no warnings.

Three prior production files change and one default reader is added. The other 6,493
preceding C# files remain unchanged. Three isolated brokers are removed. See
[validation](../../validation.md) and `/private/tmp/brighter-4170-mqtt-subscription-reader/`.
The next [reader failure-recovery draft](../mqtt-reader-failure-recovery/README.md)
remains unbuilt/unrun outside the compiled suite pending review approval.
