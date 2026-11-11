# MQTT publication writer

Status: approved, implemented and validated. Scoped publication writer increment complete; MQTT reader/lifecycle and wider release gates remain open.

Review [the six cases](When_an_mqtt_publication_selects_a_writer_should_preserve_native_content_and_gateway_controls.cs)
and [the writer](MqttProbeWireWriter.cs). This is the first MQTT implementation
increment for issue #4170, following the repository test-first workflow.

## Behavior and public contract

Sync and async sends cover legacy, custom binary, and composed default encodings.
An independent native MQTTnet client subscribes before sending and observes exact
payload bytes, the gateway's prefixed source topic, and QoS AtLeastOnce. It also
subscribes to the deliberately incorrect writer topic and the unprefixed source
topic, so a routing defect fails topic equality rather than merely timing out.
The custom writer requests AtMostOnce; the observer subscribes at ExactlyOnce,
allowing the delivered QoS to expose the producer's override. Source identity,
topic, body and bag must remain unchanged; configured writers run once per send.

Propose MqttPublication : Publication with a nullable
IAmAWireMessageWriter<MqttApplicationMessage> WireMessageWriter, and a reusable public
DefaultMqttWireMessageWriter preserving the existing full-Message JSON encoding.
The producer selects the publication's writer; existing base Publication callers
retain their default behavior. Existing producer/publisher constructors and public
methods remain compatible. The gateway applies TopicPrefix plus the source header's
topic and QoS AtLeastOnce after mapping. These contracts serve caller configuration
and composition; no internal member is exposed solely for testing.

## Approved execution

1. Capture both existing full MQTT baselines before adopting or implementing changes.
   Use isolated Docker Mosquitto matching docker-compose-mqtt.yaml, record its image
   digest, and wait for readiness. Run SDK containers sharing the broker network so
   existing localhost:1883 tests work without changing host services.
2. Adopt the test/helper unchanged. The proposed public types do not yet exist;
   compilation failure is not behavioral RED. Add the smallest public contracts and
   default adapter, leaving configured selection absent, then require the four
   custom/composed cases to fail exact payload equality. Implement the minimum
   publication selection and native sending path to make all six pass.
3. Run each named production-only mutation below independently on .NET 10. Restore
   the implemented production bytes exactly after each and require all six passes.
   Never alter tests or time bounds to manufacture RED or green.
4. Finish with all six focused cases passing on .NET 9 and .NET 10. Run both full
   affected MQTT suites, retaining all baseline identities plus six; preserve and
   report existing failures/skips. Build the MQTT library for net8.0, net9.0 and
   net10.0. Check new-file warnings and source preservation.
5. Save evidence, dispose owned clients, remove only owned broker containers and
   update local validation/release records. The operation token is 45 seconds;
   external process/hang bounds also constrain synchronous calls and disposal.

No new dependency or workflow change is expected: MQTTnet is already referenced
and the existing MQTT CI category selects these cases. Existing embedded-server
tests remain part of the full suites.

## Named production mutations

| Mutation | Required failure | Cases |
| --- | --- | ---: |
| Ignore the configured writer | Exact native payload equality for custom/composed modes | 4 |
| Omit the gateway topic override, retaining the mapped/default topic | Exact native topic equality in all modes | 6 |
| Omit the gateway QoS override | Delivered QoS equality for custom/composed modes | 4 |
| Default adapter emits raw source bytes instead of full Message JSON | Exact native payload equality for legacy/composed modes | 4 |

Eighteen intended assertion failures are expected in total; unaffected cases must
pass. Setup failures, compilation errors and unrelated timeouts do not count.

## Scope and approval boundary

Reader integration, writer throw/null/cancellation/retry/outbox behavior, retained
messages, scheduling/rejection, concurrency, reconnect recovery, protocol-v5
features and other gateways remain separate. This batch alone does not close MQTT
or whole-feature production readiness. It makes no retention-policy assertion:
live subscribers do not reliably expose a publisher's retain flag.

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
six cases, the scoped implementation, named mutations/restorations, native Docker
validation, affected regressions/builds, owned cleanup and local reports. Hosted
calls, commits, deployment and publication are excluded. The approved execution is complete.

## Results

Both baselines pass 79 cases with four skips. After adding the public contracts,
each framework shows four intended payload failures and two legacy passes. The
implemented selection makes all six pass. Four mutations produce eighteen intended
assertion failures; each exact restoration passes all six. Both complete final
suites pass 85 cases with four unchanged skips, retaining all 83 baseline outcomes.
All three library targets build without warnings/errors; no changed file adds a warning.

The approved test/helper remain byte-identical. Exactly two prior production files
change and two public types are added; 6,490 other preceding C# files remain unchanged.
All five isolated brokers are removed. See [validation](../../validation.md) and
`/private/tmp/brighter-4170-mqtt-publication-writer/` for evidence, including the initial
asset-restore/readiness setup failures and their resolution before behavioral testing.

The next [subscription reader draft](../mqtt-subscription-reader/README.md) remains
unbuilt/unrun outside the compiled suite pending review approval.
