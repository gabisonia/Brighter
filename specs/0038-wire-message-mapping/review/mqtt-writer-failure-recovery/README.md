# MQTT writer failure recovery

Status: approved, adopted unchanged and validated on .NET 9 and .NET 10.

## Result

Both initial runs pass the two throwing cases and fail the two null-result cases:
the caller receives NullReferenceException instead of InvalidOperationException.
An explicit null-result guard before native publication fixes that behavior while
preserving original writer exceptions. Only MQTTMessagePublisher and the public
MqttPublication XML documentation change among 6,501 preceding C# files.

All four focused cases pass on both frameworks. The four reviewed mutations below
produce all fourteen intended assertion failures; exact production restorations
return four passes after each. Both complete suites pass 99 cases with four existing
skips, retaining all 99 preceding identities/outcomes plus four passing cases. All
three library targets build without warnings/errors. The approved test/helper are
unchanged. All three owned brokers are removed and logs retained.

Evidence: `/private/tmp/brighter-4170-mqtt-writer-failure-recovery/`, including
`final-verification.json`, TRX/log/JSON records, snapshots, mutants and `cleanup.json`.
Wider MQTT lifecycle and whole-feature release gates remain open.

Review [the four cases](When_an_mqtt_writer_fails_should_preserve_the_error_and_recover_without_publishing_the_failed_message.cs)
and [the recoverable writer](MqttRecoverableWireWriter.cs).

## Contract and behavior

Sync/async cases each configure a writer that throws a known InvalidOperationException
or returns null on its first call, then composes the default writer on its next call.
Writer-thrown errors must reach the caller as the original exception instance. A
null native result must fail with InvalidOperationException. Neither failure may
silently return success, use default encoding as fallback, or publish the failed
source message. The same producer must subsequently publish a valid message.

An independent native MQTT client subscribes before either send. The first observed
payload must exactly equal the successful message's default JSON bytes. A further
250 ms observation must leave exactly one delivery recorded. The selected writer
runs exactly twice; the failed source's identity, binary body, topic and marker bag
stay unchanged. This bounds the observation of unwanted publication; it does not
prove unlimited absence of protocol duplicates or transactional outbox behavior.

Existing public producer/publisher APIs remain compatible. No native retry,
reconnect, cancellation, bulk, outbox, retention, scheduling or rejection behavior
is included. The proposed null-result error makes an invalid mapper result explicit;
it does not change the native MQTT client's exception contract.

## Approved execution plan (completed)

After approval, adopt the test/helper unchanged and run all four cases on .NET 9
and .NET 10. Observe actual failure locations before correcting production. Add
only the minimum mapping-result validation needed for the contract; preserve the
original writer exception and keep validation before native submission. If cases
already pass, use the reviewed mutations to establish behavioral RED.

Run these production-only mutations independently on .NET 10 after green:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Ignore the configured writer and use the default writer | Non-null error assertion in all cases | 4 |
| Suppress configured writer exceptions and null-result errors, returning normally without publishing | Non-null error assertion in all cases | 4 |
| Publish the source with default encoding before invoking the selected writer | First native payload equality in all cases | 4 |
| Omit null-result validation so the native result is dereferenced | Exact InvalidOperationException type assertion in null cases | 2 |

Fourteen intended assertion failures are expected; unaffected cases must pass.
The partial-publication mutation must still report the original mapping failure,
and the observation must fail at native payload equality. Unrelated setup/timeout
or compilation failures do not count. Restore implemented production bytes exactly
after each mutation and require four passes. Keep assertions and bounds unchanged.

Finish with focused passes on both frameworks, all three MQTT library targets
built, and both complete suites retaining all 99 preceding identities/outcomes plus
four new cases. Preserve four existing skips. Run full suites once on fresh,
readiness-checked Docker brokers and retain any failures without retrying until green.

Use the repository Mosquitto image with recorded digest/platform, unique prefixes
and client IDs, and no host port bindings. The operation token is 45 seconds and
native receive is bounded to ten seconds. External process/hang limits bound sync
calls and disposal. Save logs and remove only owned containers. Existing MQTT CI
selection covers the cases; no dependency or workflow change is expected.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
four cases, an evidenced minimum correction, fourteen mutation failures with exact
restorations, native Docker validation, affected regressions/builds, owned cleanup
and local reports. Hosted calls, commits, deployment and publication are excluded.
The approved test/helper now match their compiled copies byte-for-byte. The next
[Redis publication writer draft](../redis-publication-writer/README.md) remains
outside the compiled suite pending its own approval.
