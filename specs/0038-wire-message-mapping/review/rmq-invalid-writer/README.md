# RabbitMQ Async invalid-message writer selection

Status: approved and validated. All focused cases, seven mutations and both full
regression suites pass their required checks.
All four exact reviewed C# files have been copied into the compiled project.
This batch introduces one wire-writer strategy helper; it introduces no request
handler/mapper type, closed logging generic or dependency.

## Contract

Add nullable `RmqSubscription.InvalidMessageWireMessageWriter` with the existing
`IAmAWireMessageWriter<RmqWireMessage>` contract. Both public factory modes retain
the subscription's selection for the consumer's lifetime. Null retains the
existing default mapping. Configure the setting before creating consumers.
The setting is independent of `RequeueWireMessageWriter` and publication writers;
a reader does not imply an inverse writer.

The selected writer runs after Brighter adds rejection metadata and sets the
invalid destination topic. Full replacement controls body and application
properties; default composition retains existing fields. The gateway retains
routing, persistence, delivery tag, immediate-send delay cleanup, mandatory
publication and publisher-confirmation ownership. A failed mapping or an
unroutable mandatory publication propagates an exception, restores the source
message's application topic, and leaves the original unacknowledged. A retry can
then succeed after the mapping or binding recovers. The source is acknowledged
only after confirmed forwarding. A later acknowledgement failure can still lead
to a duplicate; this batch does not promise atomic forwarding.

RabbitMQ's broker-managed dead-lettering remains native. Delivery errors, and
unacceptable messages without an invalid route, use `BasicReject` and the queue's
DLX configuration. They retain the original wire content, including the broker's
original handled-count header. This is distinct from the pump's in-memory count.
A configured invalid writer must not intercept that path. There is no proposed
`DeadLetterWireMessageWriter`: the broker performs that transfer without calling
application code. Scheduler-backed retries remain a separate completion gate.

Public consumer constructor signatures and default formats remain unchanged.
The existing internal constructor can carry the additional writer from the
factory into the invalid-message publisher.

## Reviewable cases

There are 24 cases, to run on both supported test frameworks:

| Draft | Cases | Behavior |
| --- | ---: | --- |
| [Forwarding and recovery](When_forwarding_with_a_selected_rmq_invalid_writer_should_preserve_mapping_and_recovery.cs) | 16 | Sync/async × replacement/composition × success/throw/null/unroutable destination |
| [Consumer isolation](When_rmq_subscriptions_select_invalid_writers_should_keep_each_consumers_mapping_isolated.cs) | 4 | Sync/async × custom-first/default-first; both consumers are created by one factory before rejection |
| [Native dead-letter compatibility](When_rejection_uses_native_dead_lettering_should_preserve_original_wire_content.cs) | 4 | Sync/async × delivery error with an invalid route/unacceptable without an invalid route |

[RejectionProfileRmqWireMessageWriter](RejectionProfileRmqWireMessageWriter.cs)
encodes independently observable metadata in a JSON body or a composition header,
uses Base64 for exact binary payload proof, and deliberately supplies conflicting
persistence/delivery/delay controls. It can throw or return null. The main test
also configures a throwing retry writer to catch accidental coupling to requeue.
Fixtures are published through the native RabbitMQ SDK and outgoing properties,
headers and body are read through native `BasicGet`, not production mappers.
The source count starts at two and the test models pump bookkeeping to reach
three. No service-activator pump/handler coverage is claimed.

Recovery disposes the consumer, verifies the original native delivery and its
redelivery flag, confirms no premature destination copy, releases the native
receipt, then recreates the consumer and rejects successfully. Route recovery
restores the binding. Isolation is sequential; it does not claim concurrent
operations on one consumer or safety for mutable application converters.

## Named mutations

Missing-property compilation is expected initially and is not behavioral RED.
After implementing the reviewed setting, run these independently on .NET 9:

| Mutation | Cases | Intended assertion failure |
| --- | ---: | --- |
| Pass null instead of the selected writer to the invalid-message publisher | 4 success cases | Missing native rejection-profile header |
| Move the original acknowledgement before invalid forwarding | 8 throwing/null cases | Missing original native delivery during recovery |
| Omit the `finally` restoration of the source application topic | 4 success cases | Source topic differs after rejection |
| Publish invalid messages with `mandatory: false` | 4 unroutable cases | Missing forwarding exception |
| Cache the first invalid-writer selection, including null, in the factory and reuse it in both factory modes | 4 isolation cases | Per-consumer native profile presence differs |
| Omit native `BasicReject` for broker-managed dead-lettering | 4 native compatibility cases | No native dead-letter delivery |
| Replace publisher persistence enforcement with transient delivery | 4 success cases | Native delivery mode differs |

Seven mutations must produce 32 failures at the intended assertions; compile,
setup and unrelated errors do not count. The factory mutation uses instance
fields with a separate captured flag; each case creates a fresh factory. Restore
all temporary source changes byte-for-byte from pre-mutation snapshots.

## Approved implementation and validation

Copy these exact drafts into the test project, implement the reviewed setting,
validate the native contract and seven mutations, and confirm restored focused
GREEN on both frameworks. Build the gateway on all supported targets. Run the
complete RabbitMQ Async Release suites on .NET 9/RabbitMQ 4.2 and
.NET 10/RabbitMQ 4.3, including the existing TLS/mTLS/native-delay configuration.
Compare the prior 512 passing and two skipped cases with the 24 additions:
536 passing and two existing skips expected per suite.

Keep source snapshots and result evidence, update the local documentation and
active completion gates, and report any unvalidated behavior separately. This
batch does not complete #4170: native AWS scheduling, scheduler-backed retries,
remaining gateways, real pump telemetry and the final public-configuration review
remain open. No commit, push or review-gear change is included.

## Results

The setting is implemented in three production files: `RmqSubscription`,
`RmqMessageConsumerFactory` and `RmqMessageConsumer`. Both factory paths retain
the selected writer and pass it to the existing invalid-message publisher. Public
constructors, the publisher and default formats remain unchanged.

The initial build failed only for the missing property (CS0117); this is not
behavioral RED. After wiring the property, all 24 cases pass on both frameworks.
All seven mutations compile and fail at their intended assertions without test or
harness corrections:

| Mutation | Failures | Assertion line |
| --- | ---: | ---: |
| Ignore selected invalid writer | 4 | 209, native profile presence |
| Acknowledge before forwarding | 8 | 164, original native delivery presence |
| Omit topic restoration | 4 | 201, source topic equality |
| Disable mandatory publication | 4 | 153, forwarding exception presence |
| Cache the first factory selection | 4 | 128, per-consumer profile presence |
| Omit native rejection | 4 | 140, native dead-letter delivery presence |
| Disable persistence enforcement | 4 | 213, native persistence equality |

There are 32 intended assertion failures. Production bytes are restored exactly
before rebuilding. Both restored focused runs pass all 24 cases. The gateway
builds on netstandard2.0, net8.0, net9.0 and net10.0. Both test targets build.

| Regression | Result |
| --- | --- |
| Full RabbitMQ Async Release, net9.0 | 536 passed, two existing skips, zero failures; 11 m 23 s |
| Full RabbitMQ Async Release, net10.0 | 536 passed, two existing skips, zero failures; 11 m 11 s |

The full suites use isolated RabbitMQ 4.2/.NET 9 and RabbitMQ 4.3/.NET 10 primary
brokers with TLS/mTLS, plus separate native-delay plugin brokers. The workspace
is mounted read-only into SDK containers. Every test execution uses idle-sleep
inhibition. The missing temporary primary-broker configuration was recreated
before startup; no repository test connection was changed.

`/private/tmp/brighter-4170-rmq-invalid-writer/` contains initial/restored/full TRX
and logs, build logs, mutation evidence, source snapshots and the verifier.
`verified-results.json` confirms exact retention of the prior 512 passing and two
skipped cases plus 24 additions per suite, unchanged public constructors, exact
approved test/helper copies and only the three intended production changes.
Existing build warnings remain; the reviewed test deliberately reads the obsolete
legacy `MessageType` property to assert wire compatibility. No test assertion was
changed to suppress that diagnostic.

These results cover sequential consumer selection and the stated recovery paths.
They do not establish atomic publication/acknowledgement, converter concurrency,
scheduler-backed retry mapping or remote CI. Core and AWS code did not change and
those suites were not repeated for this batch.

The subsequent [eight real-pump telemetry cases](../aws-pump-telemetry/README.md) are now validated. Full #4170 completion remains open.
No commit or push was made; the saved stash is preserved.

The result verifier initially expected two skips in the TRX aggregate
`notExecuted` counter. VSTest reports zero there while the two individual results
are `NotExecuted`. The verifier now counts those result entries and compares their
names/outcomes with the preceding suite. No TRX or test was changed.
