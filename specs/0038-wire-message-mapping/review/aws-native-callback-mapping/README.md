# Actual AWS scheduler callback to custom business wire mapping

Status: implemented and validated locally. The four test/helper files are in the
gateway projects, both emulator pins and matching health checks are updated, and
focused checks, eight mutations and six full AWS/Core suites pass. This follows
the distinct-destination scheduler fix.

The preceding batch now passes all four complete scheduler suites (70 cases each)
and gateway suites (607 cases each) on Floci 2.3.0. It also corrected native
timestamp precision using the existing full-header regressions. These results
establish the starting checkpoint, not execution of the callback drafts below.

## Contract

A real native Scheduler schedule sends a callback to its configured SQS queue,
which differs from the business endpoint. The test receives that actual callback,
uses the public default reader and `AwsSchedulerFiredMapper`, then dispatches it
through a real CommandProcessor, AwsSchedulerFiredHandler,
FireSchedulerMessageHandler and OutboxProducerMediator. The selected publication
writer must run on this later business dispatch, not when the schedule is stored.

There are four cases per SDK, eight total: sync/async scheduling × SQS/SNS business
destination. The native callback queue is SQS in every case. Both frameworks run
all cases. The SNS business subscription uses native raw delivery to an independent
SQS observation queue. It does not use the custom reader to verify writer output.

- [V3 test](V3/When_an_aws_schedule_fires_should_map_the_business_message_at_callback_dispatch.cs)
- [V4 test](V4/When_an_aws_schedule_fires_should_map_the_business_message_at_callback_dispatch.cs)
- [V3 SNS writer](V3/NativeCallbackSnsWireMessageWriter.cs)
- [V4 SNS writer](V4/NativeCallbackSnsWireMessageWriter.cs)

Each test provisions a unique scheduler group, role, callback queue and business
queue, plus an SNS topic/subscription when needed. The role has an inline policy
allowing delivery only to its callback queue. A real schedule fires in the local
emulator; no test constructs the callback command or calls a timer to emulate
Scheduler execution. Polling uses the valid 20-second SQS maximum under a bounded
three-minute deadline. Cleanup deletes schedules, groups, policies, roles, topics
and queues.

Before callback dispatch the original message still has deposit telemetry, no
producer span exists and the business queue is empty. The callback must retain
its ID, original business topic and sync/async choice. Dispatch under a different
activity and baggage must invoke the custom writer: native business output has
one `legacy-id` attribute and a JSON body containing original payload and the
actual producer's traceparent, `vendor=dispatch` state and `stage=dispatch`
baggage. The outbox is empty after successful dispatch and the original scheduling
object remains unchanged. The test deletes both received native deliveries.

The existing approved SQS JSON-body writer is reused. The new SNS writer has its
own file and no state. Built-in scheduler request/handler types are explicitly
registered; no assembly scanning, generated conformance pump or static counters
are used. The AWS projects have no type-dispatching logging Initializer: standard
LoggerFactory supports the closed generic handler types.

These tests exercise actual scheduling and callback handler dispatch. They do not
run a service-activator pump, prove automatic callback acknowledgement, or test
writer-failure redelivery. SQS pump integration has separate completed coverage;
SNS/RabbitMQ pump telemetry and callback failure recovery remain open requirements.

## Validation environment included in the review

Floci 1.5.19, currently pinned in local Docker Compose and `aws-mock-ci`, cannot
invoke the universal Scheduler targets used by Brighter. Its stored-schedule API
works, but its dispatcher misroutes those target ARNs. The preceding investigation
preserves exact-version source and native failure logs. Floci 2.3.0 recognizes
universal SQS/SNS targets. The [two-line pin update](emulator-pins.patch) is proposed
so the new native callback cases can run in the normal local and emulator CI
suites. No live service, dependency package, workflow filter or deployment changes
are proposed. Do not skip these cases to make an unsupported emulator pass.

Run only against an explicitly configured local emulator. Real AWS IAM delivery
and remote CI remain unverified. Validate both complete AWS suites on both
frameworks against the proposed 2.3.0 pin before retaining that update.

## Work requested after approval

Copy the four exact C# drafts to the AWS gateway test projects (tests into
MessagingGateway and helpers into TestDoubles). Apply the two reviewed emulator
pins. Run all eight cases on both frameworks and fix only reproduced gaps in the
reviewed callback-to-producer contract.

Initial GREEN is possible. These independent production mutations establish RED
on .NET 9 in each SDK, with test/converter bytes unchanged:

| Mutation | Cases per SDK | Intended failure |
| --- | ---: | --- |
| Ignore the publication's custom SQS writer | Two SQS cases | Native output no longer has the single legacy ID attribute |
| Ignore the publication's custom SNS writer | Two SNS cases | Native output no longer has the single legacy ID attribute |
| Move SQS producer context propagation after serialization/send | Two SQS cases | Native body traceparent differs from the actual producer span |
| Move SNS producer context propagation after serialization/send | Two SNS cases | Native body traceparent differs from the actual producer span |

Eight mutations should produce sixteen intended assertion failures. Setup,
compilation and delivery timeouts do not count. Restore exact source bytes and
verify focused GREEN on all four SDK/framework combinations.

Build affected gateways/schedulers on all supported targets. Run all four full
AWS gateway suites on Floci 2.3.0: 611 cases expected per suite, retaining the
preceding 607 outcomes and adding four. Run both Core suites; if production
scheduler or handler changes are needed, also repeat all four full scheduler
suites. Report every failure, existing skip and environment limitation. Preserve
failure evidence and return semantic test corrections for review. All work stays
local, uncommitted and unpushed; full #4170 completion remains open.

## Validation record

Initial runs pass all four cases per SDK/framework. No production correction is
needed for this callback contract. Both copied tests required the named
`cancellationToken` argument in `CommandProcessor.SendAsync`: its third positional
parameter is a boolean. The initial V3 compilation error is retained, and no test
assertions or behavior were changed.

All eight named mutations produce the intended sixteen failures. Ignoring either
writer fails the native attribute assertion at line 275; moving propagation after
sending fails the native traceparent assertion at line 279. Each run also passes
the two unaffected cases. All 1,164 production source files match their original
hashes after restoration, and all four restored focused runs pass again.

Both gateways and schedulers build on netstandard2.0, net8.0, net9.0 and net10.0.
Both test projects build on their supported frameworks. The complete Core suites
pass 1,716 tests each with seven existing skips. All four complete gateway suites
pass 611 cases each, with zero failures/skips; every prior 607 gateway name/outcome
is preserved and exactly four callback cases are added per suite. The Core suites
also retain their prior names/outcomes. Test compilation uses the supported legacy command-processor policy
keys and can emit their CS0618 deprecation warnings; no production API changed.

The image update needed a matching health-check correction. Floci 2.3.0 has no
`curl`, so the old command fails with exit 127. Compose and `aws-mock-ci` now use
the image-provided `/usr/local/bin/healthcheck.sh`, preserving their existing
intervals/timeouts. That command succeeds inside the running image, and
`docker compose -f docker-compose-aws.yaml config --quiet` passes. The exact-version
[upstream health script](https://github.com/floci-io/floci/blob/2.3.0/docker/healthcheck.sh)
documents why it uses Bash. The originally reviewed two-line patch remains as
the proposal; final configuration changes include these two necessary health
command replacements. No workflow filters or time budgets change.

Evidence is in `/private/tmp/brighter-4170-native-callback/`: initial and restored
TRX files, eight mutation TRX files and assertion records, build logs, production
snapshots, compiler and health-check corrections, and full regression results.
The next [native SNS source-preservation tests](../aws-native-source-preservation/README.md)
are outside compiled projects, unapproved and unrun.
