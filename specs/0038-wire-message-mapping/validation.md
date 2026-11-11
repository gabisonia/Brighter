# Wire message mapping validation

## Current evidence index — 2026-11-08

The [release checkpoint](release-review.md#current-checkpoint--2026-11-08) and
[completion plan](completion-plan.md) identify the latest scoped results and remaining
gates. Entries below retain their historical source/run context; older pending
statements do not supersede newer package evidence. Full-issue sign-off remains open.

## First behavior

`When_sending_with_a_custom_wire_writer_should_use_its_native_attributes` configures an AWS V4 SQS publication with a custom writer, creates the producer through `SqsMessageProducerFactory`, and sends through sync and async public APIs. The AWS SDK reads the resulting queue message directly.

The expected message contains the original body and one `tenant-id` string attribute promoted from the Brighter bag. It contains no default `bag` or `cloudeventheaders` attributes, demonstrating full replacement. The original bag retains its value.

The initial draft references the proposed writer contract, SQS carrier, and publication property. After implementation, temporarily ignore `publication.WireMessageWriter` when selecting the writer. The test must fail on native attribute assertions. Restore the implementation and verify GREEN. This named mutation is never retained or committed.

## Required checks for every gateway

| Behavior | Evidence |
| --- | --- |
| Default compatibility | Inspect native headers/properties and body using the broker SDK or database driver; preserve old parsing fixtures and fallbacks |
| Custom outbound mapping | Configure a publication through the production factory, send, and inspect native data independently |
| Custom inbound mapping | Publish a legacy native message independently, receive through the configured public consumer, and assert Brighter fields |
| Default composition | Reuse a default mapper, replace one field, and verify other fields retain their default representation |
| Trace placement | Inspect traceparent, tracestate and baggage at the custom location and verify receive-side context extraction |
| Dispatch timing | Deposit first, clear later under a distinguishable dispatch span, and inspect the actual send-time context |
| Body mapping | Full custom body round trip plus unchanged binary/compressed bodies under default mapping |
| Endpoint isolation | Two publications/subscriptions select different mappings without global state leakage |
| Concurrency and retries | Repeated/concurrent sends produce independent native objects and do not accumulate headers or mutate source data |
| Outbound failure | Mapping failure prevents dispatch confirmation and leaves outbox work eligible for retry |
| Inbound failure | Malformed content follows the invalid-message path with acknowledgement identity preserved |
| Delivery controls | Ack/nack, retry counts, DLQ/invalid routing, scheduled sends and confirmations still work |
| API compatibility | Existing constructors and static conversion methods compile; absent configuration uses the default |

Use both sync and async paths where supported. Include batch, streaming, transaction, and republish paths where the gateway has them. A custom writer/reader round trip alone is insufficient because matching defects can cancel each other out.

## Integration environments and suites

Use the repository's Docker setup and test fixtures. Start only required services, verify health and versions, and use unique resource names with cleanup. Pin AWS test credentials and `AWS_SERVICE_URL` to the local emulator before execution so tests cannot fall through to a real account.

| Gateway | Environment | Test projects under `tests/` |
| --- | --- | --- |
| AWS v3/v4 | Floci 1.5.19, as in `docker-compose-aws.yaml` | `Paramore.Brighter.AWS.Tests`, `Paramore.Brighter.AWS.V4.Tests` |
| RabbitMQ | `docker-compose-rmq.yaml`; native-delay setup when required | `Paramore.Brighter.RMQ.Sync.Tests`, `Paramore.Brighter.RMQ.Async.Tests` |
| Kafka | `docker-compose-kafka.yaml` | `Paramore.Brighter.Kafka.Tests` |
| Azure Service Bus | Gateway suite's configured Service Bus integration environment | `Paramore.Brighter.AzureServiceBus.Tests`, relevant `Paramore.Brighter.Azure.Tests` coverage |
| GCP | `docker-compose-gcp.yaml` | `Paramore.Brighter.Gcp.Tests` |
| MQTT | `docker-compose-mqtt.yaml` | `Paramore.Brighter.MQTT.Tests` |
| Redis | `docker-compose-redis.yaml` | `Paramore.Brighter.Redis.Tests` |
| RocketMQ | `docker-compose-rocketmq.yaml` | `Paramore.Brighter.RocketMQ.Tests` |
| SQL Server | `docker-compose-mssql.yaml` | `Paramore.Brighter.MSSQL.Tests` |
| PostgreSQL | `docker-compose-postgres.yaml` | `Paramore.Brighter.PostgresSQL.Tests` |

RabbitMQ Sync requires broker 4.2; Async supports 4.2 and 4.3+. Tests using generated conformance pumps must join the corresponding configuration collection.

## Execution sequence

After the required test review, run the targeted test to establish RED, implement, establish GREEN, and apply the named behavioral mutation where initial RED was a missing API. Then run the full affected project suite. Repeat a suite only for changed code, failures, or known concurrency concerns.

Example first targeted command:

```sh
dotnet test tests/Paramore.Brighter.AWS.V4.Tests/Paramore.Brighter.AWS.V4.Tests.csproj -f net9.0 --filter FullyQualifiedName~SqsCustomWireWriterTests
```

The full affected suite must also run on `net10.0`, as declared in `tests/Directory.Build.props`. Include Core and relevant extension/service-activation coverage when shared contracts, factory registration, or outbox interactions change. Finish with `dotnet build Brighter.slnx` and the cross-transport regression matrix.

## Results

The first test was approved before execution and implementation. The following checks cover the first writer integration only.

| Check | Result |
| --- | --- |
| Initial targeted run on net9.0 | Failed compilation because the proposed interface and carrier did not exist |
| Targeted run after implementation on net9.0 | 2 passed |
| Named mutation on net9.0 | 2 failed on the native attribute assertion: seven default attributes instead of the single custom attribute |
| Mutation reverted, targeted run on net9.0 and net10.0 | 2 passed on each framework |
| Gateway build across all target frameworks | Succeeded, 0 warnings and 0 errors |
| Core full suite on net9.0 | 1,716 passed, 7 skipped, 0 failed |
| Core full suite on net10.0 | 1,716 passed, 7 skipped, 0 failed |
| S3 environment verification on net9.0 | 15 passed with the corrected emulator hostname |
| Initial AWS V4 full suite on net9.0 and net10.0 | 412 passed, 6 S3 hostname failures on each framework |
| Corrected AWS V4 full suite on net9.0 | 418 passed, 0 skipped, 0 failed |
| Corrected AWS V4 full suite on net10.0 | 418 passed, 0 skipped, 0 failed |

The local emulator is `floci/floci:1.5.19`, container `brighter-4170-aws`, exposed only on host loopback port 14566. The initial full run used `AWS_SERVICE_URL=http://127.0.0.1:14566`; S3 bucket subdomains constructed from that IP failed DNS resolution. The corrected run uses `AWS_SERVICE_URL=http://localhost.localstack.cloud:14566`, whose wildcard subdomains resolve to loopback. No production code was changed to address the environment failure.

The corrected full run completed successfully in 7m14s on net9.0 and 7m37s on net10.0. Both runs included the approved sync and async custom-writer tests. No test exclusions were used for the full regression runs.

Execution logs are in `/private/tmp/brighter-4170-{red,green,mutation,restored,build,core,full,full-localhost,s3}.log`. Test result files use the `4170-` prefix under each project's `TestResults/` directory. The restored targeted runs and corrected full rerun use `--no-build --no-restore`; the full build/test commands compile the production integration before those runs.

## Default writer composition

The composition test was approved and moved from the review directory into the AWS V4 test project. The initial run failed compilation because `DefaultSqsWireMessageWriter` was absent. Extracting the existing mapping unchanged then produced two behavioral failures: the original bag contained an additional `handled-count` entry after sending, violating the source-immutability assertion. The fix copies the filtered bag before stamping that value.

The wire representation retains all previous fields and formatting, including handled count and local-header filtering. Both the two-argument sender constructor and omitted publication configuration now use the reusable default writer.

| Check | Result |
| --- | --- |
| Composition test against extracted original behavior, net9.0 | 2 failed on source-bag mutation |
| Full replacement and default composition, net9.0 | 4 passed |
| Full replacement and default composition, net10.0 | 4 passed |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |
| AWS V4 full regression, net9.0 | 420 passed, 0 skipped, 0 failed; 7m30s |
| AWS V4 full regression, net10.0 | 420 passed, 0 skipped, 0 failed; 7m19s |

Logs: `/private/tmp/brighter-4170-default-{red,behavior-red,green,full,netstandard-build,net8-build}.log`. The full regression command builds the final sender integration before running both test frameworks. Core implementation did not change in this increment; its previous full-suite results remain applicable.

The global feature is not ready for release until the remaining gateway, reader, error-path and lifecycle tasks are complete.

## Custom SQS reader

The approved test publishes a native legacy message and receives through the public consumer factory in both sync and async modes. It checks application ID, tenant metadata, body and trace parent; the gateway restores the omitted receipt handle and overrides the reader's handled count of 99 with the first-delivery value of zero. An independent native receive after visibility expires verifies that acknowledgement deleted the message.

Initial RED was the missing `IAmAWireMessageReader<T>` contract. The first executable run exposed a test setup omission: the subscription required an explicit Reactor/Proactor pump type. That setup was corrected without changing the intended behavior. The receipt checks use `TryGetValue`, and the empty-queue assertion runs first, so a missing receipt produces a behavioral assertion failure instead of an incidental dictionary exception.

| Check | Result |
| --- | --- |
| Restored reader test, net9.0 | 2 passed |
| Restored reader test, net10.0 | 2 passed |
| Ignore configured reader mutation, net9.0 | 2 failed: generated ID instead of `legacy-order-17` |
| Omit receipt hydration mutation, net9.0 | 2 failed: native queue still contained the unacknowledged message |
| Omit broker count resolution mutation, net9.0 | 2 failed: handled count 99 instead of zero |
| Full AWS V4 regression, net9.0 | 422 passed, 0 skipped, 0 failed; 7m39s |
| Full AWS V4 regression, net10.0 | 422 passed, 0 skipped, 0 failed; 7m14s |
| Full Core regression, net9.0 | 1,716 passed, 7 skipped, 0 failed; 2m16s |
| Full Core regression, net10.0 | 1,716 passed, 7 skipped, 0 failed; 2m18s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

All three mutations were restored from the implementation snapshot before the final targeted run. The original consumer constructor remains available and delegates to the new overload, preserving its binary signature. No changes were made to default raw/SNS reader behavior in this increment.

Logs: `/private/tmp/brighter-4170-reader-{red,green,ignore-mutation,receipt-mutation,count-mutation,full,core,netstandard-build,net8-build}.log`. Full test runs write TRX files with the `4170-reader-` prefix. Both AWS runs use the same isolated Floci emulator and corrected localhost hostname described above.

The reader-failure test was subsequently approved; its results are recorded below.

## Throwing custom SQS reader

The approved test now lives in the AWS V4 test project. Its initial run failed in both modes at `Assert.Null(receiveException)`: a `FormatException` escaped the consumer. The consumer now catches custom conversion exceptions, logs the failure, and creates an unacceptable-message result before attaching native receipt metadata and resolving the delivery count. Default-parser behavior is unchanged.

The test rejects that result without configured rejection channels and uses an independent SDK receive after visibility expiry to verify deletion. This increment does not yet preserve raw body and ID for forwarding to a configured invalid-message queue.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | 2 failed: reader exception escaped receive |
| Success and failure reader tests after restoration, net9.0 | 4 passed |
| Success and failure reader tests after restoration, net10.0 | 4 passed |
| Remove receipt hydration mutation, net9.0 | 2 failed: receipt key was absent |
| Return ordinary event mutation, net9.0 | 2 failed: MT_EVENT instead of MT_UNACCEPTABLE |
| Full AWS V4 regression, net9.0 | 424 passed, 0 skipped, 0 failed; 8m12s |
| Full AWS V4 regression, net10.0 | 424 passed, 0 skipped, 0 failed; 8m22s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

Both mutations were fully restored before the final targeted tests and full regression. No shared Core implementation changed in this increment, so the previous Core regression results remain applicable.

Logs: `/private/tmp/brighter-4170-reader-failure-{red,green,receipt-mutation,type-mutation,full,netstandard-build,net8-build}.log`. Full AWS runs use `--no-build --no-restore` after the final targeted command compiled both target frameworks, and write TRX files with the `4170-reader-failure-full` prefix. The isolated emulator and hostname remain unchanged.

The invalid-message-forwarding test was subsequently approved; its results follow.

## Failed native payload forwarding

The approved test initially failed in both modes because the failure result had an empty body. The consumer now preserves the native body and uses the native SQS message ID when application mapping throws. The test checks the result and independent SDK reads of the invalid queue, then verifies source deletion after visibility expiry.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | 2 failed: empty body instead of original payload |
| All custom-reader cases, net9.0 and net10.0 | 6 passed per framework |
| Discard original body mutation, net9.0 | 2 failed on body equality |
| Omit native fallback ID mutation, net9.0 | 2 failed on ID equality |
| Full AWS V4 regression, net9.0 | 426 passed, 0 skipped, 0 failed; 7m13s |
| Full AWS V4 regression, net10.0 | 426 passed, 0 skipped, 0 failed; 7m14s |
| netstandard2.0 and net8.0 gateway builds | Succeeded, 0 warnings, 0 errors |

Mutations were restored before the final targeted build and regression run. Core was unchanged; its previous full results remain applicable. Logs use `/private/tmp/brighter-4170-reader-routing-{red,green,body-mutation,id-mutation,full,netstandard-build,net8-build}.log`. TRX files use the `4170-reader-routing-full` prefix. The isolated AWS emulator was stopped after the full suite passed.

## RabbitMQ Async custom writer

The user approved the second-transport test during AWS regression. Initial RED was the missing `RmqWireMessage` carrier. The implementation adds a typed publication property, passes it through the producer, and selects the native body/properties inside the publisher. Default mapping remains internal. The gateway applies delivery mode and delivery headers; the producer's original pending-confirmation record retains Brighter identity.

The first executable test exposed an incorrect header-count assumption: RabbitMQ.Client adds `Constants.PublishSequenceNumberHeader` for confirmation tracking. The test now requires that control header and exactly one application header, preserving the approved distinction between custom mapping and gateway delivery bookkeeping. The SDK header is not removed from production messages.

| Check | Result |
| --- | --- |
| Restored custom-writer test, net9.0 and net10.0 | 2 passed per framework |
| Ignore configured writer mutation, net9.0 | 2 failed: native ID was `order-17` rather than `legacy-order-17` |
| Omit persistence enforcement mutation, net9.0 | 2 failed: Transient instead of Persistent |
| Full RabbitMQ Async regression, net9.0 / RabbitMQ 4.2 | 374 passed, 2 existing skips, 0 failed; 8m43s |
| Full RabbitMQ Async regression, net10.0 / RabbitMQ 4.3 | 374 passed, 2 existing skips, 0 failed; 8m29s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

Host port 5672 belongs to an unrelated Azure emulator. Tests therefore use SDK containers sharing the isolated `brighter-4170-rmq` broker's network namespace. This preserves existing localhost endpoints without changing test connections or stopping other services. The .NET 9 primary broker runs `rabbitmq:4.2-management`. The .NET 10 run uses a second isolated primary, `brighter-4170-rmq43`, using `rabbitmq:4.3-management`. Both enable TLS on 5671 with certificates generated by the repository script. The native-delay broker runs the repository's pinned RabbitMQ 4.2.6 image and plugin, with Khepri disabled. `RMQ_NATIVE_DELAY_URI` points at `brighter-4170-native` on the private `brighter-4170-validation` network. All validation brokers have no host port mappings. Test assemblies are built locally and executed by actual .NET 9 and .NET 10 SDK containers with the workspace mounted read-only. Full suites include TLS and native-delay tests, with no filters.

Logs use `/private/tmp/brighter-4170-rmq-{red,build,green-net9,green-net10,ignore-mutation,persistence-mutation,full-net9,full-net10,netstandard-build,net8-build}.log`. TRX output is under `/private/tmp/brighter-4170-rmq-results/`. Temporary test certificates live in ignored `tests/certs/`. Full suites ran sequentially to avoid collisions in shared broker fixtures. Both TRX files confirm all 16 mutual-TLS cases and all 103 native-delay cases passed (19 direct, 42 classic conformance and 42 quorum conformance), as did both new writer cases. The two existing skips are the async dispatcher-builder test and the Reactor dead-letter-queue rejection test.

The full-run matrix covers .NET 9 with RabbitMQ 4.2 and .NET 10 with RabbitMQ 4.3, not every framework/broker combination. Native delay uses the pinned 4.2.6 broker in both runs. Production files match their restored mutation snapshots, and `git diff --check` passes. Global release readiness still requires the remaining tasks, including reusable RabbitMQ defaults, inbound mapping, reserved-header collision handling and other transports.

## RabbitMQ default writer composition

The approved composition test was moved into the RabbitMQ Async test project. Initial RED was the missing `DefaultRmqWireMessageWriter` API. The existing default header and property mapping was extracted into that public, sealed writer; ordinary publishing and requeue serialization now use it. Requeue-generated IDs, delivery headers, original-message identity and retry routing remain in the publisher. Custom consumer republish mapping remains pending.

The composition writer renames `tenant_id` to `tenant-id` in the returned native headers. Independent SDK observation confirms native ID, content type, body and CloudEvents ID/specversion remain unchanged, while confirmations retain original Brighter identity. Source bag and body remain unchanged. The named mutation removes the default CloudEvents ID; both test cases fail at the explicit `TryGetValue` assertion. The mutation was restored before rebuilding both frameworks.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | Compilation fails: default writer type missing |
| Composition and replacement tests, net9.0 | 4 passed after restoration |
| Composition and replacement tests, net10.0 | 4 passed after restoration |
| Omit default CloudEvents ID mutation, net9.0 | 2 failed on header-presence assertion |
| Full RabbitMQ Async regression, net9.0 / RabbitMQ 4.2 | 376 passed, 2 existing skips, 0 failed; 8m35s |
| Full RabbitMQ Async regression, net10.0 / RabbitMQ 4.3 | 376 passed, 2 existing skips, 0 failed; 8m31s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

Full suites use the same TLS-enabled primary brokers and SDK-container approach as the preceding increment. They can now run concurrently because the .NET 10 run uses a separate native-delay broker, `brighter-4170-native10`, built from the same pinned repository image with Khepri disabled. The .NET 9 run retains `brighter-4170-native`. Each framework has independent primary and native-delay fixtures. No full-suite filters are used.

Logs use `/private/tmp/brighter-4170-rmq-default-{red,build,mutation-build,mutation,restored-build,green-net9,green-net10,restored-net9,restored-net10,full-net9,full-net10,netstandard-build,net8-build}.log`. TRX files are `/private/tmp/brighter-4170-rmq-results/4170-rmq-default-net9.trx` and `4170-rmq-default-net10.trx`. Shared Core and AWS implementation are unchanged in this increment, so their preceding full-suite results remain applicable.

Both full-run TRX files confirm all 16 mutual-TLS cases, all 103 native-delay cases (19 direct, 42 classic and 42 quorum conformance), and all four writer cases passed. The two existing skips remain the async dispatcher-builder and Reactor dead-letter-queue rejection tests. The restored default writer matches its pre-mutation snapshot, and `git diff --check` passes.

## RabbitMQ immediate-send delay ownership

The approved test and double were moved into the compiled test project. Both sync and async cases initially failed at the native `x-delay` absence assertion: the custom writer's 60,000-millisecond header reached the broker despite an immediate send. `AddDeliveryHeaders` now removes the writer-supplied delay before applying any positive gateway delay. The mutation omitting that removal reproduces the same two assertion failures; the publisher was restored byte-for-byte before rebuilding both frameworks.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | 2 failed on native delay-header absence |
| All writer cases after restoration, net9.0 | 6 passed |
| All writer cases after restoration, net10.0 | 6 passed |
| Omit delay-header removal mutation, net9.0 | 2 failed on native delay-header absence |
| Full RabbitMQ Async regression, net9.0 / RabbitMQ 4.2 | 378 passed, 2 existing skips, 0 failed; 8m46s |
| Full RabbitMQ Async regression, net10.0 / RabbitMQ 4.3 | 378 passed, 2 existing skips, 0 failed; 8m41s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

The test uses a direct exchange and proves the native header contract. Custom-writer timed delivery through the delayed-message plugin is still outstanding. The unfiltered full suites include the existing delayed-delivery coverage. They run concurrently using the separate primary/native broker fixtures established in the preceding increment.

Logs use `/private/tmp/brighter-4170-rmq-delay-{red-build,red,green-build,green-net9,green-net10,mutation-build,mutation,restored-build,restored-net9,restored-net10,full-net9,full-net10,netstandard-build,net8-build}.log`. TRX files are `/private/tmp/brighter-4170-rmq-results/4170-rmq-delay-net9.trx` and `4170-rmq-delay-net10.trx`. Core and AWS implementation are unchanged, so their preceding full-suite results remain applicable.

Both TRX files confirm all 16 mutual-TLS cases, all 103 native-delay cases (19 direct, 42 classic and 42 quorum), and all six wire-writer cases passed. The two skips remain the existing dispatcher-builder and Reactor dead-letter-queue rejection tests. The restored publisher matches its pre-mutation snapshot; `git diff --check` passes.

## RabbitMQ delivery-tag ownership

The approved test covers sync and async sends with and without a source delivery tag. Initially the two no-source-tag cases failed because the writer's fabricated `DeliveryTag` of `999` reached the broker; the two source-tag cases already passed. The publisher now removes any writer-supplied delivery tag before applying the actual source tag. The public publication XML documents that ownership.

Both named mutations were applied independently. Omitting the removal fails the two native header-absence assertions. Omitting the actual tag assignment, with removal retained, fails the two native header-presence assertions. All four cases were green between mutations, and the final publisher was restored byte-for-byte before rebuilding both frameworks.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | 2 failed on tag absence; 2 source-tag cases passed |
| All writer cases after restoration, net9.0 | 10 passed |
| All writer cases after restoration, net10.0 | 10 passed |
| Omit fabricated-tag removal mutation, net9.0 | 2 failed on header absence; 2 passed |
| Omit actual-tag assignment mutation, net9.0 | 2 failed on header presence; 2 passed |
| Full RabbitMQ Async regression, net9.0 / RabbitMQ 4.2 | 382 passed, 2 existing skips, 0 failed; 10m07s |
| Full RabbitMQ Async regression, net10.0 / RabbitMQ 4.3 | 382 passed, 2 existing skips, 0 failed; 10m04s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

The test proves native publishing metadata ownership, not consumer acknowledgement or custom republishing. Full suites use independent primary/native-delay broker fixtures without filters. Logs use `/private/tmp/brighter-4170-rmq-tag-{red-build,red,green-build,green-net9,green-net10,removal-mutation-build,removal-mutation,first-restored-build,first-restored,overlay-mutation-build,overlay-mutation,restored-build,restored-net9,restored-net10,full-net9,full-net10,netstandard-build,net8-build}.log`. TRX files are `/private/tmp/brighter-4170-rmq-results/4170-rmq-tag-net9.trx` and `4170-rmq-tag-net10.trx`. Core and AWS implementation are unchanged; their preceding full-suite results remain applicable.

Both TRX files confirm all 16 mutual-TLS cases, all 103 native-delay cases (19 direct, 42 classic and 42 quorum), and all ten writer cases passed. The two skips remain the existing dispatcher-builder and Reactor dead-letter-queue rejection tests. The restored publisher matches its pre-mutation snapshot; `git diff --check` passes.

## RabbitMQ custom writer with native delay

The approved test and double were moved into the compiled test project. Both cases passed on first execution on .NET 9 and .NET 10, so this is characterization of the existing implementation. Independent native polling observes the custom message after the requested three-second delay, with the plugin's consumed header of `-3000`, despite the writer supplying 60,000 milliseconds. Body, native ID, content type, tenant metadata, persistence and original confirmation identity are checked, as are the unchanged source bag/body.

The named mutation removes only the gateway delay assignment while retaining removal of the custom header. Both cases fail the explicit native header-presence assertion. The publisher was restored byte-for-byte and rebuilt before the final targeted and full runs. No new production changes were needed.

| Check | Result |
| --- | --- |
| Initial characterization, net9.0 and net10.0 | 2 passed per framework |
| Omit gateway delay assignment mutation, net9.0 | 2 failed on native delay-header presence |
| All writer cases after restoration, net9.0 | 12 passed |
| All writer cases after restoration, net10.0 | 12 passed |
| Full RabbitMQ Async regression, net9.0 / RabbitMQ 4.2 | 384 passed, 2 existing skips, 0 failed; 8m55s |
| Full RabbitMQ Async regression, net10.0 / RabbitMQ 4.3 | 384 passed, 2 existing skips, 0 failed; 8m25s |

Both timed cases use the pinned native-delay broker (RabbitMQ 4.2.6), even when the primary broker for the rest of the suite is 4.3. The preceding netstandard2.0/net8.0 compatibility builds remain applicable because production code is unchanged. Core and AWS regression results also remain applicable. Full suites run without filters on independent primary/native broker fixtures.

Logs use `/private/tmp/brighter-4170-rmq-timed-{build,green-net9,green-net10,mutation-build,mutation,restored-build,restored-net9,restored-net10,full-net9,full-net10}.log`. TRX files are `/private/tmp/brighter-4170-rmq-results/4170-rmq-timed-net9.trx` and `4170-rmq-timed-net10.trx`.

Both TRX files confirm all 16 mutual-TLS cases, all 105 native-delay cases (19 existing direct, 2 new custom-writer, 42 classic and 42 quorum), and all twelve writer cases passed. The two skips remain the existing dispatcher-builder and Reactor dead-letter-queue rejection tests. The publisher matches its pre-mutation snapshot; `git diff --check` passes.

## Null custom SQS reader results

The approved test and double were moved into the compiled AWS V4 test project. Both modes initially failed at `Assert.Null(receiveException)` because the consumer dereferenced the null result outside its conversion try/catch. Null results now raise an invalid-operation exception within that protected conversion path and become unacceptable messages through the existing fallback. The test verifies receipt/count metadata, original body/native ID, invalid-queue forwarding and actual deletion of the source delivery.

The named mutation removes the null guard. Both cases reproduce the receive-exception assertion failure. The consumer was restored byte-for-byte before the final targeted build and full regression. The public subscription documentation now explicitly covers null results; reader implementations still have a non-null return contract.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | 2 failed: null-reference exception escaped receive |
| Null-result test, net9.0 and net10.0 | 2 passed per framework |
| Omit null-result guard mutation, net9.0 | 2 failed at receive-exception assertion |
| Restored null-result and failure-forwarding tests, net9.0 and net10.0 | 4 passed per framework |
| Full AWS V4 regression, net9.0 | 428 passed, 0 skipped, 0 failed; 11m48s |
| Full AWS V4 regression, net10.0 | 428 passed, 0 skipped, 0 failed; 11m47s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

Full suites run concurrently on the isolated Floci 1.5.19 emulator using the harness's per-process account namespaces and `AWS_SERVICE_URL=http://localhost.localstack.cloud:14566`. Both .NET runtimes are installed on the host. No filters are used for full regression. Core and RabbitMQ implementation are unchanged, so their preceding full-suite results remain applicable.

Logs use `/private/tmp/brighter-4170-reader-null-{red,green,mutation,restored,full-net9,full-net10,netstandard-build,net8-build}.log`. Full results use `4170-reader-null-full-net9.trx` and `4170-reader-null-full-net10.trx` in the AWS V4 test project's ignored `TestResults/` directory.

Both full-run TRX files confirm all eight custom-reader cases passed, including valid, throwing, forwarding and null-result cases. The consumer matches its pre-mutation snapshot and `git diff --check` passes.

## Reusable default SQS reader

The approved raw composition test and double were moved into the compiled AWS V4 project. Initial RED was the missing public reader type. `DefaultSqsWireMessageReader` now delegates to the existing parser factory, creating an independent parser on each read. Ordinary no-custom-reader consumption also uses this wrapper. Existing raw/envelope parsing and fallback semantics remain intact.

The first executable run exposed an incorrect test assumption: the existing raw parser adds UTF-8 charset metadata to JSON content types. The test now explicitly asserts `MediaType == application/json` and `CharSet == utf-8`; production normalization was unchanged. The composition only renames a tenant bag key and verifies default identity, topic, message type, correlation, version, tracing, body, unrelated bag metadata and actual acknowledgement.

The named mutation suppresses the default parsed trace parent. Both cases fail at trace-parent equality with actual null. The reader was restored byte-for-byte before rebuilding both frameworks.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | Compilation fails: default reader type missing |
| Corrected composition test, net9.0 and net10.0 | 2 passed per framework |
| Suppress parsed trace-parent mutation, net9.0 | 2 failed on trace-parent equality |
| All wire-mapping cases after restoration, net9.0 and net10.0 | 14 passed per framework |
| Initial full AWS V4 regression, net9.0 | 429 passed, 1 delay assertion failed; 4h08m elapsed with host sleep |
| Full AWS V4 regression, net10.0 | 430 passed, 0 skipped, 0 failed; 4h42m elapsed with host sleep |
| Delay cases with idle sleep inhibited, net9.0 | 8 passed, 0 failed; 10s |
| Full AWS V4 regression with idle sleep inhibited, net9.0 | 430 passed, 0 skipped, 0 failed; 7m28s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

Full suites run concurrently against isolated Floci account namespaces with the same explicit local endpoint as the preceding increment. Existing raw and SNS-envelope default paths are included with no filters. Core and RabbitMQ implementation are unchanged, so their preceding full-suite results remain applicable. Explicit custom composition of SNS envelopes remains pending.

Logs use `/private/tmp/brighter-4170-reader-default-{red,green,corrected,mutation,restored,full-net9,full-net10,netstandard-build,net8-build}.log`. Full TRX files use `4170-reader-default-full-net9.trx` and `4170-reader-default-full-net10.trx` under the AWS V4 project's ignored `TestResults/` directory.

The first full .NET 9 run failed the existing standard-SQS synchronous requeue test's before-delay assertion. A targeted retry failed the same assertion in four synchronous variants. The macOS power log confirms repeated host sleep, including a six-minute sleep during that retry; emulator timestamps also show the pause. A subsequent invocation omitted the local endpoint settings and failed during client configuration. With the explicit emulator settings restored and `caffeinate -i` scoped to the test process, all eight delay cases and then the complete .NET 9 suite passed without code changes. These diagnostics are retained in `reader-default-delay-{retry,awake,awake-configured}-net9.log`; the full rerun uses `reader-default-full-awake-net9.log`, all with the same `/private/tmp/brighter-4170-` prefix. Its TRX file is `4170-reader-default-full-awake-net9.trx` in the same results directory.

Both initial full-run TRX files and the clean .NET 9 rerun confirm all fourteen wire-mapping cases and all eighty generated SNS cases passed. The restored reader matches its pre-mutation snapshot. No source changes were needed for the rerun. The task's AWS emulator was stopped after validation.

## Composing the default SNS-envelope reader

The approved test was moved into the compiled AWS V4 test project. Both sync and async cases passed on first execution on .NET 9 and .NET 10. An independent native SQS send supplies an SNS-shaped envelope, with distinct envelope/application IDs, an inner JSON payload and message attributes. Composition renames only the tenant bag key. The assertions cover application identity, unrelated fields, tracing, the inner body, receipt/count metadata and actual deletion after acknowledgement.

The named mutation changes only the inline parser's body reader to return the serialized envelope instead of its `Message` field. Both cases fail at body equality after the preceding identity/header assertions pass. The parser was restored byte-for-byte and both frameworks rebuilt before the final targeted and full runs. No production changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial envelope composition, net9.0 and net10.0 | 2 passed per framework |
| Return whole envelope mutation, net9.0 | 2 failed at body equality |
| All wire-mapping cases after restoration, net9.0 and net10.0 | 16 passed per framework |
| Full AWS V4 regression, net9.0 | 432 passed, 0 skipped, 0 failed; 7m11s |
| Full AWS V4 regression, net10.0 | 432 passed, 0 skipped, 0 failed; 7m22s |

Full suites run without filters against Floci 1.5.19 using independent account namespaces and `AWS_SERVICE_URL=http://localhost.localstack.cloud:14566`. Each process uses `caffeinate -i` to inhibit idle sleep for its duration. The preceding netstandard2.0/net8.0 gateway builds and Core/RabbitMQ regression results remain applicable because production code is unchanged.

Logs use `/private/tmp/brighter-4170-reader-envelope-{initial-net9,initial-net10,mutation-net9,restored,full-net9,full-net10}.log`. Full TRX files use `4170-reader-envelope-full-net9.trx` and `4170-reader-envelope-full-net10.trx` under the AWS V4 project's ignored `TestResults/` directory. The shared targeted TRX filename was overwritten by the second framework; the restored-run text log retains both 16-pass summaries and the full runs use distinct filenames.

Malformed-envelope failure preservation is the next review draft. This increment does not establish SNS writer customization or malformed-envelope rejection behavior.

Both full-run TRX files confirm all sixteen wire-mapping cases, including both envelope-composition cases, and all eighty generated SNS cases passed. The parser matches its pre-mutation snapshot and has no remaining diff. `git diff --check` passes. The task emulator was stopped after validation.

## Malformed-envelope failure preservation

The approved test covers four combinations of synchronous/asynchronous receiving and ordinary/explicit default reader configuration. All four initially failed on body equality: the inline parser returned an unacceptable message with an empty body. The default reader now enriches parser failure results with the original native payload, a native fallback ID when the application ID is empty, the receipt handle and the normalized broker delivery count. Successfully parsed messages, including rejection messages carrying receipt metadata, retain their parsed representation.

All four cases pass on both frameworks, including independent native observation of the invalid-message queue and actual deletion of the original delivery. The named mutation removes only native-body restoration. All four cases reproduce the intended body-equality failure. The default reader was restored byte-for-byte before rebuilding and running the full suites.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | 4 failed at body equality; actual empty body |
| Malformed-envelope rejection, net9.0 and net10.0 | 4 passed per framework |
| Omit native-body restoration mutation, net9.0 | 4 failed at body equality |
| All wire-mapping cases after restoration, net9.0 and net10.0 | 20 passed per framework |
| Full AWS V4 regression, net9.0 | 436 passed, 0 skipped, 0 failed; 7m31s |
| Full AWS V4 regression, net10.0 | 436 passed, 0 skipped, 0 failed; 7m11s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

Full suites run without filters against Floci 1.5.19, with independent account namespaces, the explicit local endpoint and per-process idle-sleep inhibition used in the preceding increment. Core and RabbitMQ code is unchanged, so their preceding full-suite results remain applicable.

Logs use `/private/tmp/brighter-4170-reader-malformed-{red,green,mutation,restored,full-net9,full-net10,netstandard2.0-build,net8.0-build}.log`. Full TRX files use `4170-reader-malformed-full-net9.trx` and `4170-reader-malformed-full-net10.trx` under the AWS V4 project's ignored `TestResults/` directory. Targeted dual-framework runs use distinct framework-qualified TRX filenames.

The next review draft characterizes preservation of a successfully parsed rejection envelope, including its inner body and handled count. Raw-parser failures, malformed partial attributes, mixed batches and concurrent reuse still need dedicated coverage.

Both full-run TRX files confirm all twenty wire-mapping cases, including all four malformed-envelope cases, and all eighty generated SNS cases passed. The default reader matches its pre-mutation snapshot and `git diff --check` passes. The task emulator was stopped after validation.

## Preserving successfully parsed rejection envelopes

The approved test supplies an independently constructed valid SNS envelope whose application message is marked unacceptable and carries rejection metadata with handled count seven. Four combinations cover sync/async consumption and ordinary/explicit default reader configuration. All four passed on first execution on both frameworks. They verify the inner body, application identity, tracing, unrelated metadata, preserved rejection count and actual acknowledgement of the native delivery.

The named mutation removes only the default reader's receipt-presence condition. Every unacceptable result then enters failure reconstruction, replacing the inner payload with the serialized envelope. All four cases fail at the first body-equality assertion. The reader was restored byte-for-byte before both frameworks were rebuilt. No production changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial valid-rejection cases, net9.0 and net10.0 | 4 passed per framework |
| Omit parsed-rejection guard mutation, net9.0 | 4 failed at body equality |
| All wire-mapping cases after restoration, net9.0 and net10.0 | 24 passed per framework |
| Full AWS V4 regression, net9.0 | 440 passed, 0 skipped, 0 failed; 7m10s |
| Full AWS V4 regression, net10.0 | 440 passed, 0 skipped, 0 failed; 7m28s |

Full suites run without filters against Floci 1.5.19 with independent account namespaces, the explicit local endpoint and per-process idle-sleep inhibition. The preceding compatibility builds and Core/RabbitMQ regression results remain applicable because production code is unchanged.

Logs use `/private/tmp/brighter-4170-reader-rejection-{initial,mutation,restored,full-net9,full-net10}.log`. Full TRX files use `4170-reader-rejection-full-net9.trx` and `4170-reader-rejection-full-net10.trx` under the AWS V4 project's ignored `TestResults/` directory. Dual-framework targeted runs use distinct framework-qualified TRX filenames.

The next review draft covers a raw message whose application ID and topic parse before an invalid content type causes failure. It verifies that rejection retains that application identity and native payload. Mixed batches, concurrency and the remaining transport work remain pending.

Both full-run TRX files confirm all twenty-four wire-mapping cases, including all four valid-rejection cases, and all eighty generated SNS cases passed. The default reader matches its pre-mutation snapshot and `git diff --check` passes. The task emulator was stopped after validation.

## Raw-parser failure after application identity is parsed

The approved test sends a native raw message with a valid application ID and topic followed by an invalid content type. Four combinations cover sync/async consumption and ordinary/explicit default reader configuration. All four passed on first execution on both frameworks. The assertions verify unacceptable classification, original body, parsed application ID and topic, receipt/count metadata, forwarding to the invalid-message queue and actual deletion of the source delivery.

The named mutation forces the default reader to use the native SQS ID for every failure, discarding a successfully parsed application ID. All four cases fail at application-ID equality after classification and body assertions pass. The reader was restored byte-for-byte and both frameworks rebuilt before the final targeted and full runs. No production changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial raw-parser failure cases, net9.0 and net10.0 | 4 passed per framework |
| Force native ID mutation, net9.0 | 4 failed at application-ID equality |
| All wire-mapping cases after restoration, net9.0 and net10.0 | 28 passed per framework |
| Full AWS V4 regression, net9.0 | 444 passed, 0 skipped, 0 failed; 7m22s |
| Full AWS V4 regression, net10.0 | 444 passed, 0 skipped, 0 failed; 7m35s |

Full suites run without filters against Floci 1.5.19 using independent account namespaces, the explicit local endpoint and per-process idle-sleep inhibition. The preceding compatibility builds and Core/RabbitMQ regression results remain applicable because production code is unchanged.

Logs use `/private/tmp/brighter-4170-reader-raw-failure-{initial,mutation,restored,full-net9,full-net10}.log`. Full TRX files use `4170-reader-raw-failure-full-net9.trx` and `4170-reader-raw-failure-full-net10.trx` under the AWS V4 project's ignored `TestResults/` directory. Dual-framework targeted runs use distinct framework-qualified TRX filenames.

The next review draft covers a custom-reader failure among valid deliveries, requesting batches through the public factory and checking independent acknowledgement/rejection. It remains outside the compiled test project until approved.

Both full-run TRX files confirm all twenty-eight wire-mapping cases, including all four raw-parser failure cases, and all eighty generated SNS cases passed. The default reader matches its pre-mutation snapshot and `git diff --check` passes. The task emulator was stopped after validation.

## Mixed-batch custom-reader failure isolation

The approved test sends three native legacy messages in a batch, one without the tenant attribute required by the custom reader. Both sync and async cases passed on first execution on both frameworks. Every initial case logged a single receive returning three deliveries, confirming that the emulator exercised batch conversion. The assertions cover valid mapping for both successes, failure payload/native ID, distinct receipt handles, delivery counts, independent acknowledgement/rejection and actual deletion after the visibility timeout.

The named mutation filters unacceptable messages from the converted result. Its first two invocations failed to compile: the initial LINQ form lacked an import, and the `Array.FindAll` form needed the repository's scoped suppression for the obsolete message-type control signal. Neither counts as behavioral RED. With the suppression applied only around the temporary filter, both cases fail at the three-delivery assertion with actual count two. Output records two deliveries followed by empty receives. The consumer was restored byte-for-byte and both frameworks rebuilt before final validation. No production changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial mixed-batch cases, net9.0 and net10.0 | 2 passed per framework; each received 3 deliveries in one call |
| Filter failure deliveries mutation, net9.0 | 2 failed at count equality: expected 3, actual 2 |
| All wire-mapping cases after final restoration, net9.0 and net10.0 | 30 passed per framework |
| Full AWS V4 regression, net9.0 | 446 passed, 0 skipped, 0 failed; 7m34s |
| Full AWS V4 regression, net10.0 | 446 passed, 0 skipped, 0 failed; 7m35s |

Full suites run without filters against Floci 1.5.19 using independent account namespaces, the explicit local endpoint and per-process idle-sleep inhibition. The preceding compatibility builds and Core/RabbitMQ regression results remain applicable because production code is unchanged.

Logs use `/private/tmp/brighter-4170-reader-mixed-{initial,mutation,restored,mutation-fixed,mutation-behavior,final,full-net9,full-net10}.log`. The behavioral mutation result is `4170-reader-mixed-mutation-behavior.trx`; full results are `4170-reader-mixed-full-net9.trx` and `4170-reader-mixed-full-net10.trx`, under the AWS V4 project's ignored `TestResults/` directory. Dual-framework targeted runs use distinct framework-qualified TRX filenames.

The next test and writer double propose custom SNS publication mapping. They remain outside the compiled project until approved. Null/default-parser failures in mixed batches and concurrent mapping still need dedicated coverage.

Both full-run TRX files confirm all thirty wire-mapping cases passed. Both mixed-batch cases received all three deliveries in one call on each framework. All 128 cases with SNS in their test names also passed. The consumer matches its pre-mutation snapshot and `git diff --check` passes. The task emulator was stopped after validation.

## SNS publication-selected wire writing

The approved native SNS-to-SQS test initially failed to compile because `SnsWireMessage` did not exist. The implementation adds the native content carrier and an optional writer on `SnsPublication`, passes it from the producer and uses its body, subject and attributes in the publish request. The original publisher constructor remains available. Existing default serialization helpers and gateway destination/FIFO/scheduling ownership remain in place.

Both synchronous and asynchronous cases pass on both frameworks. Assertions inspect the real SNS envelope independently, including the custom body, subject, exclusive tenant attribute, configured destination and unchanged source body/subject/bag. The named mutation omits the writer when constructing the publisher. Both cases then fail at body equality: expected the `legacy:` prefix, actual original body. The producer was restored byte-for-byte before the final targeted and full runs.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | Missing native carrier compilation errors |
| Custom SNS writer, net9.0 and net10.0 | 2 passed per framework |
| Ignore publication writer mutation, net9.0 | 2 failed at native body equality |
| All wire-mapping cases after restoration, net9.0 and net10.0 | 32 passed per framework |
| Full AWS V4 regression, net9.0 | 448 passed, 0 skipped, 0 failed; 7m23s |
| Full AWS V4 regression, net10.0 | 448 passed, 0 skipped, 0 failed; 7m40s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

Full suites run without filters against Floci 1.5.19 using independent account namespaces, the explicit local endpoint and per-process idle-sleep inhibition. Core and RabbitMQ code is unchanged; their preceding regression results remain applicable. AWS test-project builds retain existing generated-source nullable warnings.

Logs use `/private/tmp/brighter-4170-sns-writer-{red,green,mutation,restored,full-net9,full-net10,netstandard2.0-build,net8.0-build}.log`. Mutation results are in `4170-sns-writer-mutation.trx`; full results are `4170-sns-writer-full-net9.trx` and `4170-sns-writer-full-net10.trx` under the AWS V4 project's ignored `TestResults/` directory. Dual-framework targeted runs use framework-qualified filenames.

Reusable default SNS composition is the next drafted test. Existing default SNS serialization still adds the handled count to the source bag; the proposal requires fresh output without that mutation. Custom FIFO/scheduling, raw SNS delivery, concurrency and the global rollout remain outstanding.

Both full-run TRX files confirm all 32 wire-mapping cases, including both custom SNS cases, passed. All 130 cases with SNS in their test names passed on each framework. The producer matches its pre-mutation snapshot and `git diff --check` passes. The task emulator was stopped after validation.

## Reusable default SNS mapping

The approved composition test initially failed to compile because `DefaultSnsWireMessageWriter` did not exist. Extracting the existing publisher mapping unchanged then produced behavioral RED in both sending modes: the source bag contained an extra `handled-count` entry. The default writer now adds that wire value to a filtered bag copy. Ordinary SNS publication and composition use the same implementation, preserving the existing body, subject and metadata format.

Both cases pass on .NET 9 and .NET 10, including independent native envelope observation of the application ID, resolved topic ARN, CloudEvents fields, added tenant attribute, serialized bag and handled count. The source body, subject, bag and handled count remain unchanged. The approved mutation removes only the native ID attribute. Both cases fail at the explicit ID-presence assertion. The writer was restored byte-for-byte before the final targeted and full runs.

| Check | Result |
| --- | --- |
| Initial RED, net9.0 | Missing default writer compilation error |
| Unchanged mapping extraction, net9.0 | 2 failed at source-bag cardinality; unexpected handled-count entry |
| Default SNS composition, net9.0 and net10.0 | 2 passed per framework |
| Omit native ID mutation, net9.0 | 2 failed at ID-presence assertion |
| All wire-mapping cases after restoration, net9.0 and net10.0 | 34 passed per framework |
| Full AWS V4 regression, net9.0 | 450 passed, 0 skipped, 0 failed; 7m26s |
| Full AWS V4 regression, net10.0 | 450 passed, 0 skipped, 0 failed; 7m11s |
| netstandard2.0 gateway build | Succeeded, 0 warnings, 0 errors |
| net8.0 gateway build | Succeeded, 0 warnings, 0 errors |

Full suites run without filters against Floci 1.5.19 using independent account namespaces, the explicit local endpoint and per-process idle-sleep inhibition. Core and RabbitMQ code is unchanged; their preceding regression results remain applicable. AWS test-project builds retain existing warnings.

Logs use `/private/tmp/brighter-4170-sns-default-{red,extracted,green,mutation,restored,full-net9,full-net10,netstandard2.0-build,net8.0-build}.log`. Behavioral RED results are in `4170-sns-default-extracted.trx` and `4170-sns-default-mutation.trx`; full results are `4170-sns-default-full-net9.trx` and `4170-sns-default-full-net10.trx`, under the AWS V4 project's ignored `TestResults/` directory. Dual-framework targeted runs use framework-qualified filenames.

The next draft characterizes custom SNS FIFO writing in raw and enveloped delivery. It remains outside the compiled project until approved. Scheduled dispatch, concurrency, remaining failure paths and the global rollout remain outstanding.

Both full-run TRX files confirm all 34 wire-mapping cases, including both default SNS composition cases, passed. All 132 cases with SNS in their test names passed on each framework. The writer matches its pre-mutation snapshot and `git diff --check` passes. The task emulator was stopped after validation.

## Custom SNS FIFO controls

The approved test publishes through the configured SNS producer factory and observes native SQS deliveries. Four cases cover sync/async sending and raw/enveloped delivery. All four pass on both frameworks. Assertions verify the original group and deduplication IDs, custom body and tenant attribute, and unchanged source message. Enveloped delivery also verifies the custom subject and configured topic ARN.

Two mutations were applied separately to `SnsMessagePublisher.ConfigureFifoSettings`. Replacing the group ID with `incorrect-group` makes every case fail at group-ID equality. Replacing the deduplication ID with `incorrect-deduplication` makes every case fail at deduplication-ID equality. The publisher was restored byte-for-byte before rebuilding both frameworks and confirming green. No production changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial FIFO cases, net9.0 and net10.0 | 4 passed per framework |
| Incorrect group-ID mutation, net9.0 | 4 failed at native group-ID equality |
| Incorrect deduplication-ID mutation, net9.0 | 4 failed at native deduplication-ID equality |
| Restored FIFO cases, net9.0 and net10.0 | 4 passed per framework |
| Full AWS V4 regression, net9.0 | 454 passed, 0 skipped, 0 failed; 7m27s |
| Full AWS V4 regression, net10.0 | 454 passed, 0 skipped, 0 failed; 7m33s |

The first sandboxed command could not create MSBuild pipes and did not execute tests. The successful rerun and subsequent commands run outside that sandbox. Full suites use Floci 1.5.19, independent account namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566` and per-process idle-sleep inhibition. Existing compatibility builds and Core/RabbitMQ results remain applicable because production code is unchanged.

Logs use `/private/tmp/brighter-4170-sns-fifo-{initial,initial-escalated,group-mutation,dedup-mutation,restored,full-net9.0,full-net10.0}.log`. TRX files use the `4170-sns-fifo-` prefix under the AWS V4 project's ignored `TestResults/` directory. Targeted dual-framework results have framework-qualified filenames; full results use `4170-sns-fifo-full-net9.0.trx` and `4170-sns-fifo-full-net10.0.trx`.

The next review draft covers a mixed batch with a default raw-parser failure. It remains outside the compiled project until approved. This FIFO increment establishes metadata forwarding; duplicate suppression and ordering under multiple publications remain separate validation work.

Both full-run TRX files confirm that all four custom FIFO cases passed. The publisher matches its pre-mutation snapshot and `git diff --check` passes. The task emulator was stopped after validation. All changes remain uncommitted and unpushed.

## Mixed batches with a default raw-parser failure

The approved test publishes two valid raw messages and one message with an invalid content type through the native SQS SDK. Four cases cover sync/async consumption and implicit/explicit default reader configuration. All four pass on both frameworks. The assertions preserve valid payloads, application IDs, trace context and tenant metadata. The malformed message retains its parsed application ID, topic, original body and rejection metadata. Native queue observation verifies invalid-message forwarding and deletion of every original delivery.

Every initial case received all three messages in one call on both frameworks. The named mutation replaces only the default reader's final reconstructed failure result with `Message.Empty`. All four cases fail at delivery-count equality: expected three, actual two. Each mutation run reports two deliveries followed by empty receives. The reader was restored byte-for-byte and both frameworks rebuilt before confirming green. No production changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial default-parser batch cases, net9.0 and net10.0 | 4 passed per framework |
| Replace reconstructed failure with empty message mutation, net9.0 | 4 failed at delivery-count equality |
| Restored default-parser batch cases, net9.0 and net10.0 | 4 passed per framework |
| Full AWS V4 regression, net9.0 | 458 passed, 0 skipped, 0 failed; 7m38s |
| Full AWS V4 regression, net10.0 | 458 passed, 0 skipped, 0 failed; 7m18s |

Full suites run without filters against Floci 1.5.19 using independent account namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566` and per-process idle-sleep inhibition. Existing compatibility builds and Core/RabbitMQ results remain applicable because production code is unchanged.

Logs use `/private/tmp/brighter-4170-reader-default-batch-{initial,mutation,restored,full-net9.0,full-net10.0}.log`. TRX files use the `4170-reader-default-batch-` prefix under the AWS V4 project's ignored `TestResults/` directory. Dual-framework targeted results have framework-qualified filenames; full results use `4170-reader-default-batch-full-net9.0.trx` and `4170-reader-default-batch-full-net10.0.trx`.

The next review draft covers a mixed batch containing a null custom-reader result. The test and reader double remain outside the compiled project until approved. Mixed batches containing malformed SNS envelopes, concurrent mapping and the remaining transport work remain outstanding.

Both full-run TRX files confirm that all four default-parser batch cases passed. Every case received three deliveries in one call during both restored targeted runs and both full suites. The reader matches its pre-mutation snapshot and `git diff --check` passes. The task emulator was stopped after validation. All changes remain uncommitted and unpushed.

## Mixed batches with a null custom-reader result

The approved test publishes three native legacy messages. The configured reader maps the two messages carrying a tenant attribute and returns null for the third. Both sync and async cases pass on .NET 9 and .NET 10. The assertions preserve valid payloads, application IDs, trace context and tenant metadata. The failed delivery retains its original body, native ID, receipt handle and normalized broker count. Native queue observation verifies invalid-message forwarding and deletion of every original delivery.

Every initial and restored case received all three messages in one call on both frameworks. The named mutation replaces the consumer's null-result exception with `Message.Empty`. Both cases fail at delivery-count equality: expected three, actual two. Each mutation run reports two deliveries followed by empty receives. The consumer was restored byte-for-byte and both frameworks rebuilt before confirming green. No production changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial null-reader batch cases, net9.0 and net10.0 | 2 passed per framework |
| Replace null-result exception with empty message mutation, net9.0 | 2 failed at delivery-count equality |
| Restored null-reader batch cases, net9.0 and net10.0 | 2 passed per framework |
| Full AWS V4 regression, net9.0 | 460 passed, 0 skipped, 0 failed; 7m11s |
| Full AWS V4 regression, net10.0 | 460 passed, 0 skipped, 0 failed; 7m11s |

Full suites run without filters against Floci 1.5.19 using independent account namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566` and per-process idle-sleep inhibition. Existing compatibility builds and Core/RabbitMQ results remain applicable because production code is unchanged.

Logs use `/private/tmp/brighter-4170-reader-null-batch-{initial,mutation,restored,full-net9.0,full-net10.0}.log`. TRX files use the `4170-reader-null-batch-` prefix under the AWS V4 project's ignored `TestResults/` directory. Dual-framework targeted results have framework-qualified filenames; full results use `4170-reader-null-batch-full-net9.0.trx` and `4170-reader-null-batch-full-net10.0.trx`.

The next review draft covers a mixed batch containing a malformed SNS envelope. It remains outside the compiled project until approved. Concurrent mapping, remaining malformed-attribute paths and the global transport rollout remain outstanding.

Both full-run TRX files confirm that both null-reader batch cases passed. Every case received three deliveries in one call during both full suites. The consumer matches its pre-mutation snapshot and `git diff --check` passes. The task emulator was stopped after validation. All changes remain uncommitted and unpushed.

## Mixed batches with a malformed SNS envelope

The approved test publishes two independently constructed SNS envelopes and one malformed JSON body through the native SQS SDK. Four cases cover sync/async consumption and implicit/explicit default reader configuration with raw delivery disabled. All four pass on .NET 9 and .NET 10. Assertions verify inner payloads, application IDs, topics, tracing and tenant metadata for valid envelopes. The malformed delivery retains its original body, native ID, receipt handle and normalized broker count. Native queue observation verifies invalid-message forwarding and deletion of every original delivery.

Every initial case received all three messages in one call on both frameworks. The named mutation replaces only the default reader's final reconstructed failure result with `Message.Empty`. All four cases fail at delivery-count equality: expected three, actual two. Each mutation run reports two deliveries followed by empty receives. The reader was restored byte-for-byte and both frameworks rebuilt before confirming green. No production changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial malformed-envelope batch cases, net9.0 and net10.0 | 4 passed per framework |
| Replace reconstructed failure with empty message mutation, net9.0 | 4 failed at delivery-count equality |
| Restored malformed-envelope batch cases, net9.0 and net10.0 | 4 passed per framework |
| Full AWS V4 regression, net9.0 | 464 passed, 0 skipped, 0 failed; 7m47s |
| Full AWS V4 regression, net10.0 | 464 passed, 0 skipped, 0 failed; 7m24s |

Full suites run without filters against Floci 1.5.19 using independent account namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566` and per-process idle-sleep inhibition. Existing compatibility builds and Core/RabbitMQ results remain applicable because production code is unchanged.

Logs use `/private/tmp/brighter-4170-reader-envelope-batch-{initial,mutation,restored,full-net9.0,full-net10.0}.log`. TRX files use the `4170-reader-envelope-batch-` prefix under the AWS V4 project's ignored `TestResults/` directory. Dual-framework targeted results have framework-qualified filenames; full results use `4170-reader-envelope-batch-full-net9.0.trx` and `4170-reader-envelope-batch-full-net10.0.trx`.

The next review draft covers repeated default SQS writer composition with independent per-message content. It remains outside the compiled project until approved. Concurrent mapping, remaining malformed-attribute paths and the global transport rollout remain outstanding.

Both full-run TRX files confirm that all four malformed-envelope batch cases passed. Each received three deliveries in one call. The reader matches its pre-mutation snapshot. Work is paused with all changes uncommitted and unpushed.

## Repeated default SQS writer composition

The approved test sends two distinct messages through one publication and a
single composed default writer. Native SQS observation checks independent IDs,
bodies, subjects, tenant attributes, CloudEvents fields and handled counts.
Both source messages retain their original content and single bag entry.

Both sync and async cases initially pass on .NET 9 and .NET 10. The named
mutation caches the first `SqsWireMessage` in an instance field and reuses it
on subsequent calls. Both cases fail at the distinct native application-ID
count: expected two, actual one. The writer was restored byte-for-byte before
rebuilding both frameworks and confirming green. All 21 production files in
the saved checkpoint still match their original hashes; this increment needs
no production changes.

| Check | Result |
| --- | --- |
| Initial writer-reuse cases, net9.0 and net10.0 | 2 passed per framework |
| Cache-first-wire-message mutation, net9.0 | 2 failed at distinct application-ID count |
| Restored writer-reuse cases, net9.0 and net10.0 | 2 passed per framework |
| Full AWS V4 regression, net9.0 | 466 passed, 0 skipped, 0 failed; 7m11s |
| Full AWS V4 regression, net10.0 | 466 passed, 0 skipped, 0 failed; 7m11s |

Full suites run without filters against Floci 1.5.19 using independent account
namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566`
and per-process idle-sleep inhibition. Logs use
`/private/tmp/brighter-4170-writer-reuse-{initial,mutation,restored,full-net9.0,full-net10.0}.log`.
TRX files use the `4170-writer-reuse-` prefix under the AWS V4 project's ignored
`TestResults/` directory.

The next review draft covers independent default/composed SQS publication
mappings in both send orders. It remains outside the compiled project until
approved. Concurrent mapping, subscription isolation, the remaining failure
paths and the global rollout remain outstanding.

Both full-run TRX files confirm that both writer-reuse cases passed. Production
files match the saved checkpoint, and `git diff --check` passes. All work remains
uncommitted and unpushed.

## SQS publication mapping isolation

The approved test configures default and composed writers on two publications
created through one producer factory. Four cases cover sync/async sends and
both publication/send orders. Native SQS observation checks that each queue
receives its own topic, application ID, body, subject and default attributes;
only the custom publication adds the tenant attribute. Source messages and
publication writer selections remain unchanged.

All four cases initially pass on .NET 9 and .NET 10. The named mutation adds a
static cached writer to `SqsMessageSender` and selects the first writer for all
later senders. Every case fails at native topic equality because a different
publication's topic reaches the destination queue. The sender was restored
byte-for-byte before both frameworks were rebuilt and confirmed green. No
production changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial publication-isolation cases, net9.0 and net10.0 | 4 passed per framework |
| Global writer-cache mutation, net9.0 | 4 failed at native topic equality |
| Restored publication-isolation cases, net9.0 and net10.0 | 4 passed per framework |
| Full AWS V4 regression, net9.0 | 470 passed, 0 skipped, 0 failed; 7m20s |
| Full AWS V4 regression, net10.0 | 470 passed, 0 skipped, 0 failed; 7m22s |

Full suites run without filters against Floci 1.5.19 using independent account
namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566`
and per-process idle-sleep inhibition. Logs use
`/private/tmp/brighter-4170-writer-isolation-{initial,mutation,restored,full-net9.0,full-net10.0}.log`.
TRX files use the `4170-writer-isolation-` prefix under the AWS V4 project's
ignored `TestResults/` directory.

The next review draft covers independent native attributes while SQS sends
overlap. The test and gating writer remain outside the compiled project until
approved. Subscription isolation, remaining concurrency and failure paths,
and the global rollout remain outstanding.

Both full-run TRX files confirm that all four publication-isolation cases
passed. All 21 production files from the saved checkpoint still match their
original hashes, and `git diff --check` passes. All changes remain uncommitted
and unpushed.


## SQS overlapping sends preserve independent attributes

The approved test sends two messages through one public producer factory and
one composed default writer. Two cases cover sync and async sends. A strategy
wrapper parks the first completed mapping until the second mapping completes,
so both native representations are outstanding before the first send can
serialize its attributes. Native SDK reads verify distinct application IDs and
each delivery's body, subject, tenant, CloudEvents ID and handled count. Source
messages remain unchanged. All sends finish before the writer, producer and
queues are disposed.

Both cases initially pass on .NET 9 and .NET 10. The named mutation adds a
reusable instance dictionary to `DefaultSqsWireMessageWriter`, clearing and
refilling it on each mapping while leaving each body unchanged. Both cases
fail at distinct native application-ID count (expected two, actual one), at
line 119 of the test. The production writer was restored byte-for-byte before
rebuilding both frameworks and confirming green. No production changes are
needed for this characterization.

| Check | Result |
| --- | --- |
| Initial overlapping-send cases, net9.0 and net10.0 | 2 passed per framework |
| Shared-attribute-dictionary mutation, net9.0 | 2 failed at distinct application-ID count |
| Restored overlapping-send cases, net9.0 and net10.0 | 2 passed per framework |
| Full AWS V4 regression, net9.0 | 472 passed, 0 skipped, 0 failed; 7m12s |
| Full AWS V4 regression, net10.0 | 472 passed, 0 skipped, 0 failed; 7m18s |

Full suites run without filters against Floci 1.5.19 using independent account
namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566`
and per-process idle-sleep inhibition. Logs use
`/private/tmp/brighter-4170-writer-overlap-{initial,mutation,restored,full-net9.0,full-net10.0}.log`.
TRX files use the `4170-writer-overlap-` prefix under the AWS V4 project's
ignored `TestResults/` directory. Both full-run TRX files confirm that both
new cases passed. The task emulator was stopped after validation.

The next review draft covers independent default/composed subscription readers
through one consumer factory, in both creation orders and sync/async paths.
It remains outside the compiled project until approved. Simultaneous reader
calls, remaining failure paths and the global rollout remain outstanding.

All 21 production files from the saved checkpoint still match their original
hashes, and `git diff --check` passes. HEAD remains
`64ba97097199c708974195f55fbdca0ce3e8487c`; the named stash and backup remain
intact. All changes remain uncommitted and unpushed.


## SQS subscription reader selection isolation

The approved test creates two subscriptions through one public consumer factory,
using the implicit default reader on one and a composed tenant-header reader on
the other. Four cases cover sync/async consumption and both creation orders.
The default reader preserves `tenant-id`; only the custom reader renames it to
`tenant_id`. Each delivery retains its own application ID, body, topic, unrelated
bag entry, delivery count and receipt handle. Both consumers acknowledge their
deliveries; native reads after visibility expiry verify deletion from each queue.

All four cases initially pass on .NET 9 and .NET 10. The named mutation caches
the first subscription's reader selection in `SqsMessageConsumerFactory`,
including null, and uses it for every subsequent consumer. All four cases fail
at tenant-key presence on the second subscription (line 133). Default-first
cases expect true but receive false; custom-first cases expect false but
receive true. The factory was restored byte-for-byte before rebuilding both
frameworks and confirming green. No production changes are needed.

| Check | Result |
| --- | --- |
| Initial subscription-isolation cases, net9.0 and net10.0 | 4 passed per framework |
| Factory-reader-cache mutation, net9.0 | 4 failed at tenant-key presence |
| Restored subscription-isolation cases, net9.0 and net10.0 | 4 passed per framework |
| Full AWS V4 regression, net9.0 | 476 passed, 0 skipped, 0 failed; 7m19s |
| Full AWS V4 regression, net10.0 | 476 passed, 0 skipped, 0 failed; 7m11s |

Full suites run without filters against Floci 1.5.19 using independent account
namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566`
and per-process idle-sleep inhibition. Logs use
`/private/tmp/brighter-4170-reader-isolation-{initial,mutation,restored,full-net9.0,full-net10.0}.log`.
TRX files use the `4170-reader-isolation-` prefix under the AWS V4 project's
ignored `TestResults/` directory. Both full-run TRX files confirm all four
new cases passed. The task emulator was stopped after validation.

The next review draft covers two consumers sharing a default reader with raw
and SNS-envelope deliveries, preserving each outstanding result and its
acknowledgement. It remains outside the compiled project until approved.
Simultaneous reader calls, remaining failure paths and global rollout remain
outstanding.

All 21 production files from the saved checkpoint still match their original
hashes, and `git diff --check` passes. HEAD remains
`64ba97097199c708974195f55fbdca0ce3e8487c`; the named stash and backup remain
intact. All changes remain uncommitted and unpushed.


## Shared default SQS reader preserves independent deliveries

The approved test shares one default reader between two subscriptions created
through a public consumer factory. Four cases cover sync/async consumption and
raw/SNS-envelope parsing. Both returned messages remain outstanding before
assertions: they retain distinct application IDs and native receipt handles,
and each keeps its body, topic, subject, correlation ID, tenant and delivery
count. Both consumers acknowledge their own deliveries; native reads after
visibility expiry verify deletion from each queue.

All four cases initially pass on .NET 9 and .NET 10. The named mutation caches
the first parsed message in `DefaultSqsWireMessageReader` and returns it on
subsequent calls. Every case fails at distinct application-ID count (expected
two, actual one), at line 136. The reader was restored byte-for-byte before
rebuilding both frameworks and confirming green. No production changes are
needed for this characterization.

| Check | Result |
| --- | --- |
| Initial shared-reader cases, net9.0 and net10.0 | 4 passed per framework |
| Parsed-message-cache mutation, net9.0 | 4 failed at distinct application-ID count |
| Restored shared-reader cases, net9.0 and net10.0 | 4 passed per framework |
| Full AWS V4 regression, net9.0 | 480 passed, 0 skipped, 0 failed; 7m34s |
| Full AWS V4 regression, net10.0 | 480 passed, 0 skipped, 0 failed; 7m12s |

Full suites run without filters against Floci 1.5.19 using independent account
namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566`
and per-process idle-sleep inhibition. Logs use
`/private/tmp/brighter-4170-reader-reuse-{initial,mutation,restored,full-net9.0,full-net10.0}.log`.
TRX files use the `4170-reader-reuse-` prefix under the AWS V4 project's
ignored `TestResults/` directory. Both full-run TRX files confirm all four
new cases passed. The task emulator was stopped after validation.

The next review draft covers explicit requeue after a custom reader throws
or returns null, with native identity and payload retained across delivery
attempts. It remains outside the compiled project until approved. Simultaneous
reader calls, remaining failure paths and global rollout remain outstanding.

All 21 production files from the saved checkpoint still match their original
hashes, and `git diff --check` passes. HEAD remains
`64ba97097199c708974195f55fbdca0ce3e8487c`; the named stash and backup remain
intact. All changes remain uncommitted and unpushed.


## Failed SQS wire mappings preserve explicit requeue eligibility

The approved test receives a native message through the public consumer factory
with a throwing or null-returning custom reader. Four cases cover sync/async
operations and both failure types. Explicit requeue succeeds, and redelivery
retains the native payload and message ID while advancing handled count from
zero to one and replacing the receipt handle. The first returned message stays
unchanged. Acknowledgement of the second delivery is verified by a native read
after visibility expiry. The queue starts with 120-second visibility, longer
than the redelivery read, so ordinary timeout expiry cannot substitute for
requeue. Only after redelivery validation does the test shorten visibility to
one second to check acknowledgement without waiting two minutes.

All four cases initially pass on .NET 9 and .NET 10. The named mutation returns
false immediately from `SqsMessageConsumer.RequeueAsync` for unacceptable
messages. Its first build stopped at CS0618 for the obsolete message-type
property; that compilation error was not counted as RED. A temporary scoped
CS0618 suppression allowed the same approved mutation to execute. Every case
then failed at `Assert.True(requeued)` (line 105), expected true, actual false.
The consumer, including the warning suppression, was restored byte-for-byte
before rebuilding both frameworks and confirming green. No production changes
are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial failure-requeue cases, net9.0 and net10.0 | 4 passed per framework |
| Unacceptable-requeue-filter mutation, net9.0 | 4 failed at requeue eligibility |
| Final restored failure-requeue cases, net9.0 and net10.0 | 4 passed per framework |
| Full AWS V4 regression, net9.0 | 484 passed, 0 skipped, 0 failed; 7m24s |
| Full AWS V4 regression, net10.0 | 484 passed, 0 skipped, 0 failed; 7m13s |

Full suites run without filters against Floci 1.5.19 using independent account
namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566`
and per-process idle-sleep inhibition. Logs use
`/private/tmp/brighter-4170-reader-requeue-{initial,mutation,mutation-compiled,restored,restored-final,full-net9.0,full-net10.0}.log`.
The `mutation` log records the build error; `mutation-compiled` records the
behavioral failures. `restored-final` is the green rebuild after those failures.
TRX files use the `4170-reader-requeue-` prefix under the AWS V4 project's
ignored `TestResults/` directory. Both full-run TRX files confirm all four
new cases passed. The task emulator was stopped after validation.

The next review draft covers outbound writer failures leaving an outbox entry
pending until recovery, using the public mediator, real SQS producer and an
in-memory outbox. It and its switchable writer remain outside the compiled
project until approved. Other failure paths, concurrency, dispatch-time trace
context, scheduled sends and global rollout remain outstanding.

All 21 production files from the saved checkpoint still match their original
hashes, and `git diff --check` passes. HEAD remains
`64ba97097199c708974195f55fbdca0ce3e8487c`; the named stash and backup remain
intact. All changes remain uncommitted and unpushed.


## SQS writer failures retain outbox entries until recovery

The approved test uses the public SQS producer factory and outbox mediator
with an in-memory outbox. Two cases cover sync/async clearing. A configured
writer throws throughout the initial clear attempt, including resilience
retries. The original entry remains pending, retaining payload and identity,
and native SDK reads verify that no message was delivered. Enabling the same
writer then allows another clear to dispatch the entry. Native delivery keeps
its application ID, body, subject and topic; the source message and tenant bag
remain unchanged.

Both cases initially pass on .NET 9 and .NET 10. The named mutation catches
`FormatException` from the configured writer in
`SqsMessageSender.CreateSendMessageRequest` and silently substitutes default
mapping. Both cases fail at the first pending-entry assertion (line 103): the
outstanding collection is empty because the incorrect fallback sent the
message and marked it dispatched. The sender was restored byte-for-byte before
rebuilding both frameworks and confirming green. No gateway or core production
changes are needed for this characterization.

| Check | Result |
| --- | --- |
| Initial writer-outbox cases, net9.0 and net10.0 | 2 passed per framework |
| Silent-default-fallback mutation, net9.0 | 2 failed at pending-entry preservation |
| Restored writer-outbox cases, net9.0 and net10.0 | 2 passed per framework |
| Full AWS V4 regression, net9.0 | 486 passed, 0 skipped, 0 failed; 7m17s |
| Full AWS V4 regression, net10.0 | 486 passed, 0 skipped, 0 failed; 7m13s |

Full suites run without filters against Floci 1.5.19 using independent account
namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566`
and per-process idle-sleep inhibition. Logs use
`/private/tmp/brighter-4170-writer-outbox-{initial,mutation,restored,full-net9.0,full-net10.0}.log`.
TRX files use the `4170-writer-outbox-` prefix under the AWS V4 project's
ignored `TestResults/` directory. Both full-run TRX files confirm both new
cases passed. The task emulator was stopped after validation.

The next review draft covers dispatch-time trace propagation through composed
SQS writing during explicit outbox clearing. It remains outside the compiled
project until approved. Scheduled sends, other failure paths, simultaneous
reader calls and global rollout remain outstanding.

All 21 production files from the saved checkpoint still match their original
hashes, and `git diff --check` passes. HEAD remains
`64ba97097199c708974195f55fbdca0ce3e8487c`; the named stash and backup remain
intact. All changes remain uncommitted and unpushed.


## SQS outbox dispatch serializes the current producer trace

The approved test deposits a message with an earlier activity's traceparent,
then clears it under a separate dispatch trace through the public mediator
and SQS producer factory. Two cases cover sync/async clearing with a composed
tenant-header writer. Native CloudEvents attributes must contain the actual
publish span's ID, and that span must belong to the dispatch trace. Identity,
body and custom tenant metadata survive, and the outbox entry is dispatched.

Two harness corrections were needed before characterization:

- `ActivityKind.Producer` also identifies the outbox-clear span. Capture now
  additionally requires the public operation tag to identify `publish`.
- An `ActivityListener` alone does not initialize OpenTelemetry propagation.
  A local diagnostic confirmed a no-op default propagator before SDK startup
  and a composite trace-context/baggage propagator afterward. The test now
  creates and disposes a normal SDK tracer provider, as existing tracing
  tests do. It does not explicitly replace global propagators or logging
  factories. The listener remains restricted to its own tracer's source
  instance, and all activities and telemetry resources are disposed.

The initial failures were harness failures, not product regressions or proof
of the approved mutation. With both corrections, both cases pass on .NET 9
and .NET 10. The named mutation moves the producer's existing trace-propagation
event to after the native send. Both cases then fail at equality between the
publish span ID and native traceparent (line 153): the wire value still carries
the deposited context. The producer was restored byte-for-byte before
rebuilding both frameworks and confirming green. No production changes are
needed for this characterization.

| Check | Result |
| --- | --- |
| Correctly initialized dispatch-trace cases, net9.0 and net10.0 | 2 passed per framework |
| Late-producer-propagation mutation, net9.0 | 2 failed at native traceparent equality |
| Restored dispatch-trace cases, net9.0 and net10.0 | 2 passed per framework |
| Full AWS V4 regression, net9.0 | 488 passed, 0 skipped, 0 failed; 7m20s |
| Full AWS V4 regression, net10.0 | 488 passed, 0 skipped, 0 failed; 7m25s |

Full suites run without filters against Floci 1.5.19 using independent account
namespaces, explicit local endpoint `http://localhost.localstack.cloud:14566`
and per-process idle-sleep inhibition. Logs use
`/private/tmp/brighter-4170-dispatch-trace-{initial,corrected,sdk,mutation,restored,full-net9.0,full-net10.0}.log`.
The `initial` and `corrected` logs record the two setup failures; `sdk` records
initial green after both harness corrections. TRX files use the
`4170-dispatch-trace-` prefix under the AWS V4 project's ignored `TestResults/`
directory. Both full-run TRX files confirm both new cases passed. The task
emulator was stopped after validation.

The next checkpoint below validates subscription-selected RabbitMQ Async
reading with native legacy messages and gateway-owned delivery metadata.
Scheduled delivery, simultaneous reader calls, additional failure paths and
global rollout remain outstanding.

All 21 production files from the saved checkpoint still match their original
hashes, and `git diff --check` passes. HEAD remains
`64ba97097199c708974195f55fbdca0ce3e8487c`; the named stash and backup remain
intact. All changes remain uncommitted and unpushed.

## RabbitMQ Async subscription-selected reader

The legacy-reader integration test uses public sync/async factories and independent native publishing and queue observation. A confirmed persistent delivery supplies a legacy application ID and tenant header. The configured reader maps content and fabricates delivery metadata. The gateway replaces delivery tag, redelivery and persistence values, and acknowledgement removes the original delivery after the consumer closes.

| Check | Result |
| --- | --- |
| Initial test build | Expected CS0117 for missing `RmqSubscription.WireMessageReader` on net9.0 and net10.0 |
| Targeted .NET 9 / RabbitMQ 4.2 | 2 passed |
| Targeted .NET 10 / RabbitMQ 4.3 | 2 passed |
| Ignore selected reader mutation, .NET 9 | Both cases fail at application ID: expected `order-17`, actual `native-message-id` |
| Skip gateway metadata mutation, .NET 9 | Both cases fail at the fabricated delivery tag: `ulong.MaxValue` survives |
| Restored targeted checks | 2 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 386 passed, 2 existing skips, 0 failed; 8 m 31 s |
| Full .NET 10 / RabbitMQ 4.3 | 386 passed, 2 existing skips, 0 failed; 8 m 40 s |
| netstandard2.0 and net8.0 gateway builds | Each succeeded, 0 warnings, 0 errors |
| Restored net9.0/net10.0 test build | Succeeded, 0 warnings, 0 errors |

The first implementation build exposed missing RabbitMQ namespace imports; these were corrected before behavioral execution. The initial clean test build includes existing analyzer warnings. The final incremental builds report no warnings.

Both mutations were restored to `/private/tmp/brighter-4170-rmq-reader-implemented.cs` byte-for-byte. Existing public constructor signatures match HEAD. All 21 production files in the saved-work manifest remain byte-identical; this increment changes three additional RabbitMQ source files. Core and AWS code are unchanged, so their prior full-suite results remain applicable.

Tests use the isolated TLS-enabled primary brokers and separate native-delay brokers described above. Full runs use no filters. The workspace is mounted read-only into .NET SDK containers. TRX evidence lives in `/private/tmp/brighter-4170-rmq-results/` with prefixes `4170-rmq-reader-green-net`, `4170-rmq-reader-restored-net` and `4170-rmq-reader-full-net`, plus `4170-rmq-reader-ignore.trx` and `4170-rmq-reader-metadata.trx`. Initial build and targeted logs use `/private/tmp/brighter-4170-rmq-reader-`.

Both full TRX files confirm all 16 mutual-TLS cases and both new reader cases passed. Native-delay coverage also passes: 19 direct cases, 42 classic conformance cases, 42 quorum conformance cases and two custom-writer delay cases. The two existing skips remain the async dispatcher-builder and Reactor dead-letter-queue rejection tests.

This completes only successful-reader mapping. Throwing/null readers, mixed batches, default-reader composition and custom republish are not covered by this checkpoint.

## RabbitMQ Async custom-reader failures

Four cases cover throwing and null-returning readers in sync and async factory paths. Confirmed native messages contain non-UTF-8 body bytes. Receive returns one unacceptable message with native ID, routing key, raw bytes and broker delivery metadata. Rejection without an invalid-message route removes the original message, verified by native `BasicGet` after consumer disposal.

| Check | Result |
| --- | --- |
| Initial behavioral RED, .NET 9 | All four cases fail at `Assert.Null(receiveException)`; `ChannelFailureException` wraps the reader exception or null-reference exception |
| Targeted GREEN | 4 passed on each framework |
| Skip failure metadata mutation, .NET 9 | All four fail at the positive delivery-tag assertion |
| Empty failure body mutation, .NET 9 | All four fail at byte equality, expected `[0, 255, 128, 65]`, actual `[]` |
| Restored targeted checks | 4 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 390 passed, 2 existing skips, 0 failed; 8 m 53 s |
| Full .NET 10 / RabbitMQ 4.3 | 390 passed, 2 existing skips, 0 failed; 8 m 38 s |
| netstandard2.0 and net8.0 gateway builds | Each succeeded, 0 warnings, 0 errors |
| Final incremental net9.0/net10.0 test build | Succeeded, 0 warnings, 0 errors |

The first implementation build caught a nullable native message-ID argument; the fallback uses an empty ID when the broker omits it. The first metadata mutation failed compilation because it inspected the obsolete message-type property. Its subsequent test execution used the preceding assembly and is excluded from mutation evidence. The corrected mutation returns directly from the failure branch before metadata stamping; after a successful build all four cases fail as intended. The verified evidence is `4170-rmq-reader-failure-metadata-verified.trx`, not `4170-rmq-reader-failure-metadata.trx`.

The consumer is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-reader-failure-implemented.cs`. No core or AWS implementation changed, and no public signatures changed. Their preceding regression results remain applicable. Test builds retain analyzer and obsolete-API warnings when recompilation occurs; production compatibility builds are clean.

Full suites use the existing isolated TLS primary brokers and independent native-delay brokers with no filters. TRX files under `/private/tmp/brighter-4170-rmq-results/` use the prefix `4170-rmq-reader-failure-`: `red`, `green-net9`, `green-net10`, `metadata-verified`, `body`, `restored-net9`, `restored-net10`, `full-net9` and `full-net10`. Build logs use the same prefix in `/private/tmp/`.

Mixed-batch preservation is covered by the next checkpoint below. Default-parser failures, invalid-message forwarding, redelivery, default-reader composition and custom republish remain outside this checkpoint.

Both full TRX files confirm four failure-reader cases, two successful-reader cases, all 16 mutual-TLS cases, 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer delay cases passed. The existing async dispatcher-builder and Reactor dead-letter-queue rejection skips are unchanged. `git diff --check` passes; HEAD and the named stash are unchanged. All work remains uncommitted and unpushed.

## RabbitMQ Async mixed custom-reader batches

Eight cases cover sync/async factories, throwing/null readers and both delivery orders. Two confirmed native messages arrive in a single receive result. Assertions cover original order, application identity, exact body bytes, distinct broker delivery tags and persistence. The valid result is acknowledged and the failure rejected, followed by consumer disposal and an independent native queue check.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 8 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 8 passed |
| Empty-batch mutation, .NET 9 | All eight fail at batch length: expected 2, actual 0 |
| Restored targeted checks | 8 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 398 passed, 2 existing skips, 0 failed; 8 m 53 s |
| Full .NET 10 / RabbitMQ 4.3 | 398 passed, 2 existing skips, 0 failed; 8 m 50 s |

The named mutation returns an empty result when any mapped message is unacceptable. Its temporary scoped obsolete-member suppression permits inspecting the retained wire control field. Both mutation and suppression are removed. The consumer matches `/private/tmp/brighter-4170-rmq-reader-batch-original.cs` byte-for-byte and also matches the preceding failure-handling implementation. No production or public API change is needed, so prior netstandard2.0/net8.0 compatibility builds and Core/AWS regressions remain applicable.

Initial and restored test builds succeed with repository analyzer and obsolete-API warnings. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers with no filters. TRX evidence under `/private/tmp/brighter-4170-rmq-results/` uses `4170-rmq-reader-batch-` followed by `initial-net9`, `initial-net10`, `mutation`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build logs under `/private/tmp/` use the same prefix.

Default-parser failure handling is covered by the next checkpoint below. Invalid-message forwarding, redelivery, default-reader composition and custom republish remain further work.

Both full TRX files confirm all eight mixed-batch cases, four custom-reader failure cases, two successful-reader cases and 16 mutual-TLS cases passed. Native-delay coverage passes with 19 direct cases, 42 classic and 42 quorum conformance cases, and two custom-writer cases. The same two existing skips remain. `git diff --check` passes; HEAD and the named stash are unchanged. No commits or pushes were made.

## RabbitMQ Async default-parser failures

Four cases cover sync/async factories and malformed native content-type/type properties. Native deliveries carry non-UTF-8 body bytes and an application topic header different from the broker routing key. Failed parsing retains the already parsed ID/topic, raw body and broker metadata; rejection settles the original delivery, verified through native `BasicGet` after consumer disposal.

| Check | Result |
| --- | --- |
| Initial behavioral RED, .NET 9 | All four fail at body equality: expected `[0, 255, 128, 65]`, actual `[]` |
| Targeted GREEN | 4 passed on each framework |
| Skip failure metadata mutation, .NET 9 | All four fail at the positive delivery-tag assertion |
| Empty failure body mutation, .NET 9 | All four fail at exact body-byte equality |
| Restored targeted checks | 4 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 402 passed, 2 existing skips, 0 failed; 9 m 3 s |
| Full .NET 10 / RabbitMQ 4.3 | 402 passed, 2 existing skips, 0 failed; 9 m 1 s |
| netstandard2.0 and net8.0 gateway builds | Each succeeded, 0 warnings, 0 errors |
| Final incremental net9.0/net10.0 test build | Succeeded, 0 warnings, 0 errors |

`RmqMessageCreator` preserves the native body in its failure branch and stamps broker metadata after both success and failure. It does not parse malformed MIME properties again while constructing the fallback body. Public signatures and successful content mapping are unchanged. No Core or AWS implementation changed, so their preceding regression results remain applicable.

Both mutations are removed. The parser matches `/private/tmp/brighter-4170-rmq-default-failure-implemented.cs` byte-for-byte. Initial test builds include repository analyzer and obsolete-API warnings; compatibility builds and the final incremental test build are clean.

Full suites use the isolated TLS-enabled primary brokers and independent native-delay brokers with no filters. TRX files under `/private/tmp/brighter-4170-rmq-results/` use the prefix `4170-rmq-default-failure-` followed by `red`, `green-net9`, `green-net10`, `metadata`, `body`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build logs under `/private/tmp/` use the same prefix.

Mixed default-parser batch preservation is covered by the next checkpoint below. Invalid-message forwarding, redelivery, default-reader composition and custom republish remain later work.

Both full TRX files confirm all four default-parser failure cases, eight mixed custom-reader batch cases, four custom-reader failure cases, two successful-reader cases and 16 mutual-TLS cases passed. Native-delay coverage passes with 19 direct cases, 42 classic and 42 quorum conformance cases, and two custom-writer cases. The same two existing skips remain. `git diff --check` passes, HEAD and the named stash are unchanged, and no commits or pushes were made.

## RabbitMQ Async mixed default-parser batches

Eight cases cover sync/async public factories, malformed native content-type/type properties and both delivery orders. A single receive returns the valid and failed messages in their original order, preserving application identity/topic, exact body bytes and distinct broker tags. Independent acknowledgement and rejection are verified through native `BasicGet` after consumer disposal.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 8 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 8 passed |
| Empty-batch mutation, .NET 9 | All eight fail at batch length: expected 2, actual 0 |
| Restored targeted checks | 8 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 410 passed, 2 existing skips, 0 failed; 8 m 56 s |
| Full .NET 10 / RabbitMQ 4.3 | 410 passed, 2 existing skips, 0 failed; 8 m 54 s |

The consumer mutation returns an empty result whenever any mapped message is unacceptable. The mutation and its scoped obsolete-member suppression are removed; the consumer matches `/private/tmp/brighter-4170-rmq-default-batch-original.cs` byte-for-byte. The default parser also matches the preceding verified implementation. No production or API change is needed, so preceding gateway compatibility builds and Core/AWS regressions remain applicable.

Test builds succeed with repository analyzer and obsolete-API warnings. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers with no filters. TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-default-batch-` followed by `initial-net9`, `initial-net10`, `mutation`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build logs under `/private/tmp/` use the same prefix.

Public default-reader composition is covered by the next checkpoint below. Invalid-message forwarding, redelivery and custom republish remain further work.

Both full TRX files confirm eight mixed default-parser cases, four default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases, two successful-reader cases and 16 mutual-TLS cases passed. Native-delay coverage passes with 19 direct cases, 42 classic and 42 quorum conformance cases, and two custom-writer cases. The same two existing skips remain. `git diff --check` passes, HEAD and the named stash are unchanged, and no commits or pushes were made.


## RabbitMQ Async default-reader composition

`DefaultRmqWireMessageReader` is a public, parameterless adapter over `RmqMessageCreator`. The unconfigured consumer and composed readers use the same parser. Two integration cases exercise the sync and async public consumer factories with a custom reader that renames a tenant bag key after default parsing. Independently published native messages provide body, identity, topic, type, content type, correlation ID, reply-to, subject, timestamp and handled count. Assertions also cover delivery metadata and acknowledgement, confirmed by a native queue check after consumer disposal.

| Check | Result |
| --- | --- |
| Initial build | Missing `DefaultRmqWireMessageReader` on net9.0 and net10.0 (CS0246) |
| Initial .NET 9 / RabbitMQ 4.2 | 2 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 2 passed |
| Empty-body mutation, .NET 9 | Both fail at body equality: expected `order accepted`, actual empty string |
| Ignored-reader mutation, .NET 9 | Both fail at the renamed `tenant_id` key presence assertion |
| Restored targeted checks | 2 passed on each framework |
| netstandard2.0 and net8.0 gateway builds | Passed, 0 warnings and 0 errors |
| Full .NET 9 / RabbitMQ 4.2 | 412 passed, 2 existing skips, 0 failed; 8 m 40 s |
| Full .NET 10 / RabbitMQ 4.3 | 412 passed, 2 existing skips, 0 failed; 8 m 46 s |

The initial compilation failure alone is not behavioral coverage. Both mutations compile successfully and fail at the intended assertions. The public reader and consumer are restored byte-for-byte to `/private/tmp/brighter-4170-rmq-reader-composition-implemented.cs` and `/private/tmp/brighter-4170-rmq-reader-composition-consumer.cs`. Core and AWS implementation files are unchanged, so their preceding full regression results remain applicable. Existing public constructor signatures are unchanged.

Test builds retain repository analyzer and obsolete-API warnings when tests recompile. Full suites use isolated TLS-enabled primary brokers and separate native-delay brokers without test filters. TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-reader-composition-` followed by `initial-net9`, `initial-net10`, `body`, `bypass`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build logs under `/private/tmp/` use the same prefix.

Independent deliveries through a shared default reader are covered by the next checkpoint below. Failure composition, concurrent reuse, native-input immutability, invalid-message forwarding, redelivery and custom republish remain further work.

Both full TRX files confirm two composition cases, eight mixed default-parser cases, four default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases and two legacy-reader cases passed. All 16 mutual-TLS cases passed, together with 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The existing async dispatcher-builder and Reactor dead-letter-queue rejection skips are unchanged.

All 21 production files in the original saved manifest still match. HEAD and the named stash are unchanged. `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async shared default-reader reuse

Two cases exercise sync and async public factories with two consumers sharing one `DefaultRmqWireMessageReader`. Independent native messages carry distinct IDs, bodies, application topics, correlation IDs, subjects, tenant values and persistence modes. Both deliveries are received before content assertions, ensuring later parsing cannot silently overwrite the first result. Assertions require separate message, header and bag objects, then change one tenant value and check that the other remains intact. Each consumer acknowledges its own delivery; native queue checks after disposal confirm settlement.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 2 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 2 passed |
| Cached-message mutation, .NET 9 | Both fail at `Assert.NotSame`: deliveries are the same instance |
| Restored targeted checks | 2 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 414 passed, 2 existing skips, 0 failed; 8 m 50 s |
| Full .NET 10 / RabbitMQ 4.3 | 414 passed, 2 existing skips, 0 failed; 8 m 36 s |

The mutation caches the first parsed result in the public reader and returns it on subsequent calls. Its build succeeds before execution; both cases fail at line 121, the distinct-message assertion. The reader is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-reader-reuse-original.cs`. No production or API change is necessary, so the preceding supported-target builds and Core/AWS regressions remain applicable.

Initial test compilation retains repository analyzer and obsolete-API warnings; the mutation and restored builds pass with zero warnings and errors. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers without test filters. TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-reader-reuse-` followed by `initial-net9`, `initial-net10`, `mutation`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build logs under `/private/tmp/` use the same prefix.

This checkpoint covers sequential reuse across consumers. It does not establish simultaneous-call safety or native-input immutability. Configured default-reader failures are covered by the next checkpoint below. Invalid-message forwarding, redelivery and custom republish also remain further work.

Both full TRX files confirm two shared-reader cases, two composition cases, eight mixed default-parser cases, four default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases and two legacy-reader cases passed. All 16 mutual-TLS cases passed, together with 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The same two existing skips remain.

All 21 production files in the original saved manifest still match. The default reader matches its pre-mutation snapshot, HEAD and the named stash are unchanged, and `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async configured default-reader failures

Eight cases combine sync/async public factories, malformed native `ContentType`/`Type` properties and direct/composed `DefaultRmqWireMessageReader` configuration. Native publication supplies the exact bytes `[0, 255, 128, 65]`, a message ID and an application topic different from the routing key. Failure results retain the unacceptable classification, parsed identity/topic, payload and broker metadata. Rejection settles each delivery, confirmed by native queue inspection after consumer disposal. The composed reader tolerates a failure result with no tenant metadata.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 8 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 8 passed |
| Empty-body mutation, .NET 9 | All eight fail at byte equality: expected `[0, 255, 128, 65]`, actual empty |
| Parser-bypass mutation, .NET 9 | All eight fail at topic equality: expected `application-topic`, actual `legacy-orders` |
| Restored targeted checks | 8 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 422 passed, 2 existing skips, 0 failed; 8 m 57 s |
| Full .NET 10 / RabbitMQ 4.3 | 422 passed, 2 existing skips, 0 failed; 9 m 9 s |

Both mutation builds succeed before execution. The empty-body mutation fails at line 126; throwing instead of invoking the parser fails at line 125 because consumer fallback uses the broker routing key. The reader is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-configured-failure-original.cs`. No production or API change is necessary, so preceding supported-target builds and Core/AWS regressions remain applicable.

Initial test compilation retains repository analyzer and obsolete-API warnings; mutation and restored builds have zero warnings and errors. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers without filters. TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-configured-failure-` followed by `initial-net9`, `initial-net10`, `body`, `bypass`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and mutation execution logs under `/private/tmp/` use the same prefix.

Overlapping reads and native-input preservation are covered by the next checkpoint below. Invalid-message forwarding, redelivery and custom republish remain further work.

Both full TRX files confirm eight configured-default failure cases, two shared-reader cases, two composition cases, eight mixed default-parser cases, four unconfigured default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases and two legacy-reader cases passed. All 16 mutual-TLS cases passed, together with 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The same two existing skips remain.

All 21 production files in the original saved manifest still match. The default reader matches its pre-mutation snapshot, HEAD and the named stash are unchanged, and `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async overlapping default-reader calls

Two cases exercise sync/async public consumer factories with one shared composing reader. Independently published messages have distinct content, metadata and persistence modes. Both receive operations run concurrently; a bounded barrier requires both calls to enter the composing reader before default parsing. The helper retains native deliveries for inspection. Assertions verify independent parsed messages, headers and bags, unchanged native bodies/properties/headers and selective renaming of only the parsed tenant key. Each consumer acknowledges its own result, followed by native queue inspection after disposal.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 2 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 2 passed |
| Locked-message-cache mutation, .NET 9 | Both fail at `Assert.NotSame`: results are the same message instance |
| Native-header-removal mutation, .NET 9 | Both fail at the native `tenant-id` presence assertion |
| Restored targeted checks | 2 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 424 passed, 2 existing skips, 0 failed; 8 m 53 s |
| Full .NET 10 / RabbitMQ 4.3 | 424 passed, 2 existing skips, 0 failed; 8 m 42 s |

Both mutation builds succeed before execution. The first caches a parsed message under a lock and fails at line 126. The second removes the native tenant header before parsing and fails at line 135. The reader is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-concurrent-reader-original.cs`. No production or API change is necessary, so preceding supported-target builds and Core/AWS regressions remain applicable.

Initial and mutation builds retain repository analyzer and obsolete-API warnings. The restored build has zero warnings and errors. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers without filters. TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-concurrent-reader-` followed by `initial-net9`, `initial-net10`, `cache`, `input`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and mutation execution logs under `/private/tmp/` use the same prefix.

The barrier establishes overlapping composition calls and concurrent access to the shared reader; it does not prescribe instruction-level interleaving inside the default parser. Invalid-message forwarding for throwing/null readers is covered by the next checkpoint below. Forwarding failure recovery, redelivery and custom republish remain further work.

Both full TRX files confirm two overlapping-reader cases, eight configured-default failure cases, two shared-reader cases, two composition cases, eight mixed default-parser cases, four unconfigured default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases and two legacy-reader cases passed. All 16 mutual-TLS cases passed, together with 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The same two existing skips remain.

All 21 production files in the original saved manifest still match. The default reader matches its pre-mutation snapshot, HEAD and the named stash are unchanged, and `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async invalid forwarding of reader failures

Four cases combine sync/async public factories and throwing/null readers. Native publication supplies exact bytes `[0, 255, 128, 65]` with a stable message ID. An independently declared invalid queue receives one copy when the mapped failure is rejected as unacceptable. Native reads verify the forwarded bytes, identity, persistence, destination and rejection metadata, while the source queue is empty after consumer disposal. The original message topic is restored after forwarding.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 4 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 4 passed |
| Empty-forwarded-body mutation, .NET 9 | All four fail at forwarded-byte equality: expected `[0, 255, 128, 65]`, actual empty |
| Skipped-acknowledgement mutation, .NET 9 | All four fail at the native source-queue empty assertion |
| Restored targeted checks | 4 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 428 passed, 2 existing skips, 0 failed; 9 m 1 s |
| Full .NET 10 / RabbitMQ 4.3 | 428 passed, 2 existing skips, 0 failed; 9 m 5 s |

Both mutation builds succeed before execution. Emptying the body immediately before invalid publication fails at line 143. Omitting acknowledgement after successful forwarding fails at line 138. The consumer is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-invalid-forwarding-original.cs`. No production or API change is necessary, so preceding supported-target builds and Core/AWS regressions remain applicable.

Initial test compilation retains repository analyzer and obsolete-API warnings. Mutation and restored builds have zero warnings and errors. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers without filters. TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-invalid-forwarding-` followed by `initial-net9`, `initial-net10`, `body`, `ack`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and mutation execution logs under `/private/tmp/` use the same prefix.

Forwarding-failure recovery is covered by the next checkpoint below. Cancellation, default-parser failure forwarding and custom republish mapping remain further work.

Both full TRX files confirm four invalid-forwarding cases, two overlapping-reader cases, eight configured-default failure cases, two shared-reader cases, two composition cases, eight mixed default-parser cases, four unconfigured default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases and two legacy-reader cases passed. All 16 mutual-TLS cases passed, together with 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The same two existing skips remain.

All 21 production files in the original saved manifest still match. The consumer matches its pre-mutation snapshot, HEAD and the named stash are unchanged, and `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async reader-failure forwarding recovery

Four cases combine sync/async public factories with throwing/null readers. After native publication and receipt, removing the invalid queue binding makes mandatory forwarding fail with `PublishException`. The original topic and body remain intact. After consumer disposal, native inspection finds the source delivery with exact bytes, stable identity and redelivery state, while the invalid queue stays empty. The test requeues that delivery, repairs the binding and receives through a new consumer. A second rejection forwards one intact copy and settles the source.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 4 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 4 passed |
| Premature-acknowledgement mutation, .NET 9 | All four fail at `Assert.NotNull(retryDelivery)`: the source is lost |
| Missing-topic-restoration mutation, .NET 9 | All four fail at original-topic equality after the failed publish |
| Restored targeted checks | 4 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 432 passed, 2 existing skips, 0 failed; 33 m 4 s including host sleep |
| Initial full .NET 10 / RabbitMQ 4.3 | 431 passed, 2 existing skips, 1 existing quorum-redelivery case failed; 33 m 10 s including host sleep |
| Sleep-inhibited targeted .NET 10 rerun | 10 passed: six existing forwarding-failure cases and four new recovery cases |
| Repeated full .NET 10 / RabbitMQ 4.3 with sleep inhibited | 432 passed, 2 existing skips, 0 failed; 8 m 41 s |

Both mutation builds succeed before execution. Acknowledging before forwarding fails at line 152. Omitting topic restoration in `finally` fails at line 142. The consumer is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-forwarding-recovery-original.cs`. No production or API change is necessary, so preceding supported-target builds and Core/AWS regressions remain applicable.

Initial test compilation retains repository analyzer and obsolete-API warnings. Mutation and restored builds have zero warnings and errors. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers without filters. TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-forwarding-recovery-` followed by `initial-net9`, `initial-net10`, `ack`, `topic`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and mutation execution logs under `/private/tmp/` use the same prefix.

Default-parser failures forwarded with exact bytes and parsed application topic are covered by the next checkpoint below. Cancellation, default-parser forwarding failure recovery and custom republish mapping remain further work.


The initial .NET 10 full run failed the existing `RmqInvalidMessageForwardingFailureTests` case for quorum, async and no cancellation at its post-Nack redelivery-ID assertion (line 129). All four new recovery cases passed. The failed test reports 2.072 seconds of execution, but its wall-clock timestamps span 16:21:39–16:37:13 UTC. macOS power logs show maintenance sleep from 16:21:27 to 16:37:02 UTC within that interval, following an earlier idle sleep. `PullConsumer.DeQueue` uses `DateTime.UtcNow` for its receive deadline, so this interruption can expire the wait before it checks for the redelivery. This is evidence of an environment-induced timeout; no production or existing-test change is made in response.

The original full TRX files are retained. Relevant power events are in `/private/tmp/brighter-4170-rmq-forwarding-recovery-sleep.log`, with broker logs in `...-broker-window.log`. Recovery validation reruns use `caffeinate -is` to inhibit idle/system sleep for the lifetime of each test process. The targeted rerun includes all six existing forwarding-failure cases and all four new recovery cases, followed by a full .NET 10 rerun.


The targeted sleep-inhibited rerun passes all ten cases. The full .NET 10 rerun passes 432 tests with two existing skips in 8 m 41 s. Its TRX is `4170-rmq-forwarding-recovery-repeat-full-net10.trx`; targeted evidence is `4170-rmq-forwarding-recovery-sleep-check-net10.trx`. Future long local regression runs should retain process-scoped sleep inhibition. No assertion, production behavior or existing test was changed to obtain the passing rerun.

The passing .NET 9 full run and .NET 10 full rerun both confirm all four new recovery cases and all six existing forwarding-failure cases passed. They also include four invalid-forwarding cases, two overlapping-reader cases, eight configured-default failure cases, two shared-reader cases, two composition cases, eight mixed default-parser cases, four unconfigured default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases and two legacy-reader cases. All 16 mutual-TLS cases passed, together with 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The same two existing skips remain.

All 21 production files in the original saved manifest still match. The consumer matches its pre-mutation snapshot, HEAD and the named stash are unchanged, and `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async invalid forwarding of default-parser failures

Eight cases combine sync/async public factories, malformed native content-type/type properties and unconfigured/explicit default readers. Native publication supplies exact bytes `[0, 255, 128, 65]`, a stable ID and an application-topic header different from the broker routing key. Rejection forwards one copy to an independently declared invalid queue. Native inspection verifies exact bytes, identity, persistence, destination and rejection metadata. The original-topic header contains the parsed application topic, the source is settled, and the original message topic is restored.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 8 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 8 passed |
| Text-round-trip mutation, .NET 9 | All eight fail at forwarded-byte equality: invalid UTF-8 bytes become replacement characters |
| Broker-topic-substitution mutation, .NET 9 | All eight fail at native original-topic equality: expected `application-topic`, actual `legacy-orders` |
| Restored targeted checks | 8 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 440 passed, 2 existing skips, 0 failed; 9 m 2 s |
| Full .NET 10 / RabbitMQ 4.3 | 440 passed, 2 existing skips, 0 failed; 9 m 3 s |

Both mutation builds succeed before execution. Replacing the body with `new MessageBody(message.Body.Value)` immediately before publication fails at line 152. Substituting `_routingKeys.First().Value` for the original-topic header fails at line 160. The consumer is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-parser-forwarding-original.cs`. No production or API change is necessary, so preceding supported-target builds and Core/AWS regressions remain applicable.

Initial test compilation retains repository analyzer and obsolete-API warnings. Mutation and restored builds have zero warnings and errors. A sandboxed mutation build stalled and was stopped; its rerun with normal SDK access succeeded before testing. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers without filters. Every test execution uses process-scoped `caffeinate -is` sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-parser-forwarding-` followed by `initial-net9`, `initial-net10`, `bytes`, `topic`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-parser-forwarding-` prefix.

Recovery after failed invalid forwarding of default-parser failures is covered by the next checkpoint below. Cancellation and custom republish mapping remain further work.

Both full TRX files confirm all eight new default-parser invalid-forwarding cases passed. They also confirm four custom-reader recovery cases, six existing forwarding-failure cases, four custom-reader invalid-forwarding cases, two overlapping-reader cases, eight configured-default failure cases, two shared-reader cases, two composition cases, eight mixed default-parser cases, four unconfigured default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases and two legacy-reader cases. All 16 mutual-TLS cases passed, together with 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The same two existing skips remain.

All 21 production files in the original saved manifest still match. The consumer matches its pre-mutation snapshot, HEAD and the named stash are unchanged, and `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async default-parser forwarding recovery

Eight cases combine sync/async public factories, malformed native content-type/type properties and unconfigured/explicit default readers. The invalid queue binding is removed after receiving the parser failure. Mandatory forwarding throws `PublishException`, restores the parsed application topic and preserves the body. After consumer disposal, native inspection finds the original source delivery with exact bytes, identity, malformed properties, application-topic header and redelivery state. The invalid queue remains empty.

The test requeues the native delivery, repairs the binding and receives through a new consumer. Parsing again produces an unacceptable message with the application topic and broker metadata. Retrying rejection forwards one intact copy and settles the source. Native observation verifies the forwarded original-topic header and exactly one destination copy.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 8 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 8 passed |
| Premature-acknowledgement mutation, .NET 9 | All eight fail at `Assert.NotNull(retryDelivery)`: the original delivery is lost |
| Missing-topic-restoration mutation, .NET 9 | All eight fail at application-topic equality immediately after failed forwarding |
| Restored targeted checks | 8 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 448 passed, 2 existing skips, 0 failed; 9 m 10 s |
| Full .NET 10 / RabbitMQ 4.3 | 448 passed, 2 existing skips, 0 failed; 8 m 52 s |

Both mutation builds succeed before execution. Moving acknowledgement ahead of forwarding fails at line 161. Removing topic restoration from `finally` fails at line 151. The consumer is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-parser-recovery-original.cs`. No production or API change is necessary, so preceding supported-target builds and Core/AWS regressions remain applicable.

Initial test compilation retains repository analyzer and obsolete-API warnings. Mutation and restored builds have zero warnings and errors. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers without filters. Every test execution uses process-scoped `caffeinate -is` sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-parser-recovery-` followed by `initial-net9`, `initial-net10`, `ack`, `topic`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-parser-recovery-` prefix.

Cancellation before invalid forwarding of custom-reader and default-parser failures is covered by the next checkpoint below. In-flight cancellation semantics and custom republish mapping remain further work.

Both full TRX files confirm all eight new default-parser recovery cases passed. They also confirm eight default-parser invalid-forwarding cases, four custom-reader recovery cases, six existing forwarding-failure cases, four custom-reader invalid-forwarding cases, two overlapping-reader cases, eight configured-default failure cases, two shared-reader cases, two composition cases, eight mixed default-parser cases, four unconfigured default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases and two legacy-reader cases. All 16 mutual-TLS cases passed, together with 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The same two existing skips remain.

All 21 production files in the original saved manifest still match. The consumer matches its pre-mutation snapshot, HEAD and the named stash are unchanged, and `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async cancellation before invalid forwarding

Six cases cover throwing/null custom readers and malformed content-type/type properties through unconfigured and explicit default readers. A separate token is cancelled before `RejectAsync` starts, while the invalid queue remains bound. Rejection reports `OperationCanceledException` and retains the original message topic and body. Native inspection after consumer disposal finds the source delivery with exact bytes, identity, original properties, application-topic header and redelivery state; the invalid queue is empty.

The test requeues the native delivery and receives through a new consumer. Retrying rejection with an uncancelled token forwards exactly one intact copy and settles the source. Custom-reader failures retain the broker routing key; default-parser failures retain the parsed application topic.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 6 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 6 passed |
| Ignored-cancellation mutation, .NET 9 | All six fail at the expected-cancellation assertion: forwarding returns successfully |
| Premature-acknowledgement mutation, .NET 9 | All six fail at `Assert.NotNull(retryDelivery)`: the original delivery is lost |
| Restored targeted checks | 6 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 454 passed, 2 existing skips, 0 failed; 8 m 55 s |
| Full .NET 10 / RabbitMQ 4.3 | 454 passed, 2 existing skips, 0 failed; 8 m 56 s |

Both mutation builds succeed before execution. Replacing the supplied token with `CancellationToken.None` fails at line 146. Acknowledging with an uncancelled token before broker validation fails at line 157. The consumer is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-cancellation-original.cs`. No production or API change is necessary, so preceding supported-target builds and Core/AWS regressions remain applicable.

Initial compilation retains repository analyzer and obsolete-API warnings. Mutation and restored builds have zero warnings and errors. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers without filters. Every test execution uses process-scoped `caffeinate -is` sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-cancellation-` followed by `initial-net9`, `initial-net10`, `token`, `ack`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-cancellation-` prefix.

These results cover cancellation requested before rejection starts, not atomic cancellation during publication. A confirmed forward followed by failed acknowledgement can still cause a duplicate on redelivery. Custom immediate requeue mapping with gateway-owned retry controls is covered by the next checkpoint below.

Both full TRX files confirm all six new cancellation cases passed. They also confirm eight default-parser recovery cases, eight default-parser invalid-forwarding cases, four custom-reader recovery cases, six existing forwarding-failure cases, four custom-reader invalid-forwarding cases, two overlapping-reader cases, eight configured-default failure cases, two shared-reader cases, two composition cases, eight mixed default-parser cases, four unconfigured default-parser failure cases, eight mixed custom-reader cases, four custom-reader failure cases and two legacy-reader cases. All 16 mutual-TLS cases passed, together with 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The same two existing skips remain.

All 21 production files in the original saved manifest still match. The consumer matches its pre-mutation snapshot, HEAD and the named stash are unchanged, and `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async subscription-selected immediate requeue mapping

`RmqSubscription.RequeueWireMessageWriter` selects the writer used by direct consumer retry publication. Both public factories capture the selection; existing public consumer constructors retain their signatures and default behavior. The publisher uses the selected writer, then stamps source persistence, handled count, a generated retry ID, retry delivery/delay metadata and the original ID from the source. An earlier original ID remains intact even when a complete custom writer omits it. The native header dictionary is created if the writer leaves it null.

Four cases combine sync/async factories and first/subsequent retries. An independent native publisher supplies the source, then native reads inspect exactly one custom retry copy after consumer disposal. Assertions cover custom body/type/content type/tenant metadata, gateway controls, original settlement and an unchanged source message and bag.

| Check | Result |
| --- | --- |
| Initial compile | CS0117 on both frameworks: missing `RequeueWireMessageWriter` |
| Configuration-only stage, .NET 9 | All four fail at native retry-body equality because requeue still uses default mapping |
| Implemented .NET 9 / RabbitMQ 4.2 | 4 passed |
| Implemented .NET 10 / RabbitMQ 4.3 | 4 passed |
| Ignored-writer mutation, .NET 9 | All four fail at native retry-body equality |
| Missing-persistence mutation, .NET 9 | All four fail at native delivery-mode equality: expected persistent, actual transient |
| Restored targeted checks | 4 passed on each framework |
| Gateway netstandard2.0 / net8.0 | Both build with zero warnings and errors |
| Full .NET 9 / RabbitMQ 4.2 | 458 passed, 2 existing skips, 0 failed; 9 m 1 s |
| Initial full .NET 10 / RabbitMQ 4.3 | 457 passed, 2 existing skips, 1 existing concurrent-attachment test failed; 8 m 56 s |
| Attachment test and new retry cases, .NET 10 / RabbitMQ 4.3 | 5 passed together on targeted repeat |
| Isolated attachment repetitions, .NET 10 / RabbitMQ 4.2 | All 10 passed on the idle primary broker |
| Repeated full .NET 10 / RabbitMQ 4.3 | 458 passed, 2 existing skips, 0 failed; 9 m 6 s |

Both mutation builds succeed before execution. Bypassing the writer fails at line 134; omitting source persistence fails at line 137. The four changed production files match the implemented snapshots in `/private/tmp/brighter-4170-rmq-requeue-green/` after restoration. Pre-increment snapshots are in `/private/tmp/brighter-4170-rmq-requeue-before/`.

Initial compilation fails only for the missing setting. Subsequent test compilation retains repository analyzer and obsolete-API warnings. Mutation, restored and compatibility builds have zero warnings and errors. Full suites use isolated TLS-enabled primary brokers and independent native-delay brokers without filters; every execution uses process-scoped `caffeinate -is` sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-requeue-` followed by `default-red`, `green-net9`, `green-net10`, `writer`, `persistence`, `restored-net9`, `restored-net10`, `full-net9`, `full-net10`, `attach-repeat-net10`, `attach-stress-1` through `attach-stress-10`, or `repeat-full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-requeue-` prefix.

This increment verifies immediate requeue. Native delayed requeue shares the direct publication path and has a new review draft outside the compiled project. Scheduler-backed retry retains its existing publication mapping. Writer failures, reserved-header collisions, subscription isolation and custom invalid forwarding remain further work. Core and AWS implementation files are unchanged, so their preceding full regressions remain applicable.

The initial .NET 10 full run failed in `RmqConcurrentAttachAsyncTests.When_sends_attach_to_a_new_channel_concurrently_should_subscribe_once`: line 70 expected one acknowledgement subscriber and observed zero. The test, its two attachment wrappers and `RmqMessageGateway` match HEAD; `RmqMessageProducer` matches the saved pre-increment manifest. This test exercises producer sends, not consumer retry publication. Inspection found an unsynchronized test-wrapper installation after the base connection method publishes the channel, making a wrapper race plausible; this is not a proven root cause. The existing test and all four new cases passed together on targeted repeat against RabbitMQ 4.3. Ten additional isolated runs of that existing test passed on .NET 10 against the idle RabbitMQ 4.2 primary broker. These repetitions do not prove the suspected cause or rule out a concurrency problem. The original failure remains recorded; no test, wrapper or producer code was changed to obtain the repeat result.

The full .NET 10 repeat passes all 458 tests, including the existing attachment test and all four new retry cases. Both passing full TRX files contain all expected reader, failure-forwarding, cancellation and retry groups, all 16 mutual-TLS cases, 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The same two existing skips remain. The initial attachment failure is intermittent in these runs; its root cause remains unconfirmed.

The four changed production files match their restored GREEN snapshots byte-for-byte. Twenty of the original manifest’s 21 production files still match; the publisher differs by the approved retry mapping changes. Other current changes are in files already changed or added after that original stash. HEAD and the named stash are unchanged, and `git diff --check` passes. No commits or pushes were made.


## RabbitMQ Async custom requeue mapping with native delay

Four cases combine sync/async factories with first and subsequent retries. A native publisher sends to two queues bound to one application topic. The consumer requests a three-second retry delay while its configured writer supplies a sixty-second delay header. Independent native reads verify custom content, gateway persistence and retry metadata, stable original identity, an unchanged source message, and exactly one retry in the original queue. The sibling receives no retry.

The delayed-message plugin reports a negative three-second header, and bounded polling observes delivery no earlier than the requested delay. Public consumer creation establishes the queue-specific retry topology. The test uses the existing native-delay collection and broker environment.

| Check | Result |
| --- | --- |
| Initial .NET 9 / native-delay broker | 4 passed |
| Initial .NET 10 / native-delay broker | 4 passed |
| Ignored-writer mutation, .NET 9 | All four fail at native retry-body equality, line 159 |
| Missing-delay-header mutation, .NET 9 | All four fail at native delay-header presence, line 174 |
| Restored targeted checks | 4 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 462 passed, 2 existing skips, 0 failed; 9 m 28 s |
| Full .NET 10 / RabbitMQ 4.3 | 462 passed, 2 existing skips, 0 failed; 9 m 6 s |

Both mutation builds succeed before execution. The publisher is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-requeue-delay-original.cs`. No production change is necessary; preceding supported-target gateway builds and Core/AWS regressions remain applicable. Initial compilation retains the existing analyzer and obsolete-API warnings. Mutation and restored builds have zero warnings and errors.

Full suites use isolated TLS-enabled primary brokers and independent RabbitMQ 4.2.6 native-delay plugin brokers. No full-suite filters are applied. Every execution uses process-scoped `caffeinate -is` sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-requeue-delay-` followed by `initial-net9`, `initial-net10`, `writer`, `header`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-requeue-delay-` prefix.

The next review draft covers immediate retry writer failures and recovery. Scheduler-backed retry, reserved-header collisions, subscription isolation and custom invalid-message forwarding remain further work.

Both full TRX files confirm all four new custom delayed-requeue cases and four immediate-requeue cases passed. They also confirm all expected reader, parser-failure, forwarding, recovery and cancellation groups, all 16 mutual-TLS cases, 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The existing concurrent-attachment test passes in both full runs. The same two existing skips remain. No full-run retry was needed at this checkpoint.

The publisher matches its pre-mutation snapshot, and all four implementation files from the prior immediate-requeue increment match their restored GREEN snapshots. HEAD and the named stash are unchanged. `git diff --check` and whitespace checks for the new test and review drafts pass. No commits or pushes were made.


## RabbitMQ Async immediate retry writer-failure recovery

Four cases combine sync/async public factories with a throwing writer or an invalid null writer result. A native publisher supplies a persistent source message. Failed requeue reports false and leaves the source message and bag unchanged. After consumer disposal, independent native inspection finds the original redelivered message with exact bytes, ID, routing key, properties and native handled count. No extra copy has been published.

The native reader requeues the original. After writer recovery, a new consumer receives it and successfully republishes one custom retry. Native inspection verifies custom content, generated retry identity, original-ID metadata, persistence, handled count and delivery marker. The source queue is then empty.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 4 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 4 passed |
| Premature-acknowledgement mutation, .NET 9 | All four fail at original native delivery presence, line 139 |
| False-success mutation, .NET 9 | All four fail at expected false requeue result, line 126 |
| Restored targeted checks | 4 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 466 passed, 2 existing skips, 0 failed; 9 m 26 s |
| Full .NET 10 / RabbitMQ 4.3 | 466 passed, 2 existing skips, 0 failed; 9 m 4 s |

Both mutation builds succeed before execution. Acknowledging immediately before the publication branch loses the original when mapping fails. Returning true from the exception handler incorrectly reports successful requeue. The consumer is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-writer-recovery-original.cs`. No production change is needed; preceding supported-target builds and Core/AWS regressions remain applicable.

Initial compilation retains existing analyzer and obsolete-API warnings. Mutation and restored builds have zero warnings and errors. Full suites use isolated TLS-enabled primary brokers and independent native-delay plugin brokers without full-suite filters. Every execution uses process-scoped `caffeinate -is` sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-writer-recovery-` followed by `initial-net9`, `initial-net10`, `ack`, `success`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-writer-recovery-` prefix.

These cases cover immediate mapping failures before publication. They do not establish atomic publication and acknowledgement or delayed/scheduler-backed failure behavior. Null remains an invalid writer result. Canonical retry-header collisions have a new review draft outside the compiled project.

Both full TRX files confirm all four new writer-recovery cases, four immediate retry mapping cases and four custom delayed-retry cases passed. They also confirm all expected reader, parser-failure, forwarding, recovery and cancellation groups, all 16 mutual-TLS cases, 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The existing concurrent-attachment test passes on both frameworks. The same two existing skips remain. Both first full runs pass.

The consumer matches its pre-mutation snapshot. The four implementation files from the immediate-requeue increment still match their restored GREEN snapshots. Twenty original production files match the saved manifest; the publisher retains only the previously approved retry changes relative to that manifest. HEAD and the named stash are unchanged. Whitespace checks and `git diff --check` pass. No commits or pushes were made.


## RabbitMQ Async canonical retry-header precedence

Eight cases combine sync/async public factories, first/subsequent retries and immediate/native delayed delivery. The configured writer supplies handled count 999, an invented original ID, delivery marker 999 and a sixty-second delay, plus custom application content, native ID and transient delivery mode. The gateway must retain custom content while supplying the real handled count, original-ID chain, delivery marker, requested delay, generated retry ID and persistence.

Independent native inspection verifies no delay header for immediate retries and the plugin's negative three-second header for delayed retries. Delayed delivery is not observed before the requested duration. Both modes publish exactly one copy to the original queue, leave a sibling queue empty and preserve the source message and bag.

| Check | Result |
| --- | --- |
| Initial .NET 9 / native-delay broker | 8 passed |
| Initial .NET 10 / native-delay broker | 8 passed |
| Missing-handled-count mutation, .NET 9 | All eight fail at native handled-count equality: expected 3, actual 999; line 174 |
| Missing-original-ID mutation, .NET 9 | All eight fail at native original-ID equality: the writer's invented ID remains; line 176 |
| Restored targeted checks | 8 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 474 passed, 2 existing skips, 0 failed; 9 m 34 s |
| Full .NET 10 / RabbitMQ 4.3 | 474 passed, 2 existing skips, 0 failed; 9 m 25 s |

Both mutation builds succeed before execution. The publisher is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-retry-headers-original.cs`. No production change is needed; preceding supported-target builds and Core/AWS regressions remain applicable. Initial compilation retains existing analyzer and obsolete-API warnings. Mutation and restored builds have zero warnings and errors.

Full suites use isolated TLS-enabled primary brokers and independent native-delay plugin brokers without full-suite filters. Every execution uses process-scoped `caffeinate -is` sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-retry-headers-` followed by `initial-net9`, `initial-net10`, `count`, `original-id`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-retry-headers-` prefix.

This coverage uses canonical header names. Code inspection found that original-ID source lookup is case-insensitive, but assignment does not clear other writer-supplied case variants. A new draft specifies one authoritative original-ID entry while retaining the source spelling and value. That expected regression has not been executed. Other case-variant controls, delayed writer failures, scheduler-backed retry, subscription isolation and custom invalid forwarding remain further work.

Both full TRX files confirm all eight new canonical-header cases, four writer-recovery cases, four immediate retry mapping cases and four custom delayed-retry cases passed. They also confirm all expected reader, parser-failure, forwarding, recovery and cancellation groups, all 16 mutual-TLS cases, 19 direct native-delay cases, 42 classic and 42 quorum conformance cases, and two custom-writer native-delay cases. The existing concurrent-attachment test passes on both frameworks. The same two existing skips remain. Both first full runs pass.

The publisher matches its pre-mutation snapshot. The four implementation files from the immediate-requeue increment still match their restored GREEN snapshots. Twenty original production files match the saved manifest; the publisher retains only the previously approved retry changes relative to that manifest. HEAD and the named stash are unchanged. New-file whitespace checks and `git diff --check` pass. No commits or pushes were made.


## RabbitMQ Async original-ID header case variants

Four cases combine sync/async public factories with first/subsequent immediate retries. The writer supplies conflicting canonical and uppercase original-ID headers. For a subsequent retry, the native source carries an uppercase original-ID header with the real original value. Native inspection requires exactly one original-ID entry, retaining the source spelling and value or using the canonical name and source ID for a first retry.

The initial run reproduces duplicate entries in every case. The publisher now finds the source original ID and removes writer-supplied variants using ordinal case-insensitive comparison. Matching native keys are snapshotted before removal. Existing source spelling/value and first-retry fallback remain intact. No public API changes are required.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | All four fail at native single-original-ID assertion: two entries remain; line 150 |
| Implemented .NET 9 / RabbitMQ 4.2 | 4 passed |
| Implemented .NET 10 / RabbitMQ 4.3 | 4 passed |
| Cleanup-omission mutation, .NET 9 | All four fail at native single-original-ID assertion: two entries remain; line 149 |
| Restored targeted checks | 4 passed on each framework |
| Gateway netstandard2.0 / net8.0 / net9.0 / net10.0 | All build; zero warnings and errors |
| Full .NET 9 / RabbitMQ 4.2 | 478 passed, 2 existing skips, 0 failed; recorded wall time 9 h 30 m 38 s |
| Full .NET 10 / RabbitMQ 4.3 | 478 passed, 2 existing skips, 0 failed; recorded wall time 9 h 30 m 44 s |

The initial test uses `Assert.Single(headers.Where(...))` and triggers xUnit2031. It is replaced with the equivalent predicate overload, removing the added warning and unused LINQ import. The expected native uniqueness assertion remains unchanged in meaning; the named mutation proves it still detects the same defect. Subsequent test builds retain the pre-existing analyzer and obsolete-API warnings. Every build succeeds before its corresponding execution.

The cleanup mutation is restored byte-for-byte to `/private/tmp/brighter-4170-rmq-original-id-case-green.cs`. The pre-increment publisher is preserved in `/private/tmp/brighter-4170-rmq-original-id-case-before.cs`. Core and AWS files are unchanged, so their preceding full regressions remain applicable.

Full suites use isolated TLS-enabled primary brokers and independent native-delay plugin brokers without full-suite filters. Every execution uses process-scoped `caffeinate -is` sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-original-id-case-` followed by `red`, `green-net9`, `green-net10`, `cleanup`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-original-id-case-` prefix, including `all-targets-build` for the gateway compatibility build.

This rule does not normalize other AMQP headers or resolve multiple original-ID entries in the source bag. Subscription retry-writer isolation has a new review draft outside the compiled project. Concurrent mapping, delayed writer failures, scheduler-backed retry and custom invalid forwarding remain further work.

Both first full runs complete successfully. The TRX files confirm all 26 expected coverage groups, including the four new original-ID cases, eight canonical-header cases, writer recovery, immediate and delayed custom retry, reader/parser failure handling, forwarding/recovery/cancellation, mutual TLS and native classic/quorum conformance. The existing concurrent-attachment test passes on both frameworks. The same two existing skips remain.

The TRX timestamps span 2026-10-19 19:43 UTC to 2026-10-20 05:13 UTC despite process-scoped sleep inhibition. The longest recorded individual test takes about 21 seconds on each framework. The cause of the wall-clock discrepancy has not been established; these successful functional runs are not performance evidence.

The publisher matches `/private/tmp/brighter-4170-rmq-original-id-case-green.cs` byte-for-byte. The subscription, consumer and consumer factory still match the three corresponding immediate-requeue GREEN snapshots. Twenty original production files match the saved manifest; the publisher differs by the approved retry mapping and original-ID cleanup changes. HEAD and the named stash are unchanged. `git diff --check` and whitespace checks for this increment and its review drafts pass. A broader untracked-file scan finds five pre-existing trailing-whitespace lines in the saved AWS default writers; those files remain byte-identical to the manifest. All four task-specific brokers are stopped. No commits or pushes were made.


## RabbitMQ Async subscription retry-writer isolation

Four cases combine both factory modes and consumer-creation orders. Both consumers are created through one factory before either requeues. One subscription uses default mapping and the other uses the legacy writer. Native inspection verifies distinct body, type, content type, tenant headers and CloudEvents ID behavior, plus gateway retry controls, source immutability and exactly one retry per queue.

The first run fails all four cases on each framework because the draft expects a default body type of `application/json`. Existing `MessageBody` parsing adds `charset=utf-8`, which the default writer preserves. The exact expected type is corrected to `application/json; charset=utf-8`; no production behavior or isolation assertion is changed. These initial failures are test-expectation errors, not evidence of an isolation defect.

| Check | Result |
| --- | --- |
| Corrected .NET 9 / RabbitMQ 4.2 | 4 passed |
| Corrected .NET 10 / RabbitMQ 4.3 | 4 passed |
| Shared-consumer-writer mutation, .NET 9 | All four fail at native body equality, line 155 |
| Restored targeted checks | 4 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 482 passed, 2 existing skips, 0 failed; 9 m 36 s |
| Full .NET 10 / RabbitMQ 4.3 | 482 passed, 2 existing skips, 0 failed; 9 m 21 s |

The named mutation replaces the per-instance readonly writer field with a static field, retaining constructor assignment. The last-created consumer then selects both consumers' mapping. Both creation orders detect incorrect native content for the first-created consumer. The consumer is restored byte-for-byte from `/private/tmp/brighter-4170-rmq-isolation-original.cs` before rebuilding and rerunning. No production change is needed; preceding supported-target builds and Core/AWS full regressions remain applicable.

Initial and corrected test builds retain the existing 2,308 analyzer/obsolete-API warnings and have no errors. Mutation and restored builds have zero warnings and errors. Every execution follows a successful build. Full suites use isolated TLS-enabled primary brokers and independent native-delay plugin brokers, with no filters and process-scoped sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-isolation-` followed by `initial-net9`, `initial-net10`, `corrected-net9`, `corrected-net10`, `mutation`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-isolation-` prefix.

These tests establish configuration isolation across consumers; the requeue operations themselves are sequential. Concurrent retry mapping remains separate work. Native delayed writer-failure recovery has a new review draft outside the compiled project.

Both first full runs pass. Their TRX files confirm all 27 expected coverage groups, including all four subscription-isolation cases, original-ID case variants, canonical retry controls, custom immediate/delayed mapping, writer recovery, reader/parser failures, forwarding/recovery/cancellation, mutual TLS and native classic/quorum conformance. The existing concurrent-attachment test passes on both frameworks. The same two existing skips remain.

The consumer matches its pre-mutation snapshot. The subscription, consumer and consumer factory retain their earlier immediate-requeue GREEN snapshots; the publisher retains its original-ID case-fix GREEN snapshot. Twenty original production files match the saved manifest, and the publisher retains only the earlier approved retry changes relative to that manifest. HEAD and the named stash are unchanged. Current-increment whitespace checks and `git diff --check` pass. No commits or pushes were made.


## RabbitMQ Async native delayed retry writer-failure recovery

Four cases combine both public factory paths with throwing and invalid null-returning writers, using the native delayed-message plugin and a three-second retry delay. Failed requeue returns false and leaves the source message and bag unchanged. After consumer disposal, independent native inspection verifies the original redelivered payload, identity, routing, persistence and handled count. With that original held unacknowledged, the queue remains empty through a 3.5-second observation window.

After the native reader requeues the original and the writer recovers, a new consumer publishes one custom delayed retry and settles the original. Native inspection verifies the requested delay, plugin's negative delay header, custom content, generated identity, original-ID metadata, gateway controls and source immutability.

| Check | Result |
| --- | --- |
| Initial .NET 9 / native-delay broker | 4 passed |
| Initial .NET 10 / native-delay broker | 4 passed |
| Premature-acknowledgement mutation, .NET 9 | All four fail at original native delivery presence, line 145 |
| False-success mutation, .NET 9 | All four fail at expected false requeue result, line 132 |
| Restored targeted checks | 4 passed on each framework |
| Full .NET 9 / RabbitMQ 4.2 | 486 passed, 2 existing skips, 0 failed; 10 m 5 s |
| Full .NET 10 / RabbitMQ 4.3 | 486 passed, 2 existing skips, 0 failed; 9 m 40 s |

Both mutation builds succeed before execution. The consumer is restored byte-for-byte from `/private/tmp/brighter-4170-rmq-delayed-recovery-original.cs`. No production change or test correction is needed. Prior supported-target builds and Core/AWS full regressions remain applicable. Initial compilation retains 2,308 existing analyzer/obsolete-API warnings and has no errors; mutation and restored builds have zero warnings and errors.

Full suites use isolated TLS-enabled primary brokers and independent native-delay plugin brokers, without filters and with process-scoped sleep inhibition. TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-delayed-recovery-` followed by `initial-net9`, `initial-net10`, `ack`, `success`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-delayed-recovery-` prefix.

The finite observation window detects unexpected deliveries at the requested delay; it does not establish the absence of arbitrarily late duplicates. Publication and acknowledgement remain non-atomic. Scheduler-backed recovery is not covered. Overlapping retry mapping has a new test and helper draft outside the compiled project.

Both first full runs pass. Their TRX files confirm all 28 expected coverage groups, including the four native delayed recovery cases, subscription isolation, original-ID case variants, canonical retry controls, custom immediate/delayed mapping, immediate writer recovery, reader/parser failures, forwarding/recovery/cancellation, mutual TLS and native classic/quorum conformance. The existing concurrent-attachment test passes on both frameworks. The same two existing skips remain.

The consumer matches its pre-mutation snapshot. The subscription, consumer and consumer factory retain their immediate-requeue GREEN snapshots; the publisher retains its original-ID case-fix GREEN snapshot. Twenty original production files still match the saved manifest; the publisher differs only by earlier approved retry changes. HEAD and the named stash are unchanged. Current-increment whitespace checks and `git diff --check` pass. No commits or pushes were made.


## RabbitMQ Async overlapping retry outputs

Two cases exercise sync and async public factory paths. Two consumers share a composing default writer and receive different source bodies, identities, tenants, correlations, subjects, persistence and handled counts. Each maps through the shared default writer under a lock, then waits at a barrier outside that lock. Both results therefore exist before either callback returns to publication.

Native inspection verifies each queue's independent payload and metadata, distinct generated retry IDs, original-ID chain, delivery marker, handled count and persistence. Both originals are settled, each queue contains one retry and the source messages and bags remain unchanged. The converter itself executes serially inside the helper; these checks establish overlapping output lifetimes, not simultaneous converter execution.

| Check | Result |
| --- | --- |
| Initial .NET 9 / RabbitMQ 4.2 | 2 passed |
| Initial .NET 10 / RabbitMQ 4.3 | 2 passed |
| Cached-first-body mutation, .NET 9 | Both fail at native body equality, line 151 |
| Restored targeted checks | 2 passed on each framework |
| Initial full .NET 9 / RabbitMQ 4.2 | 487 passed, 2 existing skips, 1 collision-test failure; 10 m 15 s |
| Initial full .NET 10 / RabbitMQ 4.3 | 487 passed, 2 existing skips, 1 original-ID delivery-presence failure; 10 m 9 s |
| Ten focused repetitions per framework | 70 passed per framework, no failures |
| Full repeat .NET 9 / RabbitMQ 4.2 | 488 passed, 2 existing skips, 0 failed; 10 m 27 s |
| Full repeat .NET 10 / RabbitMQ 4.3 | 488 passed, 2 existing skips, 0 failed; 10 m |

The named mutation adds a nullable body field to the default writer and reuses the first input body while mapping properties and headers normally. Each case detects the wrong payload in one queue. The approved test and helper are unchanged. The writer is restored byte-for-byte from `/private/tmp/brighter-4170-rmq-overlap-original.cs` before rebuilding and rerunning. No production change is needed; previous supported-target builds and Core/AWS full regressions remain applicable.

All three builds succeed with 2,308 existing analyzer/obsolete-API warnings and no errors. Every execution follows a successful build. Full suites use isolated TLS-enabled primary brokers and independent native-delay plugin brokers without filters, with process-scoped sleep inhibition.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use `4170-rmq-overlap-` followed by `initial-net9`, `initial-net10`, `mutation`, `restored-net9`, `restored-net10`, `full-net9` or `full-net10`. Build and execution logs under `/private/tmp/` use the `brighter-4170-rmq-overlap-` prefix.

The remaining specification includes simultaneous converter execution, further reserved-header semantics, scheduler-backed retry and custom invalid forwarding, other gateway integrations, reader design documentation and final compatibility review. This increment does not complete the global feature.


The first full .NET 9 run reports an existing `RmqCollidingDeliveryTagAsyncTests` failure: line 72 expects no confirmations, but one successful confirmation is present. The run is retained as `full-net9`; it is not overwritten. The test, `GatedTagReadRmqProducer`, `GatedTagReadRmqChannel` and base gateway match HEAD byte-for-byte. The producer matches the saved feature-state manifest. This test passed in both preceding delayed-recovery full runs and the earlier .NET 9 isolation run.

Code inspection identifies possible scheduling windows: the wrapper is installed after the base connection method releases its lock, and its gate waits for both sequence-number reads rather than both pending-confirmation registrations. Neither observation proves which path caused this failure. Both full repeats pass after ten passing focused runs per framework. No fixture or production change has been made to suppress the failure.

The first .NET 10 full run separately fails one original-ID case (`useAsync: true`, `hasOriginalId: false`) at line 135: the immediate native read after consumer disposal returns null. The other three cases pass, as do both new overlapping-retry cases on both frameworks. All four original-ID cases passed in the previous three full .NET 10 checkpoints. The consumer, publisher, case test and helper are unchanged in this increment. The cause of the empty read is unresolved; channel-close/redelivery timing is a possibility, not an established diagnosis.

Without changing code or rebuilding, ten fresh focused test-process runs per framework execute the collision class, all four original-ID cases and both overlap cases. Every run passes: 70 executions per framework, 140 total. TRX names are `4170-rmq-overlap-focused-net9-1` through `-10` and the equivalent `net10` names. The unfiltered repeats use `4170-rmq-overlap-full-repeat-net9` and `net10`, preserving both initial failed runs. The passing repeats do not establish the causes of the earlier failures or eliminate their intermittent risk.

Both unfiltered repeats pass. Their TRX files confirm all 30 checked coverage groups, including the overlap cases, both previously failing classes, concurrent attachment, subscription isolation, original-ID variants, canonical retry controls, immediate/delayed writer recovery, reader/parser failure handling, forwarding/recovery/cancellation, mutual TLS and native classic/quorum conformance. The same two existing skips remain. The two initial failures remain tracked as unresolved follow-up work; this is not a production-readiness claim.

The default writer matches `/private/tmp/brighter-4170-rmq-overlap-original.cs` byte-for-byte. The earlier subscription, consumer and factory GREEN snapshots and original-ID publisher snapshot remain intact. Twenty original production files match the saved manifest; the publisher retains only earlier approved retry changes relative to that manifest. Both compiled files match their approved review copies. HEAD and the named stash are unchanged. Current whitespace checks and `git diff --check` pass. No commits or pushes were made.

## RabbitMQ fixture synchronization and retry observation

The four approved drafts in `review/stabilization/` are copied unchanged into the
compiled project. The collision fixture prepares the channel before concurrent
sends and gates native publication until both pending confirmations have been
registered. Both gates fail on timeout. The test verifies two equal sequence
numbers before asserting that neither message was confirmed.

The original-ID fixture polls the independent native reader every 25 ms for up
to five seconds, within its existing overall cancellation deadline. It records
empty reads, elapsed time and redelivery status. All previous payload, property,
identity, retry-control, source-state and duplicate assertions remain.

| Check | Result |
| --- | --- |
| Initial five cases, .NET 9 and .NET 10 | 5 passed per framework |
| Replace collision removal with last-confirmation assignment, .NET 9 | 1 failed at `Assert.Empty(_confirmed)` |
| Omit original-ID cleanup, .NET 9 | 4 failed at the single-original-ID assertion |
| Omit native retry publication, .NET 9 | 4 failed at retry presence after 5 seconds |
| Restored focused runs, ten fresh processes per framework | 140 passed, 0 failed; collision, original-ID and overlapping retry cases included |
| Full .NET 9 / RabbitMQ 4.2 | 488 passed, 2 existing skips, 0 failed; approximately 10 m 17 s |
| Full .NET 10 / RabbitMQ 4.3 | 488 passed, 2 existing skips, 0 failed; approximately 9 m 45 s |

The missing-publication mutation produces 174–177 empty reads per case and then
fails at `Assert.NotNull(retry)`. The source's successful-return/acknowledgement
path still executes. Bounded polling therefore does not turn a lost retry into
a passing test. Removing original-ID cleanup still exposes two matching headers.
The collision mutation reaches the confirmation assertion without a gate timeout
or sequence-number assertion failure.

All 96 successful original-ID executions across initial, focused and full runs
report zero empty reads and `redelivered=True`. This is consistent with the retry
being released by the disposed consumer, but does not reproduce the historical
empty first read. The exact historical timing remains unconfirmed. The collision
fixture's missing synchronization is established by its control flow; the new
publish gate makes the intended pending-record collision deterministic.

The initial build succeeds with 2,308 existing analyzer/obsolete-API warnings and
no errors. Mutation builds succeed; two report 1,154 existing warnings each.
The final restored build succeeds with no warnings or errors. Both production
files match the pre-mutation byte snapshots. This increment changes test fixtures
and documentation only; previous supported-target production builds still apply.

Full suites run without filters, on isolated TLS-enabled brokers with independent
native-delay brokers. Both test-name/outcome multisets exactly match the prior
complete passing suites. Process-scoped `caffeinate -is` prevents idle sleep.
The four task-specific brokers are stopped after validation.

TRX files under `/private/tmp/brighter-4170-rmq-results/` use
`4170-stabilization-` followed by `initial-net9`, `initial-net10`, `collision`,
`original-id`, `missing-retry`, `focused-net9-1` through `focused-net9-10`,
`focused-net10-1` through `focused-net10-10`, `full-net9` or `full-net10`.
Build and execution logs under `/private/tmp/` use the
`brighter-4170-stabilization-` prefix. Snapshots are retained under
`/private/tmp/brighter-4170-stabilization-backup/`.

The separate SQS constructor compatibility draft has not been compiled or run.
It remains outside the test project pending review. No commits or pushes were made.


## SQS public constructor source compatibility

The approved test inspects only exported constructors applicable to the existing
three-argument call `new SqsMessageConsumer(connection, queueName, default)`.
It initially finds two candidates and fails at `Assert.Single` on both test
frameworks. A separate external compilation probe reports CS0121 on
netstandard2.0, net8.0, net9.0 and net10.0.

The fix makes only the new reader-taking constructor internal and removes its
internal XML documentation. The original public constructor and both constructor
bodies retain their behavior. The public factory still supplies the configured
reader through `SqsSubscription.WireMessageReader`.

| Check | Result |
| --- | --- |
| Initial compatibility assertion, .NET 9 and .NET 10 | 1 expected failure per framework at `Assert.Single` |
| Initial external legacy-call compilation | CS0121 on all four gateway targets |
| Corrected compatibility assertion plus custom-reader factory cases | 3 passed per framework |
| Re-expose the new overload publicly, .NET 9 | 1 expected failure at `Assert.Single` |
| Restored compatibility assertion plus custom-reader factory cases | 3 passed per framework |
| Corrected external legacy-call compilation | All four targets build; 0 warnings, 0 errors |
| Full AWS V4 Release, .NET 9 | 489 passed, 0 skipped, 0 failed |
| Full AWS V4 Release, .NET 10 | 489 passed, 0 skipped, 0 failed |
| Complete solution, Debug | Build succeeds; 9,646 warnings, 0 errors |
| Complete solution, Release | Build succeeds; 21,947 warnings, 0 errors |
| Three C# examples from the external documentation draft | All four targets build; 0 warnings, 0 errors |

Both full result sets contain every previously passing AWS case plus the new
compatibility test. There are no filters on the full runs. The test file matches
the approved draft. The production file matches its corrected GREEN snapshot
after the named mutation is removed.

The initial and corrected test builds each report 2,426 warnings and no errors.
The single-framework mutation build reports 1,213 warnings and no errors. The
first solution build with `--no-restore` fails because some projects lack assets,
one old assets file records a NuGet resolution error, and the net462 reference
assemblies are unavailable. `dotnet restore Brighter.slnx` succeeds, then both
complete solution builds pass without changing dependency declarations or
unrelated source files. These successful builds include affected dependents and
their configured target frameworks.

Full AWS suites run against Floci 1.5.19 at the explicit local endpoint
`http://localhost.localstack.cloud:14566`. Credentials are local dummy values,
and the existing test helper gives each process its own account namespace.
Each execution uses process-scoped idle-sleep inhibition. The task emulator is
stopped after validation. Core and RabbitMQ production files are unchanged by
this constructor correction; their preceding full regressions remain applicable.

Logs, source snapshots, external compilation probes and extracted documentation
examples are under `/private/tmp/brighter-4170-sqs-compatibility/`. TRX files in
the AWS V4 test project's ignored `TestResults/` directory use
`4170-sqs-compatibility-` followed by `red-net9.0`, `red-net10.0`, `green-net9.0`,
`green-net10.0`, `mutation`, `restored-net9.0`, `restored-net10.0`, `full-net9.0`
or `full-net10.0`.

At completion of the constructor correction, the RabbitMQ confirmation-header
test was still outside compiled projects pending review. It was subsequently
approved; its validation follows below. Native scheduler mapping and the other
checks in the release review remain outstanding. Nothing was committed or pushed.

## RabbitMQ publish-sequence ownership

The approved test and helper were copied unchanged into the RabbitMQ Async test
project. Both public producer-factory paths publish a custom legacy message whose
writer sets the SDK confirmation header to `writer-sequence`. Independent native
observation sees the SDK's first sequence number as an AMQP long, while the
confirmation identifies the original Brighter message and topic.

| Check | Result |
| --- | --- |
| Initial cases, .NET 9 and .NET 10 | 2 passed per framework |
| Disable only SDK confirmation tracking, .NET 9 | 2 expected failures at the native header-type assertion |
| Restore the gateway, .NET 9 and .NET 10 | 2 passed per framework |
| Full RabbitMQ Async Release, .NET 9 | 490 passed, 2 existing skips, 0 failed |
| Full RabbitMQ Async Release, .NET 10 | 490 passed, 2 existing skips, 0 failed |

The named mutation changes only `publisherConfirmationTrackingEnabled` from
`true` to `false` in `RmqMessageGateway.ConnectToBrokerAsync`. Publisher
confirmations and Brighter's confirmation callback remain enabled. Both cases
reach line 113 and fail because the observed header is `byte[]`, not `long`.
Neither fails by timeout. The gateway is restored byte-for-byte from its
pre-mutation snapshot and has no diff against HEAD. There is no lasting
production change in this increment.

The full suites are unfiltered and run against separate RabbitMQ 4.2 and 4.3
brokers, with TLS and independent delayed-plugin brokers enabled. They finish in
10 minutes 16 seconds and 10 minutes 23 seconds respectively. Comparison with
the preceding stabilization TRX files retains every previous test and outcome
and adds exactly the two new passing cases on each framework. The initial
Release test build has 2,308 warnings and no errors; the incremental mutation
and restored builds have no warnings or errors. The four task brokers are
stopped after validation.

Logs and the source snapshot are under
`/private/tmp/brighter-4170-confirmation-header/`. TRX files are under
`/private/tmp/brighter-4170-rmq-results/` with the `4170-confirmation-` prefix.
At that checkpoint, the separate [retry-count review](review/rmq-retry-count/README.md)
remained outside compiled projects. It was subsequently approved and validated below.
HEAD and the saved stash are unchanged. Nothing was committed or pushed.

## RabbitMQ retry-count attachment and precedence

The approved retry-cycle cases reproduce a loss of retry state through a
replacement reader. Both public consumer-factory paths return handled count zero
on the first retry, where the expected value is one. All four initial executions
fail at the handled-count assertion, after successful legacy mapping.

The consumer now attaches the canonical native handled count after custom
reading when that header exists. It reuses `RmqMessageCreator.ReadHandledCount`;
only that method's accessibility changes from private to internal within an
internal class. Its parsing body and the unconfigured reader path are unchanged.
There is no new public API. Without the count header, the reader's value remains.
The subscription documentation describes this precedence.

The two retry-cycle cases verify counts one and two after successive immediate
requeues, preserving legacy mapping and settling the final delivery. Ten
additional approved cases use a replacement reader that returns count 99.
Native integer and UTF-8 counts override it; malformed and unsupported native
values use the existing zero fallback; an absent count header preserves 99.

| Check | Result |
| --- | --- |
| Initial retry-cycle cases, .NET 9 and .NET 10 | 2 expected failures per framework: expected 1, actual 0 |
| Corrected retry-cycle cases, both frameworks | 2 passed per framework |
| Remove count attachment, retry-cycle cases on .NET 9 | 2 expected failures at the same assertion |
| Restore attachment, retry-cycle cases on both frameworks | 2 passed per framework |
| Combined retry-cycle and precedence cases, both frameworks | 12 passed per framework |
| Remove count attachment, precedence cases on .NET 9 | 8 expected failures returning 99; 2 missing-header cases pass |
| Restore attachment, combined cases on both frameworks | 12 passed per framework |
| Attach default native count unconditionally, precedence cases on .NET 9 | 2 expected failures: expected 99, actual 0; other 8 pass |
| Final byte restoration, combined cases on both frameworks | 12 passed per framework |
| Final complete Release solution build | 54 warnings, 0 errors |
| Full RabbitMQ Async Release, .NET 9 | 502 passed, 2 existing skips, 0 failed |
| Full RabbitMQ Async Release, .NET 10 | 502 passed, 2 existing skips, 0 failed |

The precedence mutations fail at line 139, the handled-count assertion. The
retry-cycle mutation fails at line 141. No timeout or mapping/setup error counts
as mutation evidence. The consumer matches its GREEN byte snapshot after each
restoration. All four test/helper files match their approved review drafts.

The first full runs were stopped when the precedence cases were approved during
execution. They provide no completed regression result. The final unfiltered
runs include both increments, TLS and native delayed-retry fixtures against
independent RabbitMQ 4.2/4.3 and delayed-plugin brokers. They use process-scoped
idle-sleep inhibition.

Both final result sets preserve every case and outcome from the preceding
confirmation-header suites, adding exactly twelve passing cases. The runs
finish in 10 minutes 15 seconds and 10 minutes 27 seconds respectively.

Logs and snapshots are under `/private/tmp/brighter-4170-retry-count/` and
`/private/tmp/brighter-4170-count-precedence/`. TRX files under
`/private/tmp/brighter-4170-rmq-results/` use the corresponding `4170-retry-count-`
and `4170-count-precedence-` prefixes.

This proves immediate retry reconsumption and the tested count representations.
Native delayed reconsumption, scheduler-backed mapping and the remaining release
checks are not established by these cases. Nothing was committed or pushed.

## RabbitMQ counts after parsing failure and native delayed retries

The approved lifecycle cases reproduce a second count-loss path. When the
default parser fails, its fallback message initially loses a canonical native
count of seven. Four unconfigured-reader cases fail per framework; four
configured-default cases pass because the consumer attaches the count afterward.
The two native delayed-retry cases also pass initially.

`RmqMessageCreator.SetMessageMetadata` now attaches the canonical count using
the existing parser after either successful parsing or failure fallback. This
adds two lines and retains payload, identity, routing and settlement behavior.
The count parser and public API remain unchanged.

| Check | Result |
| --- | --- |
| Initial lifecycle cases, both frameworks | 4 expected count failures and 6 passes per framework |
| Corrected lifecycle cases, both frameworks | 10 passed per framework |
| Remove parser metadata count attachment, .NET 9 | 4 expected unconfigured-reader failures; 4 configured-default passes |
| Restore parser metadata attachment, both frameworks | 10 passed per framework |
| Remove consumer count attachment, delayed cases on .NET 9 | 2 expected failures: expected 1, actual 0 |
| Restore both production files, all count cases on both frameworks | 22 passed per framework |
| Complete Release solution build | 54 warnings, 0 errors |
| Full RabbitMQ Async Release, .NET 9 | 512 passed, 2 existing skips, 0 failed |
| Full RabbitMQ Async Release, .NET 10 | 512 passed, 2 existing skips, 0 failed |

Parser-failure assertions fail at line 128 with expected seven and actual zero.
The delayed-retry assertions fail at line 145 with expected one and actual zero.
The mutations are applied separately and restored from GREEN byte snapshots.
Both test files match the approved drafts. No timeout or setup error is used
as mutation evidence.

The delayed cases consume two successive native-plugin retries through a
replacement reader and retain counts one and two. This extends the earlier
native retry-header observation to actual reconsumption. It does not establish
scheduler-backed retry mapping.

The final unfiltered suites complete in 10 minutes 34 seconds and 10 minutes
17 seconds respectively. Every preceding test and outcome is retained, with
exactly ten new passing lifecycle cases on each framework. The changed production
files pass `git diff --check` and match their final GREEN snapshots. The four
task-specific RabbitMQ brokers are stopped after validation. HEAD and the saved
stash remain unchanged.

Logs and snapshots are under `/private/tmp/brighter-4170-count-lifecycle/`.
TRX files under `/private/tmp/brighter-4170-rmq-results/` use the
`4170-count-lifecycle-` prefix. All work remains local and uncommitted.
At this checkpoint the [SNS overlap review](review/sns-overlap/README.md) remained outside compiled
projects, unrun and awaiting approval.


## SNS overlapping output lifetimes

The approved test and helper were copied unchanged into the AWS V4 test project.
Both sync and async cases initially pass on .NET 9 and .NET 10. One SNS producer
publishes two messages to a native SNS-to-SQS envelope subscription while the
first mapped output is held until the second mapping completes. Assertions cover
independent application IDs, body, subject, topic, tenant attribute, CloudEvents
values, serialized counts and unchanged source messages/bags. This establishes
overlapping output lifetimes, not simultaneous execution inside the converter.

The named mutation makes `DefaultSnsWireMessageWriter` clear/refill and return
one instance-level attribute dictionary. Both .NET 9 cases fail at distinct-ID
count equality: expected two, actual one. No test changes are needed. The writer
is restored byte-for-byte and both cases pass again on both frameworks. No
lasting production change is required.

| Check | Result |
| --- | --- |
| AWS V4 Release build, both frameworks | 0 errors |
| Initial overlap cases, net9.0 / net10.0 | 2 passed / 2 passed |
| Shared-attributes mutation, net9.0 | 2 intended failures, expected 2 IDs / actual 1 |
| Restored overlap cases, net9.0 / net10.0 | 2 passed / 2 passed |
| Complete AWS V4 Release, net9.0 | 491 passed, 0 failed, 0 skipped; 7 min 59 sec |
| Complete AWS V4 Release, net10.0 | 491 passed, 0 failed, 0 skipped; 7 min 10 sec |

Logs, TRX files and the original writer snapshot are under
`/private/tmp/brighter-4170-sns-overlap/`. The next
[SNS publication isolation review](review/sns-isolation/README.md) remains outside
the compiled project and has not been built or run. No commits or pushes are made.

Both full TRX files retain all 489 preceding cases and outcomes and add exactly
the two overlap cases. Mutation stack traces identify the distinct-ID assertion
at line 152. The production writer matches its original snapshot, and earlier
SQS/RabbitMQ fixes match their saved GREEN snapshots. Both compiled files match
the approved drafts. The task AWS emulator is stopped. HEAD and the named stash
remain unchanged. No new production change, commit or push is part of this increment.


## SNS publication writer isolation

The approved four-case test is copied unchanged into the compiled AWS V4 project.
Both factory/send modes and both default/custom creation and send orders pass
on .NET 9 and .NET 10. Each case uses two topics and two native SNS-to-SQS
envelope subscriptions. Assertions verify configured destination and mapped
topic, identity, payload, subject, custom tenant attribute presence/absence,
CloudEvents subject, serialized bags/counts and unchanged inputs/configuration.

The named mutation stores the first resolved writer in a static publisher cache.
Each of the four cases runs in a separate .NET 9 test process with an empty cache.
All four fail at mapped-topic equality: the second publication retains its actual
native destination but receives the first publication's mapped topic attribute.
Each TRX contains exactly its requested theory case; no setup or timeout failure
is accepted as RED. The production publisher is restored byte-for-byte, and all
four cases pass again on both frameworks. No lasting production change is needed.

| Check | Result |
| --- | --- |
| AWS V4 Release build, both frameworks | 0 errors |
| Initial isolation cases, net9.0 / net10.0 | 4 passed / 4 passed |
| Shared-writer mutation, four fresh net9.0 processes | 1 intended mapped-topic failure per process |
| Restored isolation cases, net9.0 / net10.0 | 4 passed / 4 passed |
| Complete AWS V4 Release, net9.0 | 495 passed, 0 failed, 0 skipped; 7 min 11 sec |
| Complete AWS V4 Release, net10.0 | 495 passed, 0 failed, 0 skipped; 7 min 16 sec |

Artifacts are under `/private/tmp/brighter-4170-sns-isolation/`. The
[scheduled dispatch trace draft](review/scheduled-dispatch-trace/README.md) remains
outside compiled projects. It proposes a test-only reference to the centrally
managed `Microsoft.Extensions.TimeProvider.Testing` package and has not been
built or run. It covers returning through Brighter's dispatch pipeline, not AWS
native scheduler targets. All work remains local, uncommitted and unpushed.

Both full TRX files retain all 491 preceding cases and outcomes and add exactly
the four isolation cases. The publisher matches its original byte snapshot,
and the compiled test matches the approved draft. Eight production preservation
snapshots (including earlier SQS/RabbitMQ fixes) match. `git diff --check` passes.
The task emulator is stopped; HEAD and the named stash remain unchanged.

A separate source-and-evidence audit maps RabbitMQ controls to already passing
native assertions in the [release review](release-review.md#rabbitmq-control-header-audit).
This adds no code or runtime evidence beyond the preceding 512-pass/two-skip
RabbitMQ checkpoint. Scheduling and simultaneous converter execution remain
separate outstanding work.


## Scheduled SQS dispatch maps the actual publish trace

The approved test is copied unchanged into the AWS V4 project, with the
centrally managed `Microsoft.Extensions.TimeProvider.Testing` test-only reference.
Restore and Release build succeed. Both sync/async cases initially pass on
.NET 9 and .NET 10. A 16-minute delayed send selects the producer's scheduler
branch; fake time triggers the real in-memory scheduler, command processor,
framework handler and outbox mediator before native SQS publication.

Before advancing time, the test observes no publication, outbox entry or native
delivery, and the original traceparent remains. The delivered wire traceparent
matches the actual completed publish span, differs from the scheduling span
and preserves application identity, body and the composed tenant header.
No source bag changes or outstanding outbox entries remain. This evidence does
not prescribe the scheduled trace's parent or cover AWS native scheduler targets.

The named mutation moves SQS producer trace propagation until after the native
send. Both .NET 9 cases fail exactly at publish-span-ID versus native wire
traceparent equality. The producer is restored byte-for-byte; both restored
cases pass on each framework. No lasting production change is needed.

| Check | Result |
| --- | --- |
| Test dependency restore and AWS V4 Release build | Succeeded; 0 build errors |
| Initial scheduled trace cases, net9.0 / net10.0 | 2 passed / 2 passed |
| Late-propagation mutation, net9.0 | 2 intended native traceparent assertion failures |
| Restored cases, net9.0 / net10.0 | 2 passed / 2 passed |
| Complete AWS V4 Release, net9.0 | 497 passed, 0 failed, 0 skipped; 7 min 18 sec |
| Complete AWS V4 Release, net10.0 | 497 passed, 0 failed, 0 skipped; 7 min 47 sec |

Logs, TRX files and production/project snapshots are under
`/private/tmp/brighter-4170-scheduled-trace/`. The
[SNS scheduled-dispatch draft](review/sns-scheduled-dispatch-trace/README.md)
remains outside the compiled project, unbuilt and unrun. It reuses the approved
fake-time dependency. No commit or push is made.

Both full TRX files retain all 495 preceding cases and outcomes and add exactly
the two scheduled SQS cases. The approved test is unchanged, and the producer
is restored byte-for-byte. Nine production preservation snapshots match, including
the earlier SQS/RabbitMQ fixes. `git diff --check` passes. The task emulator is
stopped; HEAD and the saved stash remain unchanged. The only project dependency
change is the approved test-only reference, using the existing central version.

The release review now records the default converters' shared state and ownership
limits from source inspection. It explicitly distinguishes this static evidence
from runtime overlap tests and does not claim exhaustive concurrent execution.


## Scheduled SNS dispatch maps the actual publish trace

The approved SNS test is copied unchanged into the compiled AWS V4 project.
Both factory/send modes initially pass on .NET 9 and .NET 10. A positive delay
selects SNS scheduling; advancing fake time executes the real in-memory scheduler,
command processor, framework handler and outbox mediator before publication.
Native SNS-to-SQS envelopes independently expose the final wire content.

Before dispatch, the original traceparent remains and there is no publish span,
outbox entry or native delivery. After dispatch, native CloudEvents traceparent
equals the completed publish span ID and differs from the scheduling span.
Actual/mapped destination, application ID, body, subject and tenant metadata
survive; source content/bag remain unchanged and no outbox entry is outstanding.

The approved mutation moves SNS trace propagation until after native publication.
Both .NET 9 cases fail at publish-span-ID versus wire traceparent equality.
The producer is restored byte-for-byte, and both restored cases pass on each
framework. There is no lasting production change or additional package reference.

| Check | Result |
| --- | --- |
| AWS V4 Release build, both frameworks | 0 errors |
| Initial SNS scheduled trace cases, net9.0 / net10.0 | 2 passed / 2 passed |
| Late-propagation mutation, net9.0 | 2 intended native traceparent assertion failures |
| Restored cases, net9.0 / net10.0 | 2 passed / 2 passed |
| Complete AWS V4 Release, net9.0 | 499 passed, 0 failed, 0 skipped; 7 min 10 sec |
| Complete AWS V4 Release, net10.0 | 499 passed, 0 failed, 0 skipped; 7 min 30 sec |

Logs, TRX files and the producer snapshot are under
`/private/tmp/brighter-4170-sns-scheduled-trace/`. This validates Brighter's
in-memory scheduler dispatch, not AWS native scheduler targets. The
[readiness-scope proposal](release-boundary-proposal.md) awaits a user decision;
no global requirement has been removed or marked complete. No commit or push is made.

Both final TRX files retain all 497 preceding cases and outcomes and add exactly
the two scheduled SNS cases. The approved test is unchanged, the producer is
restored byte-for-byte and all ten production preservation snapshots match.
`git diff --check` passes. The task emulator is stopped, HEAD and the saved stash
remain unchanged, and there is no additional dependency change in this increment.

Local CI configuration review confirms the AWS V4 test filters include these
cases on the project's net9.0/net10.0 targets. No remote CI run was started or
claimed. The scope decision remains pending; passing this increment does not
establish global #4170 completion or production readiness.


## Approved partial boundary and final local review

The user approved the AWS SDK V4/RabbitMQ Async first increment after the preceding
SNS scheduled-dispatch validation. This supersedes the pending scope decision in
earlier chronological entries, without completing the global feature.

The [release review](release-review.md) now records the final scoped assessment.
The overview, tasks, requirements and ADR text have been reconciled with that
boundary. ADRs remain Proposed for upstream acceptance. No production or test
code was changed for this checkpoint and no new tests were introduced.

The final saved TRX counters were re-read: both AWS runs contain 499 passed cases
and zero failures/skips; both RabbitMQ runs contain 512 passed cases and two
existing skips. These remain the final affected-suite results. No redundant full
rerun or remote CI run was started for documentation-only changes.

The historical original-ID empty read remains unexplained. Subsequent fixture
validation and complete regressions pass; they do not prove that event's cause.
Other gateways, AWS native scheduler target mapping and unvalidated schedulers
remain follow-ups. See the [approved boundary](release-boundary-proposal.md).

Final checkpoint checks pass: all ten production preservation snapshots match,
updated relative documentation links resolve, and `git diff --check` is clean.
HEAD remains `64ba97097199c708974195f55fbdca0ce3e8487c` and the saved stash remains
`6df7d7a832a1b9cb4eba84d30a3c31dba3055499`. Nothing was committed or pushed.


## Full implementation scope restored

The user requires full implementation following the issue reread. The earlier
partial boundary is historical and no longer determines completion. The active
scope, overview, ADR status notes and task checklist now retain every gateway and
previously excluded mapping path as required work. Existing passing results remain
valid checkpoints, not full-feature acceptance.

The first new [review draft](review/non-cloud-events-telemetry/README.md) specifies
four SQS non-CloudEvents telemetry cases with named late-propagation, lost-state
and lost-baggage mutations. Drafts remain outside compiled projects. They have
not been compiled or run, and no new passing result is claimed. No production or
compiled test code changed while preparing this review.


## Non-CloudEvents SQS telemetry round trip — approved and validated

The four approved cases are now in the AWS V4 test project, together with their
replacement writer and reader. They use sync/async factory and outbox dispatch
paths, encoding traceparent, tracestate and baggage either in custom native
attributes or in the JSON body. Independent native observation verifies the
actual publish context rather than the deliberately different deposit context,
and the absence of CloudEvents attributes. Configured reading restores payload,
application identity and all three telemetry fields. The public consumer tracer
operations restore the publish parent/trace, tracestate and baggage despite an
unrelated ambient context. Native observation after acknowledgement confirms
settlement. This covers the gateway-to-tracer boundary, not a running pump/handler.

All four cases initially pass on both frameworks. Each approved .NET 9 mutation
fails all four cases at the intended assertion: late producer propagation at
native traceparent equality (line 177), lost reader tracestate at header equality
(line 205), and lost reader baggage at header equality (line 206). Both production
files are restored byte-for-byte. No lasting production or dependency change was
needed; the new tests characterize the existing extension points.

The first build exposed two nullable type warnings in baggage setup. Changing
the dictionary value annotation from `string` to `string?` removes those warnings;
no values, setup behavior or assertions changed. The review copy and compiled test
match, and the original approved file is preserved in the artifact directory.

| Check | Result |
| --- | --- |
| Release build, both frameworks | Pass, zero errors; existing project warnings remain |
| Initial focused runs, net9.0 / net10.0 | 4 passed / 4 passed |
| Each of three named mutations, net9.0 | 4 intended assertion failures |
| Restored focused runs, net9.0 / net10.0 | 4 passed / 4 passed |
| Complete AWS V4 Release, net9.0 | 503 passed, zero failures/skips; 7 min 46 sec |
| Complete AWS V4 Release, net10.0 | 503 passed, zero failures/skips; 7 min 48 sec |

Both final test-name/outcome multisets retain all 499 preceding cases and add
exactly the four new passing cases. Logs, TRX files, producer/consumer snapshots,
the original approved test and a pre-run production source hash manifest are
under `/private/tmp/brighter-4170-non-cloud-telemetry/`. Every source file in that
manifest still matches. Usage documentation now explains paired telemetry
mapping and links the executable converter examples.

The task emulator is stopped. HEAD and stash remain unchanged; no commit or push
was made. The full feature remains incomplete. The next SDK V3 batch consists of
12 native cases adapted from the V4 endpoints, with six named mutations; its
[review drafts](review/aws-v3-mapping/README.md) remain outside compiled projects
and await approval. They have not been compiled or run.

## AWS SDK V3 endpoint integration — approved and validated

The subsequent approval authorized the
[first SDK V3 batch](review/aws-v3-mapping/README.md). Its six test files and six
helper files were copied unchanged into the SDK V3 test project. The gateway now
supports publication-selected SQS/SNS writers and subscription-selected SQS
readers, plus default mapping composition. Default serializers were extracted
from SDK V3 to preserve its native fields and SDK APIs.

| Check | Result |
| --- | --- |
| Missing API compilation | Expected missing types; not behavioral RED |
| Supported gateway targets | Release build passes netstandard2.0, net8.0, net9.0, net10.0 |
| Initial focused runs, net9.0 / net10.0 | 12 passed / 12 passed |
| Each of six named mutations, net9.0 | 2 intended assertion failures |
| Restored focused runs, net9.0 / net10.0 | 12 passed / 12 passed |
| Complete SDK V3 Release, net9.0 | 428 passed, zero failures/skips; 7m18s |
| Complete SDK V3 Release, net10.0 | 428 passed, zero failures/skips; 7m28s |

The 413 prior discovery entries are all retained. The native-redrive-limit theory
appears once in discovery and expands into four member-data cases at execution,
giving 416 pre-existing executions plus 12 additions. Both result sets match
that complete expected set. The pre-batch check was discovery only, not a full
baseline run; no claim about pre-batch outcomes is inferred from it.

Logs, TRX results, source snapshots, the pre-batch source hash manifest, mutation
runner and exact test-name comparison are in
`/private/tmp/brighter-4170-aws-v3-mapping/`. Every green source snapshot matches;
production outside SDK V3 is unchanged from the start of this batch. The gateway
keeps old public constructor signatures and makes the reader-taking consumer
constructor internal. Shared core code did not change, so V4/RabbitMQ suites
were not repeated for this batch.

The emulator is stopped. HEAD and the saved stash remain unchanged, with no
commit or push. SDK V3 custom-reader exceptions/nulls still abort reception;
default-parser failure enrichment is also pending. The next
[36 reader-failure and envelope cases](review/aws-v3-reader-failures/README.md)
are drafts outside the compiled project and await approval. Full SDK V3 parity
and full #4170 completion remain open.

## AWS SDK V3 reader failures and envelope compatibility

The [approved 36-case batch](review/aws-v3-reader-failures/README.md) now runs in
the SDK V3 project. Initial .NET 9 execution produced 30 behavioral failures and
six passing valid-envelope cases. Exceptions/null results escaped custom reading;
default-parser failures returned empty bodies. The implementation changes only
the SDK V3 consumer and default reader.

Custom-reader failures now become unacceptable messages with native identity,
body and receipt metadata. Failed default parsing preserves any parsed
application ID/topic and reconstructs the native failure body and settlement
data. Successfully parsed rejection envelopes retain their inner representation.
Each delivery is mapped independently, preserving mixed batches.

| Check | Result |
| --- | --- |
| Initial post-fix focused runs, .NET 9 / .NET 10 | 36 passed / 36 passed |
| Seven named mutations, .NET 9 | All intended failures: 4, 8, 4, 4, 2, 12 and 4 cases |
| Restored focused runs, .NET 9 / .NET 10 | 36 passed / 36 passed |
| Supported gateway targets | netstandard2.0, net8.0, net9.0 and net10.0 build |
| Complete SDK V3 Release, .NET 9 | 464 passed, zero failures/skips; 7m36s |
| Complete SDK V3 Release, .NET 10 | 464 passed, zero failures/skips; 7m35s |

The exact test-name/outcome multisets retain all 428 preceding cases and add the
36 passing cases on each framework. Every mixed-batch case in the initial GREEN
runs received three deliveries together. Mutation failures occur at native body,
application ID, inner-envelope body, delivered-message count and successful
requeue assertions. Two initial mutation compilation attempts hit obsolete-API
diagnostics; they were corrected only in the temporary mutation and are not
counted as behavioral RED.

Artifacts are under `/private/tmp/brighter-4170-aws-v3-reader-failures/`:
initial/focused/full TRX results, build logs, seven mutation results, two
compilation-attempt logs, exact source snapshots, a production hash manifest,
the mutation runner and `verified-results.json`. Only the two intended
production files differ from the pre-batch manifest. All GREEN source snapshots
and approved test copies match exactly. Core, V4 and RabbitMQ production code
did not change and their suites were not repeated for this batch.

The local emulator is stopped. HEAD and the saved stash are unchanged; no commit
or push was made. The [39-case lifecycle batch](review/aws-v3-lifecycle/README.md)
remained outside the compiled project pending the subsequent approval recorded below.

Source review also identified a separate count-preservation edge in both AWS
SDK variants: default parser catch paths discard parsed rejection metadata and
stamped handled count, so malformed routed rejection copies need dedicated
tests. Valid rejection-envelope coverage does not establish that edge. No
runtime reproduction or fix is claimed for it. It remains an explicit full
completion task alongside the remaining gateways and dispatch integrations.

## AWS SDK V3 lifecycle and compatibility

The subsequent approval authorized the
[39-case lifecycle batch](review/aws-v3-lifecycle/README.md), including its
15 named mutations. Fourteen test files and five helpers were copied unchanged
from the reviewed drafts. The only project dependency addition is
`Microsoft.Extensions.TimeProvider.Testing`, already centrally pinned to 10.10.0,
for the in-memory scheduler's test clock.

The cases cover default-writer reuse, publication/subscription isolation,
overlapping SQS/SNS sends, reader reuse, SNS FIFO controls, recoverable writer
failure through the outbox, SQS outbox and SQS/SNS scheduled dispatch tracing,
non-CloudEvents SQS telemetry and the legacy SQS constructor overload.
All initially pass, so each behavior is guarded by its reviewed mutation.
No lasting production change was needed.

| Check | Result |
| --- | --- |
| Initial focused runs, .NET 9 and .NET 10 | 39 passed per framework, zero failures/skips |
| All 15 named mutations, .NET 9 | 51 intended assertion failures across 21 executions |
| Restored focused runs, .NET 9 and .NET 10 | 39 passed per framework, zero failures/skips |
| Restored gateway builds | netstandard2.0, net8.0, net9.0 and net10.0 pass |
| Complete SDK V3 Release, .NET 9 | 503 passed, zero failures/skips; 7m21s |
| Complete SDK V3 Release, .NET 10 | 503 passed, zero failures/skips; 7m16s |

Each static SQS/SNS writer-cache theory case runs in a fresh process. The other
mutations exercise shared attribute dictionaries, cached reader selection/results,
incorrect FIFO controls, silent writer fallback, late producer trace propagation,
lost reader tracestate/baggage and an ambiguous public constructor. The mutation
runner checks the exact failure count, assertion type and source location.
All mutations compile and fail as intended without a harness correction.

The full test-name/outcome multisets retain every preceding 464 case and add
exactly the 39 approved cases on each framework. Every production source hash
matches its pre-batch snapshot. The tests, helpers and project file match the
approved drafts byte-for-byte; the project change is exactly the one test clock
dependency. Core, V4 and RabbitMQ code did not change in this batch, and their
suites were not repeated.

Artifacts are under `/private/tmp/brighter-4170-aws-v3-lifecycle/`: initial,
restored and full TRX/logs, mutation/build logs, exact source snapshots, the
production hash manifest, mutation runner and `verified-results.json`.
Tests use Floci 1.5.19 with isolated account namespaces and idle-sleep inhibition.
No real AWS deployment or remote CI result is claimed.

The telemetry tests exercise the public tracer API, not a service-activator
pump/handler. Native AWS scheduler targets, retry/invalid-forwarding mapping,
other gateways and the public configuration review remain open. The next
[32 malformed rejection-count cases](review/aws-malformed-rejection-count/README.md)
remain outside compiled projects, unrun and awaiting approval. No runtime
reproduction of that separately identified count edge is claimed yet.
All work remains local, uncommitted and unpushed.

## AWS malformed rejection-copy count and metadata preservation

The approved [32-case batch](review/aws-malformed-rejection-count/README.md)
reproduced the failure in both SDKs on .NET 9: eight routed-copy cases per SDK
expected handled count seven but received zero at line 142; eight ordinary-message
controls per SDK passed. Both raw and SNS-enveloped formats failed through sync
and async factories with implicit and explicitly configured default readers.

The raw and envelope parsers now retain their parsed bag and handled count across
failure. Metadata/count reading precedes content-type parsing. The failure path
removes any recovered receipt handle, and the default reader reconstructs the
native failure payload with the retained bag and the current native receipt.
`DeliveryCount.Resolve` can then retain routed-copy count seven while ordinary
messages present broker counts zero and one across redelivery. The catch path
performs no additional parsing. Public signatures, dependencies and shared core
serialization remain unchanged.

The first post-fix runs exposed a mistaken timestamp assertion in the reviewed
tests. The existing bag converter infers `DateTime` from the timestamp string;
this machine represents the input 12:00 UTC as 14:00 at UTC+02:00. Each run had
eight timestamp-comparison failures and eight ordinary-control passes, with all
preceding count assertions passing. The user separately approved comparing the
exact UTC instant using the existing `DateTime` representation. All other
assertions remained intact. Both compiled tests match those corrected drafts.

| Check | Result |
| --- | --- |
| Corrected focused runs, SDK V3/V4 × .NET 9/10 | 16 passed per run, zero failures/skips |
| Four count mutations | Eight intended count assertion failures per mutation, line 142 |
| Two original-topic mutations | Eight intended metadata-presence failures per mutation, line 206 |
| Restored focused runs, SDK V3/V4 × .NET 9/10 | 16 passed per run, zero failures/skips |
| Restored gateway builds, both SDKs | netstandard2.0, net8.0, net9.0 and net10.0 pass |
| Full SDK V3 Release, .NET 9 | 519 passed, zero failures/skips; 7m24s |
| Full SDK V3 Release, .NET 10 | 519 passed, zero failures/skips; 7m16s |
| Full SDK V4 Release, .NET 9 | 519 passed, zero failures/skips; 7m18s |
| Full SDK V4 Release, .NET 10 | 519 passed, zero failures/skips; 7m17s |

There are six mutations and 48 intended assertion failures. The first SDK V4
count-mutation attempt stopped before applying the mutation because its
indentation differed from the harness's search string. After correcting the
harness, both V4 count mutations failed at the intended assertions. This harness
error is not counted as behavioral RED. Every temporary mutation was restored
byte-for-byte before rebuilding and regression execution.

Artifacts are under `/private/tmp/brighter-4170-malformed-rejection-count/`:
initial and first post-fix TRX/logs, corrected/restored/full results, build logs,
mutation logs/runner, source snapshots and the pre-batch production hash manifest.
`post-fix-review.json` records the timestamp failures and intended six-file change.
The final verifier confirms each full run retains its preceding 503-case
name/outcome multiset and adds exactly sixteen passing cases. It also confirms
exact source restoration, exact approved drafts and the intended six production
changes. `verified-results.json` records those checks.

The environment is Floci 1.5.19 with per-process account isolation and idle-sleep
inhibition. No real AWS deployment or remote CI result is claimed. Core, RabbitMQ
and other gateways did not change in this batch and were not rerun. The complete
feature remains open. The next [72-case rejection-writer batch](review/aws-rejection-writers/README.md)
is outside compiled projects and awaits approval. Nothing has been committed or
pushed; the saved stash is preserved.

## AWS explicit invalid-message and dead-letter writers

The approved [72-case batch](review/aws-rejection-writers/README.md) adds separate
`SqsSubscription.InvalidMessageWireMessageWriter` and
`SqsSubscription.DeadLetterWireMessageWriter` settings in both SDKs. The consumer
factory passes each selection into the internal constructor; the consumer
retains it and configures its corresponding lazy rejection publication. Existing
public constructors are unchanged, and null settings retain default mapping.
No dependency or shared-core change is included.

The cases use independent native input and native destination observations.
They cover sync/async factories, invalid-message/dead-letter/fallback routes,
replacement/composition, and success/throwing/null-returning writers. They verify
profile selection, identity, original payload, count, rejection metadata, absence
of a copy at the other route and source settlement. Failure cases preserve native
source identity/body/topic, recover the same writer, receive again with a new
receipt and successfully forward. SQS retains its existing `Reject` return
contract; true after a handled writer failure does not imply successful forwarding.

Initial builds failed only on the missing properties (CS0117). These compilation
failures are not behavioral RED. After implementing the approved wiring, all
cases passed, and all ten named mutations established the regression guards.

| Check | Result |
| --- | --- |
| Initial focused runs, SDK V3/V4 × .NET 9/10 | 36 passed per run, zero failures/skips |
| Ignore both selected writers, each SDK | 12 failures at native profile presence, line 219 |
| Swap selected writers, each SDK | 12 failures at native profile equality, line 220 |
| Corrupt original topic, each SDK | 12 failures at native original-topic equality, line 227 |
| Clear handled count, each SDK | 12 failures at native count equality, line 226 |
| Delete source after writer failure, each SDK | 24 failures at original native delivery presence, line 166 |
| Restored focused runs, SDK V3/V4 × .NET 9/10 | 36 passed per run, zero failures/skips; 3m14s each |
| Restored gateway builds, both SDKs | netstandard2.0, net8.0, net9.0 and net10.0 pass |
| Full SDK V3 Release, .NET 9 | 555 passed, zero failures/skips; 7m33s |
| Full SDK V3 Release, .NET 10 | 555 passed, zero failures/skips; 7m9s |
| Full SDK V4 Release, .NET 9 | 555 passed, zero failures/skips; 7m36s |
| Full SDK V4 Release, .NET 10 | 555 passed, zero failures/skips; 7m21s |

All ten mutations compile and produce exactly 144 intended assertion failures,
without a harness correction. Source mutations are restored byte-for-byte before
rebuilding. Approved tests/helpers remain unchanged. Only six production files
change in this batch: subscription, factory and consumer in each SDK. The test
builds retain existing warnings; no diagnostic names the new test/helper files.

Artifacts are under `/private/tmp/brighter-4170-rejection-writers/`: initial,
restored and full TRX/logs, build/mutation logs, original and restored source
snapshots, a production hash manifest, mutation runner and final verification
script. `verified-results.json` confirms that every preceding 519-case
name/outcome multiset is retained with exactly 36 passing additions per suite.
Exact source restoration, the six intended production changes and approved
unaltered test/helper copies are verified.

Tests use Floci 1.5.19 with per-process account isolation and idle-sleep inhibition.
No real AWS deployment or remote CI result is claimed. Core and RabbitMQ code
are unchanged in this batch and their suites are not repeated. Publication and
source deletion are not atomic. Native broker redrive does not invoke these
application writers.

FIFO rejection and cross-consumer selection remain unvalidated by this batch;
the [96-case follow-up](review/aws-rejection-controls/README.md) is drafted outside
compiled projects and awaits approval. Scheduler dispatch, other transports,
converter concurrency and real pump telemetry remain required for full #4170
completion. All work stays local, uncommitted and unpushed.


## AWS FIFO rejection controls and consumer writer isolation

The approved [96-case batch](review/aws-rejection-controls/README.md) adds 48
cases to each SDK. The FIFO matrix covers sync/async factories, all three routes,
replacement/composition and success/throw/null recovery. Native outgoing system
attributes retain the original group and deduplication IDs. The isolation matrix
creates both consumers through one factory before rejection, in both default/custom
creation orders, and verifies their independent mapping and source settlement.

All tests pass initially. No production fix is needed. All ten approved mutations
compile and fail at their intended assertions without test/harness corrections:
factory-cache and ignored-selection defects fail at native profile presence,
FIFO-control defects fail at native group/deduplication equality, and early source
deletion fails at native source presence during recovery. There are 144 intended
failures. Source changes are restored byte-for-byte before rebuilding.

| Check | Result |
| --- | --- |
| Initial and restored focused runs, SDK V3/V4 × .NET 9/10 | 48 passed per run, zero failures/skips |
| Both gateway builds | netstandard2.0, net8.0, net9.0 and net10.0 pass |
| Full SDK V3 Release, net9.0 | 603 passed, zero failures/skips; 7 m 44 s |
| Full SDK V3 Release, net10.0 | 603 passed, zero failures/skips; 7 m 52 s |
| Full SDK V4 Release, net9.0 | 603 passed, zero failures/skips; 7 m 16 s |
| Full SDK V4 Release, net10.0 | 603 passed, zero failures/skips; 7 m 45 s |

`/private/tmp/brighter-4170-rejection-controls/` holds the TRX/logs, build logs,
mutation runner/results, source snapshots, production hash manifest and verifier.
`verified-results.json` confirms exact retention of every preceding 555-case
name/outcome multiset with 48 additions per suite, unchanged approved test copies,
and unchanged hashes for all 1,164 production source files. Initial builds have
existing warnings but none naming the new files; final incremental builds have
zero warnings/errors.

The environment is Floci 1.5.19 with isolated account namespaces and idle-sleep
inhibition. Native FIFO metadata and sequential consumer-selection isolation are
covered. Managed AWS ordering/failure semantics, concurrent use of application
converters and atomic publication/source settlement are not established by these
cases. No real AWS deployment or remote CI pass is claimed. Core/RabbitMQ code
is unchanged and their suites are not repeated for this batch.

The next [24 RabbitMQ invalid-writer cases](review/rmq-invalid-writer/README.md)
remain outside compiled projects and await approval. The separate
[scheduler source audit](scheduler-path-audit.md) records callback paths and
findings requiring reviewed runtime tests. Neither those findings nor the other
remaining gateway paths are marked complete. All work remains local, uncommitted
and unpushed; the saved stash is preserved.


## RabbitMQ Async explicit invalid-message writer

The [approved 24-case batch](review/rmq-invalid-writer/README.md) adds nullable
`RmqSubscription.InvalidMessageWireMessageWriter`. Both public factory modes
capture the selection, and the consumer supplies it to the existing invalid
publisher. Public constructors and default formats remain unchanged. Only the
subscription, factory and consumer change; no dependency or core change is needed.

Native fixtures and observations cover replacement/composition, throwing/null
writer recovery, unroutable mandatory-publication recovery, per-consumer default
and custom selection, and unchanged native broker dead-lettering. Successful
forwarding retains persistence and delivery controls and settles the original.
Failures propagate, restore the application topic and preserve the source.

Initial compilation failed only for the missing property; that is not behavioral
RED. All cases pass after implementation. The seven named mutations compile and
produce 32 intended failures: ignored writer (4), early acknowledgement (8), lost
topic restoration (4), nonmandatory forwarding (4), cached factory selection (4),
omitted native rejection (4) and lost persistence enforcement (4). The review
record lists exact assertion lines. No test or harness correction was needed.
All temporary source changes are restored byte-for-byte before rebuilding.

| Check | Result |
| --- | --- |
| Initial and restored focused runs, both frameworks | 24 passed per run, zero failures/skips |
| Gateway builds | netstandard2.0, net8.0, net9.0 and net10.0 pass |
| Full RabbitMQ Async Release, net9.0 | 536 passed, two existing skips, zero failures; 11 m 23 s |
| Full RabbitMQ Async Release, net10.0 | 536 passed, two existing skips, zero failures; 11 m 11 s |

The two full runs retain the exact preceding 512 passing and two skipped test-name/
outcome multisets and add exactly 24 passing cases. This includes the existing
TLS/mTLS and native-delay cases. Isolated primary brokers use RabbitMQ 4.2 for
.NET 9 and RabbitMQ 4.3 for .NET 10; independent delayed-plugin brokers prevent
cross-framework fixture collisions. The missing temporary TLS configuration was
recreated before startup. Tests use read-only workspace mounts and idle-sleep
inhibition. The four task brokers are stopped after validation.

Artifacts are in `/private/tmp/brighter-4170-rmq-invalid-writer/`: builds,
initial/restored/full TRX and logs, mutation runner/results, source snapshots,
production manifest and `verified-results.json`. Verification confirms exactly
three production files changed, public constructors are unchanged, all approved
C# copies remain byte-identical and every mutation is restored. Existing compiler
warnings remain, including the reviewed test's deliberate legacy `MessageType`
assertion. No real AWS or remote CI pass is claimed. Core and AWS code did not
change and their suites were not repeated in this batch.

The subsequent [eight real-pump telemetry cases](review/aws-pump-telemetry/README.md)
are now validated; see the following results. Scheduler paths, other
transports and final public-configuration review remain required for full #4170.
All work stays local, uncommitted and unpushed; HEAD and the saved stash are intact.

The result verifier initially expected two skips in the TRX aggregate
`notExecuted` counter. VSTest reports zero there while the two individual results
are `NotExecuted`. The verifier now counts those result entries and compares their
names/outcomes with the preceding suite. No TRX or test was changed.

## SQS non-CloudEvents telemetry through real application pumps

The user approved the [eight real-pump cases](review/aws-pump-telemetry/README.md)
and all eight named mutations. The fourteen C# drafts were copied unchanged.
All sixteen initial executions pass: four cases per SDK on each framework. No
production change is required.

The tests independently inspect native outgoing SQS attributes or JSON body,
then deliver the same message through a real ChannelFactory, Reactor/Proactor,
application mapper, CommandProcessor and handler. The completed process span
parents to the actual dispatch producer span; the handler observes that trace,
`vendor=dispatch` state and `stage=dispatch` baggage despite unrelated ambient
context. The handler receives the original identity/payload exactly once. After
pump shutdown and visibility expiry, native inspection confirms settlement.

All eight mutations compile and produce the expected 32 assertion failures:
late propagation (eight, native traceparent line 182), discarded reader traceparent
(eight, process trace line 267), discarded reader tracestate (eight, process state
line 270), omitted Reactor propagation (four, handler baggage line 274) and
omitted Proactor propagation (four, handler baggage line 274). Tests and their
converters stay unchanged. Every temporary production edit is restored exactly.

The first Reactor mutation removed an unbraced if's statement and failed to
compile. The harness was corrected to use an empty block, then both pump mutations
were verified. That compiler failure does not count as behavioral RED. A
transcript-derived correction record is retained; the rerun replaced the original
failed build log. No test semantic correction or production fix was needed.

| Check | Result |
| --- | --- |
| Restored focused tests, each SDK/framework | Four passed, zero failures/skips |
| Both gateways and ServiceActivator builds | All supported targets pass |
| Full AWS V3 Release, net9.0 | 607 passed, zero failures/skips; 7 m 41 s |
| Full AWS V3 Release, net10.0 | 607 passed, zero failures/skips; 7 m 51 s |
| Full AWS V4 Release, net9.0 | 607 passed, zero failures/skips; 8 m 5 s |
| Full AWS V4 Release, net10.0 | 607 passed, zero failures/skips; 8 m 14 s |
| Full Core Release, net9.0 | 1,716 passed, seven existing skips, zero failures; 2 m 25 s |
| Full Core Release, net10.0 | 1,716 passed, seven existing skips, zero failures; 2 m 23 s |

Each full AWS run preserves the exact preceding 603 test-name/outcome multiset
and adds the four passing real-pump cases. All 1,164 production source hashes
remain unchanged from this batch's snapshot; all fourteen compiled test/helper
files remain byte-identical to the approved drafts. Existing compiler warnings
remain in initial builds; final incremental builds report zero warnings/errors.
Core skip names are recorded in the verified result artifact.

Artifacts: `/private/tmp/brighter-4170-pump-telemetry/` contains builds,
initial/restored/full logs and TRX, mutation evidence, source snapshots,
`mutation-harness-correction.txt` and `verified-results.json`. AWS uses local
Floci 1.5.19 with per-process account isolation and idle-sleep inhibition. This is
real Brighter pump/handler execution against emulated SQS, not a real AWS or
remote CI pass. The local emulator is stopped after validation.

The next [sixteen scheduler destination-selection cases](review/aws-scheduler-destinations/README.md)
and local scheduler fixture drafts are outside compiled projects and unrun,
awaiting review. SNS/RabbitMQ telemetry, scheduled callback dispatch and retries,
other transports and the final public configuration review remain open. All work
remains uncommitted and unpushed; HEAD and the saved stash are unchanged.

## AWS scheduler destination selection and native timestamp regression

The approved sixteen cases validate distinct business/callback SQS and SNS
destinations through the stored native schedule, in both sending modes and SDKs.
Every case passes on .NET 9 and .NET 10. Native target mode keeps the business
destination; callback mode targets `SchedulerTopicOrQueue` and retains the original
message, identity, correlation, tenant metadata and deposit telemetry.

Initial V3 runs had four intended callback destination failures per framework.
V4 had two intended SNS callback destination failures and four SQS setup failures
inside the SDK's null-sensitive topic lookup. Both schedulers now look up callback
destinations from the callback message. V4 uses null-safe paginated `ListTopics`.
Dedicated multi-page coverage has not been added; the observed empty/nonempty
lookup and fallback paths pass.

Six named production mutations produce 24 intended assertion failures: original
callback routing, disabled native targeting, and omitted contained message, in
each SDK. Exact restoration returns focused GREEN. The initial ambiguous IAM
extension compilation error was corrected by qualifying the public extension.
An initial unreachable-code native mutation was replaced by a runtime-false
condition; compiler errors are retained and are not behavioral RED.

Full regression required existing fixture corrections: twelve SQS publications
now supply their required channel name, twenty files use a valid 20-second receive
timeout, and eight native message fixtures explicitly declare their UTF-8 charset.
There are 22 distinct changed fixture files. All assertions, scheduling delays and
overall polling deadlines remain unchanged. Snapshots and diff checks retain the
original evidence.

The working delivery path exposed four existing timestamp assertion failures per
SDK/framework (16 observed failures): culture-dependent serialization truncated
fractional seconds. Both native SNS and SQS serializers now use invariant `O`
formatting. Existing full-header equivalence assertions provide RED and GREEN;
no assertion was weakened. The timestamp attribute's text changes to ISO
round-trip format. Brighter's parser accepts it; external consumers tied to the
old culture-specific formatting need to accept that representation.

| Check | Result |
| --- | --- |
| Final focused destination checks | 8 passed per SDK/framework, zero failures/skips |
| Named mutations | 6 mutations, 24 intended assertion failures |
| Scheduler builds | Both SDKs: netstandard2.0, net8.0, net9.0, net10.0; zero warnings/errors |
| Scheduler and gateway test builds | Both SDKs and frameworks pass |
| Full AWS Scheduler V3 Release, net9.0 | 70 passed, zero failures/skips; 16 m 12 s |
| Full AWS Scheduler V3 Release, net10.0 | 70 passed, zero failures/skips; 16 m 12 s |
| Full AWS Scheduler V4 Release, net9.0 | 70 passed, zero failures/skips; 16 m 23 s |
| Full AWS Scheduler V4 Release, net10.0 | 70 passed, zero failures/skips; 16 m 23 s |
| Full AWS gateway V3 Release, net9.0 | 607 passed, zero failures/skips; 7 m 42 s |
| Full AWS gateway V3 Release, net10.0 | 607 passed, zero failures/skips; 7 m 54 s |
| Full AWS gateway V4 Release, net9.0 | 607 passed, zero failures/skips; 7 m 12 s |
| Full AWS gateway V4 Release, net10.0 | 607 passed, zero failures/skips; 7 m 31 s |

Final native execution uses Floci 2.3.0. Version 1.5.19 supports stored schedules
but misroutes Brighter's universal AWS SDK targets at execution. Version 2.3.0
executes them, but scheduled SNS publication loses random account scope. Four
separate scheduler containers therefore use the default test account. The local
credential helper honors `AWS_TEST_ACCOUNT_ID` only with `AWS_SERVICE_URL`; other
credential behavior is retained. Gateway runs use independent random accounts in
a separate 2.3.0 container. Repository Compose and CI pins remain at 1.5.19;
their proposed update belongs to the next review. No real AWS or remote CI pass
is claimed. All interrupted setup/old-binary runs remain explicitly non-passing
artifacts.

All 607 prior gateway names and outcomes are retained in each final suite. Both
frameworks execute the same scheduler cases and include all eight new cases per
SDK, without the remote CI fragile-test exclusion. Compared with the 1,164-file
production snapshot, only the two scheduler implementations changed. Their final
bytes match the recorded timestamp-correction snapshots. HEAD and the saved stash
are unchanged, and `git diff --check` passes. No commits or pushes were made.

Evidence is in `/private/tmp/brighter-4170-scheduler-destinations/`: initial,
focused, mutation and complete TRX files; interrupted-run logs; all-target build
logs; fixture snapshots/correction lists; native emulator diagnostics and
exact-version emulator source; `verification.json` and verification scripts.

This validates destination selection and existing native delivery regressions.
It does not establish custom writer execution or dispatch-time telemetry through
a real AWS callback. The next [eight callback cases](review/aws-native-callback-mapping/README.md)
and two emulator pin edits are concrete drafts, outside compiled projects and
unapplied. Callback failure recovery, SNS/RabbitMQ pump telemetry, other schedulers,
other transports and final configuration usability remain open.

The source audit also records native SNS source mutation when correlation ID is
empty; current destination cases supply a correlation ID and do not cover that
path. All six task emulator containers were stopped after verification. The saved
stash and branch remain intact.

## Native AWS callback to configured business writer

The approved eight cases execute real Scheduler delivery to a separate callback
SQS queue, decode the native callback with the public default reader and AWS mapper,
and dispatch it through a command processor, AwsSchedulerFiredHandler,
FireSchedulerMessageHandler and OutboxProducerMediator. Native observation of the
business SQS queue or raw SNS subscription proves that the selected writer supplies
the legacy ID attribute and JSON body. That body has the original payload and the
actual business producer's traceparent, dispatch tracestate and baggage.

All four cases per SDK pass initially and after restoration on .NET 9 and .NET 10.
They verify no early business delivery/producer span, preserved deposit telemetry
in the callback, an empty outstanding outbox after success, and an unchanged
original scheduling object. They explicitly invoke registered handlers after native
receipt; they do not run a callback service-activator pump or prove its automatic
acknowledgement. Writer-failure redelivery and real AWS permissions remain open.

Eight named production mutations produce sixteen intended assertion failures.
Ignoring each publication writer fails the native attribute assertion at line 275;
late producer propagation fails the native body traceparent assertion at line 279.
Two affected cases fail and two unaffected cases pass per mutation. Mutation code
is restored byte-for-byte, including BOMs. All 1,164 production source hashes match
the batch snapshot; no production correction was needed for this contract.

The copied tests initially failed compilation because the cancellation token was
passed as the third positional SendAsync argument, whose type is bool. Naming
`cancellationToken` fixes both tests without changing assertions or behavior. The
initial compiler output is retained and is not behavioral RED. New fixtures use
supported legacy policy-key constants, which can emit CS0618 warnings. Final
incremental builds pass with zero warnings/errors for both gateways and schedulers
on netstandard2.0, net8.0, net9.0 and net10.0, and both gateway test projects on
net9.0/net10.0. The Core test build also passes.

| Full regression | Result |
| --- | --- |
| full-v3-net9.0 | 611 passed, 0 existing skips, zero failures; 7 m 51 s |
| full-v3-net10.0 | 611 passed, 0 existing skips, zero failures; 7 m 17 s |
| full-v4-net9.0 | 611 passed, 0 existing skips, zero failures; 7 m 44 s |
| full-v4-net10.0 | 611 passed, 0 existing skips, zero failures; 7 m 29 s |
| core-net9.0 | 1716 passed, 7 existing skips, zero failures; 2 m 21 s |
| core-net10.0 | 1716 passed, 7 existing skips, zero failures; 2 m 20 s |

Each AWS suite retains all 607 preceding names/outcomes and adds exactly four
callback cases. Both Core suites retain the same 1,716 passes and seven skips by
test name/outcome. No scheduler/handler production source changed, so the preceding
four complete scheduler results (70 passes each) remain the scheduler checkpoint;
the approved plan requires repeating them only if those sources change.

Compose and `aws-mock-ci` now pin Floci 2.3.0, which can invoke the universal
Scheduler SQS target used by these cases. Native callbacks use distinct queues;
independent process account namespaces isolate simultaneous test runs. All runtime
results here are local emulator evidence, not real AWS or remote CI results.

The version update exposed a health-check incompatibility: the image lacks curl,
and executing the old health command returned 127. Both configurations now invoke
the image-provided `/usr/local/bin/healthcheck.sh`. That command returns 0 inside
the running container, and Compose configuration validation passes. This adds two
necessary health-command replacements to the originally reviewed two image pins;
workflow filters, timing, credentials and other services are unchanged. The
[versioned upstream script](https://github.com/floci-io/floci/blob/2.3.0/docker/healthcheck.sh)
uses Bash because the runtime image has no curl. Remote CI timing remains unverified.

Evidence: `/private/tmp/brighter-4170-native-callback/` contains initial/restored/full
TRX files, exact source snapshots and manifest, mutation logs/assertions, build
logs, compiler and health-check correction records, and `verification.json`.
HEAD and the saved stash are unchanged, and `git diff --check` passes. No commits
or pushes were made.

The next [native SNS source-preservation batch](review/aws-native-source-preservation/README.md)
contains eight cases outside compiled projects, unapproved and unrun. It targets
the source mutation when native SNS serialization generates a missing correlation
ID, retaining the existing generated wire value and supplied-ID behavior. Callback
failure recovery, SNS/RabbitMQ pump telemetry, other schedulers/transports and final
configuration usability remain open. This is not full #4170 acceptance.

The task's Floci container was stopped after validation. The other task emulator
containers remain stopped; no running service is required to retain these local
changes or the saved stash.


## Native SNS scheduler source and correlation preservation

The [approved eight cases](review/aws-native-source-preservation/README.md) were
copied unchanged into the two Scheduler test projects. Each schedules through the
public factory and inspects the stored native SNS request, covering sync/async
calls and missing/supplied correlation IDs. The schedule is an hour in the future
and deleted without firing. Assertions cover native destination, payload, identity,
nonempty correlation and exact serialized source preservation, including telemetry
and tenant metadata.

Initial RED reproduced the missing-ID source mutation on both frameworks in both
SDKs: two failures and two supplied-ID passes per run. Every failure is the source
correlation equality assertion at line 115. Both schedulers now keep generated
correlation in a local variable used only in native serialization. Supplied IDs
retain their exact value; source headers are unchanged. There is no public API,
callback format or dependency change.

Initial GREEN and restored focused checks each pass all sixteen executions. Six independent
.NET 9 mutations produce sixteen intended failures: writing back to the source
fails two missing-ID cases per SDK at line 115; replacing supplied correlation
fails two supplied-ID cases per SDK at line 114; omitting the attribute fails all
four cases per SDK at the attribute-presence assertion. Its TRX sequence point is
line 109; the verification script initially expected 108. Only that script's line
expectation was corrected, retaining the original V3 evidence without rerunning.
No reviewed test or assertion was altered. Every mutation restores the exact
fixed production bytes, including UTF-8 BOMs.

Both schedulers build for netstandard2.0, net8.0, net9.0 and net10.0. Both Scheduler
and AWS gateway test projects build for net9.0 and net10.0. Final incremental builds
have zero warnings/errors. Initial Scheduler test compilation reported 112 existing
nullable-context/obsolete-member warnings per project, outside the new test files;
those logs are retained and no warning suppression was added.

| Full Release regression | Result |
| --- | --- |
| Scheduler V3, net9.0 | 74 passed, zero failures/skips; 16 m 12 s |
| Scheduler V3, net10.0 | 74 passed, zero failures/skips; 16 m 12 s |
| Scheduler V4, net9.0 | 74 passed, zero failures/skips; 16 m 12 s |
| Scheduler V4, net10.0 | 74 passed, zero failures/skips; 16 m 12 s |
| Gateway V3, net9.0 | 611 passed, zero failures/skips; 7 m 20 s |
| Gateway V3, net10.0 | 611 passed, zero failures/skips; 8 m 13 s |
| Gateway V4, net9.0 | 611 passed, zero failures/skips; 7 m 7 s |
| Gateway V4, net10.0 | 611 passed, zero failures/skips; 7 m 57 s |

Each Scheduler suite retains every prior 70 test name/outcome and adds exactly
four passing cases. Each AWS gateway suite retains the exact preceding 611
name/outcome multiset. Core source is unchanged; its prior 1,716 passes and seven
existing skips per framework remain the latest Core checkpoint, not a new run.

Full Scheduler runs use four isolated Floci 2.3.0 containers and the default test
account, avoiding its native SNS account-scope limitation. Gateway runs use the
shared Floci 2.3.0 endpoint with separate process account namespaces. These are
local emulator results; real AWS permissions and remote CI remain unverified.

Evidence lives in `/private/tmp/brighter-4170-native-source-preservation/`: initial,
green, restored, full and mutation TRX/logs; original/fixed source bytes; build
logs; harness correction; source manifest; and `verification.json`. Of 1,277 source
tree files outside bin/obj, only the two Scheduler implementations changed in
this batch. Approved test bytes, HEAD and the saved stash are unchanged;
`git diff --check` passes. All five task emulators started for validation were
stopped afterward. No commit or push was made.

The next [SNS pump telemetry review](review/sns-pump-telemetry/README.md) contains
sixteen cases for raw/enveloped delivery and attribute/body telemetry across both
SDKs and pump modes. Its six files remain outside compiled projects, uncompiled
and unrun. Callback failure recovery, RabbitMQ telemetry, scheduler-backed retries,
other scheduler/transport paths and final configuration review remain open.


## SNS non-CloudEvents telemetry through raw and enveloped application pumps

The [approved sixteen cases](review/sns-pump-telemetry/README.md) cover both SDKs,
sync/Reactor and async/Proactor, raw/enveloped SNS-to-SQS delivery, and telemetry
in native attributes or JSON body. The six reviewed files were copied unchanged
into the AWS test projects. Initial and restored focused checks each pass all
32 SDK/framework executions. No production correction or test adjustment was needed.

Actual outbox dispatch runs the publication-selected SNS writer under the producer
trace. Independent native observation verifies identity, payload, wire shape and
dispatch-time traceparent/tracestate/baggage. For enveloped deliveries, it also
checks SNS notification type and topic ARN before inspecting inner content. The
subscription-selected reader restores the application message before a real pump
runs its registered handler. The pump starts under unrelated ambient telemetry;
process parentage, handler context, one invocation and acknowledgement after
visibility expiry are all asserted. These tests do not emulate tracer extraction
or use generated conformance pumps/static result counters.

Ten independent production mutations produce eighty intended failures on .NET 9.
Ignoring writer selection fails native `legacy-id` presence (line 214); late SNS
trace propagation fails native traceparent equality (237); stripping reader
traceparent or tracestate fails process trace/state assertions (322/325). Each
fails eight cases per SDK. Omitting consumer context propagation in Reactor or
Proactor fails the handler baggage assertion (329), four cases per SDK per pump.
All mutation runs compile and fail at their intended assertions. Production bytes
are restored exactly after every mutation; all 1,277 source-tree files outside
bin/obj match the pre-batch manifest. There is no production change in this batch.

Both AWS gateways and ServiceActivator build for netstandard2.0, net8.0, net9.0
and net10.0. Both gateway test projects and Core build for both test frameworks.
Final incremental builds report zero warnings/errors. Initial recompilation of
the AWS test projects reports 2,448 existing warnings per project (including
generated-test nullable annotations, obsolete members and analyzer diagnostics).
None originates in the six new files; no suppression was added.

| Complete Release regression | Result |
| --- | --- |
| full-v3-net9.0 | 619 passed, 0 existing skips, zero failures; 7 m 14 s |
| full-v3-net10.0 | 619 passed, 0 existing skips, zero failures; 7 m 20 s |
| full-v4-net9.0 | 619 passed, 0 existing skips, zero failures; 7 m 55 s |
| full-v4-net10.0 | 619 passed, 0 existing skips, zero failures; 8 m 3 s |
| core-net9.0 | 1716 passed, 7 existing skips, zero failures; 2 m 16 s |
| core-net10.0 | 1716 passed, 7 existing skips, zero failures; 2 m 21 s |

Each AWS run retains every preceding 611 test name/outcome and adds exactly eight
passing cases. Both Core runs retain the exact preceding 1,716 passes and seven
skips by name/outcome. Scheduler code is unchanged; its preceding four 74-pass
suites remain the latest Scheduler checkpoint and were not rerun here.

Runtime evidence uses local Floci 2.3.0 with separate per-process accounts. Queue
policies and SNS subscriptions are provisioned through native SDK clients, but
this does not establish real AWS IAM enforcement or a remote CI pass. No external
service or Postman workspace was published. The task emulator was stopped after
validation. Logs, TRX files, exact source snapshots, mutation assertion evidence,
build logs, manifest and `verification.json` are under
`/private/tmp/brighter-4170-sns-pump/`. HEAD and the saved stash are unchanged;
`git diff --check` passes. No commit or push was made.

The next [RabbitMQ pump telemetry batch](review/rmq-pump-telemetry/README.md)
contains four cases and eight helpers outside compiled projects, uncompiled and
unrun. Callback failure recovery, scheduler-backed retry mapping, other providers
and final public configuration review remain open for full #4170 acceptance.

## RabbitMQ non-CloudEvents telemetry through application pumps

The [approved four cases](review/rmq-pump-telemetry/README.md) cover Reactor and
Proactor with telemetry in custom AMQP headers or JSON body. Initial, restored and
final focused checks each pass all eight framework/case executions. No production
correction was needed. Nine reviewed files are in the RabbitMQ Async test project.

The initial build exposed sibling namespaces shadowing Reactor/Proactor types
(CS0118). Qualifying the two constructors corrects compilation without changing
the assertions. Final review also added null-forgiving annotations for the topic
that the fixture explicitly sets in the two new mapper helpers. Original drafts
and diagnostics are retained. These four source edits only affect type resolution
and nullable analysis; the six other helpers are exact reviewed copies. Full
regressions used the pre-annotation binaries; rebuilt final focused cases validate
the annotation cleanup without repeating semantically identical full suites.

Real outbox dispatch runs the publication writer under the producer context.
Independent BasicGet observation asserts exact body, identity, custom native
headers, traceparent/state/baggage, persistence and SDK confirmation ownership. A
native nack releases the delivery to the configured reader and actual pump. The
pump starts under unrelated ambient context; registered application handlers must
observe the producer trace, state, baggage and payload. The test joins the pump,
checks one invocation and closes its consumer channel before asserting the native
queue is empty, so an unacknowledged delivery cannot hide in an open channel.

Seven independent production mutations produce 24 intended assertion failures:

| Mutation | Failures | Assertion in compiled test |
| --- | ---: | --- |
| Ignore publication writer | 4 | Native legacy-id presence, line 183 |
| Propagate producer context after publish | 4 | Native traceparent equality, line 215 |
| Clear reader traceparent | 4 | Process trace equality, line 297 |
| Clear reader tracestate | 4 | Process state equality, line 300 |
| Omit Reactor consumer propagation | 2 | Handler baggage equality, line 304 |
| Omit Proactor consumer propagation | 2 | Handler baggage equality, line 304 |
| Omit consumer acknowledgement | 4 | Empty queue after consumer closes, line 311 |

Every mutation compiles and reaches the intended assertion. All 1,277 production
source-tree files outside bin/obj match the pre-batch manifest. RabbitMQ Async and
ServiceActivator build on netstandard2.0, net8.0, net9.0 and net10.0; test projects
build on both frameworks. The final RabbitMQ test build reports 2,318 existing
warnings across both targets, with none in the nine new files; gateway,
ServiceActivator and Core incremental builds report zero warnings/errors. No
production warning suppression was added. The initial failed test build reported
2,322 warnings, including the four mapper nullable warnings corrected above.

| Complete Release regression | Result |
| --- | --- |
| RabbitMQ net9.0 | 540 passed, 2 existing skips, zero failures; 10 m 56 s |
| RabbitMQ net10.0 | 540 passed, 2 existing skips, zero failures; 10 m 48 s |
| Core net9.0 | 1,716 passed, 7 existing skips, zero failures; 2 m 23 s |
| Core net10.0 | 1,716 passed, 7 existing skips, zero failures; 2 m 19 s |

Each RabbitMQ run retains all preceding 536 passes and two skips by exact test
name/outcome and adds four passing cases. Both Core runs retain the exact prior
1,716 passes/seven skips. RabbitMQ 4.2/.NET 9 and 4.3/.NET 10 use separate isolated
task brokers with TLS/mTLS and native-delay plugins. Runners mount the repository
read-only and do not use unrelated host port 5672. Idle sleep is inhibited. This
does not claim remote CI success or a production deployment.

Logs, TRX files, original source snapshots, mutation evidence, source manifest,
compilation corrections and verification.json are retained under
`/private/tmp/brighter-4170-rmq-pump/`. HEAD and the saved stash remain unchanged;
git diff --check passes. Task brokers are stopped after validation. No commit or
push was made.

The next [Quartz completion batch](review/quartz-job-completion/README.md) is
prepared outside compiled projects, uncompiled and unrun pending review. AWS
callback failure recovery, scheduler-backed retries, actual custom-wire callbacks
in other schedulers, remaining gateways and final public configuration review
remain open for complete issue #4170 acceptance.

## Quartz completion baseline and zero-delay scheduling obstacle

The [approved completion batch](review/quartz-job-completion/README.md) first
captured both complete Quartz baselines: 48 passed, zero failures/skips on .NET 9
and .NET 10. Each took approximately 2 m 20 s. Its four approved C# files were
then copied byte-for-byte into the Quartz test project and compiled successfully.

Each framework's initial focused run has four failures and one pass. All four
message/request × sync/async cases fail during scheduling with
`ArgumentOutOfRangeException: Invalid at, it should be in the future`. Their
`TimeSpan.Zero` becomes an absolute timestamp that is already past when the
next overload reads the current time. The missing-payload job runs and passes
its guard assertions on both frameworks.

These eight failures are not behavioral RED for successful job completion:
none reaches dispatch or `Assert.Null(jobError)`. No completion fix or named
mutation was applied, and no post-fix full-suite result is claimed. The original
four compiled files remain exact approved copies, including their zero delay.
The [review amendment](review/quartz-job-positive-delay/README.md) contains a
complete test copy changing only the delay to one second; assertions, helpers,
public paths and timeout are unchanged. It remains outside compiled projects,
uncompiled and unrun pending review.

All 1,277 production source-tree files outside bin/obj match the initial manifest.
HEAD and the saved stash are unchanged. Evidence is retained in
`/private/tmp/brighter-4170-quartz-completion/`: baseline/initial logs and TRX,
build logs, original Quartz source, source manifest and initial-verification.json.
No native service was needed or started. Core and gateway code are unchanged and
their previous results remain checkpoints; those suites were not rerun here.
No commit or push was made. Quartz completion and full #4170 acceptance remain open.

## Quartz successful callback completion fix

The [approved one-second amendment](review/quartz-job-positive-delay/README.md)
changes only the fixture delay. All original zero-delay source and failure evidence
remain retained. The amended cases compile and, on both frameworks, reproduce
four failures at `Assert.Null(jobError)` (line 136), after their successful-delivery
identity/content assertions. Quartz reports the unconditional missing-payload
exception. The missing-payload guard passes on each framework.

The production correction adds a return after each awaited recognized-payload
dispatch in `QuartzBrighterJob.Execute`. It retains the final error for a job with
no recognized payload and does not catch dispatch errors. No other production
file changes in this batch. All five cases then pass on .NET 9 and .NET 10.

| Independent .NET 9 mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Restore fallthrough after both payloads | 4 | No job error, line 136 |
| Omit message dispatch, retain return | 2 | One delivered message, line 125 |
| Omit request dispatch, retain return | 2 | One delivered message, line 125 |
| Omit missing-payload guard | 1 | Job error present, line 154 |

All four mutations compile and yield the nine intended assertion failures. Exact
GREEN source bytes are restored after each; both rebuilt focused suites pass all
five cases. Quartz builds on netstandard2.0, net8.0, net9.0 and net10.0. Final
incremental library/test builds report zero warnings/errors. The amended test
build reports 120 existing warnings across both targets, none in the four new
files; no suppressions were added.

| Full Release suite | Result |
| --- | --- |
| Quartz .NET 9 | 53 passed, zero failures/skips; approximately 2 m 25 s |
| Quartz .NET 10 | 53 passed, zero failures/skips; approximately 2 m 25 s |

Every baseline test name/outcome is retained: 48 previous passes plus five new
passes. The compiled test matches the approved positive-delay draft byte-for-byte;
its three helpers match the original drafts. Of 1,277 production source-tree
files outside bin/obj, 1,276 are unchanged. The only difference is the two early
returns in the Quartz job, checked against the original snapshot. HEAD and the
saved stash are unchanged; git diff --check passes.

Artifacts remain under `/private/tmp/brighter-4170-quartz-completion/`, including
both baseline runs, original scheduling failures, amended completion failures,
GREEN/restored/full TRX files, four mutation logs/assertion evidence, all-target
build logs, original/GREEN source snapshots, source manifest and verification.json.
No native services were started. Shared Core and gateway code are unchanged;
their prior full-suite results remain checkpoints and were not rerun here. No
remote CI result, commit or push is claimed.

The separate zero-delay timestamp rejection remains unchanged. The next
[eight native Quartz/RabbitMQ cases](review/quartz-rmq-wire-dispatch/README.md)
are prepared outside compiled projects. Their reference patch passed only the
read-only applicability check; it is unapplied, and all five draft C# files remain
uncompiled/unrun. Full #4170 acceptance still requires the remaining scheduler,
failure-recovery, gateway and configuration completion gates.

## Quartz native RabbitMQ wire dispatch

The [approved eight cases](review/quartz-rmq-wire-dispatch/README.md) run actual
Quartz message/request callbacks through the configured RabbitMQ Async writer.
They cover sync/async scheduling and dispatch, and traceparent in custom AMQP
headers or JSON body. All five reviewed files compile unchanged after adding the
single approved Quartz reference to the test project. Initial and restored focused
checks each pass all 16 framework/case executions. No production correction is
needed.

Message cases enter through the producer's delayed-send API with a Quartz scheduler
attached; request cases enter through the public request scheduler and Post path.
Native observation verifies no early publication while Quartz is stopped, then
one successfully completed job and confirmed custom publication after firing.
It checks payload/identity, exact replacement wire shape, persistence, SDK sequence
metadata and the actual producer traceparent. The original application objects'
complete serialized representations remain unchanged, the outbox settles, and no
duplicate remains. These are real callbacks and native broker observations, not
manual job execution or a generated conformance pump.

| Independent production mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Ignore publication writer | 8 | Native legacy-id presence, line 198 |
| Propagate producer context after publication | 8 | Native producer traceparent equality, line 224 |
| Corrupt deserialized message payload | 4 | Original native payload, line 222 |
| Corrupt deserialized request payload | 4 | Original native payload, line 222 |

The first writer mutation produced eight Assert.True failures, but two Release
stack traces lacked source locations. That run remains in mutation-writer-first.*.
All four mutations were then verified using DOTNET_TieredCompilation=0 and
DOTNET_JitMinOpts=1 for diagnostic source locations: 24 failures at exactly the
intended assertions. Test and mutation semantics were unchanged. Original source
bytes were restored after each mutation. Focused and full regressions use default
runtime settings.

All 1,277 production source-tree files outside bin/obj match the pre-batch manifest,
including the preceding Quartz completion fix. Five compiled test/helper files
match their reviewed drafts exactly; the test project differs only by the approved
reference. RabbitMQ Async, Quartz and ServiceActivator build on netstandard2.0,
net8.0, net9.0 and net10.0. Final incremental builds report zero warnings/errors.
The initial RabbitMQ test build has 2,318 existing warnings across both targets,
none in the five new files; no suppression was added.

| Complete Release regression | Result |
| --- | --- |
| RabbitMQ net9.0 | 548 passed, 2 existing skips, zero failures; 11 m 5 s |
| RabbitMQ net10.0 | 548 passed, 2 existing skips, zero failures; 10 m 51 s |
| Quartz net9.0 | 53 passed, zero failures/skips; 2 m 25 s |
| Quartz net10.0 | 53 passed, zero failures/skips; 2 m 25 s |

Both RabbitMQ suites retain every preceding 540 pass/two skip by exact name/outcome
and add eight passes. Both Quartz suites retain their exact preceding 53 passes.
Shared Core code is unchanged, so its previous 1,716-pass/seven-skip suites remain
checkpoints and were not rerun here. RabbitMQ 4.2/.NET 9 and 4.3/.NET 10 run on
isolated task brokers, including TLS/mTLS and native-delay coverage, without using
unrelated host port 5672. Idle sleep is inhibited. No remote CI result is claimed.

The concurrent runners initially shared console-log/result basenames. Complete
Quartz TRX files were archived as quartz-full-net*.trx, with SHA-256 hashes and
53 Quartz test names verified, before RabbitMQ finalized its own full-net*.trx.
Final verification independently compares each archived/result set with its own
baseline. Initial console logs overlap; the complete, separately named TRX files
are the authoritative results. The Quartz runner now prefixes future outputs
with quartz-. See result-naming-note.txt and quartz-archive.json for the retained
harness correction. No test behavior changed.

Artifacts are under `/private/tmp/brighter-4170-quartz-rmq/`, including logs/TRX,
source snapshots, manifest, mutation evidence, build logs and verification.json.
HEAD and the saved stash remain unchanged; git diff --check passes. Task brokers
are stopped after validation. No commit, push or external publication was made.

The next [eight writer-recovery cases](review/quartz-rmq-writer-recovery/README.md)
and one helper are prepared outside compiled projects, uncompiled and unrun.
Remaining scheduler integrations, callback-pump failure recovery, scheduler-backed
consumer retries, other gateways and final configuration review remain open for
full #4170 acceptance.

## Quartz scheduled writer recovery: initial assertion finding

The [approved recovery batch](review/quartz-rmq-writer-recovery/README.md) was
copied unchanged into the RabbitMQ Async tests. Core, RabbitMQ Async,
ServiceActivator and Quartz build on all their supported targets; all three test
projects build for .NET 9 and .NET 10. An initial sandboxed build stalled and was
stopped; retrying with build permissions completed successfully.

All eight cases on each framework/broker pair fail at line 247, asserting that
the recovery activity directly parents the publish span. Earlier assertions
verify failed writer attempts, retained original outbox data, no premature broker
publication, successful recovery confirmation, native custom output and equality
between the native traceparent and actual publish span. Assertions after line 247
are unexecuted. This is a test-contract error: both public outbox-clear methods
insert a clear span between the recovery activity and publish span.

The [amended assertion](review/quartz-rmq-recovery-trace-chain/README.md) captures
clear spans separately and verifies both parent links and trace IDs, preserving
all other checks. It remains outside compiled projects, uncompiled and unrun,
pending the repository's semantic-test approval gate. The original approved test
and helper remain unchanged in their review and compiled-source locations. The
current test batch is RED and must not be reported as complete or production ready.

Artifacts under `/private/tmp/brighter-4170-quartz-recovery/` retain both initial
TRX files, all 16 verified assertion failures, build logs, source snapshots,
manifest and red-verification.json. All 1,277 production source files match the
pre-batch manifest; HEAD and the saved stash are unchanged. No mutations or full
regression suites were run in this batch. Isolated task brokers were stopped after
the focused run. No commit, push or external publication was made.

## Quartz scheduled writer recovery validated

The [approved trace-chain amendment](review/quartz-rmq-recovery-trace-chain/README.md)
is applied with one equivalent xUnit assertion-overload cleanup; the writer helper is unchanged. All eight cases pass
on both framework/broker pairs before mutations and after exact source restoration.
The original 16 direct-parent failures remain retained as evidence of the test
contract correction; production instrumentation was not changed.

The native cases cover message/request scheduling, sync/async dispatch, and a
writer that throws or returns null. Failed publications retain one original outbox
message with no native delivery or successful confirmation. After explicit recovery,
the native custom publication uses the actual producer traceparent beneath the
fresh recovery activity's outbox-clear span. Original objects remain unchanged,
the outbox settles and no duplicate remains. All assertions now execute.

| Independent mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Fallback to default mapping after writer failure | 8 | Native queue remains empty, line 202 |
| Mark failed publication dispatched | 8 | One pending outbox entry, line 204 |
| Propagate context after publication | 8 | Native traceparent equals publish span, line 251 |

All three mutations produce 24 verified failures at the intended assertions.
Diagnostic JIT settings retain source locations in mutation runs only. Amended,
restored and full regressions use normal runtime settings. All 1,277 production
source files match the pre-batch manifest, including the earlier Quartz fix. The
compiled test differs from the amended review only by the equivalent
`Assert.Single(confirmations, result => result.Success)` overload replacing
`Assert.Single(confirmations.Where(result => result.Success))` at the same line.
This resolves xUnit2031 without changing the predicate or assertion semantics;
the helper matches the original review. No dependency or project-reference change is needed.

Core, RabbitMQ Async, ServiceActivator and Quartz build on netstandard2.0, net8.0,
net9.0 and net10.0. All affected test projects build on both test frameworks; no
warnings originate in the two new files after the overload cleanup. The earlier
amended build reported xUnit2031 once per framework; those logs are retained.
Both full RabbitMQ suites were rerun after this cleanup; their final results are
final-net9.0.trx and final-net10.0.trx.

| Complete Release regression | Result |
| --- | --- |
| RabbitMQ net9.0 | 556 passed, 2 skips, zero failures; 11 m 26 s |
| Quartz net9.0 | 53 passed, 0 skips, zero failures; 2 m 25 s |
| Core net9.0 | 1716 passed, 7 skips, zero failures; 2 m 18 s |
| RabbitMQ net10.0 | 556 passed, 2 skips, zero failures; 11 m 31 s |
| Quartz net10.0 | 53 passed, 0 skips, zero failures; 2 m 25 s |
| Core net10.0 | 1716 passed, 7 skips, zero failures; 2 m 19 s |

The initial .NET 10 full run reported 555 passes, one failure and two existing
skips. Its existing native-delay Classic/fanout routing case returned an empty
redelivery at line 88; all eight new recovery cases passed. The unchanged six-case
native-routing theory then passed three consecutive runs (18 passes), followed
by a complete .NET 10 rerun with 556 passes/two existing skips. Initial failure
TRX/logs and native-delay-finding.json remain retained. The cause was not
established; this evidence does not claim the intermittent failure is fixed.
The failed case reports 3.049 seconds of execution but 320.703 seconds between
TRX start/end timestamps. Its passing counterparts report approximately 3.6 seconds
by both measures. This discrepancy is consistent with a clock/VM interruption,
but does not establish the cause; native-delay-timing.json retains the measurements.
No test, production code, timeout or runtime option was changed for these reruns.
The diagnostic full .NET 10 pass is full-repeat-net10.0.trx; full-net10.0.trx is
the original failing run. Both final full suites also pass after the assertion
overload cleanup.

Both RabbitMQ suites retain each preceding 548 pass/two skip by exact name/outcome
and add eight passes. Both Quartz and Core suites retain every preceding outcome.
All skips are pre-existing. All result and log names are distinct in this batch.
RabbitMQ 4.2/.NET 9 and 4.3/.NET 10 run on isolated task brokers with TLS/mTLS and
native-delay coverage. Task brokers are stopped after validation. No remote CI
result is claimed.

Artifacts are under `/private/tmp/brighter-4170-quartz-recovery/`, including initial
RED evidence, amended/restored TRX, mutation logs/TRX, full regressions, source
snapshots, manifests and verification.json. HEAD and the saved stash remain
unchanged; git diff --check passes. Nothing was committed, pushed or published.

At this checkpoint, the next [six RMQ Sync endpoint cases](review/rmq-sync-endpoint-mapping/README.md)
remained outside compiled projects. Their subsequent validation is recorded below. This completed batch uses
in-memory Quartz/outbox stores and explicit recovery. Database persistence,
automatic callback-pump recovery, remaining schedulers, scheduler-backed consumer
retries, other gateways and final configuration review remain open for #4170.


## RabbitMQ Sync endpoint attempt and native-properties API amendment

The approved four original draft files were copied unchanged into the test
project. Both frameworks failed to compile on missing RmqWireMessage (CS0246).
The first implementation then failed on CS0122: RabbitMQ Client 6's concrete
BasicProperties is internal. The approved helper also directly constructs that
type. XML documentation containing the type did not establish public access.
No endpoint case or mutation ran; no endpoint implementation is claimed complete.

Both clean full pre-change baselines pass 213 cases with one existing skip:
baseline-clean-net9.0.trx and baseline-clean-net10.0.trx. Both use a fresh isolated
RabbitMQ 4.2 broker, repository TLS fixtures and the native-delay companion. The
first attempt on a reused broker encountered a durable/non-durable `mary` queue
conflict and was stopped. Its log is preserved; it did not produce a complete TRX.

All 1,277 production files were restored byte-for-byte to the pre-batch manifest.
Only the four byte-identical compiled test copies introduced by this batch were
removed. The failed implementation and test copies are saved separately under
failed-attempt. The restored library builds all supported targets, and restored
tests build on both frameworks. No semantic test change was made to the originals.

The [amendment](review/rmq-sync-native-properties/README.md) replaces direct native
properties construction with a callback receiving properties created by the
publishing channel. Both assertion files and the reader helper remain identical;
only the writer helper adopts the proposed callback. The revised files remain
outside compiled projects, uncompiled/unrun pending review. The subsequent
[mixed reader-failure tests](review/rmq-sync-reader-failures/README.md) are also
uncompiled/unrun and must follow a completed endpoint batch.

Artifacts: /private/tmp/brighter-4170-rmq-sync-endpoints/, including build logs,
baseline TRX, original source snapshots, failed-attempt and restoration.json.
HEAD and saved stash are unchanged. No commit, push, publication or remote CI run.
Full #4170 implementation and production readiness remain incomplete.


## RabbitMQ Sync callback-based endpoint integration

The approved [native-properties amendment](review/rmq-sync-native-properties/README.md)
resolves the internal RabbitMQ Client 6 constructor issue using the existing
channel's CreateBasicProperties API. All four amended draft files are copied
byte-for-byte into the compiled project. The prior missing-API RED is retained.
The default writer snapshots outgoing application values and headers; its callback
populates fresh channel properties. The gateway then applies persistence and
native delivery controls. Default retries retain their original format. The
configured reader retains custom content while the consumer supplies broker tags,
redelivery, persistence and canonical handled count. Existing public constructors,
Core contracts and package dependencies are unchanged.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Six endpoint cases, normal runtime | 6 passed | 6 passed |
| Restored endpoint cases after mutations | 6 passed | 6 passed |
| Complete Sync suite | 219 passed, 1 existing skip | 219 passed, 1 existing skip |

Every pre-change name/outcome is retained: 213 baseline passes and the existing
skip, with exactly six added passes. Both frameworks use the isolated RabbitMQ
4.2 broker, TLS fixtures and native-delay companion. Library builds cover every
supported target; test builds cover both frameworks. No new test warning is
introduced. Final builds enable normal optimization and final runs have no
mutation diagnostic JIT environment settings.

Eight independent named mutations produce 26 intended failures: ignoring the
publication writer (4), ignoring the subscription reader (2), dropping persistence
normalization (4), retaining fabricated delivery tags (4), retaining fabricated
delay (4), moving trace propagation after mapping (4), omitting reader delivery
metadata (2), and omitting reader handled count (2). All source changes are
restored after each mutation. The first optimized trace mutation already failed
all four traceparent equalities, but its PDB mapped the assertion to line 107
instead of 109. That result is retained separately; diagnostic compilation without
optimization confirms the exact source assertion, followed by the remaining reader
mutations. It is not counted twice in the eight-mutation total.

Seven existing Sync production files change and three new public adapter/contract
files are added. Every other pre-batch production file matches its manifest.
HEAD and saved stash remain unchanged, approved test bytes match, and diff checks
pass. Artifacts and verification.json are under
/private/tmp/brighter-4170-rmq-sync-endpoints/. Both task brokers are stopped after
validation. No commit, push, publication or remote CI result is claimed.

The next [mixed reader-failure batch](review/rmq-sync-reader-failures/README.md)
remains outside compiled projects, uncompiled/unrun pending review. Custom reader
exceptions/nulls currently reach the batch-level catch and can hide dequeued
messages; this is a known incomplete failure path. Other Sync failure/recovery,
retry/rejection, concurrency and scheduler behavior, other gateways and final
configuration review remain open for #4170.


## RabbitMQ Sync configured-reader failures in mixed batches

The approved [four cases](review/rmq-sync-reader-failures/README.md) are copied
unchanged with their helper into compiled projects. Both frameworks reproduce
four Assert.Null failures: configured throw/null mapping reaches the outer Receive
catch, becomes ChannelFailureException, and prevents both dequeued deliveries from
being returned. The correction catches mapping failures inside ReadMessage,
creates an unacceptable message retaining native ID/topic and exact body bytes,
and applies the broker metadata/count before returning it. The batch continues
and preserves the good delivery's mapped content. The diagnostic log distinguishes
mapping failure from broker failure. No public constructor or dependency changes.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Initial RED, intended no-exception assertion | 4 failed | 4 failed |
| Corrected cases | 4 passed | 4 passed |
| Restored cases, normal optimization/runtime | 4 passed | 4 passed |
| Full Sync suite | 223 passed, 1 existing skip | 223 passed, 1 existing skip |

Every preceding outcome is retained by exact test name: 219 passes and one existing
skip plus four new passes per framework. Tests use isolated RabbitMQ 4.2 with
TLS fixtures and the native-delay companion. Library builds cover netstandard2.0,
net8.0, net9.0 and net10.0; test builds cover net9.0/net10.0. Final library builds
have zero warnings/errors and the two new test files introduce no warnings.

Five named mutations each produce four intended assertion failures: escape to the
batch catch (no-exception assertion); successful default fallback (unacceptable
classification); omitted failed body (exact bytes); omitted failed native metadata
(valid tag); and default mapping of the good delivery (custom application ID).
The total is 20 intended failures. Diagnostic compilation/JIT settings retain
precise source locations; each mutation is restored, then final builds/runs use
normal optimization and runtime settings. Neither approved test file is edited.

Only RmqMessageConsumer.cs changes among 1,280 pre-batch production files. It adds
per-delivery recovery, a diagnostic log and XML remarks describing that behavior.
The other 1,279 source files match the manifest. HEAD and saved stash are unchanged;
diff checks and local documentation link checks pass. Artifacts are retained in
/private/tmp/brighter-4170-rmq-sync-reader-failures/, including source snapshots,
RED/GREEN/restored/full TRX, mutation evidence and verification.json. The two task
brokers are stopped after validation. No commit, push, publication or remote CI run.

The [four default-parser failure cases](review/rmq-sync-default-parser-failures/README.md)
are drafted outside compiled projects and remain uncompiled/unrun pending review.
The parser catches its own errors and still drops payload/metadata in its failure
placeholder; configured-reader recovery cannot intercept a returned placeholder.
Explicit custom unacceptable returns, invalid forwarding, writer/callback recovery,
retry selection, concurrency, schedulers and other gateways remain open for #4170.


## RabbitMQ Sync default-parser failure preservation

The approved [four cases](review/rmq-sync-default-parser-failures/README.md) are
copied unchanged into the Sync test project. On both frameworks all four first fail
at the exact binary-body assertion: the expected five bytes are replaced by an
empty body. The parser catches its own exception, so configured-reader exception
recovery cannot intercept it. The correction retains native body bytes, handled
count and broker delivery metadata in the parser's failure branch. Existing
successful parsing is unchanged. The public default reader's XML return description
now documents this behavior; no signature or dependency changes.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Initial RED, exact binary body assertion | 4 failed | 4 failed |
| Corrected cases | 4 passed | 4 passed |
| Restored cases, normal optimization/runtime | 4 passed | 4 passed |
| Initial full suite | 226 passed, 1 failed, 1 existing skip | Not started after failure |
| Queue-binding timeout case, three rechecks | 3 passed | 3 passed |
| Final full suite | 227 passed, 1 existing skip | 227 passed, 1 existing skip |

The first full .NET 9 run failed an existing Classic.Proactor rejection test while
provisioning its queue, before any message was published or parsed. The client
reports a 30-second QueueBind timeout. At the same time, RabbitMQ reports
`{error,eio}` in `ra_log_wal:flush_pending`, restarts its coordination log supervisor,
and reports `Could not add binding due to timeout`. Both broker logs and the failed
TRX are retained. No source or test changes were made for the rerun. The affected
case passes three times per framework, followed by both full suites. This supports
an infrastructure explanation; the underlying storage I/O cause is not resolved.

Four approved mutations produce 12 intended failures: omitted native body (four,
exact bytes at line 110), omitted failed metadata (two unset-reader cases, tag at
line 120), omitted failed handled count (two unset-reader cases, count at line 123),
and application-event classification (four, unacceptable signal at line 106).
Configured default readers retain consumer-level metadata/count normalization and
therefore pass the two corresponding parser mutations. The classification mutation
harness initially used an unavailable body constructor, then required nullable
constructor defaults; both failed build logs are retained and count as no evidence.
The corrected mutation compiles and produces all four intended assertion failures.
Each mutation is restored. Diagnostic builds/JIT settings retain precise source
locations; final builds and runs use normal optimization and runtime settings.

The library builds netstandard2.0, net8.0, net9.0 and net10.0 with zero warnings or
errors. The approved test is byte-identical to the draft and introduces no warnings.
Final full outcomes retain every preceding 223-pass/one-skip result by exact name,
plus four new passes on each framework. Only RmqMessageCreator.cs and
DefaultRmqWireMessageReader.cs change among 1,280 pre-batch production files; the
other 1,278 files match the manifest. HEAD and the saved stash remain unchanged.

Evidence is retained under /private/tmp/brighter-4170-rmq-sync-default-parser-failures/:
source snapshots and manifest, RED/GREEN/restored/full TRX, the initial full failure,
broker logs, six recheck TRX, mutation builds/results and verification.json. Tests
use isolated RabbitMQ 4.2 with TLS fixtures and the native-delay companion. Local
diff and documentation-link checks pass. Task brokers are stopped after validation.
No commit, push, publication or remote CI run was performed.

The next [ten invalid-forwarding/recovery cases](review/rmq-sync-invalid-forwarding/README.md)
are drafted outside compiled projects and remain uncompiled/unrun pending review.
They cover unset/explicit default readers, throw/null recovery and an explicit
unacceptable custom result, with successful routing and recovery from an unbound
invalid queue. Six named mutations define 42 intended failures. Custom invalid-writer
selection, writer/callback/outbox recovery, retry selection, concurrency, scheduler
paths and other gateways remain open for full #4170 implementation.


## RabbitMQ Sync invalid mapping forwarding and recovery

Both files from the approved [ten cases](review/rmq-sync-invalid-forwarding/README.md)
are copied unchanged into the Sync test project. They pass on first execution on
both frameworks, so the approved named mutations supply behavioural RED. No
production correction is required for the observed forwarding/recovery paths.
Coverage includes unset/explicit default readers, throwing/null readers and an
explicit unacceptable custom result, with working and unbound invalid routes.
Native publishing and BasicGet inspect payload, identity, application/native topic,
rejection metadata, count and persistence independently of production mappers.

The first build stops before test compilation because cached NuGet assemblies and
.NET 8/9 targeting packs are missing. The failed build log is retained. A scoped
`dotnet restore tests/Paramore.Brighter.RMQ.Sync.Tests/Paramore.Brighter.RMQ.Sync.Tests.csproj`
restores the dependencies; subsequent builds pass without source or package-declaration
changes. This build failure is not behavioural RED.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Initial cases, no production changes | 10 passed | 10 passed |
| Restored cases, normal optimization/runtime | 10 passed | 10 passed |
| Full Sync suite | 237 passed, 1 existing skip | 237 passed, 1 existing skip |

Every preceding outcome is retained by exact name: 227 passes and one existing skip
plus ten new passes per framework. Final library builds cover netstandard2.0,
net8.0, net9.0 and net10.0 with zero warnings/errors. Both test targets build, and
the two approved files introduce no warnings. Tests use isolated RabbitMQ 4.2 with
TLS fixtures and the native-delay companion.

| Mutation | Recorded result | Intended assertion |
| --- | --- | --- |
| Replace configured-reader output with default parsing | 2 failed, 8 passed | Explicit mapped ID, line 121 |
| Empty forwarded body | 10 failed | Exact bytes, line 195 |
| Acknowledge before forwarding | 5 failed, 5 passed | Source still present, line 148 |
| Ignore mandatory BasicReturn | 5 failed, 5 passed | Forwarding exception, line 138 |
| Omit source acknowledgement, first run | 9 failed, 1 passed | Source absent, line 192; one false negative |
| Omit source acknowledgement, unchanged repeat | 10 failed | Source absent, line 192 |
| Omit topic restoration | 10 failed | Original topic, line 139 or 190 |

The six complete mutation results total 42 intended failures. The first
acknowledgement-omission run is retained separately, not replaced or counted as a
fully verified mutation. Its explicit-reader/unbound-route case passes despite
the omitted acknowledgement. Consumer disposal aborts its channel; the immediate
one-shot BasicGet may observe the queue before broker requeue becomes visible.
This explains a plausible timing window, not a proven trace of the missed delivery.
The repeat finding does not justify claiming the check is robust.

Mutation runs use diagnostic compilation/JIT settings for exact assertion locations.
All temporary mutations are restored byte-for-byte, and normal builds/focused
runs follow. All 1,280 pre-batch production files match the source manifest; no
new production file or dependency is added. Both reviewed files remain byte-identical.
HEAD and the saved stash are unchanged. Local diff and documentation-link checks
pass; the task brokers are stopped after validation. No commit, push, publication
or remote CI run was performed.

Evidence lives under /private/tmp/brighter-4170-rmq-sync-invalid-forwarding/:
initial/restored/full TRX and logs, all mutation builds/TRX, the retained first
acknowledgement-omission miss, source manifest/snapshot and verification.json. The
verifier explicitly leaves settlement-observation robustness open. The earlier
parser batch's broker storage I/O finding remains separately recorded; this batch
does not claim to fix that infrastructure issue.

The [settlement-observation amendment](review/rmq-sync-settlement-observation/README.md)
is drafted outside compiled projects and remains uncompiled/unrun pending review.
It preserves all ten inputs and assertions while using bounded native observation
after disposal for source recovery and absence. It requires all six mutations and
three additional acknowledgement-omission runs per framework before restored full
regression. This is the active next gate. Invalid-writer feature preparation remains
incomplete and is not included in that approval. Full #4170 implementation is open.


## RabbitMQ Sync settlement-observation amendment

The approved [amended forwarding test](review/rmq-sync-settlement-observation/README.md)
is copied byte-for-byte over the compiled test. The helper and all 1,280 production
files remain unchanged. Only the source recovery/absence observations after consumer
disposal change: poll the independent native channel for up to two seconds with
25-millisecond intervals. Inputs and payload/identity/metadata assertions remain.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Initial amended cases | 10 passed | 10 passed |
| Restored cases, normal optimization/runtime | 10 passed | 10 passed |
| Complete Sync suite | 237 passed, 1 existing skip | 237 passed, 1 existing skip |

Both complete suites retain every preceding outcome by exact test name; there are
no added or removed cases. Normal Release library builds cover netstandard2.0,
net8.0, net9.0 and net10.0 with zero warnings/errors. Both test targets build and the
amended test introduces no warning.

| Named mutation, .NET 9 | Result | Intended assertion |
| --- | --- | --- |
| Replace configured-reader result with default parsing | 2 failed, 8 passed | Mapped ID, line 123 |
| Empty forwarded body | 10 failed | Exact bytes, line 210 |
| Acknowledge before forwarding | 5 failed, 5 passed | Original delivery, line 156 |
| Ignore mandatory BasicReturn | 5 failed, 5 passed | Forwarding exception, line 140 |
| Omit source acknowledgement | 10 failed | Source absent throughout observation, line 207 |
| Leave the topic at the invalid route | 10 failed | Original topic, line 141 or 198 |

All six mutations produce the required 42 intended failures. Three additional
omit-source-ack runs on .NET 9 and three on .NET 10 each produce ten failures at
Assert.Null, line 207: 60 additional intended failures, zero misses. All seven
acknowledgement-omission runs with the amendment therefore detect all ten cases.
No failure is accepted on a build error, timeout or unrelated assertion. Mutation
runs use diagnostic compilation/JIT settings; production bytes are restored before
normal optimized builds and the focused/full regressions.

The preceding batch's first omit-source-ack run (nine failures/one false-negative
pass) is still retained under its original name. The amended checks support bounded
observation in this environment, not absence forever or an atomic forward/ack
operation. The earlier parser batch's broker storage I/O finding remains unresolved;
these results do not claim to fix its infrastructure cause.

Evidence is under `/private/tmp/brighter-4170-rmq-sync-settlement-observation/`:
initial/restored/full TRX, six mutation TRX and logs, six additional acknowledgement
TRX/logs, normal/diagnostic builds, source manifest and verification.json. The
verifier checks all source hashes, exact approved amendment/helper bytes, unchanged
HEAD/stash and the next feature drafts' absence from compiled projects. Local links
and git diff whitespace checks pass. Task-owned brokers are stopped after testing.
Nothing is committed, pushed, published or asserted about remote CI.

The next [fourteen Sync invalid-writer cases](review/rmq-sync-invalid-writer/README.md)
cover replacement/composition, writer throw/null, callback failure, unbound-route
recovery, selection isolation and native dead-letter bypass. They are outside
compiled projects and remain uncompiled/unrun pending review. Full Sync parity and
complete #4170 implementation remain open.


## RabbitMQ Sync invalid-writer selection: pending test-contract correction

The approved [fourteen-case batch](review/rmq-sync-invalid-writer/README.md) is copied
unchanged into the compiled Sync project. Both test targets initially fail only
with CS0117 for the missing `InvalidMessageWireMessageWriter` setting. The local
implementation adds that nullable subscription setting with XML documentation,
captures it on each consumer and passes it to the invalid-forward publisher.
Existing public constructors, publisher behavior and dependencies are unchanged.

All four library targets and both test targets build after implementation. The
first attempted green run on .NET 9 produces ten failures and four passes; .NET 10
confirms the same result. All ten failures occur at line 192, the incorrect assertion
that the forwarded delivery-tag header is absent. Isolation and native dead-letter
bypass each pass their two cases. Assertions after line 192 in the recovery test
have not executed; those remaining guarantees are not established by these runs.

The draft overlooked that `Message.DeliveryTag` writes its value into the header
bag. The Sync parser sets this from the native delivery. The publisher removes the
writer-supplied tag and adds that native value. Changing production to omit this
established gateway header would distort the intended native-control contract.
The [review-only one-line amendment](review/rmq-sync-invalid-writer-native-tag/README.md)
therefore replaces absence with exact equality to the native delivery tag; it
remains uncompiled/unrun. All other assertions and all fourteen inputs are preserved.

Only RmqSubscription.cs, RmqMessageConsumerFactory.cs and RmqMessageConsumer.cs change
among 1,280 pre-batch production files. The other 1,277 files, dependency manifest,
approved test bytes, HEAD and saved stash are unchanged. No mutation or full suite
has run for this implementation; the prior 237-pass full suites predate it and
cannot be presented as validation of it. The planned nine mutations/22 intended
failures, restored runs and two full suites remain required after amendment approval.

Evidence is retained in `/private/tmp/brighter-4170-rmq-sync-invalid-writer/`:
`build-red-tests.log`, successful implementation builds, `green-net9.0.trx` (the
failed attempted-green run), `initial-net10.0.trx`, corresponding logs, source and
dependency manifests, pre-edit snapshots and `verification-pending.json`. The
feature remains incomplete. Task-owned brokers are stopped; no commit or push is
made. Full #4170 implementation remains open.


## RabbitMQ Sync invalid-writer validation completed

The approved [one-line native-tag correction](review/rmq-sync-invalid-writer-native-tag/README.md)
is copied unchanged over the compiled recovery test. The other two tests and helper
remain identical to the original approved batch. The first missing-setting compile
errors and both ten-failure/four-pass runs at the incorrect absence assertion remain
in the preceding evidence; they are not counted as behavioural mutation evidence.
No production behavior changes to make the corrected assertion pass.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Amended focused cases | 14 passed | 14 passed |
| Restored focused cases, normal optimization/runtime | 14 passed | 14 passed |
| Complete Sync suite | 251 passed, 1 existing skip | 251 passed, 1 existing skip |

Every preceding outcome is retained by exact name: 237 passes and one existing skip,
plus fourteen new passes per framework. Normal Release library builds cover
netstandard2.0/net8.0/net9.0/net10.0 with zero warnings/errors. Both test targets build,
and the approved new files introduce no warnings. The tests use the isolated
RabbitMQ 4.2 broker with TLS fixtures and the native-delay companion.

| Mutation, .NET 9 | Intended failures | Assertion |
| --- | ---: | --- |
| Ignore selected invalid writer | 2 | Profile present, line 186 |
| Acknowledge before forwarding | 6 | Original delivery retained, line 147 |
| Omit topic restoration | 2 | Original topic, line 178 |
| Disable mandatory forwarding | 2 | Forwarding error, line 127 |
| Cache first factory selection, including null | 2 | Per-consumer profile, line 118 |
| Omit native BasicReject | 2 | Native dead-letter delivery, line 126 |
| Force transient delivery mode | 2 | Persistence, line 190 |
| Swallow native-properties callback error | 2 | Callback error, line 127 |
| Omit reserved-header cleanup | 2 | No writer-owned delay, line 191 |

All nine mutations produce the required 22 intended failures. Actual selected cases
and exact assertion locations are checked in TRX with diagnostic compilation/JIT
settings. Every mutation is restored byte-for-byte before normal builds and the
focused/full regressions. No build error, timeout or unrelated failure is accepted
as mutation evidence.

Only RmqSubscription.cs, RmqMessageConsumerFactory.cs and RmqMessageConsumer.cs change
among the 1,280 pre-batch production files. The other 1,277 files and dependency
manifest are unchanged. Existing public constructors remain unchanged. The new
nullable public setting is documented; the factory captures it and the consumer
passes it to the existing publisher only for invalid forwarding. There are no
publisher, core, requeue or async factory/consumer changes in this increment.

Evidence is retained under `/private/tmp/brighter-4170-rmq-sync-invalid-writer/`:
amended/restored/full TRX and logs, nine mutation TRX/builds, initial failure records,
normal builds, source/dependency manifests, green snapshots and verification.json.
HEAD and saved stash remain unchanged. Local documentation links and whitespace
checks pass; task-owned brokers are stopped. No commit, push, publication or remote
CI validation is claimed. The earlier broker storage I/O finding and historical
settlement-observation false negative remain separately recorded.

The next [six publication failure/outbox recovery cases](review/rmq-sync-publication-recovery/README.md)
are prepared outside compiled projects and remain uncompiled/unrun pending review.
They cover writer throw/null and callback throw through sync and compatibility
async producer/outbox paths, a healthy message after a failed send, confirmation
identity, and recovery of the original pending message. Full Sync parity and #4170
remain open beyond this completed invalid-writer increment.


## RabbitMQ Sync publication mapping and outbox recovery completed

Both [approved files](review/rmq-sync-publication-recovery/README.md) were copied
unchanged. The six cases were green on arrival; no production correction was
needed. They cover writer throw, writer null and native-properties callback throw,
each through synchronous and compatibility asynchronous producer/outbox APIs.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Initial focused cases | 6 passed | 6 passed |
| Restored focused cases | 6 passed | 6 passed |
| Complete Sync suite | 257 passed, 1 existing skip | 257 passed, 1 existing skip |

Each full suite retains all preceding 251 passing cases and the existing skip by
exact name. Normal optimized Release builds cover all four library targets with
zero warnings/errors and both test targets without new warnings in approved files.
The isolated RabbitMQ 4.2 broker, TLS fixtures and native-delay companion were used.

| Mutation, .NET 9 | Intended failures | Assertion |
| --- | ---: | --- |
| Ignore selected publication writer | 6 | Mapping error present, line 110 |
| Swallow native-properties callback error | 2 | Mapping error present, line 110 |
| Retain failed pending-confirmation entry | 6 | Healthy confirmation ID, line 152 |
| Publish empty mapped body | 6 | Healthy native bytes, line 165 |
| Prematurely dispatch failed outbox entry | 6 | Original remains pending, line 129 |

All five mutations produce 26 intended failures. TRX verifies exact case counts,
assertions and source locations. Mutations to producer, publisher and mediator were
restored byte-for-byte before normal focused and full regressions. Compile errors,
timeouts and unrelated failures are not accepted as mutation evidence.

The cases establish failure without delivery/confirmation, original pending content,
healthy-message progress with correct confirmation identity, and repaired original
message dispatch. Confirmation callbacks are drained before final count checks.
They use a real producer, real broker confirmations and an in-memory outbox, with
bounded observation. They do not prove outage atomicity or exactly-once delivery.

All 1,280 pre-batch production files and dependency hashes remain unchanged; no new
production file is added. Approved test bytes, HEAD and the saved stash match their
snapshots. Evidence lives in `/private/tmp/brighter-4170-rmq-sync-publication-recovery/`:
initial/restored/full TRX and logs, five mutation records and builds, manifests,
normal builds, snapshots and verification.json. Local documentation links and
whitespace checks pass; task-owned brokers are stopped. Nothing was committed,
pushed or published, and remote CI was not run. Earlier broker storage I/O and
settlement-observation findings remain recorded above.

The next [eighteen immediate-retry cases](review/rmq-sync-requeue-writer/README.md)
are outside compiled projects and remain uncompiled/unrun pending approval. They
cover replacement/composition, first/subsequent identity, writer/callback failure
recovery, native controls and per-consumer selection. Delayed and scheduler-backed
retry, concurrency, other transport rollout and full #4170 remain open.


## RabbitMQ Sync immediate retry mapping completed

All three [approved files](review/rmq-sync-requeue-writer/README.md) were copied
unchanged. The initial build reports the missing `RequeueWireMessageWriter` property
on both targets (four CS0117 errors). This is not behavioural RED. Four production
files implement the contract before the named mutation checks establish the guards.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Initial focused cases after implementation | 18 passed | 18 passed |
| Restored focused cases | 18 passed | 18 passed |
| Complete Sync suite | 275 passed, 1 existing skip | 275 passed, 1 existing skip |

Both full suites retain all preceding 257 passes and the existing skip by exact name.
Normal optimized Release builds pass for all four library targets with zero
warnings/errors, and both test targets. One xUnit2031 style warning remains at recovery-test line 211:
`Assert.Single` recommends its predicate overload instead of filtering with `Where`.
It appears on both initial target builds and on the final rebuilt .NET 9 target.
An equivalent one-line cleanup is drafted with the next batch; approved test bytes
remain unchanged. The planned no-new-test-warning check is therefore not met yet.
Local RabbitMQ 4.2, TLS fixtures and the native-delay companion provide the fixtures.

| Mutation, .NET 9 | Intended failures | Assertion location |
| --- | ---: | --- |
| Ignore retry writer | 4 | Profile presence, line 201 |
| Acknowledge before mapping | 12 | Original delivery retained, line 144 |
| Omit source acknowledgement | 4 | Source routing or extra delivery, line 196/221 |
| Keep writer-owned native ID | 4 | Fresh ID, line 199 |
| Keep writer-owned persistence | 4 | Native persistence, line 205 |
| Keep writer-owned handled count | 4 | Count, line 208 |
| Keep writer-owned delivery tag | 4 | Retry marker, line 209 |
| Keep writer-owned delay | 4 | Immediate retry delay absent, line 210 |
| Keep writer original-ID variants | 4 | One original-ID entry, line 211 |
| Overwrite original identity | 2 | Source key/value, line 212/213 |
| Cache first factory selection, including null | 2 | Per-consumer profile, line 121 |
| Empty retry body | 4 | Native payload, line 203 |
| Route through application exchange/topic | 4 | Source-queue routing, line 196 |
| Mutate source handled-count bag entry | 4 | Source bag unchanged, line 185 |

All fourteen mutations produce 60 intended failures. Exact case counts, assertion
kinds and line locations are checked from TRX. Two harness problems were corrected:
the acknowledgement replacement initially matched two sites, so it was narrowed to
the retry return block before running that mutation; factory-cache fields initially
split a method's XML documentation and caused CS1572, so those temporary fields were
moved beside existing fields. Neither harness failure counts as behavioural RED.
The failed compiler log and harness notes are retained. Approved tests were never
edited. Every production mutation is restored byte-for-byte before normal builds
and the focused/full regressions.

Only RmqSubscription.cs, RmqMessageConsumerFactory.cs, RmqMessageConsumer.cs and
RmqMessagePublisher.cs change among 1,280 pre-batch production files. The other
1,276 files and all dependency hashes remain unchanged. Existing public constructors
are unchanged. The nullable public setting has XML documentation. The factory
captures selection per consumer; the direct retry publisher uses it, then applies
native identity, persistence, count, delivery/delay and original-ID controls. Immediate
retries clear writer-supplied delay. Original-ID replacement removes ordinal
case-insensitive variants and preserves source spelling/value. Source values remain
unchanged by gateway mapping; the supplied custom strategy must also avoid mutation.

The tests establish immediate retry replacement/composition, first/subsequent
identity, writer throw/null/callback failure recovery and factory selection isolation.
They inspect native payloads independently of readers. The original source remains
available after mapping failure, and a repaired strategy permits recovery through a
new consumer. Source settlement observations are bounded to two seconds; publication
and acknowledgement are not atomic. Native delayed and scheduler-backed mapping,
simultaneous execution and other transports are not established by these new cases.

Evidence lives in `/private/tmp/brighter-4170-rmq-sync-requeue-writer/`: initial build
errors, green/restored/full TRX and logs, fourteen mutation builds/results, the invalid
harness build, harness notes, production/dependency snapshots and verification.json.
Approved bytes, HEAD and saved stash are unchanged. Local links and whitespace
checks pass; task-owned brokers are stopped. No commit, push, publication or remote
CI result is claimed. Earlier broker storage I/O and settlement-observation findings
remain recorded above.

The next [eight native-delay cases](review/rmq-sync-native-retry-writer/README.md)
remain outside compiled projects and uncompiled/unrun pending approval, together
with the one-line xUnit assertion cleanup. They reuse the approved writer helper and cover requested delay, source-queue isolation,
replacement/composition and mapping-failure recovery using the native plugin.
Full #4170 remains open beyond this completed immediate-retry increment.


## RabbitMQ Sync native delayed retry mapping completed

The [eight native-delay cases and approved immediate assertion cleanup](review/rmq-sync-native-retry-writer/README.md)
were copied unchanged for the first run. All 26 focused cases passed immediately
on both frameworks. No production correction was required. The approved predicate
overload resolves xUnit2031 while preserving the original-ID uniqueness guard.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Initial focused cases (18 immediate + 8 native) | 26 passed | 26 passed |
| Restored focused cases after mutations | 26 passed | 26 passed |
| Focused cases after equivalent xUnit2012 cleanup | 26 passed | 26 passed |
| Complete Sync suite | 283 passed, 1 existing skip | 283 passed, 1 existing skip |

Both full suites retain all preceding 275 passes and the existing skip by exact name.
All four library targets build normally in optimized Release with zero warnings or
errors. Both test targets build; the affected tests/helper introduce no warnings
in the final cleaned build. Existing unrelated test warnings remain outside this
increment. Tests use local RabbitMQ 4.2, TLS fixtures and the native-delay companion.

| Mutation, .NET 9 | Intended failures | Assertion |
| --- | ---: | --- |
| Ignore retry writer | 2 | Native profile present, line 219 |
| Acknowledge before mapping | 6 | Original delivery retained, line 160 |
| Omit source acknowledgement | 2 | Source routing or extra source, line 213/241 |
| Replace requested delay with one millisecond | 2 | Elapsed delay or consumed header, line 217/228 |
| Route retry through application exchange/topic | 2 | Native routing or neighbor isolation, line 213/242 |
| Empty mapped body | 2 | Native payload, line 221 |
| Keep conflicting original-ID variants (immediate cases) | 4 | Amended single-match guard, line 211 |

All seven mutations produce the required 20 failures at the intended assertions.
The first delay mutation also produced both intended failures, but the harness
expected the generic Assert.True message rather than the custom elapsed-delay
message. The matcher was corrected to require that exact message pattern at line
217, or the approved Equal assertion at line 228. The original TRX/log are retained;
no test was changed to make a mutation fail. Every production mutation was restored
byte-for-byte before normal focused and full regressions.

The native test initially emitted xUnit2012 at line 173 for
`Assert.True(originalHeaders.Keys.Any(predicate))`. A mechanical style cleanup uses
`Assert.Contains(originalHeaders.Keys, predicate)` with the same predicate and
expectation. The original approved review file and pre-cleanup compiled bytes are
preserved. All 26 focused cases were rerun on both frameworks after that cleanup,
then both complete suites ran. The full-suite stage was deferred until the cleanup
was built; no incomplete run is counted as a full-suite pass. The seven mutation
assertions are on unchanged lines, separate from the cleanup at line 173.

The cases inspect real native retries after replacement/composition and writer
throw/null/callback recovery. Requested three-second delay replaces the writer's
60-second header; native delivery occurs after the requested interval and carries
the plugin's consumed -3000ms header. Retry reaches only the original queue, while
a neighbor bound to the same application topic receives no retry during bounded
observation. Native identity, persistence, count, marker, original-ID chain, payload
and profile are checked. Source values remain unchanged. Mapping failures retain
the native original, and repaired mapping recovers through a new consumer. The batch
uses direct topology; existing default-mapping tests cover topic/fanout separately.
Publication and acknowledgement remain separate operations; no outage atomicity,
exactly-once delivery or unbounded absence of duplicates is established.

All 1,280 pre-batch production files and dependency hashes are unchanged, with no
new production files. The existing writer helper is unchanged. The immediate test
matches its approved amendment exactly; the native test differs from its approved
draft only by the equivalent xUnit2012 cleanup. HEAD and the saved stash match their
snapshots. Evidence lives in `/private/tmp/brighter-4170-rmq-sync-native-retry-writer/`:
initial/restored/cleaned/full TRX and logs, seven verified mutation results, the
original custom-message matcher evidence, style-cleanup notes and snapshots,
production/dependency manifests and verification.json. Local links and whitespace
checks pass; task-owned brokers are stopped. No commit, push, publication or remote
CI result is claimed. Earlier broker storage I/O and settlement-observation findings
remain recorded above.

The next [eight concurrency cases](review/rmq-sync-concurrent-mapping/README.md)
remain outside compiled projects and uncompiled/unrun pending approval. They cover
shared writer/reader isolation across separate endpoints, preserving the producer's
existing serialized send behavior and requiring mapper overlap only for consumer
paths. Scheduler-backed retry, other transports and full #4170 remain open.


## RabbitMQ Sync concurrency checks — one mutation amendment pending

The [eight approved cases](review/rmq-sync-concurrent-mapping/README.md) and two
helpers were copied unchanged. All eight pass initially and after restoring the
temporary production mutations on both .NET 9 and .NET 10. No production correction
was required. Normal optimized Release builds pass for all four library targets and
both test targets. The library has zero warnings/errors, and the four new files
introduce no warnings.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Initial focused | 8 passed | 8 passed |
| Restored focused | 8 passed | 8 passed |
| Full Sync suite | 291 passed, 1 existing skip | 291 passed, 1 existing skip |

Both full suites retain every preceding 283 passing case and the existing skip by
exact name. Eleven verified mutations produce 30 failures at their approved
assertions: ignore publication/retry/invalid writers (six total), empty body (six),
force transient persistence (six), ignore configured reader (two), cache mapped
message (two), omit handled-count/persistence/redelivery/tag overrides (eight).

The twelfth mutation, sharing native properties before mapping callbacks, fails
all four selected cases in each attempt, but at least one case fails at the earlier
operation-success assertion on line 154. That does not satisfy the required member
isolation assertion on line 174 and is not included in the verified total. The
other cases reach the intended assertion. Both attempt logs/TRX are retained.
Concurrent writes to the deliberately shared headers are a plausible explanation;
the swallowed production exception was not captured, so this is not a proven cause.
No tests or assertions were weakened, and the failed attempt was not discarded.

A [review-only amendment](review/rmq-sync-concurrent-cache-amendment/README.md)
caches completed native properties at the publish calls. It retains the intended
shared-state defect while avoiding concurrent dictionary writes. That patch has
not been applied, compiled or run. This concurrency batch is **not complete** until
all four amended mutation cases fail at the intended assertion and normal focused
and full suites pass after restoration.

The cases cover shared writer isolation for publication, immediate retry and
invalid forwarding, plus overlapping shared-reader mapping on separate consumer
channels. Publication retains its existing global send lock; the consumer paths
require mapping overlap. Native content, identity, routing and gateway controls
remain per message, and source content survives. Bounded post-disposal observations
check settlement. This is not a stress test or a new single-channel thread-safety
promise.

All 1,280 pre-batch production files and dependencies are unchanged. The two tests
and two helpers match their approved drafts byte for byte. HEAD and stash retain
the saved values. Evidence is in
`/private/tmp/brighter-4170-rmq-sync-concurrent-mapping/`: initial/restored/full logs
and TRX, eleven verified mutations, both original shared-properties attempts,
source/dependency manifests, build logs and verification.json. Owned brokers are
stopped after validation; local links and whitespace checks pass. No commit, push,
remote CI run or production-readiness claim is made. Scheduler-backed retry and
the remaining transport/scheduler/DSL completion gates remain open.


## RabbitMQ Sync concurrency cache amendment completed

The approved [completed-properties cache patch](review/rmq-sync-concurrent-cache-amendment/README.md)
was applied exactly as reviewed, verified against the stored unified diff. It caches
completed properties per shared connection configuration at native publish calls.
All four selected retry/invalid-forwarding cases fail with `Assert.Equal` at the
native member-isolation assertion, line 174. No test or helper changed. The earlier
two attempts remain preserved and are not counted as successful mutation evidence.

All twelve mutation checks now produce the required 34 intended failures. The
publisher, producer and consumer are restored byte for byte. Normal optimized
Release builds pass for all four library targets and both test targets. The library
has zero warnings/errors, and the four affected files introduce no warnings.

| After amended mutation restoration | .NET 9 | .NET 10 |
| --- | --- | --- |
| Focused concurrency cases | 8 passed | 8 passed |
| Full Sync suite | 291 passed, 1 existing skip | 291 passed, 1 existing skip |

Both full suites retain all preceding 283 passes and the existing skip by exact
name, adding exactly the eight approved concurrency cases. They also match the
291-pass full suites before the amendment. All 1,280 production files, dependencies,
approved tests/helpers, HEAD and saved stash are unchanged. No production fix was
required. Task-owned brokers were stopped after validation. No commit, push or
remote CI result is claimed.

Evidence remains under `/private/tmp/brighter-4170-rmq-sync-concurrent-mapping/`:
`mutation-shared-properties-completed.trx`, `build-amended-*`,
`restored-amended-net9.0.trx`, `restored-amended-net10.0.trx`,
`full-amended-net9.0.trx`, `full-amended-net10.0.trx`, and
`verification-amended.json`. Earlier logs, TRX, snapshots and the incomplete
verification report remain intact. The reviewed patch was validated against exact
source bytes before application and restored in a finally block.

The next [four Kafka default adapter cases](review/kafka-default-wire-mapping/README.md)
remain outside compiled projects, uncompiled/unrun pending review. They cover
native default mapping, the legacy header-builder extension and independent
outputs. No Kafka source has changed. Scheduler-backed RabbitMQ retry, Kafka
endpoint configuration and all remaining global completion gates remain open.


## Kafka default adapters — reader compatibility expectation pending

Both unmodified complete Kafka suites pass 265 cases with no failures or skips on
.NET 9 and .NET 10, using task-owned Apache Kafka 4.0.2 and schema registry 8.0.6.
Tests run in the Kafka container's network namespace, with the schema registry
sharing that namespace for the existing localhost addresses. Existing unrelated
containers/data are untouched. The first no-restore build failed because local
NuGet package files were missing; restoring existing dependencies fixed it without
changing package versions. The failed build log is retained.

The four approved tests and helper were copied unchanged for the first RED build.
That build found the missing adapter types and a namespace collision: the test
assembly has a MessagingGateway.PartitionKey namespace. The compiled tests now
qualify `new Paramore.Brighter.PartitionKey(...)`, with no assertion or behavior
change. Original review files, pre-qualification copies and the failed build are
preserved. Both test targets then fail only for the missing adapter types.

The implementation adds DefaultKafkaWireMessageWriter and DefaultKafkaWireMessageReader,
then routes the existing publisher and consumer through them. The writer reuses
the existing header builder and key/body mapping; the reader delegates to the
unchanged KafkaMessageCreator. Two existing files change and two adapters are added.
All other 1,278 production files and all 268 dependency-manifest entries are unchanged.
Public existing constructors, header-builder API, creator extension points and
native delivery operations are preserved.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Unmodified complete baseline | 265 passed | 265 passed |
| Initial four approved cases after implementation | 3 passed, 1 failed | 3 passed, 1 failed |
| Three writer cases after mutation restoration | 3 passed | 3 passed |
| Post-change full suite | Pending | Pending |

Both initial reader cases fail at line 90: the test expects a ContentType without
charset, but the unchanged MessageBody constructor adds UTF-8 to the parsed type
passed by KafkaMessageCreator. Actual content type is
`application/octet-stream; charset=utf-8`. Production was not changed to strip it.
The [review-only correction](review/kafka-default-content-type/README.md) retains
exact equality and adds the existing charset to the expected value. That amended
test has not been compiled or run.

Seven independent mutations pass their required RED checks on .NET 9: empty writer
key (Equal, line 70), empty writer body (Equal, 71), ignored legacy builder (Equal, 54),
cached native writer result (NotSame, 55), cached header collection (NotSame, 56),
cached reader Message (NotSame, 81), and empty reader body (Equal, 89). The traceparent
and offset mutations remain pending because the incorrect content-type expectation
precedes their assertions. The correction proposes an additional charset mutation,
for a final total of ten. No failure at a different assertion was accepted as proof.
Every production mutation is restored byte for byte.

Normal optimized Release builds pass for all four library targets and both test
targets, with no warnings in the new files. Approved test files differ only by
explicit PartitionKey qualification; the helper is unchanged. HEAD and stash match
their snapshots. Evidence is under
`/private/tmp/brighter-4170-kafka-default-wire-mapping/`: baseline/initial/restored-writer
TRX and logs, original and qualified RED builds, seven mutation results, source and
dependency snapshots, qualification notes and verification-pending.json. Task-owned
services are stopped after these checks. No commit, push, remote CI pass or final
regression success is claimed.

The next [eight endpoint cases](review/kafka-endpoint-wire-mapping/README.md) are
outside compiled projects and uncompiled/unrun. They cover configured writer and
reader selection, native observation, dispatch trace, confirmation identity and
acknowledgement offset. Their review follows completion of the current compatibility
correction; all global #4170 gates remain accurately open.


## Kafka default adapters — approved charset correction completed

The preceding pending checkpoint is superseded by this completed validation. The
approved amendment changes only the reader's exact ContentType expectation to
include the existing UTF-8 charset. No Core or legacy Kafka creator change was made.
The original failing .NET 9/.NET 10 runs and pre-amendment test remain preserved.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Unmodified full baseline | 265 passed | 265 passed |
| Four cases with approved amendment | 4 passed | 4 passed |
| Four cases after all mutations restored | 4 passed | 4 passed |
| Complete final Kafka suite | 269 passed | 269 passed |

There are no failures or skips in these final suites. Every baseline outcome is
retained by exact test name. The remaining mutations fail at their intended reader
assertions: traceparent Equal at line 96, native offset Equal at line 100, and exact
ContentType Equal at line 90. Together with the previous seven independent checks,
all ten named mutations are verified. Production bytes are restored exactly before
normal optimized builds and final regressions.

All four library targets and both test targets build. No warnings occur in the new
files. Relative to this batch's snapshot, only KafkaMessagePublisher and
KafkaMessageConsumer change, and the two default adapters are added. The other
1,278 production files and all 268 dependency-manifest entries are unchanged.
The approved tests match their reviewed forms with the recorded PartitionKey
qualification and approved charset amendment. HEAD and stash remain unchanged.

Evidence: `/private/tmp/brighter-4170-kafka-default-wire-mapping/` contains
`initial-amended-*.trx`, `restored-*.trx`, `full-*.trx`, `mutations.json`,
`verification-amended.json`, all build/mutation logs and preserved original failures.
Task-owned Kafka and schema-registry services are stopped after verification.
No commit, push, remote CI result or global production-readiness claim is made.

The next [eight endpoint cases](review/kafka-endpoint-wire-mapping/README.md) remain
outside compiled projects, uncompiled and unrun pending their own review. They cover
selected mapping, dispatch trace, original confirmation identity and native offset
acknowledgement. Kafka and #4170 completion gates remain open.


## Kafka endpoint wire mapping

The eight approved cases cover sync/async publication and subscription with selected
replacement/composition strategies. Native broker observation checks key, body,
headers and dispatch trace. Confirmations retain source identity even when wire
identity differs. Readers retain mapped application fields; the consumer replaces
custom offset metadata and acknowledgement commits the real native position plus one.
The live commit check uses bounded polling because commits run in the background.

The preceding 269-pass full runs on both frameworks are reused as this batch's
baseline, before any endpoint production changes. Initial RED fails for missing
WireMessageWriter/WireMessageReader properties and two test import/name-resolution
errors. Adding OpenTelemetry.Trace and qualifying Confluent.Kafka.Acks.All leaves
only the missing properties on both targets. After implementation, .NET 9 passes
four writer cases but fails four reader cases at subscription construction: the
fixture omitted the mandatory pump type. Adding the intended Reactor/Proactor
selection changes no assertion. Original review files, failed logs/TRX and all
pre-correction copies remain preserved.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 269 passed | 269 passed |
| Eight cases after fixture correction | 8 passed | 8 passed |
| Eight cases after mutation restoration | 8 passed | 8 passed |
| Complete final Kafka suite | 277 passed | 277 passed |

No final failures or skips occur. Every baseline case is retained by exact name.
Eight independent mutations each fail four cases at their intended Equal assertion:
ignore writer (line 102), late trace (110), wire confirmation identity (104), empty
mapped body (109), ignore reader (109), wrong offset (115), cleared reader trace
(113), and omitted acknowledgement (130). The acknowledgement harness initially
stops before mutation because its selector matches three methods; narrowing the
selector to Acknowledge yields four intended failures. This harness stop is retained
and is not counted as mutation evidence. All six production files are restored
byte for byte before normal optimized builds and regressions.

Six existing Kafka files change: publication/subscription settings, consumer
factory/consumer and producer/publisher. No source files are added in this increment.
All other 1,276 production files and 268 dependency entries remain unchanged.
Existing public constructors remain intact. Default header-builder and parser paths
retain their prior behavior; selected readers get gateway-owned offset metadata.
All four library targets and both test targets build, with no warnings in new files.
HEAD and stash match their snapshots. No commit or push was made.

Evidence is under `/private/tmp/brighter-4170-kafka-endpoint-wire-mapping/`:
`initial-corrected-*.trx`, `restored-*.trx`, `full-*.trx`, `mutations.json`,
`verification.json`, original RED/setup failures, pre-correction files, source and
dependency snapshots, build/mutation logs and task-owned service logs. Kafka and
schema-registry services are stopped after verification. These are local integration
results; no remote CI or global production-readiness claim is made.

The next [eight reader-failure cases](review/kafka-reader-failures/README.md) are
outside compiled projects, uncompiled/unrun. Custom throw/null recovery and all
remaining Kafka/global completion gates remain open.


## Kafka configured-reader failure recovery

The eight reviewed cases and strategy were copied unchanged. The preceding full
277-pass runs are reused as baselines before this increment's production change.
All eight cases fail on both frameworks at Assert.Null(receiveError), line 122:
configured exceptions escape Receive and null results produce a NullReferenceException.
No test correction, expectation amendment or package change was needed.

KafkaMessageConsumer now isolates configured mapping failure per delivery. It uses
readable default metadata, marks the result unacceptable, retains native payload,
topic, key and offset, and logs the mapping failure. Default parsing remains unchanged;
successful custom mappings retain gateway-owned offset normalization. Later good
deliveries still use the selected reader. Acknowledging both deliveries and closing
the consumer commits the actual final offset, observed by a separate native client.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 277 passed | 277 passed |
| Initial RED | 8 intended assertion failures | 8 intended assertion failures |
| Initial GREEN | 8 passed | 8 passed |
| Restored focused cases | 8 passed | 8 passed |
| Complete final Kafka suite | 285 passed | 285 passed |

No final failures or skips occur; every baseline outcome is retained by exact name.
Nine independent mutations each fail all eight cases at the intended assertion:
escaping failure (Null, 122), successful fallback (Equal, 128), cleared identity
(Equal, 130), empty body (Equal, 133), cleared handled count (Equal, 134), cleared
trace (Equal, 135), omitted offset (True, 136), default-mapped good result (Equal, 138),
and omitted acknowledgement (Equal, 157). The good-result mutation still invokes
the configured reader, so the invocation-count assertion does not mask identity.
The identity mutation temporarily changes KafkaMessageCreator's mapped identity;
both creator and consumer bytes are restored before normal optimized builds.

Only KafkaMessageConsumer changes relative to this batch's snapshot. No production
file is added; all other 1,281 source files and 268 dependency entries are unchanged.
The two approved files remain byte-for-byte identical. All four library targets and
both test targets build, with no warnings in the new files. HEAD and stash retain
their original values. No commit, push or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-reader-failures/`: original RED,
initial/restored/full TRX, nine mutation logs and TRX, mutations.json, verification.json,
source/dependency snapshots, approved file checks and build logs. Task-owned Kafka
and schema-registry services are stopped after verification.

The next [eight default-parser cases](review/kafka-default-parser-failures/README.md)
are outside compiled projects, uncompiled/unrun pending review. Malformed default
messages can return a placeholder without throwing and need separate preservation.
The Kafka and global #4170 completion gates remain open.


## Kafka required-header failure preservation

The eight reviewed default-parser cases were copied unchanged. The preceding full
285-pass runs are reused as baselines before this increment's production change.
All eight cases fail on both frameworks at exact failed-body equality, line 134:
the invalid MessageType produces an unacceptable placeholder with an empty body.
No test correction, expectation amendment or dependency change was needed.

KafkaMessageCreator now maps the message before checking required-header success,
marks failures unacceptable, and attaches native offset and custom headers through
the common path. It preserves readable identity/topic, key, binary body, handled
count and trace alongside the tenant header. The private mapping helper is renamed
from SuccessMessage to MapMessage; the public API and protected virtual ReadBagEntry
extension remain unchanged. Parser-thrown exceptions remain separate work.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 285 passed | 285 passed |
| Initial RED | 8 intended assertion failures | 8 intended assertion failures |
| Initial GREEN | 8 passed | 8 passed |
| Restored focused cases | 8 passed | 8 passed |
| Complete final Kafka suite | 293 passed | 293 passed |

No final failures or skips occur; every baseline outcome is retained by exact name.
Eight independent mutations produce 60 intended failures: empty body (Equal, 134),
cleared identity (Equal, 135), cleared key (Equal, 137), cleared count (Equal, 138),
cleared trace (Equal, 139), omitted parser offset (True, 140), skipped custom headers
(True, 142), and event classification (Equal, 132). Each fails all eight cases except
omitted parser offset, which fails the four unset-reader cases while retaining four
explicit-default-reader passes. Consumer offset normalization was left intact.
Production bytes are restored before normal optimized builds and final suites.

Only KafkaMessageCreator changes relative to this batch's snapshot. No production
file is added; all other 1,281 source files and 268 dependency entries are unchanged.
The approved test remains byte-for-byte identical. All four library targets and
both test targets build, with no warnings in the new file. HEAD and stash retain
their original values. No commit, push or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-default-parser-failures/`: original
RED, initial/restored/full TRX, eight mutation logs and TRX, mutations.json,
verification.json, source/dependency snapshots, approved file checks and build logs.
Task-owned Kafka and schema-registry services are stopped after verification.

The next [eight parser-exception cases](review/kafka-parser-exceptions/README.md) remain
outside compiled projects, uncompiled/unrun pending review. A malformed content type
can still cause an outer-parser catch to return a placeholder. Kafka and the global
#4170 completion gates remain open.


## Kafka content-type parser exception preservation

The eight reviewed cases were copied unchanged. The preceding full 293-pass runs
are reused as baselines before this increment's production change. All eight cases
fail on both frameworks at exact failed-body equality, line 134: an invalid
CloudEvents content-type value causes the outer parser catch to return an empty
unacceptable placeholder. No test or expectation amendment was needed.

KafkaMessageCreator now contains FormatException at MIME parsing and continues
reading independent headers. Content-type parse failure contributes to unacceptable
classification. Missing content type is represented as a successful plain-text
default, preserving the prior mapped result. Successful MIME values and header
precedence remain unchanged. The public creator API and protected virtual custom-header
extension are unchanged. Other parser/custom-header exceptions remain separate work.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 293 passed | 293 passed |
| Initial RED | 8 intended assertion failures | 8 intended assertion failures |
| Initial GREEN | 8 passed | 8 passed |
| Restored focused cases | 8 passed | 8 passed |
| Complete final Kafka suite | 301 passed | 301 passed |

No final failures or skips occur; every baseline outcome is retained by exact name.
Nine independent mutations produce 68 intended failures: empty body (Equal, 134),
cleared identity (Equal, 135), cleared key (Equal, 137), cleared count (Equal, 138),
cleared trace (Equal, 139), omitted parser offset (True, 140), skipped custom headers
(True, 142), event classification (Equal, 132), and omitted acknowledgement storage
(Equal, 163). Every mutation fails eight cases except omitted parser offset: it
fails four unset-reader cases and retains four explicit-reader passes. Consumer
normalization remains intact. Creator and consumer bytes are restored before normal
optimized builds, restored focused runs and final suites.

Only KafkaMessageCreator changes relative to this batch's snapshot. No production
file is added; all other 1,281 source files and 268 dependency entries are unchanged.
The approved test remains byte-for-byte identical. All four library targets and both
test targets build with no warnings in changed files. HEAD and stash retain their
original values. No commit, push or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-parser-exceptions/`: original RED,
initial/restored/full TRX, nine mutation logs and TRX, mutations.json, verification.json,
source/dependency snapshots, approved file checks and build logs. Task-owned Kafka
and schema-registry services are stopped after verification.

The next [eight explicit-unacceptable cases](review/kafka-explicit-unacceptable/README.md)
and their strategy remain outside compiled projects, uncompiled/unrun pending review.
Kafka and the global #4170 completion gates remain open.


## Kafka explicit unacceptable reader results

The eight reviewed characterisation cases and strategy were copied unchanged. The
preceding full 301-pass runs are reused as baselines. All eight cases pass initially
on both frameworks without production changes. A selected reader's unacceptable
result retains its application body, identity, topic, key, handled count, trace and
tenant value; the consumer replaces deliberately incorrect offset metadata with the
actual native offset. The second delivery still uses the selected reader in either
order. A separate native observer verifies acknowledgement after consumer disposal.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 301 passed | 301 passed |
| Initial characterisation | 8 passed | 8 passed |
| Restored focused cases | 8 passed | 8 passed |
| Complete final Kafka suite | 309 passed | 309 passed |

No final failures or skips occur; every baseline outcome is retained by exact name.
Ten independent mutations each fail all eight cases at the intended assertion:
ignored selected reader (Equal, 126), default replacement of explicit unacceptable
(Equal, 130), cleared selected body (Equal, 132), native topic overwrite (Equal, 134),
native key overwrite (Equal, 135), cleared count (Equal, 136), cleared trace (Equal, 137),
removed tenant (True, 140), omitted offset normalization (Equal, 139), and omitted
acknowledgement storage (Equal, 164). These are 80 intended failures. Except the
ignored-reader mutation, invocation behavior is preserved so the call-count assertion
does not mask the intended failure. Consumer bytes are restored before normal optimized
builds, restored focused runs and full suites.

All 1,282 production files and 268 dependency entries remain unchanged. No production
file is added. Both approved files are byte-for-byte identical. All four library
targets and both test targets build with no warnings in new files. HEAD and stash
retain their original values. No commit, push or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-explicit-unacceptable/`: initial,
restored and full TRX, ten mutation logs and TRX, mutations.json, verification.json,
source/dependency snapshots, approved-file checks and build logs. Task-owned Kafka
and schema-registry services are stopped after verification.

The next [eight writer-recovery cases](review/kafka-writer-recovery/README.md) and their
strategy remain outside compiled projects, uncompiled/unrun pending review. Kafka
and the global #4170 completion gates remain open.


## Kafka writer failure and direct retry recovery

The eight reviewed cases and strategy were copied unchanged. The preceding full
309-pass runs are reused as baselines. All eight cases pass initially on both
frameworks without production changes. Throwing and null-returning strategies surface
a send error; the original binary body remains intact. Healthy messages still publish
and the same failed Message can be retried through the same producer after repair.
Registry disposal flushes delivery reports and drains confirmations before exact
callback counts and the native partition high watermark are asserted. The native
records retain selected bodies, keys and wire identities, while confirmations keep
source IDs. No extra record is published for the failed attempt.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 309 passed | 309 passed |
| Initial characterisation | 8 passed | 8 passed |
| Restored focused cases | 8 passed | 8 passed |
| Complete final Kafka suite | 317 passed | 317 passed |

No final failures or skips occur; every baseline outcome is retained by exact name.
Nine independent mutations each fail all eight cases at the intended assertion:
swallowed mapping error (NotNull, 126), false successful confirmation (Equal, 131),
extra publication without a callback (Equal, 139), original body replacing mapped
body (Equal, 144), original key replacing mapped key (Equal, 142), source ID replacing
wire ID (Equal, 143), wire identity in confirmations (Equal, 132), fatal producer latch
after mapping failure (Null, 127), and cleared source body on failure (Equal, 128).
These are 72 intended failures. Failure mutations cover both thrown exceptions and
null results in sync/async publication; tests are unchanged. Both publisher and
producer bytes are restored before normal optimized builds, focused reruns and full
suites. No compilation failure or timeout is counted as mutation proof.

All 1,282 production files and 268 dependency entries remain unchanged. No production
file is added. Both approved files are byte-for-byte identical. All four library
targets and both test targets build with no warnings in new files. HEAD and stash
retain their original values. No commit, push or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-writer-recovery/`: initial,
restored and full TRX, nine mutation logs and TRX, mutations.json, verification.json,
source/dependency manifests, production snapshots and build logs. Task-owned Kafka
and schema-registry services are stopped after verification.

The next [six outbox/subscriber recovery cases](review/kafka-outbox-recovery/README.md)
remain outside compiled projects, uncompiled/unrun pending review. These cover pending
messages after writer failures and a throwing confirmation subscriber, including the
expected duplicate delivery on recovery from the latter. Kafka and the global #4170
completion gates remain open.


## Kafka outbox and confirmation-subscriber recovery

The six approved cases were copied unchanged; the existing recoverable writer was
reused unchanged. The preceding full 317-pass runs are reused as baselines. All six
cases pass initially on both frameworks without production changes. A throwing or
null-returning writer leaves the original message pending after retry exhaustion.
Its binary body, identity, topic and tenant remain intact. A healthy message still
dispatches; repairing the writer lets the retained message dispatch and complete.

The third failure mode is a throwing confirmation subscriber registered before the
mediator. Native publication succeeds, but the earlier subscriber prevents the
mediator from handling that confirmation. The original entry remains pending while
a healthy message and its later confirmation succeed. Repairing the subscriber and
clearing the original entry republishes it and completes dispatch. These two cases
expect three native records, including two for the original message; they establish
recovery with possible duplicates, not exactly-once delivery. Both synchronous and
asynchronous events and clear APIs are exercised with a real Kafka broker and an
in-memory outbox. Persistent outbox storage and automatic sweepers remain separate.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 317 passed | 317 passed |
| Initial characterisation | 6 passed | 6 passed |
| Restored focused cases | 6 passed | 6 passed |
| Complete final Kafka suite | 323 passed | 323 passed |

No final failures or skips occur; every baseline outcome is retained by exact name.
The .NET 10 run spans 5 hours 31 minutes of wall-clock time. Its TRX reports only
about 11.6 minutes summed test duration, with the six new cases each taking 1.2–1.8
seconds. Several wall-clock start/end intervals are much larger than their reported
test duration. This is consistent with execution pauses or clock discontinuities;
the root cause is not established. The run completed without termination or rerun.
The original TRX and long-run-timing.json preserve this anomaly; no normal-duration
or performance guarantee is claimed from this run.

Seven independent mutations produce 40 intended failures: ignored writer (four
NotNull failures, line 131); premature outbox dispatch (six Single failures, 147);
original bodies replacing selected bodies (six Equal failures, 164/195); original
keys replacing selected keys (six Equal failures, 162/193); source IDs replacing wire
IDs (six Equal failures, 163/194); wire identity in confirmations (six Equal failures,
159/186); and omitted async confirmation outbox marking (six Single failures, 189).
The two locations distinguish callback-failure cases from writer-failure cases.

The first mutation-runner attempt stopped after verifying ignored-writer failures
because its next source anchor matched both bulk and non-bulk async dispatch. It
stopped before applying that mutation, restored production bytes, and was corrected
to require the exact non-bulk indentation. This harness exception is preserved in
harness-notes.txt and is not counted as mutation proof. Approved assertions did not
change. All seven mutations subsequently failed at the intended assertions, with no
compilation error, missing confirmation or unrelated timeout counted as proof.

All 1,282 production files and 268 dependency entries remain unchanged. No production
file is added. The approved test and reused helper are byte-for-byte identical.
Temporary changes to Kafka publisher/producer and Core outbox mediator were restored
before normal optimized builds and full suites. All four Kafka library targets and
both test targets build with no new warning diagnostics. HEAD and stash are unchanged.
No commit, push or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-outbox-recovery/`: initial,
restored and full TRX, seven mutation logs and TRX, mutations.json, verification.json,
source/dependency manifests, helper/production snapshots and build logs. Task-owned
Kafka and schema-registry services are stopped after verification.

The next [eight factory-isolation cases](review/kafka-factory-isolation/README.md)
remain outside compiled projects, uncompiled/unrun pending review. Kafka and the
global #4170 completion gates remain open.


## Kafka factory strategy isolation

Eight approved cases cover shared-factory publication and subscription selection in
both creation orders and both API modes. Publishers retain separate bodies, native
keys and wire IDs, with source IDs in confirmations. Consumers in separate groups
retain default/selected mappings, actual offsets and independent acknowledgement
commits after disposal flushes partial batches. Existing writer/reader helpers are
reused unchanged. No production change was needed.

The subscription draft required two mechanical setup corrections: qualification of
native `Confluent.Kafka.Acks.All` and explicit Reactor/Proactor selection matching the
existing sync/async mode. The initial build failed with CS0104; the first .NET 9 run
then passed four publication cases but failed four subscription cases in construction.
Original source, failed build and unconfigured TRX/log are retained. Assertions,
expected values, case data and assertion lines remain unchanged. Setup failures are
not counted as behavioral RED. All eight corrected cases then passed both frameworks.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 323 passed | 323 passed |
| Corrected initial characterisation | 8 passed | 8 passed |
| Restored focused cases | 8 passed | 8 passed |
| Complete final Kafka suite | 331 passed | 331 passed |

No final failures or skips occur; every baseline outcome is retained by exact name.
Eight independent mutations produce four intended failures each: shared writer and
ignored writer (Equal at line 104); source body replacing selected body (125); wire
confirmation identity (106); shared reader and ignored reader (113); omitted native
offset normalization (120); and omitted acknowledgement storage (140). All 32 reach
the intended assertions. Compile errors, missing deliveries and unrelated timeouts
are not counted as mutation evidence.

All 1,282 production files and 268 dependency entries remain unchanged; no source
file is added. Both reused helpers are byte-for-byte unchanged. Reviewed and compiled
tests match; the subscription test differs from its original only by the two recorded
setup corrections. Temporary mutations are restored before optimized builds and full
suites. All four library targets and both test targets build with no new warning
diagnostics. HEAD and stash remain unchanged. No commit, push or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-factory-isolation/`: initial,
restored and full TRX; eight mutation logs/TRX and mutations.json; source/dependency
manifests; helper/production snapshots; original draft and qualification-notes.txt;
verification.json and optimized build logs. Task-owned Kafka/schema services are
stopped after verification. The earlier outbox batch's wall-clock anomaly remains
recorded above; no performance guarantee is inferred from these regression runs.

The next [sixteen custom-header cases](review/kafka-custom-header-failures/README.md)
remain outside compiled projects, uncompiled/unrun pending review. Kafka and the
global #4170 completion gates remain open.


## Kafka custom-header error preservation

All sixteen approved cases were copied unchanged. Both frameworks reproduced the
same exact-body failure at line 150: duplicate or null-valued custom headers caused
the outer catch to replace a mapped delivery with an empty placeholder. The first
optimized .NET 9 run also failed all sixteen cases with an empty body, but some stack
traces exposed only cleanup locations. The strict line checker stopped. Its original
TRX/log are preserved; rerunning the unchanged test with Optimize=false and JIT
optimization/inlining disabled verified line 150 in every case on both frameworks.

KafkaMessageCreator now catches each custom-header error around the existing virtual
ReadBagEntry hook. It marks the existing message unacceptable and continues the next
entry. Parsed identity, topic, key, count, trace, binary body and offset survive.
Readable entries before and after the bad header survive. Duplicate headers retain
the first successfully read value; null values gain no new bag representation. The
default reader's rejection classification is unchanged. The valid neighboring record
remains consumable in both orders, and both records can be acknowledged through native
committed offsets. Public signatures and the protected virtual hook are unchanged.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 331 passed | 331 passed |
| Initial RED | 16 intended body failures | 16 intended body failures |
| Initial GREEN | 16 passed | 16 passed |
| Restored focused cases | 16 passed | 16 passed |
| Focused after warning suppression | 16 passed | 16 passed |
| Complete final Kafka suite | 347 passed | 347 passed |

No final failures or skips occur; every baseline outcome is retained by exact name.
Ten independent mutations on .NET 9 produce 144 intended failures: empty body (16 Equal, line
150); cleared identity (16 Equal, 151); key (16 Equal, 153); count (16 Equal, 154);
trace (16 Equal, 155); omitted native offset (8 True, 156); omitted preceding tenant
(16 True, 158); omitted following header (16 True, 160); classification as event
(16 Equal, 148); and removed first duplicate value (8 True, 164). The eight explicit
default-reader cases pass the offset mutation because consumer normalization restores
it. The eight null-valued cases pass the duplicate-value mutation as expected.

Two temporary mutation build failures are preserved: the classification guard first
needed CS0618 suppression, then applying that suppression inside the identity
constructor expression caused CS1040. The harness was corrected and production
bytes restored after each attempt. Neither compilation failure is counted as proof.
All ten final mutations reached their intended assertions without changing the test.

The final diagnostics check identified CS0618 in the valid-event assertion at line
167. Extended the existing compatibility suppression, preserving every assertion and
line number. The original draft and earlier build logs are retained. All sixteen
focused cases pass again on both frameworks after that warning-only adjustment.
The 347-pass full-suite results precede this compiler-warning change; production and
behavioral test code are identical to those full runs.

Only KafkaMessageCreator changes among 1,282 production files. No production file
is added; the other 1,281 source files and all 268 dependency entries are unchanged.
Only the test's existing compatibility-warning suppression was extended; every assertion
and assertion line is unchanged. Mutation snapshots match the restored
implementation. Optimized builds cover all four library targets and both test targets
with no new warning diagnostics against the preceding batch's initial and final
build logs (the latter omitted incremental .NET 10 diagnostics). HEAD and stash remain unchanged; no commit, push
or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-custom-header-failures/`: optimized
initial and verified initial RED TRX/logs, green/restored/full TRX, ten mutation results,
original and implementation snapshots, source/dependency manifests, build logs,
harness-notes.txt and verification.json. Task-owned Kafka/schema services are stopped
after verification. Historical anomalies recorded in earlier batches remain preserved.

The next [four invalid-message writer cases](review/kafka-invalid-writer/README.md)
remain outside compiled projects, uncompiled/unrun pending review. Rejection mapping,
other Kafka gaps and the global #4170 completion gates remain open.


## Kafka invalid-message writer selection

The four reviewed cases and helper were copied unchanged. The expected missing
InvalidMessageWireMessageWriter property produced a compile error, preserved as
compile-red.log and not counted as behavioral proof. After adding only the nullable
subscription property, all four cases failed writer-call equality at line 114 on
both frameworks. Factory/consumer selection had not yet been implemented.

KafkaSubscription exposes the selected strategy using the existing native message
contract. KafkaMessageConsumerFactory captures it through an internal init-only
consumer property. The lazy invalid-channel producer receives that selection; the
retry and dead-letter producers retain default writer selection. Public consumer
constructors are unchanged. Rejection metadata is available before mapping, native
routing remains gateway-owned and the original source offset is used for successful
acknowledgement. Both Reject methods restore the source topic in finally; this also
corrects the source-topic side effect of default rejection without changing its
wire serialization. Forwarding and acknowledgement are not atomic.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 347 passed | 347 passed |
| Behavioral RED after property-only addition | 4 intended failures | 4 intended failures |
| Initial GREEN | 4 passed | 4 passed |
| Restored focused cases | 4 passed | 4 passed |
| Complete final Kafka suite | 351 passed | 351 passed |

Every prior outcome is retained by exact name, with no final failures or skips.
Nine independent .NET 9 mutations produce 34 intended failures: ignored selection
(4 Equal, line 114); source bytes replacing selected body (2 Equal, 126); source key
(4 Equal, 124); source wire ID (4 Equal, 125); cleared original topic (4 Equal, 128);
cleared reason (4 Equal, 129); cleared tenant (4 Equal, 130); omitted source-topic
restoration (4 Equal, 131); and omitted rejection acknowledgement storage (4 Equal,
137). The two composition cases pass the body mutation because they deliberately
retain default binary bytes. No compilation failure, missing delivery or unrelated
timeout is counted as proof. No test/setup correction was needed.

Only KafkaSubscription, KafkaMessageConsumerFactory and KafkaMessageConsumer change
among 1,282 production files. The other 1,279 files and all 268 dependency entries
remain unchanged; no production file is added. Both approved files are byte-for-byte
unchanged. Publisher/factory/consumer mutations are restored before optimized builds
and full suites. All four library targets and both test targets build with no new
warning diagnostics, compared against preceding initial/final build logs so that
incremental omission of old warnings is not mistaken for a regression. HEAD and
stash are unchanged. No commit, push or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-invalid-writer/`: compile RED,
behavioral RED/green/restored/full TRX, nine mutation logs/TRX, mutations.json,
original/implementation snapshots, manifests, build logs and verification.json.
Task-owned Kafka/schema services are stopped after verification.

This batch validates successful explicit invalid-channel forwarding. Existing
forwarding-error handling still returns success and loses the source message's bag
offset. The next [four recovery cases](review/kafka-invalid-writer-recovery/README.md)
remain outside compiled projects, uncompiled/unrun pending review. Default/dead-letter
forwarding recovery, cancellation, isolation, other Kafka gaps and global #4170
completion gates remain open.


## Kafka invalid-message writer failure recovery

The four reviewed sync/async × throw/null cases were copied unchanged. All four
failed at Assert.False, line 118, on .NET 9 and .NET 10 before production changes.
Reject/RejectAsync now return false and restore the captured native source offset
when forwarding throws, without storing an acknowledgement. Their existing finally
restores the source topic; successful rejection still stores the original offset.
Only KafkaMessageConsumer changes. Public signatures and dependencies are unchanged.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 351 passed | 351 passed |
| Behavioral RED | 4 intended failures | 4 intended failures |
| Initial GREEN | 4 passed | 4 passed |
| Restored focused cases | 4 passed | 4 passed |
| Complete final Kafka suite | 355 passed | 355 passed |

Every prior outcome is retained by exact name; no final failures/skips occur.
Eight independent .NET 9 mutations produce 32 intended failures, four per mutation:
reporting success on failure (False, 118); premature acknowledgement (Equal, 140);
omitted offset restoration (True, 129); cleared source body (Equal, 125); recovered
native body replacement (Equal, 150); key replacement (Equal, 148); wire-ID replacement
(Equal, 149); and omitted successful acknowledgement (Equal, 151). Settlement
assertions inspect native group offsets after disposal flushes buffered state.
Recovery observes original-source redelivery to a fresh consumer, exactly one invalid
record with selected native fields, and the original offset committed only on success.
No compilation error, unrelated exception or timeout counts as mutation proof.

All mutations are restored before optimized builds, focused reruns and full suites.
All four library targets and both test targets build with no new warning diagnostics.
The other 1,281 production files and 268 dependency entries remain unchanged; no
production file is added. The approved test and reused helper are byte-for-byte
unchanged. HEAD and stash are unchanged. No commit, push or remote CI claim is made.

Evidence is under `/private/tmp/brighter-4170-kafka-invalid-writer-recovery/`: initial,
green, restored and full TRX/logs; eight mutation TRX/logs and mutations.json; original
and implementation source snapshots; source/dependency manifests; build logs and
verification.json. Task-owned Kafka/schema services are stopped after verification.

The catch behavior applies to rejection forwarding generally, but these tests prove
selected invalid-channel writer throw/null recovery only. Negative broker delivery
confirmations can occur without exceptions; that behavior remains unresolved.
The next [two broker-failure cases](review/kafka-invalid-broker-recovery/README.md)
are outside compiled projects, uncompiled/unrun pending review. Default/dead-letter
failure behavior, same-consumer retry, cancellation, later offsets in the same
partition, concurrency and other global #4170 completion gates remain open.
Source inspection also shows that Reactor/Proactor ignore false rejection results
for unacceptable messages and continue receiving. Since Kafka reduces buffered
acknowledgements to the highest offset per partition, a later acknowledged message
can advance the commit past an earlier failed forward. This is an outstanding
production-path gap, not proof of end-to-end pump recovery.
Forwarding and source acknowledgement are not atomic. Full production readiness is
not established by this batch.


## Kafka invalid-channel broker-rejection recovery

The two reviewed sync/async cases use a dedicated invalid topic with a 1,024-byte
record limit. Native preflight confirms rejection of a 4,096-byte record. The
selected writer returns a valid oversized native message, so the failure comes from
the broker rather than a writer exception/null result. Before production changes,
both cases fail at Assert.False, line 125, on .NET 9 and .NET 10.

The initial test draft's MessageSizeTooLarge enum member did not compile, including
a qualification-only attempt. The installed SDK XML documents MsgSizeTooLarge;
line 82 now uses Confluent.Kafka.ErrorCode.MsgSizeTooLarge. This is the only test
correction: intended broker-error assertion and line numbers are unchanged. Original
approved bytes, compile-red.log and compile-qualified-enum.log are retained; neither
compile failure is counted as behavioral proof. The helper is unchanged, and both
compiled files equal the reviewed versions after the spelling correction.

KafkaMessageProducer adds internal per-send confirmation overloads using the existing
PublishConfirmationResult contract. Public send/delay signatures and queued external
confirmation behavior are preserved. The per-operation callback prevents an earlier
late result from being mistaken for a later attempt with the same source identity.
External notification is enqueued before releasing the waiting rejecting consumer.
KafkaMessageConsumer bounds confirmation waits to five seconds, links async caller
cancellation, and only stores the source acknowledgement after confirmed persistence.
Negative confirmation or send/wait exceptions return false and restore the native
source offset through the existing failure path. No public testing API is added.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 355 passed | 355 passed |
| Behavioral RED | 2 intended failures | 2 intended failures |
| Initial GREEN | 2 passed | 2 passed |
| Final restored focused cases | 2 passed | 2 passed |
| Complete final Kafka suite | 357 passed | 357 passed |

Eight independent final .NET 9 mutations produce 16 intended failures, two each:
false success (False, 125); premature source acknowledgement (Equal, 147); omitted
failed offset restoration (True, 136); cleared source body (Equal, 132); recovered
native body replacement (Equal, 157); key replacement (Equal, 155); wire-ID replacement
(Equal, 156); and omitted successful rejection acknowledgement (Equal, 158).
Recovered-output mutations apply only below the topic's size limit, retaining the
first oversized broker failure. Native source commits are observed after disposal
flushes any buffered acknowledgement. Compilation errors, missing deliveries and
unrelated timeouts are not mutation proof.

An earlier implementation also passed all eight mutations and both full 357-case
suites. Review moved the internal callback after external-notification enqueue.
Both suites finished before stop requests reached their containers; those requests
stopped no container. The pre-ordering results are preserved separately. All eight
mutations, optimized builds, both focused runs and both complete suites were repeated
after the final ordering change. Only these repeated results establish final status.
Every one of the 355 prior outcomes is retained by exact name, with no final failures
or skips. All four library targets and both test targets build with no new warnings.

Only KafkaMessageConsumer and KafkaMessageProducer change among 1,282 source files;
the other 1,280 and all 268 dependency entries are unchanged. No source file is added.
All mutation bytes are restored before final optimized builds/runs. HEAD and stash
are unchanged. No commit, push or remote CI claim is made. Evidence is under
`/private/tmp/brighter-4170-kafka-invalid-broker-recovery/`: approved-original.cs,
compile and initial RED logs, green/restored/full TRX, eight mutation TRX/logs,
mutations.json, original/implementation snapshots, manifests, build logs and
verification.json. Earlier runs are under pre-ordering/. Task-owned Kafka/schema
services are stopped after verification.

These tests prove selected invalid-channel broker-size-rejection recovery after
consumer recreation. The forwarding helpers also serve dead-letter routes, but
other route failures, cancellation, missing-confirmation timeout and producer
initialization failure remain unverified. Source inspection shows CreateProducer
returns null after initialization errors and rejection then acknowledges through its
no-producer path; that failure behavior is still unresolved. Forwarding and source acknowledgement
are not atomic. Pumps ignore false rejection results and a later acknowledgement
can advance the partition commit past a failed forward. The next
[four offset-barrier cases](review/kafka-failed-forward-offset-barrier/README.md)
remain outside compiled projects, uncompiled/unrun pending review. Automatic pump
recovery and the other global #4170 production-readiness gates remain open.


## Kafka failed-forward source-offset barrier

The four reviewed cases combine sync/async rejection with commit batch sizes 1/10.
Two source records are received before rejection. Native preflight verifies that the
invalid topic's 1,024-byte record limit rejects the selected writer's 4,096-byte body.
After the first record fails forwarding, the later record is acknowledged and the
consumer is disposed. All four cases initially fail Assert.True, line 174, on both
frameworks: the native group commit has advanced past the failed source.

Both rejection catches now record the earliest failed native offset per partition.
The common offset reduction caps commit requests at that offset; it is used by batch,
disposal and revocation commit paths. Tests exercise batch/disposal, not a live
revocation. A new consumer in the same group receives both records again, forwards
the repaired first record, acknowledges the later one and commits its native offset
plus one. The failed body, identity, topic, key, handled count, tenant and offset
survive. Exactly one invalid record carries selected native body/key/wire identity.
No producer/publisher code or public API changes in this batch.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 357 passed | 357 passed |
| Initial RED | 4 intended failures | 4 intended failures |
| Initial GREEN | 4 passed | 4 passed |
| Restored focused cases | 4 passed | 4 passed |
| Complete final Kafka suite | 361 passed | 361 passed |

Eight .NET 9 mutations produce 32 intended failures, four each: permit commits past
the failed record (True, 174); report failed forwarding as successful (False, 149);
omit failed native offset restoration (True, 160); clear source binary body (Equal,
156); replace recovered native body (Equal, 185), key (Equal, 183) or wire identity
(Equal, 184) with source values; omit ordinary acknowledgement storage while
retaining rejection settlement (Equal final committed later offset, 186).
Recovered-output mutations apply only to the smaller repaired record. No compile
error or unrelated timeout counts as proof. All mutation bytes are restored before
optimized final builds and focused/full runs. Every prior outcome is retained by
exact name with no final failures or skips.

Only KafkaMessageConsumer changes among 1,282 production files; the other 1,281 and
all 268 dependency entries are unchanged. No production source is added. Approved
test bytes and reused helper hash are unchanged. Public declaration lines are
retained. All four library targets and both test targets build with no new warning
diagnostics. HEAD/stash are unchanged; no commit or push is made. Evidence is under
`/private/tmp/brighter-4170-kafka-failed-forward-offset-barrier/`, including RED,
green/restored/full logs/TRX, eight mutation logs/TRX, source snapshots, manifests,
all-target build logs and verification.json. Task-owned services are stopped after
verification. These are local results, not a remote CI or production readiness claim.

The per-partition limit is conservative and lasts for the consumer lifetime. A
successful retry on that same consumer does not yet release it, and limited commits
drain later acknowledgements from the existing bag. The next
[four same-consumer recovery cases](review/kafka-failed-forward-same-consumer-recovery/README.md)
are drafted outside compiled projects and remain uncompiled/unrun pending review.
Thread-safe limit storage alone does not synchronize with a commit already in
flight. Multiple failed records, concurrent commits, rebalance/partition isolation,
producer initialization failures, other rejection routes, missing confirmations,
cancellation and automatic pump retry remain unresolved or unverified. Pumps still
ignore false rejection results. Global #4170 scope remains incomplete.


## Kafka failed-forward recovery on the same consumer

Four cases combine sync/async rejection with commit batch sizes 1/10. Two source
records are read before the first fails forwarding due to a native size limit.
The later record is acknowledged, the writer is repaired, and the same original
record is rejected again on the same consumer. No second acknowledgement of the
later record is supplied. Both frameworks initially fail all four cases at Equal,
line 190: after successful forwarding and disposal, the commit remains at the failed
source instead of the later record plus one.

Successful rejection now removes only a failed limit matching its native partition
and offset. For affected partitions, the commit reducer retains the highest
acknowledgement observed, including acknowledgements beyond a pending failure,
after removal from the pending bag. This prevents a lower acknowledgement from the retry from replacing that
later progress. The bookkeeping is one entry per affected partition for the consumer
lifetime. No public API or producer/publisher implementation changes in this batch.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 361 passed | 361 passed |
| Initial RED | 4 intended failures | 4 intended failures |
| Initial GREEN | 4 passed | 4 passed |
| Restored focused cases | 4 passed | 4 passed |
| Complete final Kafka suite | 365 passed | 365 passed |

Eight .NET 9 mutations produce 32 intended failures, four each: retain the failed
limit after successful retry (Equal, 190); omit ordinary acknowledgement storage
while retaining rejection settlement (Equal, 190); report failed forwarding as
successful (False, 147); omit failed native offset restoration (True, 155); clear
failed source body (Equal, 153); replace recovered native body (Equal, 184), key
(Equal, 182), or wire identity (Equal, 183) with source values. Output mutations
apply only to the smaller repaired record, preserving the initial broker failure.
No compile error or unrelated timeout counts as proof. All mutation bytes are
restored before optimized final builds and focused/full runs.

All 361 previous outcomes are retained by exact name, with no final failures/skips.
All four library targets and both test targets build without new warnings. Only
KafkaMessageConsumer changes among 1,282 production files; the other 1,281 and all
268 dependency entries remain unchanged. No source file is added. Approved test
bytes and reused helper hash are unchanged; public declaration lines are retained.
HEAD/stash are unchanged and nothing is committed or pushed. Evidence is under
`/private/tmp/brighter-4170-kafka-failed-forward-same-consumer-recovery/`, including
initial RED, green/restored/full logs/TRX, eight mutation logs/TRX, mutation report,
source snapshots, manifests, all-target build logs and verification.json. An earlier
implementation reinserted capped offsets into the pending bag. Review removed this
unnecessary reinsertion because the per-partition record already retains progress
and reinsertion would also affect revoked partitions. Both earlier full 365-case
suites completed before stop requests reached their containers; no container was
stopped. Earlier results are under pre-retention-review/. All focused, mutation,
build and full-suite checks were repeated after the correction; only the repeated
runs establish final status. Task-owned
services are stopped after verification. These local checks do not establish remote
CI or global production readiness.

The pre-recovery native observation accepts unset commits because flushing runs in
the background. It does not prove that a limited flush completed before retry; final
state is observed after disposal flushes pending acknowledgements. These tests cover
one failed record, not controlled concurrency or reassignment. At this checkpoint only the earliest failed record per partition was stored;
recovering it could remove protection for later failures. The subsequent multiple-
failure batch below closes that sequential partial-recovery gap. Concurrent commits/
rebalance, initialization failures, cancellation, missing confirmations, other routes
and automatic pump retry remain unresolved or unverified.
The other #4170 production-readiness gates also remain open.


## Kafka partial recovery with several failed forwards

Four cases combine sync/async rejection with two recovery orders. Four native source
records are received before the first three fail forwarding due to a native size
limit. The fourth is acknowledged; the writer is repaired and the first two failed
records are retried in either order. Disposal must commit exactly at the third failed
record. A recreated consumer must receive that record and the fourth, then advance
to the fourth record plus one after successful forwarding and acknowledgement.

Both frameworks initially fail all four cases at Equal, line 189: the commit skips
to the fourth record plus one despite the unresolved third failure. The consumer
now stores every outstanding failed native offset in a per-partition sorted set,
removes only the successfully recovered offset, and caps commit reduction at the
earliest remaining failure. Existing highest-acknowledgement retention preserves
later progress. A short lock protects this metadata; native I/O occurs outside it.
No public API, producer/publisher or dependency changes are required.

| Check | .NET 9 | .NET 10 |
| --- | --- | --- |
| Full baseline | 365 passed | 365 passed |
| Initial RED | 4 intended failures | 4 intended failures |
| Initial GREEN | 4 passed | 4 passed |
| Restored focused cases | 4 passed | 4 passed |
| Complete final Kafka suite | 369 passed | 369 passed |

Eight .NET 9 mutations produce 32 intended failures, four each: clear all failed
offsets in a partition after any successful retry (Equal, 189); retain recovered
offsets (Equal, 189); omit ordinary acknowledgement storage while retaining rejection
settlement (Equal, 235); report failed forwarding as successful (False, 144); omit
failed native offset restoration (True, 152); replace recovered native body (Equal,
186), key (Equal, 184), or wire identity (Equal, 185) with source values. Output
mutations apply only to repaired records smaller than 1,024 bytes, preserving the
initial broker failure. No compile error or unrelated timeout counts as proof.
All mutation bytes are restored before optimized final builds and focused/full runs.

All 365 prior outcomes are retained by exact name, with no final failures/skips.
All four library targets and both test targets build without new warnings. Only
KafkaMessageConsumer changes among 1,282 production files; the other 1,281 and all
268 dependency entries remain unchanged. No source file is added. Approved test
bytes and the reused helper hash are unchanged, and public declaration lines are
retained. HEAD/stash remain unchanged; nothing is committed or pushed. Evidence is
under `/private/tmp/brighter-4170-kafka-multiple-failed-forwards/`, including RED,
green/restored/full logs/TRX, eight mutation logs/TRX and report, source snapshots,
manifests, all-target build logs and verification.json. Task-owned services are
stopped after verification. These local results do not establish remote CI readiness.

Final native state is observed after disposal flushes acknowledgements. This covers
sequential partial recovery in one partition, not controlled concurrent registration
of failures versus native commits or reassignment. The metadata lock does not make
forwarding and settlement atomic. Highest-acknowledgement lifetime across rebalances,
initialization failures, cancellation, missing confirmations, other routes and
automatic pump recovery remain separately unresolved or unverified. The next
[four partition-isolation cases](review/kafka-partition-forward-isolation/README.md)
remain outside compiled projects, uncompiled/unrun pending review. Kafka and the
other #4170 production-readiness gates remain open.


## Kafka failed-forward partition isolation validated

The approved [four partition-isolation cases](review/kafka-partition-forward-isolation/README.md)
were copied unchanged into the Kafka test project. All four pass initially and
after mutation restoration on .NET 9 and .NET 10 without production corrections.
Eight named mutations produce 32 intended assertion failures on .NET 9, including
clearing every partition's failed offsets and applying one partition's limit globally.
All mutation bytes are restored before the optimized regression builds.

| Framework | Initial focused | Restored focused | Full suite | Previous outcomes retained |
| --- | ---: | ---: | ---: | ---: |
| .NET 9 | 4 passed | 4 passed | 373 passed, 0 failed/skipped | 369 |
| .NET 10 | 4 passed | 4 passed | 373 passed, 0 failed/skipped | 369 |

All four Kafka library targets and both test targets build without new warning
diagnostics. The approved test and reused helper are byte-identical to their
reviewed/pre-batch copies. All 1,169 production C# files and 268 dependency/build
files match the pre-batch manifests. HEAD and stash remain unchanged.

Evidence: `/private/tmp/brighter-4170-kafka-partition-forward-isolation/`, including
initial/restored/full logs and TRX, eight mutation logs/TRX, production snapshots,
manifests, all-target build logs and `verification.json`. The initial sandbox build
stalled before emitting output; it was stopped and successfully rerun outside the
sandbox. This was a build-environment interruption, not a failed behavioral test.

This validates sequential isolation across two partitions, partial recovery and
redelivery after consumer recreation. Concurrent commits, rebalance/lost-partition
lifecycle, automatic pump recovery, cancellation and missing confirmations remain
separate open gates. No commit or push; global production readiness remains open.


## Kafka lost-partition recovery: historical fixture-amendment checkpoint

The approved [four lost-partition cases](review/kafka-lost-partition-recovery/README.md)
compile unchanged and fail at the final commit assertion on both frameworks.
RoundRobin reports offset 0 instead of 3; CooperativeSticky reports offset 2.
The candidate cleanup of forwarding state on revoked/lost partitions passes both
RoundRobin cases on .NET 9 but leaves the two CooperativeSticky failures.

Temporary diagnostics establish that CooperativeSticky can return the final
record before the original consumer has broker-recognized group membership. Its
commit fails with `Broker: Unknown member`. Consequently, those two cases do not
yet isolate stale-state cleanup after ownership has returned. The proposed
[assignment-observation amendment](review/kafka-lost-partition-assignment-observation/README.md)
requires native group/assignment confirmation before publishing the final record.
It is outside compiled projects and has not been compiled or run.

The original approved compiled test remains byte-identical. Candidate production
changes and temporary diagnostics are restored to the pre-batch source; all 1,169
production C# files and 268 dependency/build files match the initial manifests.
The candidate remains in the evidence directory for review. No mutation checks
or full suites ran, and no completed production fix is claimed.

Evidence is under `/private/tmp/brighter-4170-kafka-lost-partition-recovery/`: initial
logs/TRX for both frameworks, first-correction and diagnostic runs on .NET 9,
source snapshots and manifests. Earlier 373-case full-suite results belong to the
preceding partition-isolation batch and do not validate this new batch.


## Kafka lost-partition recovery validated with broker-confirmed assignment

The [approved amendment](review/kafka-lost-partition-assignment-observation/README.md)
adds native group membership and partition-assignment observation before the
final record is published. All four cases reproduce offset 0 instead of 3 at the
final assertion on both frameworks, isolating the stale-state failure after
ownership returns. Cleanup of forwarding state on lost/revoked partitions makes
all four cases pass, initially and after restoration, on both frameworks.

Eight mutations produce 32 intended assertion failures. Both full suites pass
377 cases with zero failures/skips, retaining all 373 preceding outcomes by exact
test name. All four library and both test targets build without new warning
diagnostics. Only KafkaMessageConsumer changes among 1,169 production C# files;
all 268 dependency/build files, public declarations, the approved amended test
and the reused helper are unchanged. HEAD/stash are unchanged; no commit or push.

Evidence: `/private/tmp/brighter-4170-kafka-lost-partition-assignment-observation/`.
Full framework suites run concurrently on separate Kafka 4.0.2 and schema-registry
8.0.6 instances. Logs/TRX, the isolated compose configuration, source snapshots,
manifests, mutation results and final verification are retained. The preceding
unamended failures remain historical evidence rather than final validation.

Concurrent commits, partial revocation, late acknowledgements, pre-rejoin native
delivery behavior and automatic pump recovery are not established by this batch.
Global production readiness remains open.

## Kafka in-flight rejection commit barrier validated

The approved [offset-metadata amendment](review/kafka-inflight-forward-offset-amendment/README.md)
retains the four synchronous/asynchronous and first-write success/failure cases.
All four reproduce a premature native commit on both .NET 9 and .NET 10: while
forwarding offset 0 is gated, acknowledging offset 1 commits 2 instead of 0.
Registering the unresolved source offset before forwarding fixes the defect;
failure retains the barrier and confirmed success releases it. Later acknowledgements
remain available for the final commit. Source metadata checks after failure and
native mapped key, wire ID and binary-body assertions remain intact.

All four pass on both frameworks before and after mutation restoration. Nine
mutations run against the new cases: eight yield 30 intended failures; releasing
the barrier on failure survives all four new cases because no subsequent commit
is attempted before retry. That same mutation is detected by all four existing
KafkaFailedForwardOffsetBarrierTests at the broker-commit assertion. This is 34
intended failures across ten mutation executions, including the explicitly recorded
survivor. No approved assertion was changed to improve those results.

Both complete Kafka suites pass **381 cases, zero failures and zero skips**,
retaining every one of the preceding 377 passing cases. The library builds for
netstandard2.0, net8.0, net9.0 and net10.0; tests build for net9.0 and net10.0.
Only KafkaMessageConsumer changes in production relative to this batch's baseline;
public declarations, dependency files and the approved amended test/helper remain
unchanged. All mutations are restored. No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-inflight-forward-amended/`, including
RED/green/restored/full TRX files, mutation results, supplementary survivor check,
source snapshots, build logs and verification.json. The original unamended test's
incorrect success-offset expectation remains documented in
`/private/tmp/brighter-4170-kafka-inflight-forward/`.

This proves the barrier for acknowledgements initiated after the writer is entered.
It does not establish safety for commits already reduced before registration,
rebalance during forwarding, late acknowledgements from a prior assignment,
concurrent retries of one record, missing confirmations, or automatic pump recovery.
The full-feature production-readiness gates remain open.

## Kafka immediate retry writer selection validated

The approved [eight retry-writer cases](review/kafka-immediate-requeue-writer/README.md)
cover sync/async calls, null/zero delay and selected/default mapping through the
public subscription and factory. The initial compilation confirms the missing
`KafkaSubscription.RequeueWireMessageWriter` property. With only that nullable
property added as a scaffold, all four selected-writer cases fail because the
writer is never called; all four default cases pass on both frameworks. This
provides behavioral RED independently of the initial compilation failure.

The subscription now exposes the nullable selection, the factory captures it on
the consumer, and the lazy retry producer uses it for its publication. Null
preserves default mapping. Retry selection is independent of invalid-message
selection; no existing constructor signatures or dependencies change.

All eight cases pass on .NET 9 and .NET 10 before and after mutation restoration.
Eight mutations produce 44 intended assertion failures: missing factory/producer
selection, use of the invalid writer, source key/ID/body substituted for mapped
values, omitted source acknowledgement, and committing past the new retry. Native
Kafka observations verify the exact retry offset and contents, an empty invalid
topic, and a commit that advances past the source only.

Both full Kafka suites pass **389 cases, zero failures and zero skips**, preserving
all 381 preceding outcomes. All four library targets and both test targets build
with no new warning diagnostics. Only KafkaSubscription, KafkaMessageConsumerFactory
and KafkaMessageConsumer change relative to this batch's production baseline.
The approved test and reused helper remain byte-identical; all mutations are
restored. No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-writer/`, including the initial
compile diagnostics, behavioral RED/green/restored/full TRX files, mutation logs,
source snapshots, build logs and verification.json. Optimized RED stack traces
point to line 125 while the invocation-count assertion is on line 126; recorded
failure messages confirm expected 1 versus actual 0. The initial .NET 9 harness
stopped on that line-number check; its four intended failures and four default
passes are retained and verified independently.

This closes immediate retry selection only. Writer exception/null recovery,
negative delivery confirmations, missing confirmations, cancellation,
scheduler-backed dispatch, concurrent lazy initialization and consumer-pump retry
behavior remain unverified or unfinished. In particular, send-plus-flush must not
be treated as proof that failed forwarding prevents source acknowledgement.
Global production readiness remains open.

## Kafka immediate retry broker-rejection recovery validated

The approved [two broker-recovery cases](review/kafka-requeue-broker-recovery/README.md)
reproduce false success after Kafka rejects an oversized retry on both .NET 9 and
.NET 10. Both sync and async requeue previously returned true. An independent
native publication verifies the broker's message-size rejection before the tested
operation, and native observers verify source commits and retry contents.

Immediate requeue now registers the unresolved source offset and waits for
per-send persistence confirmation. A negative confirmation returns false and
restores source-offset metadata; later acknowledgements remain capped at that
record. Successful retry releases its barrier while retaining later acknowledgements.
The existing confirmation helpers are shared with rejection under names that
reflect forwarding rather than a particular destination. Their five-second bound
remains unchanged. Delayed dispatch calls remain unchanged. The immediate path
handles ChannelFailureException here; other mapping exceptions, cancellation and
missing confirmations remain separate coverage gaps.

Both cases pass on both frameworks before and after mutation restoration. Eight
mutations produce 16 intended failures: ignored negative confirmation, omitted
source-offset restoration, omitted barrier, retained barrier after recovery,
discarded later acknowledgement and replacement of mapped key, wire ID or body.
The restoration mutation fails at the expected missing source-offset lookup; all
other failures are the intended result, commit or native-content assertions.

Both complete Kafka suites pass **391 cases, zero failures and zero skips**,
preserving all 389 preceding outcomes. All four library targets and both test
targets build with no new warning diagnostics. Only KafkaMessageConsumer changes
relative to this batch's production baseline. External public declarations, all
268 dependency/build files, the approved test and reused helper are unchanged.
All mutations are restored; no commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-broker-recovery/`, including
RED/green/restored/full TRX files, mutation logs, source snapshots, build logs and
verification.json. This establishes an explicit caller retry on the same consumer
after confirmed broker rejection. Automatic pump recovery, writer exception/null
handling, cancellation, missing confirmations, general concurrency/rebalance safety
and scheduler-backed retry mapping remain unverified or unfinished. Full-feature
production readiness remains open.

The next [writer-failure recovery batch](review/kafka-requeue-writer-recovery/README.md)
is drafted outside compiled projects and is unrun pending approval.

## Kafka immediate retry writer-failure recovery validated

The approved [four writer-recovery cases](review/kafka-requeue-writer-recovery/README.md)
cover sync/async retry and writers that throw InvalidOperationException or return
null. Initial runs on both frameworks produce two passes and two intended failures:
the producer already wraps the tested writer exception in ChannelFailureException,
so its recovery path works, while a null native message escapes as
NullReferenceException. The initial runner expected four failures and stopped after
.NET 9; its expectation was corrected after inspecting the producer's wrapping
behavior, then .NET 10 confirmed the same result. No test assertions changed.

The immediate retry catch in each sending mode now handles non-cancellation
exceptions, so null-result failures reach the existing source-offset restoration,
false result and commit-barrier recovery path. OperationCanceledException remains
excluded, preserving the existing propagation policy; its source-restoration and
recovery behavior are not established by these cases. Delayed dispatch is unchanged.

All four cases pass on both frameworks before and after mutation restoration.
Nine mutations produce 36 intended failures: restricting recovery to negative
broker confirmation, reporting success after failure, omitting source-offset
restoration or the barrier, retaining the barrier after recovery, discarding the
later acknowledgement, and replacing the mapped native key, wire ID or body.
The first mutation also establishes sensitivity of the throwing cases that were
green on arrival. Native observations verify commit 0 after the later acknowledgement,
commit 2 after recovery, and exactly one correctly mapped retry at offset 2.

Both complete Kafka suites pass **395 cases, zero failures and zero skips**,
preserving all 391 preceding outcomes. All four library targets and both test
targets build with no new warning diagnostics. Only KafkaMessageConsumer changes
relative to this batch's production baseline: two exception filters and their XML
remarks. External public declarations, all 268 dependency/build files, the approved
test and reused helper are unchanged. All mutations are restored. No commit or
push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-writer-recovery/`, including
initial mixed results, green/restored/full TRX files, mutation logs, source
snapshots, build logs and verification.json. The evidence covers the tested writer
exception and null result, not every possible application exception type.
Cancellation recovery, missing confirmations, concurrent forwarding/rebalance,
scheduler-backed retries and automatic pump recovery remain unverified or unfinished.
Full-feature production readiness remains open.

The next [cancelled retry recovery case](review/kafka-requeue-cancellation-recovery/README.md)
is now validated (396 passing cases per full suite). It permits a
copy from the cancelled attempt because cancellation does not establish whether
Kafka persisted it; it requires source recovery and leaves all retry copies unacknowledged.

## Kafka cancelled immediate retry recovery validated

The approved [cancellation-recovery case](review/kafka-requeue-cancellation-recovery/README.md)
reproduces a missing source offset on both frameworks after caller cancellation
during the retry writer's gated mapping call. Cancellation already propagates,
but the source-offset-presence assertion fails. The initial .NET 9 runner expected
xUnit's default assertion label; xUnit emits the test's custom message instead.
The runner check was corrected and .NET 10 confirmed the same assertion failure.
No test or helper was changed.

Async immediate requeue now restores the extracted source offset before rethrowing
OperationCanceledException. It retains the unresolved barrier. A later source
acknowledgement commits offset 0, and explicit retry with a live token on the same
consumer succeeds and advances the commit to 2. Native observations allow one or
two mapped retry copies: cancellation does not prove the first attempt was absent
from Kafka. This is not an exactly-once claim. Synchronous and delayed paths are
unchanged relative to this batch's baseline.

The case passes before and after mutation restoration on both frameworks. Eight
mutations fail at their intended assertions: lost offset restoration, swallowed
cancellation, premature barrier release, a retained barrier after success, a lost
later acknowledgement, and substituted native key, wire ID or body. All mutations
are restored. Both complete Kafka suites pass **396 cases, zero failures and zero
skips**, retaining all 395 preceding outcomes. All four library targets and both
test targets build with no new warning diagnostics. Only KafkaMessageConsumer
changes relative to the production baseline; public declarations, all 268
dependency/build files, the approved test and existing helper remain unchanged.
No commit or push is performed.

The first .NET 9 full run stalled in the existing Fragile=CI
KafkaMessageConsumerCommitRevokeConcurrency test during native assignment inside
consumer disposal. A non-terminating createdump stack report identified the
blocked call; the stalled test container was then stopped and the full suite rerun
with five-minute inactivity hang diagnostics. The final .NET 9 pass is that rerun,
not a clean first-run result. This intermittent rebalance/disposal stall remains
an unresolved production-readiness concern; no speculative fix or test weakening
was made. The stack report and stalled-run console log are retained alongside the
successful results.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-cancellation-recovery/`, including
initial/green/restored/full TRX files, mutation logs, source snapshots, build logs
and verification.json. This establishes explicit async caller recovery after
cancellation during mapping. Cancellation before entry, internal confirmation
timeouts, synchronous writer-thrown cancellation, general forwarding/rebalance
concurrency, scheduler-backed retries and automatic pump recovery remain open.
The full feature is not yet production-ready.

The next [confirmation-timeout cases](review/kafka-requeue-confirmation-timeout-recovery/README.md)
are now validated (398 passing cases per full suite). They
separate internal timeout failure from caller cancellation and permit late retry
copies without claiming cancellation of broker writes.

## Kafka immediate retry confirmation-timeout recovery validated

The [two approved timeout cases](review/kafka-requeue-confirmation-timeout-recovery/README.md)
initially fail at the no-exception assertion on both frameworks: the internal
five-second deadline escapes as TaskCanceledException even though the caller token
remains live. The synchronous and asynchronous confirmation helpers now convert
internal deadline cancellation into ChannelFailureException. Async conversion
explicitly excludes caller cancellation. Existing immediate-retry failure handling
returns false, restores source metadata and retains the unresolved commit barrier.
These helpers are shared with rejection forwarding, whose existing failure handling
continues to catch the exception; the new deadline scenario specifically tests retry.

Both timeout cases pass before and after mutation restoration on both frameworks.
The existing caller-cancellation recovery case also passes on both frameworks.
Native observations establish commit 0 after a later source acknowledgement and
commit 2 after retry with the same consumer; one or two correctly mapped retry
copies are permitted because deadline expiry cannot roll back a broker write.
The confirmation deadline cannot interrupt a blocked synchronous application writer.

Nine mutation variants produce 18 intended assertion failures: propagating timeout,
reporting success after timeout, losing the source offset, prematurely releasing
the barrier, retaining it after successful retry, losing the later acknowledgement,
and substituting the native key, wire ID or body. The first mutation attempt did
not compile because its exception variable became unused under warnings-as-errors;
that temporary mutation was corrected and rerun. The compile failure is retained
as runner evidence and is not counted as a behavioral mutation failure. No test
assertions changed, and all mutations are restored.

Both full Kafka suites pass **398 cases, zero failures and zero skips**, preserving
all 396 preceding outcomes. Both full runs include five-minute inactivity hang
diagnostics. All four library targets and both test targets build without new
warning diagnostics. Only KafkaMessageConsumer changes relative to this batch's
production baseline. Public declarations, delayed-dispatch calls, all 268
dependency/build files, the approved test and reused helper remain unchanged.
No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-requeue-confirmation-timeout-recovery/`,
including initial/green/restored/cancellation/full TRX results, mutation logs,
source snapshots, build logs and verification.json. The prior .NET 9 native
assignment/disposal stall remains unresolved even when this batch passes. These
cases do not establish every missing-callback, broker-outage, restart, cancellation,
rebalance, concurrency, scheduler or automatic-pump behavior. Full-feature
production readiness remains open.

## Kafka disposal stall investigation — not reproduced, still open

The retained stack from the first cancellation-recovery .NET 9 full run shows
`rd_kafka_assign` waiting during a rebalance callback invoked by
`rd_kafka_consumer_close`. The managed path runs through consumer Close and disposal
inside the existing `KafkaMessageConsumerCommitRevokeConcurrency` test. That
snapshot is not waiting on Brighter's commit semaphore, and does not establish
which component caused the native wait. No speculative shutdown change was made.

The unchanged existing RoundRobin/CooperativeSticky cases were repeated five times
per framework against separate Kafka brokers, with five-minute hang diagnostics.
All **20 case executions passed**, generally in seven to eight seconds per case.
The built test artifacts reference Confluent.Kafka and librdkafka.redist 2.15.1.
All production and compiled test source files match the audit baseline. These
focused repetitions do not reproduce the full-suite history or resolve the stall;
they are not another full regression run and do not replace the preceding 398-case
results. No commit or push is performed.

Evidence: `/private/tmp/brighter-4170-kafka-disposal-stall-audit/` contains all ten
TRX/log pairs, per-framework duration reports, the source manifest and verification.
The original stack remains in the cancellation-recovery evidence directory.
A future reproduction should collect native group/assignment diagnostics and
identify the assignment strategy and full-suite preconditions before changing
shutdown behavior or dependencies.

At the preceding checkpoint, the next [Azure native wire-writer batch](review/asb-native-wire-writer/README.md)
is drafted outside compiled projects: twelve queue/topic, sync/async, default/custom
cases plus one strategy helper and isolated emulator entity configuration. It is
unbuilt and unrun pending test review. Service Bus test credentials are not
configured in this shell; isolated emulator provisioning is part of the proposed
execution plan, and any unavailable hosted regression coverage must remain explicit.


## Azure Service Bus individual native writer selection

The [approved writer batch](review/asb-native-wire-writer/README.md) adds nullable
`AzureServiceBusPublication.WireMessageWriter` and the composable
`DefaultAzureServiceBusWireMessageWriter`. Individual sends select the writer,
then apply configured publication TTL. Custom body and application metadata are
preserved. The static publisher API and batch mapping remain unchanged. The shared
delayed-send path also selects the writer, but delayed native delivery has not been
validated by this batch.

The corrected tests initially fail only on the two missing public APIs. After
implementation, all twelve queue/topic, sync/async, default/custom native cases pass
on .NET 9 and .NET 10 against a dedicated Service Bus emulator. Ten approved
mutations produce 52 intended assertion failures on .NET 9. Temporary mutation
build errors (obsolete body accessor and unreachable TTL branch) were corrected
before recording behavioral results; neither counts as a successful mutation.
All mutations are restored. Both frameworks pass 539 available cases: 536 in the
main regression run plus three existing session mapping cases omitted by the
initial class-discovery filter. There are zero failures or skips. The source-based
scope manifest covers all 65 locally available existing test classes plus the new
native class; 52 hosted test classes are explicitly unexecuted. No hosted Azure
credentials were available, and the isolated static fixture is not a replacement
for their broader infrastructure.

All library targets (netstandard2.0, net8.0, net9.0, net10.0) build; final library and
test builds report zero warnings/errors. Source verification finds only the two
intended existing production files changed, one new default writer, and the two
approved test/helper files. No dependencies changed. The approved files match the
review copies exactly. Work remains uncommitted and unpushed.

Evidence: `/private/tmp/brighter-4170-asb-native-wire-writer/` contains build logs,
TRX files, ten mutation outcomes, the regression scope manifest, container image
IDs and source verification. This closes individual immediate-send writer mapping
only. ASB readers/settlement, batches, scheduling, failures/null results, sessions,
concurrency, endpoint isolation, handler telemetry and hosted compatibility remain
open, as do the Kafka disposal stall and other cross-transport release gates.


## Azure Service Bus native reader selection and settlement

The [approved reader batch](review/asb-native-wire-reader/README.md) adds nullable
`AzureServiceBusSubscription.WireMessageReader` accepting `ServiceBusReceivedMessage`
and the reusable `DefaultAzureServiceBusWireMessageReader(subscription)`. Production
consumers pass the actual native delivery to the configured reader, then attach its
native lock token and sequence number. Custom identity, subject, correlation and
binary body survive mapping. Existing default mapping and the public creator API
remain compatible; the native accessor is internal on the existing internal wrapper.
Configured readers explicitly reject legacy wrappers that cannot supply native SDK
deliveries; this compatibility boundary is documented, not separately validated here.

All twelve queue/topic, sync/async, implicit-default/explicit-default/custom cases
pass on .NET 9 and .NET 10. Native SDK sends keep producer and reader evidence
independent. Each case proves nack/redelivery with a fresh lock, then acknowledgement
with a native peek at the explicit sequence number. Ten mutations produce **64
intended assertion failures** on .NET 9. Omitting lock attachment and reusing a stale
mapped message are caught at the receive assertion because dispatch rejects the
invalid lock; the later metadata/fresh-instance assertions are not reached.

The initial nack-omission mutation also saw cross-case identity failures after the
fixture's one-minute locks expired. Those contaminated outcomes are preserved but
excluded from clean mutation evidence. The repeat used five-minute locks only for
that mutation: all twelve cases then failed at the intended second receive, verified
from TRX stack locations. The original one-minute fixture was restored before final
regressions. Fixture image pulls delayed an intermediate restart; subsequent runs
pin the same cached SQL/emulator image IDs and disable pulls. No production dependency
changed. All mutations are restored.

Each framework passes **551 available ASB regressions, zero failures/skips**, covering
all 65 existing locally available classes plus the writer and reader native classes.
The preceding 539 cases are retained. Fifty-two hosted test classes remain unexecuted;
this isolated emulator result does not close hosted compatibility. All four library
targets build. The approved legacy MessageType assertion produces one CS0618 warning
per test target, alongside existing test warnings; incremental zero-warning builds
do not erase those diagnostics.

The initial approved draft failed at subscription construction before sending. The
user approved the generic subscription/request-type and explicit Reactor/Proactor
correction before it was applied and rerun. No assertions or helper semantics changed.
The compiled test and helper match the corrected review copies byte for byte. Source
verification limits this batch to the subscription, consumer, internal wrapper, new
default reader, and approved test setup correction; the helper is unchanged. All work
remains uncommitted and unpushed.

Evidence: `/private/tmp/brighter-4170-asb-native-wire-reader/` contains build logs,
TRX files, mutation reports including the excluded first nack run, repeat-stack
verification, fixture configuration, scope manifest and source verification.
Reader exceptions/null results, mixed batches, sessions, retry counts, forwarding,
concurrent reuse, endpoint isolation and handler telemetry remain open. The earlier
Kafka disposal stall and broader transport/scheduler release gates remain open.


## ASB reader exception/null recovery — historical pre-correction checkpoint

The [approved recovery batch](review/asb-reader-failure-recovery/README.md) first
reproduced eight failures on .NET 9: four InvalidOperationException cases and four
NullReferenceException cases. AzureServiceBusConsumer now creates an unacceptable
message with the original ID and binary body for non-cancellation reader exceptions
and null results, then attaches native lock/sequence metadata. Cancellation still
propagates. No dependencies or public API changed; only this existing production
file changed in the batch. All library targets build.

Do not treat the subsequent eight-pass native matrices on both frameworks as final
sign-off. Fresh isolated runs reproduced a native DLQ completion fault on the first
topic case on both frameworks, with seven other cases passing. Emulator logs show an
internal duplicate SequenceNumber:1 insertion, topic abort, and forced link detachment
during CompleteMessageAsync on the native DLQ observer. The native payload/reason
assertions had passed before that cleanup failure. Earlier shared-entity runs also
showed redelivery contamination after failed cases. Per-case entity overrides now
isolate the runner; they do not by themselves resolve the first-topic fault.

Mutation validation is incomplete. The initial exception-bypass mutation reproduces
eight exceptions, but the null-check mutation's additional failures are contaminated
and are not accepted as clean mutation evidence. Every mutation is restored. The
compiled test/helper remain the originally approved versions. A review-only correction
uses ReceiveAndDelete for the independent native DLQ observation and removes explicit
DLQ completion cleanup. All assertion lines are preserved; the correction is unrun
and awaits approval. Brighter rejection and healthy-message acknowledgement remain
under test. No speculative Brighter change was made for the emulator's internal fault.

Evidence: `/private/tmp/brighter-4170-asb-reader-failure-recovery/`, especially
`emulator-logs/log2026102622.txt` (duplicate-sequence error and entity abort),
`isolated-green-net9-isolated.json`, `isolated-green-net10-isolated.json`, original
and contaminated TRX/logs, and `source-verification.json`. Hosted compatibility,
mutation completion, mixed-batch behavior and the wider release gates remain open.
Work remains uncommitted and unpushed.

Existing available regressions pass **551 cases on each framework, zero failures/skips**, retaining exactly the previous case multiset. This excludes the eight new recovery cases and does not complete their validation.


## ASB recovery observer correction applied — broker validation blocked

The approved ReceiveAndDelete correction is now applied and executed; compiled test
and helper match their review copies. It removes only independent DLQ completion
cleanup and preserves all assertions. The earlier pending-approval checkpoint above
is historical. No further approval is needed for this correction.

Fresh isolated runs still produce seven passes and one failure on each framework:
the first topic case receives no native DLQ message. Changing from AMD64 emulation
to the native ARM64 image of the same registry release does not fix it, nor does an
additional 45-second readiness delay. ARM64 image digest:
`sha256:4dab6cd564aa723b99a32af955959acc739bfe831778fe131bd138dd977ca09c`.
A subsequent queue-only mutation run also hits the native DLQ fault. Logs record
`Cannot add the same item 'S.SP-SequenceNumber:1' more than once.` followed by entity
abort. Queue-only execution is therefore not a reliable workaround.

The queue exception-bypass mutation produces four expected exceptions. The next
null-check mutation produces three failures instead of its expected two because of
the DLQ fault, so that run is not accepted as clean mutation evidence. The ten-mutation
plan remains incomplete. Every mutation is restored; no speculative production
change was made for the emulator failure.

Final restored library and test builds succeed. Existing regressions pass **551 on
.NET 9 and 551 on .NET 10, zero failures/skips**, with exact prior case/outcome multisets
verified. These runs exclude the eight recovery cases and do not establish their
correctness. Fifty-two hosted test classes remain unexecuted. A healthy broker is
required to finish recovery and mutation validation; hosted credentials are not
currently configured. The batch and broader production-readiness gates remain open.

Evidence: `/private/tmp/brighter-4170-asb-reader-failure-recovery/`, including
`corrected-baseline-net9.trx`, `corrected-baseline-net10.trx`,
`corrected-baseline-verification.json`, `queue-mutations.json`,
`queue-only-emulator-logs/`, `corrected-emulator-logs/`, and source verification.
The dedicated recovery containers are stopped after validation. Changes remain
uncommitted and unpushed.


## ASB small-message batch writer selection validated

The [approved six-case batch](review/asb-batch-wire-writer/README.md) reproduced ignored
writer selection: queue and topic custom cases expected three writer calls and observed
zero; the four default cases passed. Four ASB production files now map each source once
before packing, apply publication TTL, and pass the native message through batch helpers
while retaining original source IDs. Existing public helper signatures retain default
mapping. Dependencies and the approved test are unchanged.

All six cases pass on .NET 9 and .NET 10, initially and after restoration. Seven named
mutations produce **26 intended assertion failures**: bypassed writer (2), empty explicit
default (2), omitted TTL (6), native IDs replacing source IDs (2), repeated first source
(6), double mapping (2), and mutated source subject (6). The uniqueness mutation exposed
a reporting-script assumption about xUnit custom messages; all six original stacks
confirm the intended assertion. The reporter was corrected without changing tests.
All production mutations are restored.

Final available suites pass **557 cases on each framework, zero failures/skips**. Exact
case/outcome comparison retains all prior 551 cases and adds only the six new passes.
All library targets build. Source verification limits this increment to four existing
ASB production files and the approved test. No public API or dependency changes.

This closes small-message batch writer selection only. Native batch splitting, oversized
single-message fallback, failures/cancellation, callbacks and sessions remain unverified
by these cases. Fifty-two hosted classes remain unrun; the eight reader-recovery cases
remain separately blocked by the emulator DLQ fault and are not included in the 557.
The Kafka disposal stall, remaining transports/schedulers and global release gates remain
open. Full production readiness is not achieved.

Evidence: `/private/tmp/brighter-4170-asb-batch-wire-writer/` contains RED/green/restored
TRXs, mutations.json, the preserved original report, full regression TRXs, exact baseline
comparisons and source verification. The dedicated fixture containers are stopped after
validation. No changes committed or pushed.


## ASB native batch splitting validated

The [approved splitting cases](review/asb-batch-wire-splitting/README.md) negotiate native
batch capacity and send three distinct custom binary messages, each 60% of that capacity,
through public bulk producers for queues and topics. Both cases pass on .NET 9 and
.NET 10 initially and after mutation restoration. They establish three nonempty native
batches, exactly one writer call per source, preserved original batch IDs, native custom
mapping and publication TTL, distinct deliveries, and unchanged source metadata/body.
No production correction was needed after the preceding batch-selection implementation.

Six mutations produce **12 intended assertion failures**: remapping on split, default
metadata on retry, dropping a completed batch, losing a retry source ID, reusing the
first native message, and losing TTL on retry. The initial default conversion mutation
also expanded binary bytes and reached the wrong batch-shape assertion; it is excluded
from clean mutation proof. Repeating with unchanged native body bytes isolates the
intended metadata loss. An initial ID-loss mutation compile error is also excluded;
correcting its obsolete property reference yielded the intended source-ID failure.
Tests remain byte-identical to approval and all production source hashes are restored.

Both final available suites pass **559 cases, zero failures/skips**, retaining the exact
557 prior case/outcome pairs and adding only the two splitting cases. All library targets
build. Eight DLQ-recovery cases remain separately blocked by the emulator fault; 52 hosted
test classes remain unavailable. Oversized single-message fallback, cancellation,
failure recovery, sessions and broader production-readiness gates remain open.

Evidence: `/private/tmp/brighter-4170-asb-batch-wire-splitting/` contains all original and
corrected mutation evidence, initial/restored and full-regression TRXs, baseline comparisons
and source verification. The dedicated fixture containers are stopped after validation.
No changes committed or pushed.


## ASB oversized fallback construction validated

The [approved six cases](review/asb-oversized-batch-fallback/README.md) exercise native
batch-capacity rejection with oversized messages first, middle and last for queues and
topics. Public bulk creation preserves custom binary content, native metadata,
publication TTL and original source IDs in the single-message fallback. Normal batches
remain usable, each source is mapped once, and empty trailing native batches are excluded.
Native SDK receivers verify and settle the two normal messages in each case. The oversized
fallback itself is inspected through its public Content API and is deliberately not sent.

All six cases pass on .NET 9 and .NET 10 initially and after restoration. Seven named
mutations produce **38 intended assertion failures**: default fallback conversion (6),
mapped ID replacing original bookkeeping ID (6), lost fallback TTL (6), dropped fallback
(6), remapped fallback (6), discarded normal batches (6), and returned empty batches (2).
The last mutation fails only oversized-last cases; all four other cases pass as expected.
Every mutation is restored and production source hashes match the batch baseline.
The approved test is unchanged. No production correction or dependency change was needed.

Both final available suites pass **565 cases, zero failures/skips**, retaining all 559
previous case/outcome pairs and adding only these six passes. All library targets build.
Fifty-two hosted classes remain unrun and eight DLQ-recovery cases remain blocked by the
emulator fault. Successful oversized delivery, rejection/recovery, sessions, cancellation,
concurrency and broader transport/scheduler release gates are not established here.

Evidence: `/private/tmp/brighter-4170-asb-oversized-batch-fallback/` includes initial and
restored matrices, all seven mutation results, full-regression TRXs, exact baseline
comparisons and source verification. Dedicated fixture containers are stopped after
validation. Nothing committed or pushed. Full production readiness remains incomplete.


## ASB oversized send rejection and explicit recovery

The [approved two cases](review/asb-oversized-send-recovery/README.md) pass on .NET 9
and .NET 10 initially and after mutation restoration. A native SDK control establishes
MessageSizeExceeded for the oversized body in this fixture. Sending the mapped fallback
through the public bulk producer exposes the same non-transient failure, and a subsequent
healthy batch through the same producer reaches the native queue/topic with custom mapping,
TTL and original bookkeeping IDs intact. This does not identify the enforcement location
or prove automatic retry, outbox recovery or successful oversized delivery.

Six mutations produce **12 intended assertion failures**: swallowed failure, changed
exception type, changed reason, lost healthy-message writer, fallback remapping during
send, and cleared failed source body. Tests remain unchanged and production source hashes
are restored. All library targets build; no production correction was required.

.NET 9 passes **567 cases** on its first final run. The first .NET 10 run passes 566 and
fails a session-lock renewal case (queue/async/buffer size 1). The focused unchanged class
rerun passes 15 of 16 and fails a different session case (topic/sync/buffer size 1).
Both fail with MessageLockLost during in-memory receiver completion. The next unchanged
full .NET 10 run passes **567 cases**. Successful final runs retain exactly all 565 prior
case/outcome pairs plus the two new cases; both failed runs remain recorded.

Session-lock renewal instability is an unresolved readiness finding. These existing
tests use two-second locks and real-time delays, but no causal diagnosis is established.
A later green run does not resolve it. No tests, timing or production were changed to
obtain that repeat. Eight DLQ-blocked cases and 52 hosted classes remain excluded.
The wider transport/scheduler, successful oversized-delivery and release gates remain open.

Evidence: `/private/tmp/brighter-4170-asb-oversized-send-recovery/` includes mutation,
initial/restored and full-regression TRXs, the focused failed class repeat,
lock-renewal-instability.json, exact baseline comparisons and source verification.
Dedicated fixture containers are stopped after validation. Nothing committed or pushed.


## ASB session-lock timing audit — unresolved

The [bounded audit](session-lock-renewal-audit.md) investigated the two prior .NET 10
MessageLockLost failures without changing existing tests/helpers. Temporary production
timing diagnostics were removed and original source rebuilt afterward. The full 16-case
lock-renewal class passes on both frameworks under diagnostics; the four short-lock
session cases pass 20 repeated executions with diagnostics and 20 without. All 72
executions pass, but none reproduces the original failures.

The focused diagnostic samples show at least approximately 1,001 ms of lock lifetime
remaining when completion begins, with a maximum 3.16 ms stop-to-completion interval.
These samples do not establish the cause of the earlier failures. No production fix,
timeout adjustment or test weakening was made; all production hashes are restored.
The session-lock readiness finding remains open. Capturing timing during an actual
failure or constructing a deterministic reproduction is needed before claiming resolution.
Evidence is under `/private/tmp/brighter-4170-asb-session-lock-audit/`.


## Full-suite diagnostic follow-up

Two further .NET 10 runs exercised the entire available 567-case ASB suite with
temporary renewal/expiry/settlement timing diagnostics and session-to-delivery
identifiers. Each run used a fresh dedicated emulator fixture. Both passed all 567
cases, with exact prior case/outcome multisets retained. The earlier failure did not
recur. These runs preserve the suite composition; they cannot recreate unknown host
scheduling conditions from the original failures, and instrumentation can affect timing.

All temporary source diagnostics were restored byte-for-byte and library/test projects
rebuilt successfully. Existing tests/helpers were unchanged. The session-lock finding
remains unresolved: neither the 72 earlier focused executions nor these two full runs
establish a cause or a fix. Further green repetitions alone are not a release criterion.

Evidence: `/private/tmp/brighter-4170-asb-session-lock-full-audit/` contains both full
TRXs, detailed timing logs, source backups, scripts and verification.json. The dedicated
fixture containers are stopped after the audit. Nothing committed or pushed.


## ASB batch writer failure recovery validated

The [approved four cases](review/asb-batch-writer-failure-recovery/README.md) reproduce
a null configured writer result escaping batch creation as NullReferenceException.
The two writer-exception cases already pass. AzureServiceBusMessageProducer now checks
for null before applying publication TTL and raises InvalidOperationException; writer
exceptions preserve their original identity. Only this guard and its XML exception
documentation change in production. Public signatures and dependencies are unchanged.

Four native queue/topic × exception/null cases pass initially after the fix and after
mutation restoration on both frameworks. They check failed creation, source preservation,
writer invocation counts and subsequent healthy sending through the same producer.
Six mutations produce **20 assertion failures**: removed guard (2), partial success (4),
wrapped writer exception (2), lost recovery writer (4), cleared failed body (4), and
premature publication (4). The last mutation fails three immediate-peek assertions and
one downstream native-ID assertion: the topic peek briefly sees empty before the
prematurely published first message becomes visible. This limits the peek's standalone
guarantee; the later identity check still detects the actual defect. The original report
and reviewed assertion evidence are preserved. No approved tests/helpers were changed.

Both final available suites pass **571 cases, zero failures/skips**, on their first runs,
retaining exactly all 567 prior outcomes plus four new passes. All library targets build
and all mutations are restored. Previous session-lock instability remains unresolved;
eight DLQ-recovery cases and 52 hosted classes remain excluded. Other ASB and global
production-readiness requirements remain open.

Evidence: `/private/tmp/brighter-4170-asb-batch-writer-failure-recovery/` contains RED,
initial/restored matrices, mutation results and assertion-location review, final full
TRXs, baseline comparisons and source verification. Dedicated fixture containers are
stopped after validation. Nothing committed or pushed.


## ASB individual writer failure recovery

The [approved eight-case batch](review/asb-individual-writer-failure-recovery/README.md)
covers queue/topic, sync/async and writer exception/null. Initial RED passes the four
throwing cases and fails all four null cases with an accidental NullReferenceException.
A minimal null-result guard now produces InvalidOperationException inside the existing
ChannelFailureException wrapper. Original writer exception identity, same-producer
recovery, native mapping/TTL and source preservation are verified.

Six named mutations produce 40 intended assertion failures; all are restored. Approved
tests/helpers are unchanged. Both available ASB suites pass **579 cases, zero failures
or skips**, on their first runs, retaining all 571 previous outcomes. All library
targets build. Session-lock instability remains unresolved; eight DLQ-blocked cases
and 52 hosted classes remain excluded. Other ASB and global release requirements stay
open. Scheduled sends, cancellation and broker-failure recovery are outside this batch.

Evidence: `/private/tmp/brighter-4170-asb-individual-writer-recovery/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB publication writer isolation

The [approved six-case batch](review/asb-publication-writer-isolation/README.md)
validates custom/default writer and TTL isolation between queue and topic publications
in one registry, through sync, async and bulk sends. Both placements of the custom
writer pass initially and after mutation restoration on .NET 9 and .NET 10.

Six named mutations produce 36 intended assertion failures. All 1,171 non-generated
production C# files remain unchanged; approved test/helper are unchanged. All library
targets build. Both full available suites pass **585 cases, zero failures/skips**,
on their first runs, retaining exactly all 579 preceding outcomes.

This covers sequential publication isolation, not concurrency or two distinct custom
writer implementations. Session-lock instability remains unresolved despite passing
here. Eight DLQ-blocked cases and 52 unavailable hosted classes remain excluded;
scheduled sends, cancellation, reader isolation and broader release gates stay open.
Evidence: `/private/tmp/brighter-4170-asb-publication-isolation/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB concurrent publication writer

The [approved three-case batch](review/asb-concurrent-publication-writer/README.md)
validates one safe shared writer across queue/topic producers through sync, async and
bulk sends. A bounded barrier inside the writer establishes overlapping mappings.
Native SDK observers verify per-message identity, bytes, trace state and publication
TTL. Cases pass initially and after mutation restoration on .NET 9 and .NET 10.

Six mutations produce 18 intended assertion failures, including shared native-body
cache contamination detected by payload assertions. All mutations are restored; all
1,171 non-generated production C# files and approved tests/helpers are unchanged.
All library targets build. Both full available suites pass **588 cases, zero failures
or skips**, on their first runs, retaining exactly all 585 previous outcomes.

This is bounded concurrency across two distinct producers, not a stress test or a
thread-safety guarantee for arbitrary user state or one shared producer. Reader
concurrency, scheduled sends, cancellation, session-lock instability, eight DLQ-blocked
cases, 52 unavailable hosted classes and broader release gates remain open.
Evidence: `/private/tmp/brighter-4170-asb-concurrent-writer/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB concurrent reader

The [approved two-case batch](review/asb-concurrent-reader/README.md) validates sync
and async consumers sharing one safe reader across queue/topic deliveries. A bounded
barrier proves overlapping mapping calls. Native delivery records verify per-message
identity/body and lock/sequence ownership. Native peeks observe each locked delivery
before acknowledgement and removal afterward.

All six named defects have intended assertion evidence. Five initial mutations yield
ten intended failures. The initial whole-Message cache catches sync sharing but async
rejects the corrupted lock before the identity assertion; that result is preserved and
not counted as identity evidence. A refined cache mutation copies the cached header
before attaching native delivery metadata, isolating the same identity defect; both
cases then fail at the intended mapped-ID assertion. Final qualified evidence totals
twelve intended failures, with the initial cache attempt reported separately.

All mutations are restored. All 1,171 non-generated production C# files and approved
test/helper are unchanged. All library targets build. Both full available suites pass
**590 cases, zero failures/skips**, on their first runs, retaining exactly all 588
preceding outcomes. This covers two distinct consumers and subsequent individual
acknowledgements, not concurrent settlement or repeated reads on one consumer.

Session-lock instability, scheduled sends, cancellation, eight DLQ-blocked cases,
52 unavailable hosted classes and broader release gates remain open. Evidence:
`/private/tmp/brighter-4170-asb-concurrent-reader/`. Dedicated fixture containers are
stopped after validation. Nothing committed or pushed.


## ASB scheduled custom writer

The [approved four-case batch](review/asb-scheduled-wire-writer/README.md) validates
native queue/topic scheduled sends through sync/async producer interfaces. A five-second
delay preserves custom wire identity, body and metadata, publication TTL and source
state. Native scheduled timestamps and observed delivery satisfy the approved bounds
with one-second tolerance. Cases pass initially and after restoration on both frameworks.

Six mutations produce 24 intended assertion failures, including immediate-send bypass
and scheduling without the requested delay. All mutations are restored; all 1,171
non-generated production C# files and approved test/helper remain unchanged. All library
targets build. Both full available suites pass **594 cases, zero failures/skips** on
their first runs, retaining exactly all 590 prior outcomes.

This covers direct producer scheduling with a custom writer, not the separate Azure
scheduler package, default scheduling, cancellation or scheduled failure recovery.
Session-lock instability, eight DLQ-blocked cases, 52 unavailable hosted classes and
broader release gates remain open. Evidence: `/private/tmp/brighter-4170-asb-scheduled-writer/`.
Dedicated fixture containers are stopped after validation. Nothing committed or pushed.


## ASB send cancellation recovery

The [approved four-case batch](review/asb-send-cancellation-recovery/README.md)
validates queue/topic × immediate/scheduled async sends cancelled deterministically
after custom mapping and before native submission. The existing ChannelFailureException
wrapper retains an inner OperationCanceledException-compatible cause. The same producer
recovers with an active token, preserving custom mapping, TTL and source state.

Six mutations produce 24 intended assertion failures. All mutations are restored;
all 1,171 non-generated production C# files and approved tests/helpers are unchanged.
All library targets build. Both full available suites pass **598 cases, zero failures
or skips**, on their first runs, retaining exactly all 594 previous outcomes.

The immediate empty peek remains best-effort; these cases do not establish broker
atomicity for cancellation during I/O or retract an accepted message. Bulk/consumer
cancellation and scheduled-message cancellation remain separate. Session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader release
gates remain open. Evidence: `/private/tmp/brighter-4170-asb-send-cancellation/`.
Dedicated fixture containers are stopped after validation. Nothing committed or pushed.


## ASB bulk cancellation recovery

The [approved four-case batch](review/asb-bulk-cancellation-recovery/README.md)
reproduces cancellation being ignored after the second source maps into an existing
batch. Both frameworks initially fail both creation cases because no exception is
thrown; both cancelled-submission/reuse cases already pass. A token check immediately
after mapping now aborts creation before packing and leaves the third source unvisited.
The same producer recovers, and a batch cancelled before submission remains reusable
with an active token, without remapping.

Only the producer guard and XML documentation change. Approved test and reused helpers
are unchanged. Six mutations produce 16 intended assertion failures; all are restored.
All library targets build. Both full available suites pass **602 cases, zero failures
or skips**, on their first runs, retaining exactly all 598 prior outcomes.

Immediate peek remains best-effort. In-flight cancellation, oversized fallback,
consumer cancellation, session-lock instability, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader release gates remain open. Evidence:
`/private/tmp/brighter-4170-asb-bulk-cancellation/`. Dedicated fixture containers are
stopped after validation. Nothing committed or pushed.


## ASB pre-entry receive cancellation — incomplete

The [approved two-case batch](review/asb-receive-cancellation-recovery/README.md)
reproduces ignored pre-entry cancellation in the queue case. A minimal token check
at ReceiveAsync entry and XML exception documentation are implemented. Fresh isolated
queue cases pass on both frameworks. Topic cases stop earlier at a null immediate
native peek, exposing a setup visibility race. Original cross-framework leftovers
also caused source-ID mismatches; fresh per-case fixtures eliminate that contamination.
All raw results are retained.

A [bounded initial visibility wait](review/asb-receive-cancellation-visibility/README.md)
is drafted outside compiled projects for review. Original test/helper are unchanged;
mutation validation has not started. Both existing full available suites retain
exactly **602 passes, zero failures/skips** against the guard, excluding this pending
batch. All library targets build. This batch is not complete and does not add two
validated cases to the baseline. Other release gaps remain open.

Evidence: `/private/tmp/brighter-4170-asb-receive-cancellation/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB pre-entry receive cancellation — completed with approved visibility amendment

The [approved initial visibility wait](review/asb-receive-cancellation-visibility/README.md)
is adopted unchanged. Queue and topic cases now pass on both frameworks, initially
and after mutation restoration. The production change remains the ReceiveAsync entry
cancellation check and XML exception documentation. Pre-cancelled receives do not map
or consume the delivery, and the same consumer recovers with delivery count one and
supports native acknowledgement.

Six mutations produce 12 intended assertion failures. All are restored. Compiled test
matches the approved amendment; the reused reader is unchanged. All library targets
build. Both final full available suites pass **604 cases, zero failures/skips** on
their first runs, retaining exactly all 602 prior outcomes. The original visibility
race and cross-framework leftovers remain recorded. Fresh fixtures isolate each
focused/mutation case. A harness precondition abort occurred before changing source;
it was corrected to target only the receive-entry guard and is not mutation evidence.

This closes the previously pending batch only. Cancellation during broker waits,
mapping, buffered deliveries or settlement remains separate. Session-lock instability,
eight DLQ-blocked cases, 52 unavailable hosted classes and broader release gates stay
open. Evidence: `/private/tmp/brighter-4170-asb-receive-cancellation/`. Dedicated fixture
containers are stopped after validation. Nothing committed or pushed.


## ASB cancellation during mapping

The [approved two-case batch](review/asb-mapping-cancellation-recovery/README.md)
reproduces ignored cancellation after the reader returns on both frameworks. A
post-mapping token check alone reports cancellation but fails both recovery receives
on both frameworks: reset alone does not release the native delivery promptly. The
final correction explicitly abandons the current native delivery before propagating
cancellation through the existing reset path. Recovery on the same consumer occurs
before the original lock expires, with a fresh lock and delivery count two.

Six mutations produce 12 intended assertion failures, including omitted explicit
release reproducing empty recovery receives. All are restored; approved test/helper
and reused reader remain unchanged. Only AzureServiceBusConsumer changes. All library
targets build. Both full available suites pass **606 cases, zero failures/skips** on
their first runs, retaining exactly all 604 previous outcomes. Original and guard-only
failures are preserved. The initial Docker address-pool exhaustion was resolved by
removing the previous completed batch's stopped fixture/network, before any tests ran.

Scope remains one non-session delivery and cancellation observed after mapping.
Buffered multi-message cancellation, broker-wait/settlement cancellation, session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader release
gates remain open. Evidence: `/private/tmp/brighter-4170-asb-mapping-cancellation/`.
This batch's fixture containers and network are removed after validation. Nothing
committed or pushed.

## ASB buffered mapping cancellation

The [approved queue/topic batch](review/asb-buffered-mapping-cancellation/README.md)
reproduces incomplete recovery on both frameworks: the earlier mapped delivery
remains locked while the cancellation-triggering delivery recovers. The correction
abandons the received batch before propagating cancellation through the existing
reset path. Both messages then recover before original lock expiry with fresh locks,
correct mapping and native delivery metadata, and successful acknowledgement.

Six mutations produce 12 targeted failures: ten xUnit assertion failures and two
mapped-identity cardinality failures at the deliberate Single lookup. Those two
are not described as xUnit assertion failures. A mutation-only compilation error
from unqualified LINQ is preserved and excluded; its corrected version detects the
intended incomplete recovery. All mutations are restored and approved sources match
compiled copies byte-for-byte. Only AzureServiceBusConsumer changes among 1,171
non-generated production C# files; no public signatures or dependencies change.

Both full available suites pass **608 cases, zero failures/skips**, on their first
runs, retaining exactly all 606 prior outcomes. The explicit two-message native
receive precondition holds in every execution; SDK batch-size variability remains
a documented setup limitation. Unvisited buffered deliveries, abandonment failures,
broker-wait/settlement cancellation, sessions, session-lock instability, eight
DLQ-blocked cases, 52 unavailable hosted classes and broader release gates remain
open. Evidence: `/private/tmp/brighter-4170-asb-buffered-cancellation/`. Nothing
committed or pushed.

This batch's isolated emulator containers and network were removed after validation.

## ASB cancellation with an unvisited buffered delivery

The [approved queue/topic characterisation](review/asb-unvisited-buffered-cancellation/README.md)
cancels after mapping two of three native deliveries. All three recover on the same
consumer with correct custom mapping, native body/sequence metadata and successful
acknowledgement. The previously unvisited message has exactly one mapping invocation
and native delivery count two, proving it was acquired by the cancelled receive.
Recovery precedes the earliest recorded original lock deadline. The third original
lock itself is not exposed to the reader, as documented in the approved contract.

All four initial and four restored executions pass without production changes. Six
named mutations produce 12 intended xUnit assertion failures on .NET 9, including
incomplete release, extra mapping after cancellation, and explicit mapped-identity
failure when the reader is lost. No setup or compile failure is counted. All mutations
are restored. The adopted test matches the approved draft; both reused helpers and
all 1,171 non-generated production C# files remain unchanged.

Both full available suites pass **610 cases, zero failures/skips**, on their first
runs, retaining exactly all 608 previous case/outcome pairs. All library targets
build. Native batch acquisition is established in every execution, but SDK maximum
batch size still does not guarantee a minimum for future runs. Abandonment failures,
broker-wait/settlement cancellation, sessions, session-lock instability, eight
DLQ-blocked cases, 52 unavailable hosted classes and broader transport/release gates
remain open. Evidence: `/private/tmp/brighter-4170-asb-unvisited-cancellation/`.
Nothing committed or pushed.

This batch's isolated emulator containers and network were removed after validation.

## ASB abandonment failure during cancellation — precondition not met

The [approved native queue/topic batch](review/asb-cancellation-abandon-failure/README.md)
was built and executed on .NET 9 and .NET 10. All four executions returned
OperationCanceledException instead of ServiceBusException(MessageLockLost), failing
at line 96 after confirming two mapper calls and one completed public Nack callback.
The chosen repeat-abandonment mechanism did not produce a native settlement failure.
The evidence does not distinguish SDK behavior from emulator behavior. It does not
establish a cleanup defect, and recovery assertions were not reached.

No production changes or named mutations were applied. The unchanged approved test
and helper remain in the review directory, with adopted copies moved to the evidence
directory outside the compiled project. They were not weakened or skipped. All
1,171 non-generated production C# files are unchanged. A replacement mechanism must
establish a native abandonment failure while other buffered locks are still live,
or validation needs a suitable hosted environment. This gate remains unchecked.

After removing only these two newly adopted files from the compiled project, both
full available suites pass **610 cases, zero failures/skips**, retaining exactly all
610 baseline case/outcome pairs. All library targets build. Evidence:
`/private/tmp/brighter-4170-asb-abandon-failure/` contains original initial TRXs,
logs, preserved adopted files, restored-baseline TRXs and source verification.
No commits or pushes. Existing session-lock instability, eight DLQ-blocked cases,
52 unavailable hosted classes and broader release gates remain unresolved.

The isolated emulator containers and network were removed after verification.

## ASB cancellation during an empty receive

The [approved queue/topic batch](review/asb-broker-wait-cancellation/README.md)
reproduces ignored cancellation on both frameworks: the pending ten-second receive
returns successful emptiness after its wait. Passing the caller token to the native
SDK receive through an internal wrapper overload fixes cancellation. A filtered
OperationCanceledException catch resets the receiver and preserves direct caller
cancellation rather than wrapping it in ChannelFailureException. Same-consumer
recovery retains custom mapping, native metadata and successful acknowledgement.

The public receiver-wrapper interface and its existing signatures remain unchanged.
External wrapper implementations retain their existing receive path; this change
does not promise in-flight cancellation support for arbitrary external wrappers.
No detached receive, new public API or dependency change is introduced. Only the
consumer and internal native wrapper change among 1,171 non-generated production
C# files. The approved test and reused reader remain unchanged.

Six mutations produce 12 intended xUnit assertion failures, including an elapsed-time
failure when cancellation is checked only after the ten-second wait. All mutations
are restored. All library targets build. Both full available suites pass **612 cases,
zero failures/skips**, on their first runs, retaining all 610 prior outcomes.
Evidence: `/private/tmp/brighter-4170-asb-wait-cancellation/`. Nothing committed or pushed.

This covers an empty non-session receive after warm-up. Arrival/cancellation races,
provisioning, session acquisition, settlement cancellation, the unavailable native
abandonment-failure mechanism, session-lock instability, eight DLQ-blocked cases,
52 unavailable hosted classes and broader transport/release gates remain open.

The isolated emulator containers and network were removed after validation.

## ASB acknowledgement cancelled before entry

The [approved queue/topic batch](review/asb-acknowledgement-cancellation/README.md)
reproduces ignored pre-entry cancellation on both frameworks. AcknowledgeAsync now
checks the token before accessing the channel or receiver and documents the exception.
Both cases confirm the native delivery remains present after cancellation, mapped
content and lock metadata stay intact, and an active-token retry on the same mapped
message and consumer completes the delivery without another receive or mapping.

Four named mutations produce eight intended xUnit assertion failures. Checking the
token only after native completion fails the retained-delivery assertion even though
it reports cancellation. All mutations are restored. Approved test and reused reader
remain unchanged. Only AzureServiceBusConsumer changes among 1,171 non-generated
production C# files, adding the entry guard and XML documentation; no public
signatures, receiver-wrapper contracts or dependencies change.

All library targets build. Both full available suites pass **614 cases, zero
failures/skips**, on their first runs, retaining all 612 previous outcomes. Evidence:
`/private/tmp/brighter-4170-asb-ack-cancellation/`. Nothing committed or pushed.

In-flight settlement cancellation, nack/reject cancellation, receive-arrival races,
provisioning, session acquisition, abandonment-failure recovery, session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. Pre-entry cancellation does not establish
rollback or safe retry after a native settlement may already have committed.

The isolated emulator containers and network were removed after validation.

## ASB negative acknowledgement cancelled before entry

The [approved queue/topic batch](review/asb-nack-cancellation/README.md) reproduces
ignored pre-entry cancellation on both frameworks. NackAsync now checks the caller
token before accessing the channel or receiver and documents the exception. The
cancelled call retains the delivery, while active-token retry permits redelivery on
the same consumer with a fresh lock, delivery count two and preserved custom/native
mapping. Final acknowledgement removes the native message.

Five mutations produce ten intended xUnit assertion failures. In particular, checking
cancellation after native abandonment allows the competing receiver to acquire the
message and fails its Assert.Null in both cases. Thus the bounded observation detects
premature release in this fixture. All mutations are restored. Approved test and
reused reader remain unchanged. Only AzureServiceBusConsumer changes among 1,171
non-generated production C# files: the entry guard and XML documentation. No public
signatures, wrapper contracts or dependencies change.

All library targets build. Both full available suites pass **616 cases, zero
failures/skips**, on their first runs, retaining all 614 prior outcomes. Evidence:
`/private/tmp/brighter-4170-asb-nack-cancellation/`. Nothing committed or pushed.

In-flight settlement cancellation, reject cancellation, receive-arrival races,
provisioning, session acquisition, abandonment-failure recovery, session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. Existing NackAsync native error handling and
rollback after a settlement may have committed are not established by this batch.

The isolated emulator containers and network were removed after validation.

## ASB rejection cancelled before entry

The [approved queue/topic batch](review/asb-reject-cancellation/README.md) reproduces
ignored pre-entry cancellation on both frameworks: RejectAsync returns successfully
without a cancellation exception. An entry guard now rejects the call before channel
or receiver access, with XML exception documentation. The native delivery and mapped
metadata remain intact, permitting later acknowledgement on the same consumer and
mapped message without redelivery or another mapping.

Three mutations produce six intended xUnit assertion failures. Removed-guard cases
return successfully and fail the expected cancellation assertion; no emulator failure
or hang is counted. This narrower path does not encounter the anticipated blocker,
but it does not validate DLQ reading or resolve the eight earlier blocked cases.
All mutations are restored. Approved test and reused reader remain unchanged. Only
AzureServiceBusConsumer changes among 1,171 non-generated production C# files, adding
the entry guard and XML documentation; no public signatures or dependencies change.

All library targets build. Both full available suites pass **618 cases, zero
failures/skips**, on their first runs, retaining all 616 prior outcomes. Evidence:
`/private/tmp/brighter-4170-asb-reject-cancellation/`. Nothing committed or pushed.

In-flight settlement/rejection cancellation, successful DLQ validation, receive-arrival
races, provisioning, session acquisition, abandonment-failure recovery, session-lock
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. Pre-entry rejection cancellation does not
establish rollback after a native settlement may already have committed.

The isolated emulator containers and network were removed after validation.

## ASB cancellation during session acquisition

The [approved queue/topic batch](review/asb-session-acquisition-cancellation/README.md)
reproduces ignored cancellation on both frameworks: an empty session acquisition
waits for the native timeout and returns without the expected cancellation exception.
The native provider now forwards the caller token to AcceptNextSessionAsync. Queue
and topic consumers propagate caller cancellation directly. The same consumer can
subsequently acquire a newly published session, preserve custom mapping and native
session/lock metadata, and acknowledge the delivery.

A scoped AsyncLocal carries the token through the existing virtual acquisition hook
and restores its previous value in finally. This preserves existing subclass dispatch
and the public provider/receiver interfaces. Existing external providers retain their
original call path; arbitrary external-provider cancellation is not established.
The native forwarding consumer uses the same token path, but has no dedicated
forwarding-session test in this batch. Five production files change among 1,171
non-generated C# files: the base, queue, topic and forwarding consumers, and the
internal ServiceBusReceiverProvider. No dependency changes.

The approved native-client helper bounds SDK acquisition to ten seconds with no
retries. Empty session-enabled entities exercise pending acquisition, potentially
including connection setup; the test does not locate the precise AMQP request instant.
Both cases require direct cancellation within five seconds and same-consumer recovery.
All four initial cases fail the intended cancellation assertion. Six named mutations
on .NET 9 produce twelve intended xUnit assertion failures. All mutations are restored;
approved test/helper sources, entity fragment and reused reader are unchanged.

All library targets build. Restored focused cases and both full available suites pass
on their first runs: **620 cases, zero failures/skips per framework**, retaining all
618 prior outcomes. Evidence: `/private/tmp/brighter-4170-asb-session-cancellation/`.
Nothing committed or pushed.

Session contention/renewal, receive-arrival races, provisioning, in-flight settlement
cancellation and abandonment-failure recovery remain unvalidated. Session-lock renewal
instability, eight DLQ-blocked cases, 52 unavailable hosted classes and broader
transport/release gates remain open. This batch does not establish production readiness.

The isolated emulator containers and network were removed after validation.

## Session-lock failure-state diagnostic audit — inconclusive

The [approved helper amendment](review/asb-session-lock-failure-diagnostics/README.md)
adds temporary diagnostics to the existing in-memory receivers, preserving test
assertions. It separates closed receiver, absent token and expired lock states and
records session receive/renewal wall-clock and monotonic timestamps. No behavioral
production correction was made.

Both focused lock-renewal classes passed 16/16 cases, on .NET 9 and .NET 10. Both
bounded full diagnostic .NET 10 runs passed all 620 available cases. The original
session-lock test failure did not recur. The smallest observed session lifetime
remaining at receive was 1,996.4882 ms of the configured 2,000 ms; these samples do
not show setup consuming the initial lock budget. They do not exclude that hypothesis
in the archived failures, nor establish a renewal/settlement scheduling cause.

Each full diagnostic run recorded three lock-exception events: two closed receivers
with absent tokens, and one present token whose expiry had passed. All tests passed,
and none of those entities matched a recorded session-receive entity. These events
are not reproductions of the original failing session test. The adapter captures
stderr at run level, so no individual test attribution is claimed for those events.
Logging itself can perturb scheduling. These are in-memory diagnostic observations,
not native broker session-renewal validation.

All 6,454 non-generated production/test C# files match their pre-audit hashes after
restoration. Both restored full available suites pass **620 cases, zero failures or
skips**, with exact baseline case/outcome equality. Library and test builds pass.
A sandboxed build initially stalled without output; only its owned processes were
stopped, sources verified restored, and the approved audit rerun with required access.
That infrastructure interruption is not counted as test evidence.

Evidence: `/private/tmp/brighter-4170-asb-session-failure-diagnostics/`, including raw
TRXs/logs, per-run event extracts, timing-summary.json, source hashes and verification.
No tests were added, assertions weakened, or permanent source changes made. Nothing
committed or pushed. The session-lock readiness finding remains open. Further green
repetitions alone are not a release criterion; capture an actual failing session path
or establish a separately reviewed deterministic reproduction before changing behavior.

The isolated diagnostic emulator containers and network were removed after validation.

## Native session renewal control — expiry cause not established

The [approved native queue/topic batch](review/asb-native-session-renewal/README.md)
initially passes on both .NET 9 and .NET 10 without production changes. Each case
receives a custom-mapped session delivery from an entity configured with a five-second
lock, waits eight seconds, acknowledges it, and confirms removal by native peek.

Disabling only the session renewal budget produces two intended Assert.Null failures
at the settlement assertion (line 131), but the recorded exception is
ObjectDisposedException: the native ServiceBusReceiver is already closed. Neither
case reports ServiceBusException with a lock-loss reason. Thus the control detects
the absence of renewal, but does not directly establish the approved native
lock-expiry prerequisite or explain why the SDK receiver closed. The runner records
validated=false and stops. No test or expected exception was weakened to accept this
result, and the other three mutations were not run. These are assertion failures,
not setup failures, but are insufficient for the stronger planned expiry claim.

The mutation is restored. All 1,171 non-generated production C# files match pre-batch
hashes; reused reader and native-client helper are unchanged. The approved test is
preserved byte-for-byte in its review directory and temporary evidence, outside the
compiled project pending resolution of the prerequisite. No production fix or new
passing baseline case is claimed.

Restored library and test builds pass. Both full available suites pass **620 cases,
zero failures/skips**, retaining the exact prior case/outcome multisets. Evidence:
`/private/tmp/brighter-4170-asb-native-session-renewal/`, including four initial passing
cases, the two control failures, mutation/source verification and both regression TRXs.
Nothing committed or pushed.

A follow-up needs direct evidence linking native session expiry to receiver closure,
or a separately reviewed control that observes native session-lock state and release.
Do not reclassify ObjectDisposedException as a native lock-loss reason without that
evidence. The intermittent in-memory session-lock finding, eight DLQ-blocked cases,
52 unavailable hosted classes and broader release gates remain open.

The isolated emulator containers and network were removed after validation.

## Native session SDK investigation

The [version-specific SDK trace](asb-native-session-sdk-trace.md) identifies how session
link closure can dispose the transport before acknowledgement, causing the observed
ObjectDisposedException. It does not establish why the broker closed the link. A
[review-only amendment](review/asb-native-session-expiry-observation/README.md) directly
asserts advancement of the native session expiry while retaining mapping, completion
and removal checks. No new tests ran or production sources changed; baseline is 620
per framework and the native renewal gate remains open pending review and validation.

## Native ASB session-expiry observation — validated

The [approved amendment](review/asb-native-session-expiry-observation/README.md)
validates native emulator session renewal with a custom wire reader for queue and
topic deliveries. A real SDK client records the accepted receiver and original
broker-reported SessionLockedUntil before returning it to Brighter. After eight
seconds, the original five-second lock has expired, the native expiry has advanced,
and the receiver remains open. The same mapped message is acknowledged successfully
and native peek confirms removal. Custom identity/body/headers, native session/lock
metadata and exactly one reader invocation are preserved.

All four initial cases pass without production changes. Disabling the session renewal
budget makes both cases fail the explicit native-expiry-advance assertion at line 135.
This establishes the renewal dependency directly, independently of the prior
ObjectDisposedException result. Bypassing the reader, replacing the mapped body and
omitting native completion each produce the intended assertion failure in both cases.
Four named mutations therefore yield eight intended assertion failures on .NET 9;
all mutations are restored. Restored focused cases pass on both frameworks.

All library targets and tests build. Both expanded full available suites pass
**622 cases, zero failures/skips**, retaining all 620 prior case/outcome pairs and
adding exactly the two new cases. All 1,171 non-generated production C# files match
pre-batch hashes. Adopted test and both observing helpers match the approved drafts;
the reused external reader and approved fixture fragment are unchanged. No production
correction, public contract change or dependency change was needed.

Two harness corrections are preserved in the evidence: the reader-bypass validator
initially expected line 116 instead of the actual reader-count assertion at line 111;
raw failures were validated without changing or rerunning the test. The first .NET 9
regression filter omitted the new class and passed only the 620 baseline cases.
The exact-count check caught this omission; the filter was expanded and the complete
622-case suite passed. Neither correction changed test expectations or masked a failure.

Evidence: `/private/tmp/brighter-4170-asb-session-expiry/`, including raw TRXs, initial
and restored focused results, mutations.json, the original validator report,
baseline-only-net9.0 artifacts, expanded regressions and source verification.
Nothing committed or pushed.

This closes the narrow native emulator renewal/mapping/completion gate. It does not
prove the causal link event behind the earlier disposed-receiver exceptions, resolve
the intermittent in-memory session-lock failure, or establish hosted Service Bus
behavior. Session contention, renewal failure recovery, other session/cancellation
paths, eight DLQ-blocked cases, 52 unavailable hosted classes and broader transport
and release gates remain open. Overall production readiness is not established.

The isolated emulator containers and network were removed after validation.

## Native session renewal after partial settlement — validated

The [approved queue/topic batch](review/asb-session-partial-settlement/README.md)
receives two native deliveries sequentially on the same session receiver, with both
initially unsettled. After acknowledging the first, the native session remains open.
The test records SessionLockedUntil after that settlement, waits eight seconds with
a configured five-second lock interval, verifies the recorded expiry has elapsed
and the current native expiry has advanced, then completes the second delivery.
Both messages preserve custom/native mapping and distinct lock/sequence metadata;
reader count remains two and final native peek confirms removal.

All four initial cases pass without production changes. Five named mutations on
.NET 9 produce ten intended assertion failures: stopping renewal after the first
settlement fails expiry advancement (line 178); closing the session after the first
acknowledgement fails the immediate open-receiver assertion (line 168); bypassing the
reader fails reader count (line 110); replacing custom body fails equality (line 115);
omitting completion fails final native peek (line 197). Both queue/topic cases detect
each defect. All mutations are restored, and restored focused cases pass on both
frameworks without test changes or harness corrections.

All library targets and tests build. Both full available suites pass **624 cases,
zero failures/skips**, retaining all 622 prior case/outcome pairs and adding exactly
the two new cases. All 1,171 non-generated production C# files match pre-batch hashes.
The adopted test matches the approved draft; all three reused helpers are unchanged,
and the approved PT5S session entity fragment is present in the isolated configuration.
No production correction, public API change or dependency change was required.

Evidence: `/private/tmp/brighter-4170-asb-partial-settlement/`, including initial and
restored focused results, ten mutation failures, both expanded regressions, exact
baseline comparisons and source verification. Nothing committed or pushed.

This closes the narrow native emulator partial-acknowledgement renewal gate for two
deliveries received sequentially on one session. It does not cover concurrent calls,
native multi-message batch sizing, abandonment/rejection, competing consumers, renewal
failure recovery or hosted Service Bus. The intermittent in-memory session-lock
finding, eight DLQ-blocked cases, 52 unavailable hosted classes and broader transport
and release gates remain open. Overall production readiness is not established.

The isolated emulator containers and network were removed after validation.

## Native session renewal after partial abandonment — validated

The [approved queue/topic batch](review/asb-session-partial-abandonment/README.md)
receives two native deliveries sequentially on the same session receiver. After
abandoning the first, it verifies that the session stays open and its native expiry
advances past the value recorded after abandonment while the second remains unsettled.
Only then does it receive the first message again, requiring native delivery count
two, a fresh lock token, original sequence and preserved native/custom mapping.
Both deliveries are acknowledged, reader count remains three, and final native peek
confirms removal. Successful NackAsync return alone cannot satisfy the test.

All four initial cases pass without production changes. Six named mutations on
.NET 9 produce twelve intended assertion failures, two for each defect: stopping
renewal fails expiry advancement (line 178); closing after abandonment fails the
immediate open-receiver assertion (line 168); omitting native abandonment fails
redelivery Assert.Single (line 183); bypassing the reader fails reader count (line
110); replacing custom body fails equality (line 115); omitting completion fails
final native peek (line 226). All mutations are restored. Restored focused cases
pass on both frameworks; no test changes or harness corrections were needed.

All library targets and tests build. Both full available suites pass **626 cases,
zero failures/skips**, retaining all 624 prior case/outcome pairs and adding exactly
the two new cases. All 1,171 non-generated production C# files match pre-batch hashes.
The adopted test matches the approved draft, all three reused helpers are unchanged,
and the approved PT5S session entity fragment is present in the isolated configuration.
No production correction, public API change or dependency change was required.

Evidence: `/private/tmp/brighter-4170-asb-partial-abandonment/`, including initial and
restored focused results, twelve mutation failures, both expanded regressions,
exact baseline comparisons and source verification. Nothing committed or pushed.

This closes the narrow native emulator partial-abandonment/renewal/redelivery gate.
It does not validate abandonment failure recovery, rejection/DLQ, concurrent calls,
competing consumers, native multi-message batch sizing or hosted Service Bus. The
intermittent in-memory session-lock finding, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader transport/release gates remain open. Overall production
readiness is not established.

The isolated emulator containers and network were removed after validation.

## Native session renewal-budget expiry and recovery — validated

The [approved queue/topic batch](review/asb-session-renewal-budget-recovery/README.md)
observes one native session delivery with a five-second lock and six-second renewal
budget. After thirteen seconds, native SessionLockedUntil has advanced beyond its
original value and is in the past: renewal occurred and then stopped. The same
consumer reacquires the session through a different native receiver and obtains the
same message with delivery count two, fresh lock, original sequence and preserved
native/custom mapping. Reader count remains two. Acknowledgement and native peek
confirm removal.

The approved recovery contract permits immediate recovery or one ChannelFailureException
followed by one successful receive, preserving the gateway's existing replacement
boundary. A green result alone does not distinguish those permitted paths; no claim
of transparent single-call recovery is made.

All four initial cases pass without production changes. Five named mutations on
.NET 9 produce ten intended assertion failures: ignoring the budget fails native
expiry (line 134); disabling renewal fails initial expiry advancement (line 132);
clearing the reader during receiver reset fails recovery reader count (line 153);
replacing custom body fails equality (line 114); omitting completion fails final
native peek (line 184). Both cases detect every defect. All mutations are restored;
restored focused cases pass on both frameworks. No test or harness amendments were
needed during execution.

All library targets and tests build. Both full available suites pass **628 cases,
zero failures/skips**, retaining all 626 prior case/outcome pairs and adding exactly
the two new cases. All 1,171 non-generated production C# files match pre-batch hashes.
The adopted test matches the approved draft, all three reused helpers are unchanged,
and the approved native entity fragment is present. No production correction,
public API change or dependency change was required.

Evidence: `/private/tmp/brighter-4170-asb-budget-recovery/`, including initial/restored
focused runs, ten mutation failures, both expanded regressions, exact baseline
comparisons and source verification. Nothing committed or pushed.

This closes the narrow native emulator budget-expiry and bounded same-consumer
recovery gate. It does not validate arbitrary renewal failures, network partitions,
contention, concurrent calls, abandonment failure recovery, DLQ or hosted Service Bus.
The intermittent in-memory session-lock finding, eight DLQ-blocked cases, 52 unavailable
hosted classes and broader transport/release gates remain open. Overall production
readiness is not established.

The isolated emulator containers and network were removed after validation.

## GCP rollout source audit

The initial [source audit](gcp-rollout-audit.md) found an unimplemented wire-mapping rollout:
publication/subscription selection is absent and producer, pull and stream paths call
the static parser directly. Pull AckId and streaming callback receipt ownership must
be preserved separately. The audit records implementation order and the mixed Pub/Sub,
Firestore and Spanner test-project boundary. No tests ran or production code changed;
ASB remains at 628 validated cases per framework. Overall readiness remains open.


## GCP native publication writer selection

The [writer batch](review/gcp-native-wire-writer/README.md) reproduces ignored custom
mapping in six sync/async/bulk × ordering cases on each framework. Publication-level
selection, a default parser adapter and a shared producer mapping step fix both send
paths. The gateway retains control of the native ordering key.

All six initial, restored and final CI-marked cases pass on net9.0 and net10.0.
Six named production mutations produce 30 intended assertion failures on net10.0;
all are restored. The corrected review sources match their adopted copies.

Combined selected regressions pass 228 cases with 30 existing skips per framework.
Exact comparison retains all 252 pre-change outcomes and adds only the six writer
cases. Main runs pass 220 cases; comparison catches eight streaming purge cases
omitted by a namespace filter. Those eight pass in separate runs on both frameworks.
The initial comparison failure and both filters are preserved; no missing case is
silently dropped from the final comparison.

Initial missing SDK assemblies are restored without dependency edits. Two draft
references to Header.Id are corrected to Header.MessageId. The new emulator-only
trait and a two-line CI change include these cases in the emulator job and exclude
them from hosted execution. Final builds pass; CI selection passes six cases on
each framework and the hosted exclusion selects none.

Only two existing production files change; one adapter, one test and one helper are
added. The remaining 6,458 pre-existing production/test C# files are unchanged.
Evidence is retained in `/private/tmp/brighter-4170-gcp-writer/`. The isolated native
emulator is removed after validation. Nothing is committed or pushed.

This is a selected Pub/Sub regression result, not a whole-project pass. Full stream
conformance, Firestore, Spanner, Cloud Storage and hosted-only suites remain unrun.
Reader selection, writer failure recovery, concurrency/isolation, scheduler paths and
broader transport/release gates remain open. Overall production readiness is not
established; the separate ASB baseline remains 628 per framework.


## GCP native pull reader selection

The [pull reader batch](review/gcp-native-pull-reader/README.md) adds subscription-level
PullWireMessageReader selection through both public consumer factory paths. A default
adapter retains parser behavior. Both receive paths map once, then attach the native
AckId and resolve delivery counts from the native envelope. An internal constructor
overload carries the selection; the existing public constructor remains unchanged.

Both initial RED runs fail because the reader is never invoked. Initial and restored
implementation runs pass both sync/async cases on net9.0 and net10.0. Native observation
confirms requeue before a 30-second lease expires and absence after acknowledgement
past the ten-second deadline, with a fresh marker as the positive observation control.
The unavailable native counter retains wire HandledCount=7 over the reader's 777.

Eight named production mutations produce 14 intended failures on net10.0. Native
release omission fails the pre-expiry observation; acknowledgement omission exposes
the original native ID after expiry. All mutations are restored. The test and helper
remain byte-for-byte identical to their reviewed drafts.

Each complete selected Pub/Sub regression run passes 230 cases with 30 existing skips.
Exact comparison retains all 258 prior outcomes and adds only the two reader cases.
All eight streaming purge cases are included. Regressions use independent saved green
binaries whose hashes remain unchanged throughout mutation execution. The only change
after saving those binaries removes an obsolete parser comment; restored focused
checks and final builds pass.

The emulator CI filter includes the new category, and the hosted exclusion selects
none. Four existing production files change and one default adapter is added; the
other 6,459 pre-existing production/test C# files remain unchanged. Evidence is in
`/private/tmp/brighter-4170-gcp-reader/`, including raw runs, mutations, exact outcome
comparisons, source hashes and the scoped workflow delta. The isolated emulator is
removed after validation. Nothing is committed or pushed.

This is not a whole-project pass. Full streaming conformance, Firestore, Spanner,
Cloud Storage and hosted-only suites remain unrun; Fragile=CI cases remain excluded.
Streaming mapping, failed/null readers, mixed batches, isolation/concurrency,
populated counters, rejection metadata and scheduler paths remain open. Overall
production readiness is not established. The separate ASB baseline remains 628.


## GCP native streaming reader selection

The [stream reader batch](review/gcp-native-stream-reader/README.md) adds
StreamWireMessageReader selection, a default adapter and factory wiring while
preserving the public consumer constructor. Receive maps once, then restores the
actual native callback receipt and native delivery-count policy. Existing
acknowledgement, nack, requeue and disposal implementations are unchanged.

Four unchanged reviewed cases fail at reader invocation before integration, then
pass initially and after mutation restoration on both target frameworks. The tests
observe native redelivery, SDK callback Ack/Nack completion and continued delivery
of a fresh marker. They do not claim durable acknowledgement or hosted behavior.
Eight production mutations produce 28 intended failures, including bounded failures
when nack, requeue or acknowledgement is omitted. All mutations are restored.

The established Pub/Sub selection passes 234 cases with 30 existing skips on each
framework, retaining all 260 preceding outcomes exactly. Expanded stream coverage
adds 163 outcomes, overlapping eight purge cases. Combined unique final results:
net9.0 364 passes and 55 skips; net10.0 363 passes, one failure and 55 skips.

The pre-implementation net10.0 stream baseline has four failures: ordered nack
redelivery, sync/async disposal with buffered redelivery, and sync disposal with an
unsettled message. The final run retains the async buffered-redelivery failure;
no previously passing stream case regresses. Prompt disposal succeeds, but a
replacement channel receives an empty message within its twenty-second window
instead of the original native ID (test line 87). The other baseline failures
passing later does not mean they are fixed. These lifecycle failures remain open
release blockers and require a separate diagnosis.

The new category is included in emulator CI and excluded from hosted execution;
selection checks pass four cases per framework and hosted selection finds none.
Final normal builds pass with existing warnings. Four existing production files
change, one adapter is added and 6,462 other existing C# source files are unchanged.
The test and helper match their approved drafts exactly. Regression binaries are
preserved independently of mutation builds. Evidence, filters, TRX and exact outcome
comparison are in `/private/tmp/brighter-4170-gcp-stream-reader/`.

No dependencies are changed; nothing is committed or pushed. This is not a
whole-project pass. Fragile=CI, Firestore, Spanner, Cloud Storage and hosted-only
checks remain excluded. Mapping exceptions/nulls, isolation/concurrency, populated
counters, rejection and scheduler paths, remaining transports and the existing
stream lifecycle failures prevent a production-ready claim.

## GCP streaming reader failure recovery — scoped validation

The [approved eight-case batch](review/gcp-stream-reader-failures/README.md)
reproduces throwing/null reader failures on both frameworks and passes after the
stream consumer creates an unacceptable message from native identity, topic and
raw bytes. Gateway-owned callback receipt and handled count remain available for
explicit nack/requeue and acknowledgement. Mapping recovery and subsequent native
delivery pass without changing the approved test or helper.

Thirteen temporary production mutations produce 96 intended assertion failures
on net10.0. All mutations are restored; eight focused cases pass again on each
framework. Library builds pass for netstandard2.0, net8.0, net9.0 and net10.0.
The existing emulator CI category already includes the new cases; remote CI is unrun.

The complete affected Pub/Sub selection retains all 419 preceding case identities
and adds eight passing cases. Results are **370 passes, two failures and 55 skips
on net9.0**, and **371 passes, one failure and 55 skips on net10.0**. Failures concern
ordered async nack/redelivery and async unsettled disposal on net9.0, and sync
unsettled disposal on net10.0. All three passed in the preceding saved full results.
The earlier net10.0 buffered-redelivery failure passes this time, without a related fix.

All three failing cases pass three focused pre-change binary repetitions and one
current-build recheck. The full-suite failures are not reproduced by those controls;
their causes remain unresolved and the change is not declared regression-free.
Only the stream consumer changes among existing C# files; 6,470 other hashed files
are unchanged. Test/helper match their approved drafts exactly. No dependency or
public-constructor changes, commits or pushes are included.

Evidence: `/private/tmp/brighter-4170-gcp-stream-failures/`, including raw TRX,
mutation assertions, control runs, exact case comparisons and source verification.
The two emulators created for this batch are removed. This closes the scoped
stream mapping-recovery behavior. Pull recovery, lifecycle findings, unavailable
hosted/database coverage and broader feature gates remain open.

## GCP full pre-change lifecycle controls

The complete combined Pub/Sub filter was run against the preserved binaries from
before the streaming reader-recovery correction, on separate isolated emulators.
No production or compiled test source changed during this investigation.

Results: net9.0 **363 passes, one failure, 55 skips**; net10.0 **364 passes,
zero failures, 55 skips**. All 419 prior case identities are present.
The net9.0 ordered async nack/redelivery test fails at the same missing-nacked-ID
assertion as the current build. This establishes that this specific failure
predates the reader-recovery correction, without identifying its cause.

Neither full control reproduces the current build's unsettled-disposal failures.
Those remain unresolved; the passing control does not prove a regression or
disprove one. Existing buffered tests do not observe actual callback arrival,
and async disposal tests do not await the disposal task's result. The sync disposal
tests do propagate faults through Task.Wait.

The [next eight diagnostic cases](review/gcp-stream-shutdown-observation/README.md)
observe native shutdown nack completion, actual buffered arrival and disposal
task completion, then compare native-pull and replacement-stream recovery.
They are outside compiled projects, unbuilt and unrun pending review. No speculative
shutdown correction, timeout relaxation or SDK update is made.

Evidence: `/private/tmp/brighter-4170-gcp-lifecycle/`, including full-control TRX/logs,
parsed failures and pinned SDK source inspected for the diagnostic boundary.
Feature-wide production readiness remains open.

## GCP native shutdown observation results

The approved [eight shutdown diagnostics](review/gcp-stream-shutdown-observation/README.md)
are adopted. A compile-only correction qualifies `Grpc.Net.Client.GrpcChannelOptions`;
no behavior, bounds or assertions change. The focused cases pass on both frameworks
before mutations and again with production sources restored. Native traces observe
actual buffered callbacks, successful shutdown nacks, and recovery through independent
native pull or a replacement stream. These tests intentionally bypass the factory.

Four approved production mutations produce 28 failures. Omitting shutdown produces
eight missing-nack assertions; omitting callback cancellation produces eight observed
disposal timeouts; omitting consumer Accept produces four replacement-callback timeouts
while four native-pull cases pass. Premature native Ack produces four expected buffered
arrival timeouts and four earlier missing-nack assertions, despite successful unwanted
native Acknowledge requests in their traces. Those last four outcomes differ from the
approved assertion expectation. The [assertion-order amendment](review/gcp-stream-shutdown-ack-observation/README.md)
is a review draft only; the compiled assertions remain unchanged. The task is not
marked complete pending that review. Every mutation is restored byte for byte and
followed by eight passing net10.0 cases; the final net9.0 focus also passes eight.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 380 | 0 | 55 | 435 |
| net10.0 | 379 | 1 | 55 | 435 |

Both runs retain all 427 prior case identities and add exactly eight passing cases.
The net10.0 failure is the existing async unsettled-disposal case, at the replacement
message ID assertion on line 83: an empty message arrives from the bounded receive.
The same case failed on net9.0 in the preceding reader-recovery batch. This run
confirms the finding remains open; it does not establish its cause. Passing ordered
nack and buffered-disposal cases do not close earlier intermittent failures.

The GCP library builds netstandard2.0, net8.0, net9.0 and net10.0 with zero warnings
or errors. The emulator CI filter includes GcpNativeStreamLifecycle; hosted runs
already exclude its Requires=PubSubEmulator trait. Hosted/Spanner/remote CI coverage
is not claimed. All 6,472 source hashes are compared: the test's type qualification
is the only post-adoption C# difference, and all existing C# files are unchanged.
The review copy and compiled test match. No production fix, dependency update or
timeout relaxation is introduced. All three batch-owned emulators are removed.

Evidence: `/private/tmp/brighter-4170-gcp-shutdown-observation/` contains full TRXs,
native traces, emulator logs, builds, four mutation/restoration pairs and
`final-verification.json`. Feature-wide production readiness remains open.

## GCP shutdown assertion-order amendment completed

The approved [assertion-order amendment](review/gcp-stream-shutdown-ack-observation/README.md)
is adopted exactly. The only compiled C# change moves the existing forbidden native
Ack assertion before the shutdown-nack assertion. All operations, cases and bounds
remain unchanged. The amended premature-Ack mutation produces four forbidden-Ack
failures at line 157 and four buffered-arrival failures at line 136. The other three
approved mutations are rechecked: eight missing-shutdown-nack assertions, eight
disposal timeout assertions and four replacement-acknowledgement timeouts, with
four native-pull cases passing in the last mutation. All **28** failures now reach
their intended assertions. Each mutation is byte-restored and followed by eight
passing net10.0 cases; final focused runs pass eight on both frameworks.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 379 | 1 | 55 | 435 |
| net10.0 | 379 | 1 | 55 | 435 |

All 435 prior case identities remain present, including eight passing diagnostics.
The net9.0 ordered async two-message nack case fails at line 158; the net10.0 sync
counterpart fails at line 152. Both are the first final assertion, checking the
**nacked message's redelivery**, not the other queued message. The sync failure is
newly observed in this batch and its cause is not established. Disposal tests pass
this run; that nonreproduction does not close the prior intermittent findings.

A source-line audit also corrects the earlier full pre-change control description:
its line 158 failure checks the nacked message's ID. Earlier wording that called it
a missing second/other message was incorrect. The generated source hashes match the
saved recovery-batch snapshot, and the preserved control TRX identifies line 158.
The baseline failure still demonstrates that the async ordering failure predates
the reader-recovery change; the correction changes its interpretation, not its
outcome or timing.

The GCP library builds all four targets with zero warnings/errors. All 6,472 existing
src/tests C# files are compared: only the approved test assertion order differs,
and all production mutations are byte-for-byte restored. The compiled test exactly
matches the approved amendment. No production correction, dependency update or
bound increase is introduced. All three emulators created for this batch are removed.

Evidence: `/private/tmp/brighter-4170-gcp-shutdown-ack-observation/`, including mutation
and restored TRXs, full-suite results, native traces, emulator logs, library builds,
source snapshots and `final-verification.json`. Earlier artifacts are preserved.
The scoped observation amendment is complete; feature-wide readiness remains open.

The [next factory shutdown draft](review/gcp-factory-stream-shutdown/README.md) covers
real factory-created channels, stopped-cache replacement and SDK-owned connections
with one/two native clients. Its eight cases and four named mutations are outside
compiled projects, unbuilt and unrun pending review. It does not claim to explain or
cover the ordering failure, and changes no existing test or production code.

## GCP factory shutdown characterization completed

The approved [eight factory shutdown cases](review/gcp-factory-stream-shutdown/README.md)
now run through the public channel factory with one/two configured native clients,
SDK-owned stream connections and same-subscription cache replacement. The initial
net9.0 run fails during subscription construction because the draft omitted the
required request type. Adding the existing TestDoubles.MyCommand type corrects that
fixture setup only; it is not behavioral RED or a production correction. The original
approved file is preserved, and no assertion, bound or operation sequence changes.

All eight configured cases pass on both frameworks. Four approved production
mutations produce **24 intended assertion failures**: eight pending callbacks when
native shutdown is omitted, eight disposal timeouts when cancellation is removed,
four missing second factory builds when stopped-cache eviction is omitted, and four
replacement acknowledgement timeouts when Accept is omitted. Native-pull branches
pass the last two mutations. Each mutation is restored byte for byte and followed
by eight passing net10.0 cases; final focus passes eight on both frameworks.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 388 | 0 | 55 | 443 |
| net10.0 | 388 | 0 | 55 | 443 |

Both matrices retain every one of the 435 prior case identities and add exactly
eight passing cases. The earlier ordered-nack and disposal failures are not
reproduced. Their cause remains unresolved; no production code changes in this
batch, so these passes do not establish a fix for the intermittent findings.

All four GCP library targets build with zero warnings/errors. The test's diagnostic
MessageType log emits the existing property's CS0618 deprecation warning in test
builds. All 6,472 pre-existing src/tests C# files match their saved hashes. The only
new compiled source is the approved test plus its required request-type configuration;
it matches the review copy. Existing CI categories select it for the emulator and
exclude it from hosted runs. No hosted/Spanner/remote CI coverage is claimed. All
three isolated emulators created for this batch are removed; no commit is made.

Evidence: `/private/tmp/brighter-4170-gcp-factory-shutdown/`, including initial setup
failure TRXs, configured green runs, native traces, four mutation/restoration pairs,
full-suite results, emulator logs, library builds and `final-verification.json`.
This closes the scoped factory characterization, not feature-wide readiness.

At the factory-shutdown checkpoint, source inspection identified the next mapping
gap: unprotected pull-reader throw/null results. The [eight recovery cases](review/gcp-pull-reader-failures/README.md)
were then drafted outside compiled projects for review. The approved execution and
correction are recorded in the following section; ordering, streaming lifecycle and
mixed batch findings remain separate gates.

## GCP pull reader recovery completed

The approved eight pull-reader recovery cases and configured reader were adopted
unchanged. Before implementation, all eight fail at the initial Assert.Null(error)
on both frameworks: four escaping reader exceptions and four null dereferences.
The pull consumer now catches thrown/null reader results, logs the mapping failure,
constructs an unacceptable message from native ID/topic/raw bytes without reparsing
invalid MIME, and attaches the native receipt and delivery count. The caller retains
settlement ownership and the configured reader remains selected after recovery.
Public Receive/ReceiveAsync remarks describe that behavior. No Nack, timeout, SDK,
factory, streaming or scheduler policy changes are included.

All eight cases pass initially after correction and in final restored runs on both
frameworks. Eight named production mutations produce **64 intended assertion
failures**, covering the unprotected reader, invalid-parser fallback, reader bypass,
fabricated receipt, premature acknowledgement, missing release, missing acknowledgement
and loss of the configured reader after failure. Each is restored exactly and followed
by eight passing net10.0 cases. The first auto-ack mutant did not compile because
MessageHeader.MessageType is obsolete and production warnings are errors. It was
replaced with a local failure flag in the temporary mutant; tests and intended behavior
were unchanged. That compile error is not counted as RED, and its log is retained.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 395 | 1 | 55 | 451 |
| net10.0 | 395 | 1 | 55 | 451 |

Every one of the 443 prior case identities is retained, with exactly eight new passing
cases per framework. Observed full-run failures:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

The ordered nack failures are the previously recorded missing nacked-message IDs
at the first final Assert.Contains (generated Proactor line 158 / Reactor line 152).
These failures predate this pull-only correction; the async failure was also observed
in the preserved pre-stream-recovery control. Their causes remain unresolved.
These remain release blockers; a passing focus run is not substituted for the
full result. The baseline comparison records changed outcomes explicitly rather
than claiming every previous pass is retained.

All four GCP library targets build with zero warnings/errors. Of 6,473 pre-existing
src/tests C# files, 6,472 remain byte-identical; only GcpPullMessageConsumer changes.
The two added compiled files exactly match the approved review copies. Existing CI
traits include native emulator execution and exclude hosted runs. No hosted,
Firestore, Spanner or remote-CI validation is claimed.

Evidence: `/private/tmp/brighter-4170-gcp-pull-failures/`, including initial RED,
green, mutation/restoration and full TRXs/JSON, logs, before/after source copies,
source hashes, all-target build and `final-verification.json`. The first full runner
launch encountered a Docker name collision before any tests ran; its logs are kept
separately and the runner names were corrected. All three owned emulators are
removed after validation; unrelated containers are untouched. No commit is made.

At the pull-recovery checkpoint, the [writer recovery draft](review/gcp-writer-failure-recovery/README.md)
was outside compiled projects pending review. Its approved execution is recorded in
the subsequent writer-recovery section. It covers sync/async/single-
message bulk throw/null behavior, no publication on mapping failure, source preservation,
real publication after explicit retry and continued custom mapping. Mixed batches,
concurrency, rejection/scheduler selection, historical lifecycle failures and the
remaining gateway integrations still prevent feature-wide production readiness.

## GCP writer failure recovery completed

The approved six cases and writer were adopted unchanged. Initial runs on both
frameworks reproduce three null-result failures at the InvalidOperationException
type assertion: the actual exception is NullReferenceException. The three throwing
cases already pass. An explicit null guard in GcpMessageProducer.CreateWireMessage
now produces the reviewed error before native publication. Existing thrown exceptions
retain their identity. Public send XML remarks describe mapping failure and the
possibility that earlier bulk messages have already been published.

All six corrected cases pass on both frameworks. Six approved production mutations
produce **30 intended assertion failures**: three removed null guards, six configured
writer bypasses, three changed exception identities, six fallback publications,
six lost ordering keys and six omitted native publications. Each mutation is restored
exactly and followed by six passing net10.0 cases; both final focused suites pass six.
The tests prove native publication absence on mapping failure, explicit same-producer
retry, fresh-message delivery, source preservation and gateway ordering control.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 402 | 0 | 55 | 457 |
| net10.0 | 401 | 1 | 55 | 457 |

All 451 prior case identities remain, with exactly six new passing cases per framework.
The comparison records existing outcome changes explicitly. Full-run findings:

- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

The first full launch raced emulator startup, producing connection-refused setup
failures. Both invalid runs were stopped and their logs/runner exit records archived;
they do not establish behavioral RED or a valid full matrix. Fresh emulators passed
an explicit TCP-listener readiness probe before the complete runs above. Source and
tests were unchanged for the rerun.

All four library targets build with zero warnings/errors. Of 6,475 pre-existing
src/tests C# files, 6,474 remain byte-identical; only GcpMessageProducer changes.
The two new compiled sources exactly match the approved review copies. Existing CI
traits include emulator execution and exclude hosted runs. No hosted, Firestore,
Spanner or remote-CI coverage is claimed. All batch-owned emulators are removed;
unrelated containers remain untouched, and no commit is made.

Evidence: `/private/tmp/brighter-4170-gcp-writer-failures/` contains RED/green,
mutation/restoration and full TRXs/JSON, archived startup failures, readiness evidence,
emulator logs, source snapshots, all-target builds and `final-verification.json`.

At the writer-recovery checkpoint, the [send-cancellation draft](review/gcp-send-cancellation/README.md)
was pending review. That batch has since been approved and validated; see its
execution record for the correction and remaining lifecycle failures. It covers cancellation before
entry or during mapping for immediate async and single-message bulk sends. Native
in-flight cancellation, partial batches, reader isolation/concurrency, scheduler and
rejection mapping, historical lifecycle failures, hosted checks and the remaining
five gateway integrations still prevent feature-wide production readiness.

## GCP send cancellation before native submission completed

The approved four cases and configured writer were adopted unchanged. All four
initially fail on both frameworks at the OperationCanceledException assertion:
the send completes without cancellation. Immediate individual sends now check the
caller token before mapping and immediately before native submission. Bulk sends
apply those checks per message. Cancellation retains the caller's token and prevents
submission at those boundaries; the active-token retry uses the same producer and
selected writer. Once submitted, the existing native publication await remains.
Public XML remarks and exception tags state that cancellation cannot recall an
already submitted message. Scheduler and batch-creation behavior remain unchanged.

All four corrected cases pass on both frameworks. Five named production mutations
produce **16 intended assertion failures**: two removed entry checks, two removed
post-mapping checks, four lost caller tokens, four publications before cancellation
and four omitted recovery publications. Each mutation is restored exactly and followed
by four passing net10.0 cases; both final focused suites pass four. Tests observe no
native publication for cancellation before entry or during mapping, then real retry
and fresh-message publication with preserved source data and native ordering.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 405 | 1 | 55 | 461 |
| net10.0 | 404 | 2 | 55 | 461 |

All 457 prior case identities remain, with exactly four new passing cases per framework.
Existing outcome changes are recorded explicitly. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.Stream.GcpStreamDisposeWithUnsettledMessageAsyncTests.When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_async_should_return_promptly — Assert.Equal() Failure: Values differ
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpFactoryStreamShutdownRecoveryTests.When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(useAsync: False, clientCount: 2, nativeRecovery: False) — Assert.Equal() Failure: Values differ
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async — Assert.Contains() Failure: Item not found in set

The factory shutdown failure is newly observed in this batch, not established as a
pre-existing failure of that case. Its net10.0 sync/two-native-client/replacement-channel
trace records successful disposal, a cancelled callback and a second factory build,
then MT_NONE after the 20-second receive window (test line 155). It uses native
publication and the unchanged streaming path, outside the producer method changed
here. Three focused runs of the saved pre-change binaries and one current run each
pass all eight factory cases. This non-reproduction does not resolve the full-suite
finding or establish its cause. No lifecycle fix or wider bound is introduced.
The 322 saved baseline binary artifacts retain their hashes, and the baseline producer
source matches the pre-batch snapshot exactly. The async ordered-nack failure remains
the previously recorded missing nacked-message ID at generated Proactor line 158.
The net9.0 async unsettled-disposal case also reproduces its previously recorded
empty replacement receive at line 83; its cause remains open.

All four library targets build with zero warnings/errors. Of 6,477 pre-existing
src/tests C# files, 6,476 remain byte-identical; only GcpMessageProducer changes.
The two added compiled sources exactly match their approved review copies. Both full
runs start only after their fresh emulator listener is ready. Existing CI traits
include emulator execution and exclude hosted runs; no hosted, Firestore, Spanner
or remote-CI coverage is claimed. All three batch-owned emulators are removed;
unrelated containers are untouched, and no commit is made.

Evidence: `/private/tmp/brighter-4170-gcp-send-cancellation/` contains RED/green,
mutation/restoration and full TRXs/JSON, native lifecycle traces and focused controls,
baseline artifact hashes, readiness evidence, emulator logs, source snapshots,
all-target build, cleanup record and `final-verification.json`.

At the send-cancellation checkpoint, the [partial-bulk cancellation draft](review/gcp-partial-bulk-cancellation/README.md)
was pending review. It has since been approved and characterized below. It covers a confirmed
first publication followed by cancellation before or during the second mapping,
then explicit caller retry of only the remainder. This adds no general progress API
or duplicate-free whole-batch retry guarantee. Other partial batches, in-flight
cancellation, reader concurrency, scheduler/rejection mapping, hosted checks,
historical lifecycle findings and five remaining gateway integrations remain open.

## GCP cancellation after partial bulk progress characterized

The [approved two cases](review/gcp-partial-bulk-cancellation/README.md) are adopted
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

At the partial-bulk cancellation checkpoint, the [writer-failure draft](review/gcp-partial-bulk-writer-failures/README.md)
was pending review. It has since been approved and characterized below. It covers configured
writer throw/null after confirmed progress. Other partial-batch and concurrency
paths, in-flight cancellation, scheduler/rejection mapping, historical lifecycle
findings, hosted checks and five remaining gateway integrations still prevent
feature-wide production readiness.

## GCP writer failure after partial bulk progress characterized

The [approved two cases](review/gcp-partial-bulk-writer-failures/README.md) are adopted
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

At the partial-bulk writer-failure checkpoint, the [factory native shutdown observation amendment](review/gcp-factory-native-shutdown-observation/README.md)
was pending review. It has since been approved and characterized below. It adds native nack/ack
RPC evidence to the existing eight factory cases through the public SDK settings
hook, retaining native connection ownership, case identities, assertions and bounds.
Other batch/concurrency paths, scheduler/rejection mapping, hosted checks, historical
lifecycle findings and five remaining gateway integrations still block feature-wide
production readiness.

## GCP factory native shutdown observation characterized

The [approved amendment](review/gcp-factory-native-shutdown-observation/README.md)
replaces the existing factory test unchanged from review, retaining all eight case
identities. All eight pass initially and after restoration on net9.0/net10.0 without
a production correction. The public SDK ApiSettings.Interceptor hook captures native
unary RPCs while the factory-created SDK clients retain their own connections.

Each case observes a successful deadline-zero ModifyAckDeadline request for the
exact subscription and first factory generation, started after disposal began and
containing acknowledgement IDs. No completed native acknowledgement precedes recovery.
The original disposal, callback, recovery, identity/content, replacement-build and
receipt-acknowledgement assertions remain. SDK versions and all time bounds remain
unchanged. Native acknowledgement IDs are recorded, but this amendment does not capture
streaming delivery responses to independently associate each acknowledgement ID with
a native message ID. Every scenario publishes only one message to its unique subscription.

Four production-only mutations produce **24 intended assertion failures**: omitting
native shutdown fails all eight native-nack assertions; removing callback cancellation
causes eight disposal timeouts; omitting stopped-client eviction causes four missing
replacement builds; omitting Accept causes four replacement-acknowledgement timeouts.
Each exact restoration returns eight net10.0 passes; final focused suites pass eight
on both frameworks. No setup/build failure is counted as behavioral RED.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 408 | 2 | 55 | 465 |
| net10.0 | 410 | 0 | 55 | 465 |

All 465 prior case identities remain unchanged, including eight passing amended cases
on each framework. No new cases are added. Outcome changes are recorded in
final-verification.json. Full-run findings:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.Stream.GcpStreamDisposeWithUnsettledMessageAsyncTests.When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_async_should_return_promptly — Assert.Equal() Failure: Values differ
- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second — Assert.Contains() Failure: Item not found in set

The net9.0 failures are the missing nacked-message ID in generated Reactor line 152
and the empty replacement receive in async unsettled disposal at line 83. Both have
been observed previously. The net10.0 matrix is green in this run; it does not close
those intermittent findings.

The historical ordered-nack, unsettled/buffered disposal and sync/two-client factory
replacement findings remain open. Successful focused observation and later passing
recovery do not establish their cause or a fix. Native RPC completion remains distinct
from independently observed redelivery.

Of 6,481 pre-existing src/tests C# files, 6,480 remain byte-identical; only the approved
factory test changes. All production sources and reused helpers are unchanged, and
no compiled source is added. All four library targets build without warnings/errors.
Test-project builds retain warnings. Full runs wait for fresh emulator readiness.
Existing CI traits select emulator execution and exclude hosted execution; no hosted,
Firestore, Spanner or remote-CI result is claimed. All three batch-owned emulators are
removed; unrelated containers remain untouched. Nothing is committed or published.

Evidence: `/private/tmp/brighter-4170-gcp-factory-native-shutdown/` contains
initial/restored, mutation and full TRXs/JSON; successful native nack observations;
original test and source snapshots; readiness/emulator logs; library build;
cleanup.json and final-verification.json.

At the factory-observation checkpoint, the [ordered-nack observation draft](review/gcp-ordered-nack-native-observation/README.md)
was outside compiled projects pending review. Its approved execution and unresolved failure are recorded below. Six sync/async × default/
one/two-client cases correlate native streaming delivery acknowledgement IDs with
nack RPCs while preserving immediate receive/ack sequencing. Three named mutations
are specified. Other lifecycle, concurrency, scheduler/rejection, hosted and gateway
rollout requirements remain open; full-feature production readiness is not established.


## GCP ordered-nack observation: native failure reproduced

The [approved six cases](review/gcp-ordered-nack-native-observation/README.md) and two
passive helpers are adopted unchanged. Initial runs pass six cases on both frameworks.
All three approved production mutations produce six intended missing-native-nack
assertions each: **18 intended failures**. Exact production restoration is verified.
The first two restoration runs pass six cases each. The third has **five passes and
one failure**: async/default SDK clients successfully nack the observed first native
ack ID, but that message is not redelivered within thirty seconds. The original
receipt completes with Nack and the other message is received/acknowledged.

Raw StreamingPull observation contains no second native delivery of the missing
message. The full net9.0 matrix independently reproduces the same diagnostic failure.
Both failing traces include an overlapping lease extension and second-message ack;
completion timestamps do not establish the server's internal processing order.
Final focused runs pass six on each framework, but do not clear either captured failure.
**The restoration gate remains unresolved; characterization is not marked complete.**
No production correction, timeout increase, client-count reduction or SDK upgrade
is made.

| Full affected Pub/Sub matrix | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 413 | 3 | 55 | 471 |
| net10.0 | 415 | 1 | 55 | 471 |

All 465 prior case identities remain, with six diagnostics added. Full failures:

- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpOrderedNackRpcRecoveryTests.When_nacking_an_ordered_gcp_delivery_should_send_a_native_nack_and_recover_both_messages(useAsync: True, clientCount: 0)
- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async
- net9.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second
- net10.0: Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async

Read-only inspection of the pinned emulator 0.8.36 jar identifies a possible
interaction between deferred ordered acknowledgements and stream ownership changes.
This remains a hypothesis: current traces lack server stream ownership and processing
order. It does not establish a Brighter defect or a hosted Pub/Sub defect.
The [native SDK control](review/gcp-ordered-nack-native-control/README.md) is prepared
outside compiled tests, unbuilt/unrun pending review, to investigate without Brighter
in the delivery path. Its bounded outcomes will not replace the Brighter regressions.

All 6,481 pre-existing src/tests C# files are byte-identical to the pre-batch snapshot;
only the three approved compiled files are added. All four library targets build with
zero warnings/errors. Existing test-build warnings remain. All three owned emulators
are removed after saving logs; no unrelated container is changed. Nothing is committed
or published. Hosted/Firestore/Spanner/remote-CI validation is not claimed. Historical
disposal, other GCP mapping/concurrency/scheduler gaps and five remaining gateway
integrations continue to block full-feature production readiness.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-observation/` contains all
initial/mutation/restored/final/full TRXs and JSON, native trace summaries, exact source
snapshots/hashes, emulator bytecode inspection, all-target build, readiness/cleanup and
`final-verification.json`. The failed restoration is retained as
`restored-disconnect-receipt.trx` and `restored-failure-native-trace.txt`.


## GCP ordered nack reproduced without Brighter

The [approved native SDK control](review/gcp-ordered-nack-native-control/README.md)
builds without warnings/errors and executes unchanged. The dependency manifests contain
Google.Cloud.PubSub.V1 3.38.0 and Google.Api.Gax.Grpc 4.13.1, with no Brighter production
assembly. Both frameworks run the fixed four rounds × default/one/two clients.

| Native control | Observations | Both IDs recovered | Missing first ID within 30s | Correlated native nacks completed |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 12 | 11 | 1 | 12 |
| net10.0 | 12 | 11 | 1 | 12 |

Both missing-redelivery observations occur in round 3 (the fourth round), with client
count unset and Environment.ProcessorCount 11. Each records only one raw delivery of
the first native message, a successful nack for its exact observed ack ID, and the
second message's successful acknowledgement. No Brighter receipt, mapper, channel or
factory participates. All one/two-client observations recover both IDs; this is not a
recommendation to change the default count or evidence that a lower count is always safe.
The concurrent framework runs and small fixed sample do not establish a failure rate.

This demonstrates that Brighter is not necessary to trigger the observed failure.
It does not yet distinguish an SDK defect from emulator behaviour, prove permanent
message loss, establish hosted behaviour, or clear the existing Brighter restoration
gate. The same overlapping ack/nack/lease-extension pattern remains visible. No
production correction is justified by these observations alone.

The [prepared protocol control](review/gcp-ordered-nack-protocol-control/README.md)
removes high-level subscriber handling and automatic lease management. Its four native
RPC/stream-membership combinations and separately recorded recovery intervention are
outside compiled tests, unbuilt/unrun pending review. See also the local
[upstream reproduction report](gcp-ordered-nack-upstream-reproduction.md), not published.

All 6,484 pre-existing src/tests C# files remain byte-identical. No production or compiled
test source changes, no new test assertions/mutations, and no dependency upgrades occur.
The two owned emulators are removed after saving logs. The earlier full Brighter matrix
remains the regression checkpoint: net9.0 413 passed/3 failed/55 skipped; net10.0 415
passed/1 failed/55 skipped, with all four library targets built. Those suites are not
rerun because this control changes no production/test source. Other release gaps remain
open; nothing is committed or published.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-native-control/` contains approved
source snapshots, build/dependency artifacts, all 24 RESULT lines and full native
traces, both isolated failure traces, per-framework JSON, readiness/emulator logs,
source hashes, cleanup.json and final-verification.json.


## GCP ordered-nack emulator protocol stall isolated

The [approved protocol experiment](review/gcp-ordered-nack-protocol-control/README.md)
uses generated gRPC service clients directly: no Brighter, high-level subscriber
handler, automatic lease management, substituted response or emulator-internal call.
After correcting readiness setup, both frameworks complete the same four observations:

| Add a second stream | Complete second ack before first nack | net9.0 redelivery within 30s | net10.0 redelivery within 30s |
| --- | --- | --- | --- |
| No | No | Yes | Yes |
| No | Yes | Yes | Yes |
| Yes | No | Yes | Yes |
| Yes | Yes | No | No |

For the stalled combination, a successful correlated nack is followed by thirty seconds
without the first native message. Repeating only the second message's original ack
then releases the first message onto stream 1 with the same native ID/body and a new
ack ID: 37.620 ms after the intervention on net9.0 and 46.755 ms on net10.0. The initial
failure remains recorded; subsequent recovery is not counted as passing that bound.
The new stream's readiness is independently observed through a separate marker before
settling either target message.

This isolates a failure mode in the pinned emulator's ordered delivery protocol that
does not require Brighter or high-level SDK handling. Read-only bytecode inspection is
consistent with a deferred acknowledged entry preventing ownership transfer after the
earlier delivery is nacked. The live server's internal state was not instrumented.
This does not prove every historical failure has the same cause or establish hosted
Pub/Sub behaviour. Original Brighter tests, defaults and bounds remain intact; their
restoration gate and unrelated lifecycle findings remain open.

The first run exposed a readiness setup race on both frameworks: a marker reached the
original stream before the second was registered. Two single-stream observations per
framework completed, then setup aborted. These logs and nonzero exits are preserved
under initial-setup-error; they are not behavioural RED. The sole executable correction
acknowledges such an early marker and waits for a subsequent marker on stream 1 within
the same fifteen-second setup bound. Exact comparison confirms all settlement,
observation and recovery code remains unchanged. Initial failed runners were stopped
after reporting their unhandled setup error to avoid waiting for crash-dump writing.
A fresh pair of emulators is used for the corrected matrix.

Both initial and corrected project builds have zero warnings/errors. All 6,484 existing
src/tests C# files remain byte-identical. Both initial and both corrected owned emulators
are removed, and native logs are saved. The prior full Brighter regression/library
checkpoint remains unchanged; it is not rerun for this standalone diagnostic. No
production fix, dependency upgrade, test skip, commit or publication is included.

The local [upstream reproduction report](gcp-ordered-nack-upstream-reproduction.md)
now includes the discriminating protocol matrix. The next [hosted control draft](review/gcp-ordered-nack-hosted-control/README.md)
explicitly targets the locally configured brighter-pubsub-validation project, whose
ADC file exists but validity/permissions are unverified. It is unbuilt/unrun pending
review; no hosted API call or credential disclosure occurs. Its results cannot replace
Brighter hosted integration or broader release validation.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-protocol-control/` contains
approved/executed source snapshots, readiness-only.patch, initial setup failures,
all eight final observations, isolated failure/intervention traces, dependency/build
artifacts, source hashes, emulator logs, cleanup.json and final-verification.json.

## Hosted native SDK comparison (2026-10-29)

The approved [hosted control](review/gcp-ordered-nack-hosted-control/README.md)
ran its unchanged four-round × default/one/two-client matrix against
`brighter-pubsub-validation`, sequentially on both frameworks. Every observation
recovered both application IDs within the original thirty-second loop and recorded
successful native nack completion correlated to the first delivery. Native traces
show two copies of the first message, preserving its native ID, application ID,
binary body and ordering key, with a new acknowledgement ID on redelivery.

| Framework | Observations | Correlated native nacks | Both IDs recovered | Missing first ID |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 12 | 12 | 12 | 0 |
| net10.0 | 12 | 12 | 12 | 0 |

Execution used local macOS/Darwin 25.6.0 arm64, eleven reported processors,
Microsoft.NETCore.App 9.0.20/10.0.11 and unchanged Pub/Sub 3.38.0/GAX gRPC 4.13.1.
The emulator comparison ran in Linux containers, so this is not a controlled
single-variable comparison of server implementations. The raw protocol experiment
independently isolates the emulator handoff stall; these hosted observations did
not reproduce missing redelivery. Neither result establishes absence of intermittent
hosted failure or validates Brighter's hosted integration.

Both builds have zero warnings/errors; both processes exit zero, completing every
owned-resource delete. Read-only project listings independently verify that all
24 owned topics and 24 owned subscriptions are absent. Exactly 48 synthetic messages
were published. ADC was used without copying or printing credentials. No auth,
permission, quota, setup or cleanup error occurred. Traces retain eleven/net9 and
twelve/net10 client-cancelled lease-extension RPCs after the RESULT records during
subscriber shutdown; these do not replace the successfully completed nacks.

All 6,484 pre-existing src/tests C# files and the approved executable/project remain
byte-identical. No production change, mutation, SDK upgrade, timeout increase,
client-count reduction, skipped assertion, repeated-until-green run, commit or
publication is included. This standalone control does not rerun or supersede the
Brighter regression matrix or clear its failed restoration gate.

Evidence is in `/private/tmp/brighter-4170-gcp-ordered-nack-hosted-control/`:
source and review snapshots, build log, lock/dependency artifacts, runtime/environment
records, both full native logs and twelve-row results, `final-verification.json`,
read-only post-delete listings and `cleanup.json`.

The next [hosted Brighter adapter](review/gcp-ordered-nack-hosted-brighter/README.md)
is prepared through the test-first review workflow, unbuilt and unrun. It reuses the
existing six sync/async × client-count diagnostic assertions through public Brighter
factories, with hosted targeting shown in a mechanical diff. Its proposed twelve
observations across both frameworks use at most twelve resource pairs and twenty-four
synthetic messages in the same explicitly named project. Earlier SDK-only approval
does not execute this new adapter. Other GCP lifecycle, mapping, generated-pump and
broader release requirements remain open.

## Hosted Brighter ordered-nack comparison (2026-10-30)

The approved [hosted Brighter adapter](review/gcp-ordered-nack-hosted-brighter/README.md)
passes all twelve fixed observations against `brighter-pubsub-validation`. The matrix
runs sync/async operations with SDK-default, one and two subscriber clients once per
framework. Unlike the preceding SDK-only control, these observations use Brighter's
public producer/channel factories and actual send/receive/nack/acknowledge paths.

| Framework | Fixed cases | Passed | Failed | Correlated native nacks | Both application IDs recovered |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 6 | 6 | 0 | 6 | 6 |
| net10.0 | 6 | 6 | 0 | 6 | 6 |

The existing diagnostic assertions execute unchanged: binary body, application/native
identity, native ordering key, initially pending receipt, successful native deadline-zero
request correlated to the first delivery, receipt Nack completion and both recovered
application IDs. The original immediate receive/ack sequence and all observation bounds
remain intact. Independent trace verification finds two raw copies of every first
message with the same identity/body/order and a new acknowledgement ID on redelivery.

Both frameworks build with zero errors and four total CS0618 warnings: two uses of
the existing obsolete MessageHeader.MessageType per framework in the copied diagnostic.
Those checks and traces were retained, not suppressed or rewritten. The linked gateway
and core are built from the current source baseline. Execution uses local Darwin 25.6.0
arm64, eleven processors, .NET runtimes 9.0.20/10.0.11, Pub/Sub 3.38.0 and GAX gRPC
4.13.1. Six client-cancelled native RPC observations per framework remain in the full
traces; all required nacks complete successfully and no setup/cleanup failure occurs.

Both processes exit zero. The twelve unique topic/subscription pairs were deleted,
and subsequent read-only project listings independently confirm all twelve topics and
twelve subscriptions are absent. Exactly twenty-four synthetic messages were published.
ADC was used locally; no credential was copied or printed. No repeat-until-green,
mutation, default-client reduction, timeout increase, dependency upgrade or production
correction is included. All 6,484 existing src/tests C# files and the approved adapter
code/project remain byte-identical.

This supplies bounded hosted evidence for the ordered-nack path, closing that specific
missing comparison. It does not demonstrate absence of intermittent failures, reproduce
every provisioning/pump detail of the generated tests, or clear the retained failing
emulator restoration/regression evidence. No full regression suite was rerun for this
unchanged-source environment comparison. Buffered/unsettled disposal, factory replacement,
other mapping/lifecycle/concurrency coverage and overall production readiness remain open.

Evidence: `/private/tmp/brighter-4170-gcp-ordered-nack-hosted-brighter/` contains approved
snapshots, source hashes, build/artifact/dependency records, runtime/environment records,
both full logs and six-row result files, `final-verification.json`, post-delete listings
and `cleanup.json`. No commit, deployment or publication occurred.

The next [hosted factory shutdown adapter](review/gcp-hosted-factory-stream-shutdown/README.md)
is prepared under the test-first review workflow, unbuilt and unrun. It reuses the
existing eight sync/async × one/two-client × native/replacement recovery assertions,
with the hosted adaptation shown in a mechanical diff. The proposed sixteen cases
across both frameworks create at most sixteen resource pairs and sixteen synthetic
messages in the same explicitly named project. It targets the separate shutdown/
replacement finding; it does not cover buffered-after-requeue disposal. Previous
ordered-nack approval does not execute this new batch.

## Hosted factory stream shutdown comparison (2026-10-30)

The approved [hosted factory adapter](review/gcp-hosted-factory-stream-shutdown/README.md)
passes all sixteen fixed cases against `brighter-pubsub-validation`. Each framework
runs sync/async × one/two subscriber clients × native Pull/replacement-channel recovery.
The public factory/channel path and existing assertions execute unchanged.

| Framework | Fixed cases | Passed | Failed | Observed disposal duration |
| --- | ---: | ---: | ---: | --- |
| net9.0 | 8 | 8 | 0 | 0.135493–0.175947 seconds |
| net10.0 | 8 | 8 | 0 | 0.128783–0.185121 seconds |

Every case disposes within the original ten-second bound, observes a successful
native deadline-zero request during shutdown for the exact subscription and first
SDK client generation, observes no native acknowledgement from that generation,
and satisfies the pending callback completion/cancellation assertions. Native Pull
or a replacement channel recovers the same native/application identity and binary
body. Replacement cases build generation two, obtain a distinct receipt and complete
its Ack reply. Native-Pull cases retain one SDK stream generation. The shutdown nack
assertion observes a nonempty ack-ID set; it does not newly correlate that set to a
raw StreamingPull response. The trace verifier preserves that limit.

Both frameworks build successfully with zero errors and two total CS0618 warnings
from the existing MessageHeader.MessageType trace, one per framework. Execution uses
Darwin 25.6.0 arm64, eleven processors, .NET runtimes 9.0.20/10.0.11, Pub/Sub 3.38.0
and GAX gRPC 4.13.1. All 6,484 existing src/tests C# files and the approved adapter
code/project remain byte-identical. No mutation, implementation change, SDK upgrade,
client-count change, timeout increase or repeat-until-green is included.

Both processes exit zero, including cleanup. Sixteen synthetic messages were published
across sixteen unique topic/subscription pairs. Subsequent read-only project listings
independently confirm all sixteen owned topics and sixteen owned subscriptions are
absent. ADC remained local; no credentials were copied or printed. No assertion,
setup, auth, quota or cleanup failure occurred. No commit, deployment or publication.

This closes the missing bounded hosted factory shutdown/replacement comparison.
It does not establish a universal cause for the historical empty replacement receives,
erase the retained emulator failures, exercise buffered-after-requeue disposal, or
complete broader lifecycle/concurrency/mapping validation. No full regression suite
was rerun for this unchanged-source environment comparison. Production readiness
remains open.

Evidence: `/private/tmp/brighter-4170-gcp-hosted-factory-stream-shutdown/` contains
approved/source snapshots, isolated build/dependency artifacts, environment/runtime
records, complete traces, per-framework eight-row results, disposal timings and
`final-verification.json`, read-only post-delete listings and `cleanup.json`.

The next [buffered-redelivery hosted adapter](review/gcp-hosted-buffered-stream-shutdown/README.md)
is prepared through the test-first review workflow, unbuilt and unrun. It reuses four
existing buffered cases per framework, with actual public buffer readiness/peek
assertions before disposal. Its explicit adaptation replaces the old manually
constructed SDK client/no-op disposal callback with an SDK-owned client and lets the
production consumer own shutdown/cleanup. It composes the public gateway components
with real SDK collaborators; it does not claim to drive the channel factory or pump.
The proposed eight cases create at most eight resource pairs and eight synthetic
messages in the same named project. Earlier factory approval does not execute it.

## Hosted buffered-redelivery shutdown comparison (2026-10-30)

The approved [buffered-shutdown adapter](review/gcp-hosted-buffered-stream-shutdown/README.md)
passes all eight fixed cases against `brighter-pubsub-validation`. Each framework
runs sync/async × native Pull/replacement-consumer recovery with one SDK-owned
subscriber client. Every case establishes unread buffered redelivery through the
public Reader.WaitToReadAsync/TryPeek assertions before disposal; no readiness sleep
or assumption replaces that precondition.

| Framework | Fixed cases | Passed | Failed | Observed disposal duration |
| --- | ---: | ---: | ---: | --- |
| net9.0 | 4 | 4 | 0 | 0.121941–0.237365 seconds |
| net10.0 | 4 | 4 | 0 | 0.121745–0.164711 seconds |

The buffered delivery has the original native ID/body, a distinct pending receipt
and an incomplete callback before shutdown. All cases complete disposal inside the
original ten-second bound, observe a successful deadline-zero native request started
after shutdown begins, prohibit premature native acknowledgement and satisfy callback
cancellation/Nack assertions. Native Pull or a replacement consumer then recovers the
same application/native identity and bytes. Replacement acknowledgement completes.
Trace verification independently confirms BUFFERED precedes DISPOSE, the completed
shutdown nack and the same native ID on recovery. The assertion observes a nonempty
ack-ID set, not a new raw StreamingPull/ack-ID correlation check.

The approved ownership adaptation executes unchanged: SubscriberClientBuilder owns
connections; GcpStreamConsumer owns native stop/disposal. The old no-op disposal
callback/shared SDK implementation is not used. Public gateway components are composed
with real SDK collaborators; this comparison does not claim to drive the factory or
generated pump. The preceding sixteen-case hosted comparison covers the factory path.

Both builds succeed with zero warnings/errors. Both processes exit zero. Eight
synthetic messages were published across eight unique topic/subscription pairs;
read-only project listings independently confirm all eight topics and eight subscriptions
are absent after deletion. There are no assertion, setup, auth, quota or cleanup errors.
ADC remained local and credentials were not copied or printed.

Execution uses Darwin 25.6.0 arm64, eleven processors, .NET runtimes 9.0.20/10.0.11,
Pub/Sub 3.38.0 and GAX gRPC 4.13.1. All 6,484 existing src/tests C# files and the
approved adapter code/project remain byte-identical. No production correction,
mutation, SDK upgrade, timeout/client-count change, retry-until-green, commit,
deployment or publication is included. No full regression matrix is rerun for this
unchanged-source environment comparison.

This closes the missing bounded hosted buffered-redelivery comparison. It does not
establish the cause of every historical emulator failure, erase the retained failed
restoration/regression evidence, or validate general concurrency, mapping or pump
behavior. Overall issue #4170 production readiness remains open.

Evidence: `/private/tmp/brighter-4170-gcp-hosted-buffered-stream-shutdown/` contains
approved/source snapshots, isolated build/dependency artifacts, runtime/environment
records, full traces, four-row per-framework results, disposal timing verification,
`final-verification.json`, read-only post-delete listings and `cleanup.json`.

The next [publication writer-isolation review](review/gcp-publication-writer-isolation/README.md)
is prepared through the test-first workflow, unbuilt/unrun and not adopted into the
compiled suite. Six cases cross sync/async/bulk sends with publication creation order,
using custom A/default/custom B endpoints in one public producer factory. Native body/
attribute isolation, source preservation and writer selection are checked. Two named
production-only mutations, exact restorations, library builds and full available
Pub/Sub regression matrices are proposed. This next batch uses isolated emulators;
no hosted calls are proposed.

## GCP publication writer isolation (2026-10-30)

The approved [six-case isolation batch](review/gcp-publication-writer-isolation/README.md)
is adopted and validated against isolated native Pub/Sub emulators. Sync/async/bulk
sends preserve custom A/default/custom B mapping when the same public factory creates
all three producers in either publication order. Native binary bodies, endpoint/source
attributes, canonical default identity, writer selection/call counts and source
preservation are checked. This establishes sequential endpoint isolation, not concurrent
operations or scheduler/rejection mapping.

All six cases pass initially on both frameworks. The initial build identifies xUnit2031
in the new test; an equivalent Assert.Single(collection, predicate) overload removes
that warning without changing its predicate, assertion order, data or bounds. The
approved original and two-line style diff are retained. Both focused suites pass again.

Two named production-only mutations on .NET 10 produce twelve intended native-body
assertion failures. Ignoring configured writers removes custom A's binary prefix in
all six cases. Reusing the first publication's writer adds A's prefix to the default
endpoint in forward creation order, and applies B's prefix to A in reverse order.
Every mutation is restored byte-for-byte and followed by six passes. Final focused
runs pass six cases on each framework. No implementation correction is needed.

| Framework | Passed | Failed | Skipped | Total | New isolation cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 421 | 1 | 55 | 477 | 6 |
| net10.0 | 422 | 0 | 55 | 477 | 6 |

Both full matrices preserve all 471 prior case identities and add exactly six. The
net9.0 failure is the previously recorded generated async ordered-nack case:
`StreamOrdering.Proactor.WhenNackingAMessageItShouldBeRedeliveredAsync.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second_async`,
at its missing-redelivery Assert.Contains on line 158. The prior sync ordered-nack
and async/default-client native diagnostic failures pass in this run; the previous
net10.0 async ordered-nack failure also passes. Those outcome changes do not establish
a fix or erase the retained intermittent failures. No retry-until-green or test
exclusion is used. Full-feature production readiness remains open.

All four GCP library targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero
warnings/errors. The larger test dependency graph builds with repository warnings;
the final build has no warning attributed to either new file. All 6,484 pre-existing
src/tests C# files remain unchanged. Exactly the approved test/helper are added; the
helper is byte-identical to approval and the test differs only by the equivalent
assertion style cleanup. Both production mutation targets are restored exactly.

All three batch-owned emulators are removed and their logs retained. There are no
hosted calls, credential use, dependency upgrades, unrelated container changes,
commits, deployments or publication. This Pub/Sub selection does not execute the
unavailable Firestore/Spanner/hosted suites or constitute whole-project verification.

Evidence: `/private/tmp/brighter-4170-gcp-publication-writer-isolation/` contains approved
snapshots, all source hashes, `assertion-style-only.patch`, initial/style/mutation/
restored/final focused TRX/log/JSON records, both full 477-case matrices and outcome
comparisons, all-target library build evidence, emulator logs, `final-verification.json`
and `cleanup.json`.

The next [concurrent writer batch](review/gcp-concurrent-publication-writer/README.md)
is a concrete unbuilt/unrun review draft. Three cases overlap two mappings through
the same factory-created producer in sync/async/bulk modes using a thread-safe writer
and explicit barrier. One named native-result-cache mutation, exact restoration,
library builds and full available regressions are proposed. This next batch remains
local to isolated emulators and does not include hosted execution.

## GCP concurrent publication writer (2026-10-30)

The approved [three-case concurrent writer batch](review/gcp-concurrent-publication-writer/README.md)
is adopted unchanged. Two operations overlap through the same public-factory-created
producer in sync, async and bulk modes. A two-party reader-independent writer barrier
requires overlapping mappings. Independent native observation checks both distinct
binary bodies before per-message identity, custom attributes and source preservation;
the gateway must still clear a writer-supplied ordering key for unordered publication.
Each bulk operation owns one source message; this does not prove concurrent batch
internals, arbitrary user-writer thread safety, scheduling or all concurrency paths.

All three cases pass initially on both frameworks. The named production-only native
result cache mutation then makes all three .NET 10 cases fail at the intended payload
multiset assertion: expected ["VwAA/wGA", "VwD+AAJ/"] versus actual
["VwD+AAJ/", "VwD+AAJ/"]. Both writer calls and the overlap barrier still complete;
compilation/setup/timeout errors are not counted as mutation evidence. The producer
is restored byte-for-byte, then all three cases pass again on both frameworks.
No implementation correction is needed.

| Framework | Passed | Failed | Skipped | Total | New concurrency cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 425 | 0 | 55 | 480 | 3 |
| net10.0 | 424 | 1 | 55 | 480 | 3 |

Both full matrices retain all 477 preceding case identities and add exactly three,
all passing. Each full matrix runs once against its own fresh, readiness-checked
pinned emulator. No retries, exclusions or altered time bounds are used to turn a
behavioral failure green. Full-run failures are retained:

- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpFactoryStreamShutdownRecoveryTests.When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(useAsync: False, clientCount: 2, nativeRecovery: False)` — Assert.Equal() Failure: Values differ


The sole failure is the previously recorded synchronous/two-client factory replacement
case at line 185: shutdown's deadline-zero ModifyAckDeadline completes successfully,
disposal completes and a second native client generation is built, but replacement
Receive returns MT_NONE after twenty seconds. This is retained as a redelivery finding,
not attributed to the new writer characterization or claimed fixed by earlier hosted
passes. The preceding net9.0 async ordered-nack failure passes in this run; the net10.0
factory case passed in the preceding run. Neither outcome change clears the historical
intermittent lifecycle gates. All other prior case outcomes are unchanged.

All 6,486 pre-existing src/tests C# files remain byte-identical. Exactly the approved
test and helper are added; their compiled and review copies match their approved
snapshots. All four gateway library targets build with zero warnings/errors. Test dependency builds retain
repository warnings; none is attributed to the new files. No production, dependency,
or CI selection change is needed for this characterization.

The first sandboxed emulator launch was blocked, and an early runner invocation
found no built assembly and executed zero tests. These infrastructure attempts are
retained separately as launcher-before-build-net9.0.log/json; they are not behavioral
runs. After the build completed, both initial focused suites ran once and passed.

All three owned emulators are removed and their logs retained; unrelated containers
are untouched. No hosted API, credentials, Firestore/Spanner suite, remote CI,
commit, deployment or publication is involved. The selected Pub/Sub regressions do
not establish whole-project or full-feature production readiness. Historical emulator
lifecycle failures, remaining gateway integrations and other release gates remain open.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-publication-writer/` contains
approved snapshots, all source hashes, native-cache mutant, initial/mutation/restored
TRX/log/JSON records, both full 480-case matrices with prior-identity/outcome comparison,
all-target library build, emulator logs, final-verification.json and cleanup.json.

The next [concurrent pull reader batch](review/gcp-concurrent-pull-reader/README.md)
is prepared as an unbuilt/unrun review draft. Two consumers share a safe reader and
must preserve overlapping mapping, per-delivery receipts/counts and subsequent native
acknowledgement. Four named mutations, exact restoration, affected regressions and
owned cleanup are proposed; no hosted execution is included.

## GCP concurrent pull reader (2026-10-30)

The approved [two-case shared pull-reader batch](review/gcp-concurrent-pull-reader/README.md)
is adopted unchanged. Separate public-factory-created consumers share a safe reader;
sync/async receives overlap through a two-party mapping barrier. Both successful
barrier crossings must be observed before comparing mapped bodies as a multiset.
Each consumer retains its own mapped identity, native content and custom marker,
while the gateway replaces fabricated receipt handles and handled counts with that
delivery's native values. Native inputs remain unchanged and mapped objects stay
separate. Delivery lookup uses source markers rather than cross-topic ID uniqueness.

Each consumer acknowledges its own message. After twelve seconds, beyond the native
ten-second ack deadline, a six-second native observation on each subscription sees
its fresh marker and no original message. This is bounded settlement evidence;
it does not establish unlimited absence of duplicates or streaming/concurrent settlement.

Both baseline cases pass on net9.0 and net10.0 without a production change. Four
named production-only mutations on net10.0 yield eight intended failures:

- Cached mapped body: both cases expect ["UgAA/wGA", "UgD+AAJ/"] but observe
  ["UgAA/wGA", "UgAA/wGA"] at the payload multiset assertion.
- Omitted receipt override: both cases observe reader-invalid-receipt instead of the
  corresponding native acknowledgement handle.
- Omitted handled-count override: both cases expect native count 3 but observe the
  configured reader's 777.
- Omitted native acknowledgement: the observer sees the original delivery alongside
  the fresh marker, failing Assert.DoesNotContain in both cases.

Every mutation is restored byte-for-byte and followed by two net10.0 passes. The final
net9.0 run also passes both cases. The last net10.0 restoration is its final focused
run. No compile/setup failure is accepted as mutation evidence, no assertions/bounds
are altered, and no behavioral retry-until-green is used.

| Framework | Passed | Failed | Skipped | Total | New reader cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 426 | 1 | 55 | 482 | 2 |
| net10.0 | 426 | 1 | 55 | 482 | 2 |

Both full available Pub/Sub matrices run once on separate fresh, readiness-checked
pinned emulators. All 480 preceding case identities remain, plus exactly two new
passing cases. Full-run failures are retained:

- net9.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Assert.Contains() Failure: Item not found in set
- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Assert.Contains() Failure: Item not found in set

Changes from the immediately preceding full matrices:

- net9.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Passed → Failed.
- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second` — Passed → Failed.
- net10.0: `Paramore.Brighter.Gcp.Tests.MessagingGateway.GcpFactoryStreamShutdownRecoveryTests.When_disposing_a_factory_stream_channel_should_release_the_delivery_and_replace_a_stopped_client(useAsync: False, clientCount: 2, nativeRecovery: False)` — Failed → Passed.


Both current failures are the generated synchronous ordered-nack case at Reactor
line 152: the expected nacked-message ID is missing from the final redelivery set.
This known emulator finding predates this pull-reader batch. The preceding net10.0
synchronous/two-client factory replacement failure passes here. All other prior
case outcomes remain unchanged. No streaming source changed in this increment.

A passing historical failure does not establish its cause or a fix. The emulator
lifecycle gates, broader mapping/scheduler paths and remaining gateway integrations
remain open. These results do not establish full-feature production readiness.

All 6,488 pre-existing src/tests C# files remain byte-identical. Exactly the approved
test/helper are added, and their compiled/review copies match the approved snapshots.
All four library targets build with zero warnings/errors. The test dependency graph
retains repository warnings, including CS0618 for the new test's deliberate legacy
MessageHeader.MessageType compatibility assertion. The approved assertion is retained;
no test change is needed and the helper introduces no warning. Existing CI categories
already select these cases; there is no workflow or dependency change.

All three owned emulators are removed and their logs retained; unrelated containers
are untouched. There are no hosted calls, credential use, commits, deployment or
publication. The available Pub/Sub selection excludes unavailable Firestore/Spanner
and hosted suites, and is not whole-project or remote-CI verification.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-pull-reader/` contains approved
snapshots, 6,488 source hashes, four saved production mutants, initial/mutation/restored/
final TRX/log/JSON records, both full 482-case matrices with case/outcome comparisons,
all-target library build, emulator logs, final-verification.json and cleanup.json.

The next [concurrent stream-reader batch](review/gcp-concurrent-stream-reader/README.md)
is a concrete review-only draft, unbuilt/unrun and outside the compiled suite.
Two factory-created streaming consumers share a safe reader; mapping, receipt ownership,
callback completion and subscription-specific native acknowledgement must remain
independent. Five named mutations, exact restorations, library builds and affected
regressions are proposed, using isolated emulators only.

## GCP concurrent stream reader (2026-10-30)

The approved [shared stream-reader batch](review/gcp-concurrent-stream-reader/README.md)
is adopted unchanged. Two sync/async cases establish overlapping mapping across
separate public-factory consumers sharing a safe reader, with one SDK client per
subscription. Exact mapped body/identity/marker isolation, unchanged native inputs,
native wrapper/count ownership and independent callback completion are verified.
Each consumer's acknowledgement also completes a native Acknowledge RPC for its
own subscription with one nonempty acknowledgement ID. This is bounded native
settlement evidence; it does not decode streaming delivery ack IDs independently
or establish unlimited absence of redelivery or concurrent settlement.

Both initial cases pass on .NET 9 and .NET 10 without production corrections.
Five independent production mutations on .NET 10 produce ten intended failures:
cached bodies fail payload multiset equality; omitted receipt override fails exact
wrapper identity; omitted count override exposes 777 instead of native 3; omitted
Accept leaves the callback incomplete; replacing its completed Ack with native Nack
leaves the native acknowledgement incomplete after the original callback succeeds.
All mutations fail at their reviewed assertion, not setup or compilation. Both
production targets are restored byte-for-byte after each mutation, followed by two
passes. Final focused runs pass both cases on each framework.

| Framework | Passed | Failed | Skipped | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 429 | 0 | 55 | 484 | 2 |
| net10.0 | 428 | 1 | 55 | 484 | 2 |

Each full available Pub/Sub matrix runs once on a fresh readiness-checked pinned
emulator. All 482 preceding case identities remain, plus exactly two new passing
cases. The .NET 10 failure is the known generated synchronous ordered-nack case:
StreamOrdering.Reactor.WhenNackingAMessageItShouldBeRedelivered.
When_nacking_first_of_two_messages_should_redeliver_nacked_then_receive_second,
at the final Assert.Contains redelivery assertion. The same case changes from
failed to passed on .NET 9; all other preceding outcomes are unchanged. Passing in
one run establishes neither a root cause nor a fix. Earlier lifecycle findings and
broader release gates remain open. There is no retry-until-green or bound change.

All 6,490 pre-existing src/tests C# files are byte-identical. Exactly the approved
test/helper are added and match their review and approved snapshots. All four GCP
library targets build with zero warnings/errors. The test dependency graph retains
3,320 repository warnings; neither new file introduces a warning. Existing CI
categories select the new cases; no workflow/dependency change is needed.

All three owned emulators are removed, logs retained and unrelated containers left
untouched. No hosted calls, credential use, commits, deployment or publication occur.
The selection excludes unavailable Firestore/Spanner and hosted coverage and is not
whole-project or remote-CI verification. Whole-feature production readiness remains
open, including the five remaining gateway integrations.

Evidence: `/private/tmp/brighter-4170-gcp-concurrent-stream-reader/` contains source
hashes, approved snapshots, five saved mutants, initial/mutation/restored/final
TRX/log/JSON results, both full matrices and outcome comparisons, the all-target
library build, final-verification.json, emulator logs and cleanup.json.

The next [MQTT publication writer batch](review/mqtt-publication-writer/README.md)
is a concrete review-only draft, unbuilt/unrun and outside the compiled suite.
Six sync/async cases cover legacy, custom and composed default payloads with native
broker observation, gateway-owned routing/QoS and source preservation. Four named
mutations propose eighteen assertion failures. Implementation, native regression
baselines and execution await the repository's test-review approval.

## MQTT publication writer (2026-10-30)

The approved [publication writer batch](review/mqtt-publication-writer/README.md)
is implemented. MqttPublication selects an optional native writer, and the reusable
DefaultMqttWireMessageWriter preserves full-Message JSON serialization. Both producer
paths forward the publication selection through internal publisher overloads. The
publisher applies its configured topic prefix and source header topic, then QoS
AtLeastOnce, after mapping. Existing public constructors/methods and base Publication
callers remain compatible. No internal member is exposed for testing.

Both unchanged full baselines pass 79 cases with four existing skips. A stale restored
asset graph initially omitted MQTTnet; a locked restore fixes compilation without
source, package-version or lockfile changes. The first immediate broker readiness
probe precedes startup and is refused; bounded readiness polling succeeds before
any tests run. Neither setup problem is counted as behavioral RED.

The six approved cases and helper are adopted unchanged. With only the new public
contracts/default adapter present, both frameworks pass the two legacy cases and
fail all four custom/composed cases at exact native payload equality. Wiring writer
selection makes all six pass. The independent native MQTT client observes exact
bytes, gateway topic and QoS; source data stays unchanged and each selected writer
runs once. The observer also subscribes to incorrect candidate topics, so routing
mutations fail topic equality rather than timing out.

Four independent .NET 10 production-only mutations yield eighteen intended failures:
ignoring the writer fails four payload assertions; omitting the gateway topic
fails six topic assertions; omitting QoS fails four QoS assertions; replacing default
JSON with raw source bytes fails four legacy/composed payload assertions. Unaffected
cases pass. Every mutation is restored byte-for-byte and followed by six passes.
Final focused cases pass on both frameworks; no assertions or time bounds change.

| Framework | Baseline passed/skipped | Final passed | Failed | Skipped | Total |
| --- | --- | ---: | ---: | ---: | ---: |
| net9.0 | 79 / 4 | 85 | 0 | 4 | 89 |
| net10.0 | 79 / 4 | 85 | 0 | 4 | 89 |

Both complete test assemblies run once on separate fresh brokers, including existing
embedded MQTTnet server tests. All 83 preceding identities and outcomes remain,
plus six new passes. Four generated nack-conformance cases remain skipped under
issue #4240. All three library targets (net8.0/net9.0/net10.0) build with zero warnings
and errors. The test build retains 738 existing warnings; no new or changed file
introduces a warning. Existing MQTT CI selection already covers the new tests.
These are local native-broker results, not a remote CI run.

Of 6,492 preceding src/tests C# files, 6,490 remain byte-identical; only the MQTT
producer/publisher change. Exactly four files are added: the publication, default
writer, approved test and helper. All implemented source matches the saved restored
snapshots. No dependency, workflow or lockfile changes are made in this increment.

Mosquitto matches the repository image, pinned for these runs to
`efrecon/mosquitto@sha256:9f77138ef91011b49f1a10cabb0a919a5f1491b2c5f6ea91ddb29690237941e4`.
The image is linux/amd64 on an arm64 Docker host; SDK runners are native arm64.
No host port is bound. All five owned brokers are removed and logs retained;
unrelated containers are untouched. No hosted calls, credentials, commits,
deployment or publication occur.

Evidence: `/private/tmp/brighter-4170-mqtt-publication-writer/` contains approved
snapshots, source hashes, baseline/RED/green/mutation/restored/final TRX/log/JSON,
saved production mutants, complete regression comparisons, library build,
final-verification.json, broker logs and cleanup.json.

MQTT reader integration, writer failure/cancellation/recovery, retention,
concurrency and remaining scheduler/rejection paths remain open. Redis, RocketMQ,
SQL Server and PostgreSQL integrations and broader release gates also remain open;
this increment does not establish whole-feature production readiness.

The next [MQTT subscription reader batch](review/mqtt-subscription-reader/README.md)
contains six concrete review-only cases and a composing reader. They remain
unbuilt/unrun outside the compiled project pending test-review approval.

## MQTT subscription reader (2026-10-30)

The approved [subscription reader batch](review/mqtt-subscription-reader/README.md)
is implemented. MqttSubscription selects an optional native MQTT application-message
reader. Both public consumer factories capture it through an internal constructor;
all existing public constructor signatures remain compatible. Conversion occurs
synchronously in the SDK callback before the existing non-null buffer guard.
The default path still invokes MqttMessageCreator, preserving its JSON rules and
log-and-drop handling. The reusable DefaultMqttWireMessageReader composes that same
parser and throws FormatException if it cannot return a decoded message.

The reviewed fixture omitted two required base-subscription arguments. The first
.NET 9 run stops all six cases at the missing pump type; the next stops them at the
missing request type. The fixture now explicitly selects Reactor/Proactor from the
case and supplies the existing MyCommand as request metadata. No new request,
handler or logging registration is needed. Original snapshots and both setup-failure
runs remain recorded. Only these two fixture arguments change; the helper, assertions
and time limits are unchanged. Neither setup failure is behavioral RED.

With the corrected fixture and only the public contract/default adapter present,
each framework passes two legacy cases and fails four custom/composed cases at exact
body equality. Factory selection and callback mapping make all six pass. Native
observations verify the configured body, application identity/topic, marker bag,
one reader call and unchanged payload during mapping. The SDK continues to own
protocol acknowledgement; no settlement or redelivery guarantee is inferred.

Four .NET 10 production-only mutations yield eighteen intended assertion failures:
ignoring the reader fails four custom/composed body assertions; empty default-decoded
bodies fail four legacy/composed body assertions; overriding application topics with
native broker topics fails all six topic assertions; calling the configured reader
twice fails four invocation-count assertions. Unaffected cases pass. Every mutant
is restored byte-for-byte and followed by six passes. Final focused cases pass on
both frameworks. No failure is accepted from compilation or setup, and no behavioral
retry-until-green occurs.

| Framework | Passed | Failed | Skipped | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 91 | 0 | 4 | 95 | 6 |
| net10.0 | 91 | 0 | 4 | 95 | 6 |

Both complete assemblies run once on fresh, readiness-checked isolated brokers.
All 89 preceding identities and outcomes remain, including writer selection,
existing malformed-parser checks and four nack-conformance skips under #4240.
The existing malformed-input tests exercise the public parser; they do not establish
custom-reader failure recovery or native recovery after malformed deliveries.
All three MQTT library targets build without warnings/errors. The green test build
retains 738 existing warnings; no new or changed file introduces a warning.
Existing MQTT CI selection covers the cases; remote CI is not run here.

Of 6,496 preceding src/tests C# files, 6,493 remain byte-identical. Only the MQTT
subscription, consumer and factory change. Exactly three files are added: the
default reader, test and helper. MqttMessageCreator is restored exactly after its
mutation. The preceding writer implementation and tests remain unchanged. The
compiled/review test matches the documented corrected snapshot; the helper matches
its original approval snapshot. No dependency, lockfile or workflow change occurs.

The repository Mosquitto image is pinned to
`efrecon/mosquitto@sha256:9f77138ef91011b49f1a10cabb0a919a5f1491b2c5f6ea91ddb29690237941e4`.
It runs as linux/amd64 on an arm64 host, with native arm64 SDK runners and no host
port bindings. All three owned brokers are removed and logs retained; unrelated
containers are untouched. No hosted calls, credentials, commits, deployment or
publication occur.

Evidence: `/private/tmp/brighter-4170-mqtt-subscription-reader/` contains approval
and corrected-fixture snapshots, source hashes, setup/RED/green/mutation/restored/
final TRX/log/JSON records, saved mutants, both full matrices with outcome checks,
library build, final-verification.json, broker logs and cleanup.json.

This closes successful reader selection, not MQTT or whole-feature production
readiness. The configured-reader exception path currently delegates to the SDK;
throw/null recovery still needs native evidence. Writer failure/cancellation,
retention, concurrency, scheduler/rejection paths, the four remaining gateway
integrations and broader release gates remain open.

The next [MQTT reader failure-recovery batch](review/mqtt-reader-failure-recovery/README.md)
contains four review-only cases. They require the failed mapping to be dropped
without default fallback or a placeholder, followed by the next valid delivery
through the same consumer. Four named mutations propose fourteen intended failures.
The draft is unbuilt/unrun outside the compiled suite pending test-review approval.

## MQTT reader failure recovery (2026-10-30)

The approved [reader failure-recovery batch](review/mqtt-reader-failure-recovery/README.md)
is adopted unchanged. All four sync/async × throw/null cases pass on both frameworks
before production changes. The configured reader observes a native failed delivery,
then the same consumer receives the next valid delivery with exact ID, binary body,
application topic and marker. The reader runs twice and the subsequent 250 ms
observation returns the no-message sentinel. Failed inputs are valid default JSON,
so falling back to the default parser would expose a wrong message identity.

No production correction is needed for this bounded recovery behavior. The existing
buffer guard drops null results; the observed SDK dispatch continues after the
configured InvalidOperationException. The consumer still delegates exception
handling to the SDK. This evidence does not establish a new Brighter catch boundary,
log capture, manual settlement, dead-letter forwarding or unlimited absence of
protocol duplicates. Other exception types, explicit default-reader failure
composition, non-JSON input and reconnect paths remain separate.

Four independent .NET 10 production-only mutations produce fourteen intended failures:

- Latching a reader exception and dropping later mappings fails the good-ID assertion
  in both throwing cases; null cases pass.
- Substituting empty messages for throw/null failures fails all four good-ID assertions.
- Falling back to default JSON decoding after throw/null failures fails all four
  good-ID assertions by delivering the failed message instead of the good one.
- Emptying mapped bodies fails all four exact good-body assertions.

Each mutant is restored byte-for-byte and followed by four passes. The final .NET 9
run also passes four cases; the last restored .NET 10 run is its final focused run.
No test/helper changes, time-bound changes or behavioral retry-until-green occur.

| Framework | Passed | Failed | Skipped | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 95 | 0 | 4 | 99 | 4 |
| net10.0 | 95 | 0 | 4 | 99 | 4 |

Both complete test assemblies run once on separate fresh, readiness-checked brokers.
All 95 preceding identities and outcomes remain, plus four new passes. The four
nack-conformance skips under #4240 are unchanged. All three MQTT library targets
build with zero warnings/errors. The test dependency graph retains 738 existing
warnings; neither new file introduces a warning. Existing MQTT CI selection covers
these tests; remote CI is not run here.

All 6,499 pre-existing src/tests C# files remain byte-identical. Exactly the approved
test/helper are added, matching their review and approval snapshots. The production
mutation target is restored exactly; the reader and writer implementations remain
unchanged. No dependency, lockfile or workflow change occurs.

Native runs use the repository Mosquitto image pinned to
`efrecon/mosquitto@sha256:9f77138ef91011b49f1a10cabb0a919a5f1491b2c5f6ea91ddb29690237941e4`.
The broker is linux/amd64 on an arm64 host; SDK runners are native arm64. No host
port is bound. All three owned brokers are removed and logs retained; unrelated
containers are untouched. No hosted calls, credentials, commits, deployment or
publication occur.

Evidence: `/private/tmp/brighter-4170-mqtt-reader-failure-recovery/` contains approved
snapshots, source hashes, four saved production mutants, initial/mutation/restored/
final TRX/log/JSON records, both full matrices and prior-outcome comparisons, library
build, final-verification.json, broker logs and cleanup.json.

This closes the scoped reader recovery characterization. MQTT writer failures,
cancellation, retention, concurrency, scheduler/rejection and wider lifecycle paths,
the four missing gateway integrations and broader release gates remain open.
Whole-feature production readiness is not established.

The next [MQTT writer failure-recovery batch](review/mqtt-writer-failure-recovery/README.md)
contains four review-only cases for original writer errors, explicit null-result
errors and same-producer recovery with native publication observation. Four mutations
propose fourteen intended failures. The draft is unbuilt/unrun outside the compiled
suite pending test-review approval.

## MQTT writer failure recovery (2026-10-30)

The approved [writer failure-recovery batch](review/mqtt-writer-failure-recovery/README.md)
is adopted unchanged. Initial runs on both frameworks pass the two throwing cases
and fail both null-result cases at the exact exception-type assertion: the actual
error is NullReferenceException. MQTTMessagePublisher now rejects a null mapped
result with InvalidOperationException before native publication. Writer-thrown
errors continue to propagate as their original instance. MqttPublication documents
both outcomes; no existing public signature changes.

All four focused cases pass on both frameworks. An independent native observer
subscribes before the failed send, then observes the next successful message's exact
default JSON bytes through the same producer. The additional 250 ms window records
no further delivery. The selected writer runs twice and the failed source ID,
binary body, topic and marker bag remain unchanged. This is bounded evidence for
these throw/null cases, not an unlimited duplicate-absence or outbox guarantee.

Four independent .NET 10 production-only mutations produce fourteen intended failures:

- Ignoring the configured writer fails all four non-null-error assertions.
- Suppressing mapping/null-result errors fails all four non-null-error assertions.
- Publishing default content before mapping fails all four first-native-payload
  assertions; the required mapping errors still reach the caller.
- Removing the null-result guard fails both null cases at the exception-type
  assertion with NullReferenceException; both throwing cases pass.

Exact byte restoration after each mutation returns four passes. Final focused
results are four passes on each framework. The approved test/helper and time bounds
are unchanged; no behavioral retry-until-green occurs.

| Framework | Passed | Failed | Skipped | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 99 | 0 | 4 | 103 | 4 |
| net10.0 | 99 | 0 | 4 | 103 | 4 |

Both complete assemblies run once on separate fresh, readiness-checked brokers.
All 99 preceding test identities and outcomes remain, plus four new passes. The
four nack-conformance skips under #4240 remain unchanged. All three MQTT library
targets build with zero warnings/errors. The initial test dependency build retains
738 existing warnings; no new/changed file has warnings, and the incremental green
build has zero warnings/errors. Existing MQTT CI selection covers these cases;
remote CI is not run here.

Of 6,501 preceding src/tests C# files, 6,499 remain byte-identical. Only the publisher
null guard and publication XML documentation change. Exactly the approved test/helper
are added, matching review/approval snapshots. Both production files match their
implemented snapshots after mutations. No dependency, lockfile or workflow changes
are made in this batch.

Native runs use the repository Mosquitto image pinned to
`efrecon/mosquitto@sha256:9f77138ef91011b49f1a10cabb0a919a5f1491b2c5f6ea91ddb29690237941e4`.
The broker is linux/amd64 on an arm64 host; SDK runners are native arm64. No host
port is bound. All three owned brokers are removed with logs retained; unrelated
containers are untouched. No hosted calls, credentials, commits, deployment or
publication occur.

Evidence: `/private/tmp/brighter-4170-mqtt-writer-failure-recovery/` contains approved
snapshots, source hashes, initial/green/mutation/restored/final TRX/log/JSON records,
saved mutants, complete matrices and preceding-outcome comparisons, library build,
`final-verification.json`, broker logs and `cleanup.json`.

This closes the scoped writer recovery correction. MQTT cancellation, retention,
concurrency, scheduler/rejection and wider lifecycle paths remain open. Four gateway
integrations and the broader release gates also remain open; the whole feature is
not production-ready.

The next [Redis publication writer batch](review/redis-publication-writer/README.md)
contains six review-only sync/async × legacy/custom/composed cases. Native storage
observations cover exact framing, source-topic routing, counters, two-queue fan-out,
expiry and source preservation. Five named mutations propose 26 intended assertion
failures. The draft is unbuilt/unrun outside the compiled suite pending test-review
approval. Redis production code and compiled tests are unchanged in this batch.

## Redis publication writer selection (2026-10-30)

The approved [Redis writer batch](review/redis-publication-writer/README.md) is adopted
unchanged. RedisMessagePublication now optionally selects an
IAmAWireMessageWriter<string>. DefaultRedisWireMessageWriter composes a fresh legacy
RedisMessagePublisher per conversion, avoiding accumulated serializer state. Both
producer send paths choose the configured/default writer before counter advancement,
storage and queue fan-out. Existing producer signatures and native delivery-control
code remain unchanged.

An initial baseline build exposed missing local ServiceStack assemblies. Locked
restore resolved that setup failure without dependency or lockfile changes. Both
complete unchanged baseline suites then pass 96 cases with four existing skips.
After adoption, the absent adapter causes the expected compilation failure. Adding
only the property/default adapter, without producer selection, produces four intended
payload failures and two legacy passes on each framework. Actual native strings
remain legacy envelopes where the custom/composed strings are required.

Minimal producer wiring makes all six cases pass on each framework. Each case sends
two distinct messages through one producer. Native Redis observation verifies exact
stored strings, source-header topic routing distinct from the publication topic,
counter advancement to two, ordered fan-out to two queues, an unused decoy route,
positive one-hour expiry, two configured invocations and unchanged source IDs,
Unicode text bodies, topics and marker bags. The default compatibility oracle is
an independently created instance of the existing public serializer, not a separate
specification of its format. Arbitrary binary encoding is not established.

Five independent .NET 10 production-only mutations produce 26 intended failures:

| Mutation | Intended failed assertion | Failed | Passed |
| --- | --- | ---: | ---: |
| Ignore the configured writer | First stored payload, custom/composed | 4 | 2 |
| Default adapter emits raw body | First stored payload, legacy/composed | 4 | 2 |
| Keep publication topic instead of source topic | Source counter equals two | 6 | 0 |
| Omit queue fan-out | First recipient queue contains IDs 1 and 2 | 6 | 0 |
| Omit stored-message expiry | First TTL is within 3000–3600 seconds | 6 | 0 |

Every production file is restored byte-for-byte after its mutation, followed by six
passes. Final focused results are six passes on each framework. The test/helper,
assertions and time bounds are unchanged. Async cleanup stack locations point beyond
the assertion even in an optimization-disabled diagnostic run; assertion kinds and
expected/actual value signatures establish the intended failures instead. Both
initial run sets are retained. The evidence parser initially expected untyped xUnit
collection output and a negative no-expiry sentinel; it was corrected to accept the
actual typed display and ServiceStack's maximum TTL value. These script corrections
change neither test assertions nor production behavior; saved mutation failures are
validated without rerunning those mutants.

| Framework | Baseline passed | Final passed | Failed | Existing skips | Final total |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 96 | 102 | 0 | 4 | 106 |
| net10.0 | 96 | 102 | 0 | 4 | 106 |

Both complete final suites run once on separate fresh, readiness-checked brokers,
without a category/Fragile filter. All 100 baseline identities and outcomes remain,
plus six new passes. The four existing nack-conformance skips remain unchanged.
All four library targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero warnings
and errors. The baseline/green test dependency graphs each report 604 existing
warnings; no new or changed source file has a warning. Existing redis-ci selection
includes these tests; no remote CI run is claimed.

Of 6,503 preceding src/tests C# files, 6,501 remain byte-identical. Only
RedisMessageProducer and RedisMessagePublication change. Exactly the default adapter,
approved test and approved helper are added. The shared RedisMessageGateway mutation
target is restored exactly; consumers, other transports, dependencies, lockfiles and
workflows remain unchanged in this batch.

Native validation uses the repository Redis image resolved to
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`;
its reported server version is 8.10.2. Broker and SDK runners use linux/arm64. No host
ports are bound. Unique test keys are cleaned individually. All five owned brokers
are removed with logs retained, and unrelated containers remain untouched. No hosted
calls, credentials, commits, deployment or publication occur.

Evidence: `/private/tmp/brighter-4170-redis-publication-writer/` contains approval and
source snapshots, restore/build logs, baseline and final TRX/log/JSON matrices,
initial/diagnostic/green/mutation/restored/final focused records, saved mutants,
image/version records, `final-verification.json`, broker logs and `cleanup.json`.

This closes successful Redis publication writer selection. Redis reader integration,
writer failures/nulls, consumer retry/rejection mapping, cancellation, scheduling,
concurrency and wider lifecycle gates remain open. RocketMQ, SQL Server and PostgreSQL
integrations and broader cross-transport release work remain open. Whole-feature
production readiness is not established.

The next [Redis subscription reader batch](review/redis-subscription-reader/README.md)
contains six review-only cases covering both public factories, legacy/custom/composed
mapping, native queue ownership, the in-flight acknowledgement gate and stored-input
preservation. Four mutations propose twenty intended failures. The draft remains
unbuilt/unrun outside the compiled suite pending test-review approval.

## Redis subscription reader selection (2026-10-30)

The approved [Redis reader batch](review/redis-subscription-reader/README.md) is
implemented. RedisSubscription.WireMessageReader selects an
IAmAWireMessageReader<string>; DefaultRedisWireMessageReader composes the legacy
RedisMessageCreator. Both public consumer factories capture the configured reader
through a new internal constructor. Existing public consumer signatures remain
unchanged. Both receive paths use the selected reader while retaining native queue
removal, native ID tracking and acknowledgement logic.

The initial build confirms the missing default adapter. Once the contract/default
adapter is present, the build exposes a fixture name collision between Brighter's
and ServiceStack's RedisSubscription. Qualifying the Brighter constructor resolves
it. The next run stops at the existing subscription configuration guard; the fixture
also needs requestType and the corresponding Reactor/Proactor messagePumpType.
Supplying these required arguments resolves setup. The original build/run failures
and each corrected snapshot are retained. No setup failure is counted as behavioral
RED. These constructor-reference/argument fixes are the only test changes: all six
cases, assertions and bounds are unchanged, and the helper is byte-identical to its
approval snapshot.

With the corrected fixture and selection unwired, both frameworks pass two legacy
cases and fail four custom/composed cases at the first mapped-ID assertion. The
legacy parser returns an empty identity for the custom formats. Minimum factory
and consumer wiring then makes all six cases pass on both frameworks.

A separate native client seeds two stored strings and queue IDs 1 and 2, independent
of Brighter publication. Both factory modes return the exact application IDs,
Unicode text bodies, application topic and markers for legacy/custom/composed
input. The application topic differs from the native topic, and mapped IDs differ
from native queue IDs. After the first receive, only ID 2 remains queued. Receiving
before acknowledgement raises ChannelFailureException without removing that ID;
acknowledging the mapped first message allows the second message. Acknowledging the
second permits a subsequent empty receive. Both stored strings remain unchanged,
and configured readers see exactly the original inputs in order.

Four independent .NET 10 production-only mutations produce twenty intended failures:

| Mutation | Intended failed assertion | Failed | Passed |
| --- | --- | ---: | ---: |
| Ignore configured readers | First mapped ID, custom/composed | 4 | 2 |
| Default reader replaces the body | First mapped body, legacy/composed | 4 | 2 |
| Replace application topic with native topic | First mapped topic | 6 | 0 |
| Omit in-flight tracking | ChannelFailureException before acknowledgement | 6 | 0 |

Failure types and source locations match the reviewed assertions. Each production
mutation is restored byte-for-byte and followed by six passes. Final focused
results are six passes on each framework. No assertions or time bounds are relaxed,
and no behavioral regression is retried until green.

| Framework | Passed | Failed | Existing skips | Total | New cases passed |
| --- | ---: | ---: | ---: | ---: | ---: |
| net9.0 | 108 | 0 | 4 | 112 | 6 |
| net10.0 | 108 | 0 | 4 | 112 | 6 |

Both full suites run once on separate fresh, readiness-checked brokers without a
category/Fragile filter. All 106 preceding identities and outcomes remain, plus six
new passes. Four existing nack-conformance skips remain unchanged. All four library
targets (netstandard2.0/net8.0/net9.0/net10.0) build with zero warnings/errors. The
test dependency build retains 604 existing warnings, with none in new/changed files.
Existing Redis CI selection includes these cases; no remote CI result is claimed.

Of 6,506 preceding src/tests C# files, 6,503 remain byte-identical. Only the
subscription, consumer and consumer factory change. Exactly the default reader,
test and helper are added. The corrected compiled/review fixture matches the
recorded mechanical transformation of the approval snapshot. All implemented
production files match their restored snapshots; the public consumer constructor
signature is verified unchanged. Dependency, lockfile and workflow hashes retain
their pre-batch values, including the pre-existing CI edit. The publication writer
implementation and tests are unchanged.

Native validation uses
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
reporting Redis 8.10.2. Broker and SDK runners use linux/arm64. No host ports are bound.
Unique keys are cleaned individually. All three owned brokers are removed with logs
retained; unrelated containers remain untouched. No hosted calls, credentials,
commits, deployment or publication occur.

Evidence: `/private/tmp/brighter-4170-redis-subscription-reader/` contains approval,
qualified and corrected fixture snapshots, setup/RED/green/mutation/restored/final
TRX/log/JSON records, source/protected-file hashes, saved mutants, full matrices and
preceding-outcome comparisons, library build, `fixture-correction.json`,
`final-verification.json`, broker logs and `cleanup.json`.

This closes successful Redis subscription reader selection and preserves the tested
in-flight acknowledgement behavior. It does not add broker settlement or crash
redelivery. Redis reader/writer failures, malformed/default parsing, retry/rejection
mapping, cancellation, connection recovery, concurrency and scheduling remain open.
RocketMQ, SQL Server and PostgreSQL integrations and broader release work also remain
open. Whole-feature production readiness is not established.

The next [Redis writer failure-recovery batch](review/redis-writer-failure-recovery/README.md)
contains four review-only throw/null cases. They require original writer errors,
explicit invalid-null errors before native mutation and same-producer recovery,
observed through counters, stored payloads and queue entries. Four named mutations
propose fourteen intended failures. The draft remains unbuilt/unrun outside the
compiled suite pending test-review approval.

## Redis writer failure recovery (2026-10-30)

The approved [four-case batch](review/redis-writer-failure-recovery/README.md)
is complete. On each framework, the initial throwing cases passed and both null
cases failed at `Assert.NotNull(error)`: null writer results silently succeeded.
The initial failures precede native-state assertions; they do not establish the
initial counter/storage/queue values. Both send paths now throw
InvalidOperationException on null before native mutation, retaining original writer
exceptions. Publication XML documentation records the contract.

All four unchanged approved cases pass on .NET 9 and .NET 10. Independent native
observations establish absent counter/payload and an empty queue after failure,
then exact legacy content at ID 1, counter 1 and one queue entry after recovery
through the same producer. Source identity, Unicode body, topic and marker remain
unchanged. Four independent .NET 10 mutations produced fourteen intended failures:
ignoring the writer (4), suppressing mapping errors (4), publishing before mapping
(4 at the absent-counter assertion), and accepting null as an empty string (2).
The premature-publication mutation preserved the required mapping error. Each
byte-exact production restoration returned four passes; final focused results pass
on both frameworks. No fixture, assertion or bound changes were needed.

Both unfiltered complete suites ran once on separate fresh ready brokers:
**112 passed, zero failed, four existing skips** each. All 112 preceding identities
and outcomes are retained, with four new passes. The four existing nack-conformance
skips are unchanged. Redis library builds pass on netstandard2.0, net8.0, net9.0
and net10.0 with zero warnings/errors. The initial dependency build reports 604
existing warnings, none in new/changed files; the incremental green build is clean.
Existing CI selection includes the cases; hosted CI was not run.

Source verification retains 6,507 of 6,509 preceding C# files byte-for-byte, changing
only RedisMessageProducer and RedisMessagePublication. The two approved test/helper
additions match their review snapshots exactly. All dependency, lockfile and workflow
hashes match the pre-batch state, including pre-existing edits. Public signatures
remain unchanged.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. TRX, logs, mutation
snapshots, source/protected-file hashes, final verification and cleanup records are
retained in `/private/tmp/brighter-4170-redis-writer-failure-recovery/`.

Redis reader error handling, consumer retry/rejection mapping, native failures after
partial publication, cancellation, concurrency and wider lifecycle coverage remain
open, alongside the other release workstreams. This does not establish whole-feature
production readiness.

The next [Redis reader failure-recovery draft](review/redis-reader-failure-recovery/README.md)
contains four sync/async × throw/null cases requiring a rejectable message retaining
native content/identity, observed invalid-channel publication and same-consumer
recovery. Four reviewed mutations propose sixteen intended failures. It remains
outside the compiled suite, unbuilt/unrun pending test-review approval.

## Redis reader failure recovery (2026-10-30)

The approved [four-case batch](review/redis-reader-failure-recovery/README.md)
is complete. Both frameworks reproduced four intended failures at the captured
receive-error assertion: configured InvalidOperationException escaped Receive and
null reader results caused NullReferenceException. These failures establish the
missing configured-reader boundary; they are not fixture or compilation failures.

Both receive paths now convert configured-reader throw/null into rejectable failure
messages retaining the exact raw stored string, native queue ID and source topic.
Unconfigured default parsing and native connection/command calls remain outside
the mapping catch. Existing public constructors remain unchanged; subscription XML
documentation describes failure conversion and destructive queue removal.

All four unchanged approved cases pass on .NET 9 and .NET 10. Independent native
observations establish that the second source ID remains queued, explicit rejection
publishes one invalid-channel message with original content/identity and rejection
metadata, and the same consumer subsequently reads and acknowledges the valid
delivery. The configured reader sees exactly the original two inputs. This is
single-line Unicode rejection evidence, not arbitrary multiline/binary round-trip
coverage or invalid-channel failure recovery.

Four independent .NET 10 mutations produced sixteen intended assertion failures:
default-parser fallback (4 at failure type), empty native ID (4), empty raw body (4),
and skipped invalid-channel send while reporting successful rejection (4 at the
native counter). Each byte-exact production restoration returned four passes.
Final focused results pass on both frameworks; no approved cases, assertions, helper
or time bounds changed.

Both unfiltered complete suites ran once on separate fresh ready brokers:
**116 passed, zero failed, four existing skips** each. All 116 preceding identities
and outcomes are retained with four new passes. The existing nack-conformance skips
are unchanged. The Redis library builds on netstandard2.0, net8.0, net9.0 and
net10.0 with zero warnings/errors. Initial and green test dependency builds each
report 604 existing warnings, none in new/changed files. Existing CI selection
includes the cases; hosted CI was not run.

Of 6,511 preceding C# files, 6,509 remain byte-identical; only RedisMessageConsumer
and RedisSubscription changed. The two compiled test/helper additions match their
approved snapshots exactly. Dependency, lockfile and workflow hashes match the
pre-batch state, including pre-existing edits.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after saving logs, leaving unrelated containers untouched. TRX, logs, mutation
snapshots, source/protected-file hashes, verification and cleanup evidence are
retained in `/private/tmp/brighter-4170-redis-reader-failure-recovery/`.

The next [reader error-boundary draft](review/redis-reader-error-boundaries/README.md)
contains eight cases covering unconfigured malformed JSON and empty-message
sentinels, configured default-parser composition failures, and native WRONGTYPE
errors followed by same-consumer recovery. Four named mutations propose eight
intended failures. The draft remains outside the compiled suite, unbuilt/unrun
pending test-review approval.

Retry/rejection writer selection, cancellation, connection recovery, concurrency,
automatic pump behavior and wider lifecycle paths remain open, alongside the other
gateway and release workstreams. Whole-feature production readiness remains open.

## Redis reader error boundaries (2026-10-30)

The approved [eight-case characterization](review/redis-reader-error-boundaries/README.md)
is complete. All eight cases passed initially on both frameworks, without a
production correction. The approved test/helper, cases, assertions and time bounds
remain byte-identical.

The native cases establish four sync/async paths: unconfigured malformed JSON still
raises JsonException; an invalid envelope prefix retains the empty-message sentinel;
a configured reader composing the default parser converts malformed JSON into a
failure preserving native ID/topic and complete multiline content; and a native
WRONGTYPE queue command produces ChannelFailureException with a RedisException inner
error without calling the configured reader. After each outcome, including repair
of only the owned wrong-type key, the same consumer reads and acknowledges a valid
message and reaches an empty receive. Stored source strings remain unchanged.

Four independent .NET 10 mutations produced eight intended failures: catching
unconfigured parser errors (2), bypassing the configured catch (2), replacing legacy
sentinels (2), and swallowing native RedisException as an empty receive (2). Each
mutation's other six cases passed. Every byte-exact restoration returned eight
passes, and final focused results pass on both frameworks. Compilation, setup and
timeout failures were not counted.

Both unfiltered full suites ran once on separate fresh ready brokers:
**124 passed, zero failed, four existing skips** each. All 120 preceding identities
and outcomes are retained, with eight new passes. The existing nack-conformance
skips are unchanged. The Redis library builds on netstandard2.0, net8.0, net9.0
and net10.0 with zero warnings/errors. The initial test dependency build reports
604 existing warnings, none in the new files; the final restored build is clean.
Existing CI selection includes these cases; hosted CI was not run.

All 6,513 preceding C# files remain byte-identical. Only the approved test/helper
are added to the compiled sources. Dependency, lockfile and workflow hashes match
the pre-batch state, including pre-existing edits.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. TRX, logs, mutation
snapshots, source/protected-file hashes, verification and cleanup records are
retained in `/private/tmp/brighter-4170-redis-reader-error-boundaries/`.

The next [immediate requeue-writer draft](review/redis-immediate-requeue-writer/README.md)
combines ten sync/async legacy/custom/composed/throw/null cases. It proposes native
ID/routing/counter/expiry preservation, no sibling fanout, mapping failure before
native changes, retained in-flight state on failure and successful recovery through
the same consumer. Four named mutations propose 32 intended failures. A missing
public selection property is expected to need compilation scaffolding before
behavioral RED; the review plan distinguishes those stages. The draft remains
outside the compiled suite, unbuilt/unrun pending test-review approval.

Other parser/native errors, connection recovery, cancellation, concurrency,
arbitrary multiline/binary rejection round trips, retry/rejection writer selection,
schedulers and automatic pump behavior remain open alongside the other gateway and
release workstreams. Whole-feature production readiness remains open.

## Redis immediate requeue writer (2026-10-30)

The approved [ten-case batch](review/redis-immediate-requeue-writer/README.md) is
complete. Both frameworks initially reported CS0117 for the missing public
RequeueWireMessageWriter property. Adding only the approved nullable auto-property
allowed behavioral RED: four custom/composed native-string failures and four missing
throw/null errors, with two legacy passes per framework. The compile failures were
recorded separately and were not counted as behavioral evidence.

Both factories now capture an immediate retry writer per consumer. Null selection
uses the default adapter. Both immediate paths map and validate the string before
storing it at the existing source ID, enqueueing that ID on the source queue and
releasing the in-flight gate. Original writer exceptions propagate; null throws
InvalidOperationException before native mutation. Existing public constructors and
delayed/scheduler-backed retry mapping are unchanged. Subscription XML documentation
records the selected-writer contract and delayed-retry boundary.

All ten unchanged approved sync/async legacy/custom/composed/throw/null cases pass
on both frameworks. Independent native observations establish exact strings, native
ID reuse, source-topic storage despite a different application topic, unchanged
counter, configured expiry, no sibling fanout and source preservation. Mapping
failure leaves the previous stored payload and in-flight gate intact; retry through
the same consumer succeeds. The observer consumes the inspected retry before a
separate valid legacy delivery establishes gate release. This does not claim that
the default reader understands arbitrary custom retry formats.

Four independent .NET 10 mutations produced 32 intended failures: ignoring the
writer (8, with two legacy passes), enqueueing before mapping (4, with six unaffected
passes), skipping payload replacement (10), and retaining the in-flight entry after
success (10). Every byte-exact restoration returned ten passes. Final focused
results pass on both frameworks. No approved test/helper, fixture, assertion or
time-bound changes were needed.

Both complete suites ran once, unfiltered, on separate fresh ready brokers:
**134 passed, zero failed, four existing skips** each. All 128 preceding identities
and outcomes are retained, with ten new passes. Existing nack-conformance skips are
unchanged. Redis library builds pass on netstandard2.0, net8.0, net9.0 and net10.0
with zero warnings/errors. Initial and green dependency builds each reported 604
existing warnings, none in new/changed files; the final restored build was clean.
Existing CI selection includes the tests; hosted CI was not run.

Of 6,515 preceding C# files, 6,512 remain byte-identical; only RedisMessageConsumer,
RedisMessageConsumerFactory and RedisSubscription changed. The two compiled
test/helper additions match the approved snapshots. Dependency, lockfile and
workflow hashes match the pre-batch state, including pre-existing edits.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. Compile/behavioral
failure logs, TRX, mutation snapshots, source/protected-file hashes, verification
and cleanup records are retained in
`/private/tmp/brighter-4170-redis-immediate-requeue-writer/`.

The next [retry-writer isolation draft](review/redis-requeue-writer-isolation/README.md)
contains four sync/async × legacy-first/custom-first cases. Three consumers share
one factory; later subscription-setting changes must not replace their captured
selections. Two distinct retries per endpoint establish repeated native payload
isolation. Three mutations propose twelve intended failures. The draft remains
outside the compiled suite, unbuilt/unrun pending test-review approval.

Endpoint isolation, concurrent mapping, cancellation, native errors after partial
progress, custom retry-reader compatibility, delayed retry/rejection writer selection
and broader lifecycle paths remain open, alongside the other gateway and release
workstreams. Whole-feature production readiness remains open.

## Redis retry-writer isolation (2026-10-30)

The approved [four-case characterization](review/redis-requeue-writer-isolation/README.md)
is complete. All four cases passed initially on .NET 9 and .NET 10 without production
corrections. Approved tests, helper, fixtures, assertions and bounds remain unchanged.

Four sync/async × legacy-first/custom-first cases exercise 24 immediate native
requeues. Three consumers share one factory and retain their captured
legacy/custom/composed writers after subscription settings are replaced with a
decoy. Each endpoint retries two distinct messages; native observations verify exact
strings, reused IDs and queue order without a publication counter. Removing each
retry from the native list leaves the next original delivery available to the same
consumer. Each selected writer runs twice and the decoy never runs.

Three independent .NET 10 mutations produced twelve intended native-string failures:
caching the first factory selection including null (4), resolving the subscription's
current setting on every retry (4), and accumulating serializer content between
default/composed conversions (4). Every byte-exact restoration returned four passes;
final focused results pass on both frameworks. Setup/compiler/timeout failures were
not counted as mutation evidence.

Both unfiltered complete suites ran once on separate fresh ready brokers:
**138 passed, zero failed, four existing skips** each. All 138 preceding identities
and outcomes are retained, with four new passes. The existing nack-conformance skips
are unchanged. Redis library builds pass on netstandard2.0, net8.0, net9.0 and
net10.0 with zero warnings/errors. Initial and final restored dependency builds each
reported 604 existing warnings, none in the new files. Existing CI selection includes
the cases; hosted CI was not run.

All 6,517 preceding C# files remain byte-identical; only the approved test/helper
are added to compiled sources. Dependency, lockfile and workflow hashes match the
pre-batch state, including pre-existing edits.

Native linux/arm64 Redis 8.10.2 used pinned image
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. TRX, logs, mutation
snapshots, source/protected-file hashes, verification and cleanup records are
retained in `/private/tmp/brighter-4170-redis-requeue-writer-isolation/`.

The next [rejection-writer draft](review/redis-rejection-writers/README.md) combines
twenty sync/async × invalid/dead-letter × legacy/custom/composed/throw/null cases.
It requires independent writer selection, native forwarding with rejection metadata,
mapping failure before native changes, restoration of source topic and retention
of its in-flight entry on mapping failure, and same-consumer recovery. Existing
native publication error handling remains a separate boundary. Four mutations
propose 64 intended failures. Missing public-property compilation scaffolding is
distinguished from behavioral RED. The draft remains outside the compiled suite,
unbuilt/unrun pending test-review approval.

Concurrent mapping, cancellation, native errors after partial progress, custom
retry-reader compatibility, delayed retry/rejection writer selection and broader
lifecycle paths remain open alongside the other gateway and release workstreams.
Sequential endpoint isolation does not establish concurrency safety. Whole-feature
production readiness remains open.

## Redis rejection writers (2026-10-30)

The approved [twenty-case rejection-writer batch](review/redis-rejection-writers/README.md)
is implemented and validated. Both factories capture independent invalid-message and
dead-letter writer selections. Null selection retains legacy encoding. Mapping sees
the chosen destination and refreshed rejection metadata. Mapping exceptions propagate
unchanged, null results throw, and both restore the source topic while retaining its
in-flight entry for a second rejection attempt. Internal mapped-send methods reuse
the producer's native publication code without mapping twice. Existing native-error
handling remains logged, returns true and releases the entry; this is not a guarantee
of forwarding. Public constructors are unchanged; two nullable subscription properties
are the public API additions.

Missing-property compilation errors were recorded separately. Property-only scaffolding
then produced sixteen intended behavioral failures and four legacy passes on each
framework. Optimized traces omitted payload assertion locations. Two incremental
diagnostic attempts failed location verification (missing or preceding sequence points);
a clean diagnostic rebuild verified all sixteen intended assertion lines on both
frameworks. These retained diagnostic attempts are not additional defects or mutation
failures. Approved test/helper bytes, fixtures, assertions and time bounds never changed.

All twenty cases pass on both frameworks. Four independent production mutations
produced 64 intended assertion failures: ignored writers (16), premature in-flight
removal (8), wrong destination (20), and empty native payload (20). Expected unaffected
cases passed, and each byte-exact restoration returned twenty passes. Final focused
runs used a clean optimized rebuild with diagnostic JIT settings. Both full suites
used those optimized assemblies with ordinary JIT and ran once on separate fresh
brokers: **158 passed, zero failed, four existing skips each**. All 142 preceding
identities/outcomes were retained, with twenty new passes per framework.

The Redis library builds for netstandard2.0, net8.0, net9.0 and net10.0 with zero
warnings/errors. Test dependency builds reported 604 existing warnings, none in
new/changed files. Hosted CI was not run. Of 6,519 preceding C# sources, 6,515 are
byte-identical; only consumer, factory, producer and subscription changed. Exactly
two compiled test/helper sources were added, matching approved snapshots. Dependency,
lockfile and workflow hashes remain unchanged.

Validation used native arm64 Redis 8.10.2 pinned to
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. Evidence, including TRX,
diagnostic findings, mutations/restorations, scoped source diff, source/protected-file
verification and cleanup, is retained in
`/private/tmp/brighter-4170-redis-rejection-writers/`.

The next [twelve-case rejection-boundary draft](review/redis-rejection-boundaries/README.md)
covers default/custom DLQ fallback, absent/ineligible destinations and native counter
failures through both routes. It reuses the unchanged approved helper and proposes
four mutations with sixteen intended failures. It remains outside the compiled suite,
unbuilt/unrun pending review. It characterizes the existing lossy native-error behavior,
not reliable forwarding or crash redelivery.

Rejection fallback/native-error boundaries and endpoint isolation, delayed retry mapping,
concurrency, cancellation, native failures after partial progress and broader lifecycle
coverage remain open alongside other gateway, scheduler, configuration and release
workstreams. Whole-feature production readiness remains open.

## Redis rejection boundaries (2026-10-30)

The approved [twelve-case boundary batch](review/redis-rejection-boundaries/README.md)
is characterized and validated without production changes. Sync/async cases establish
legacy/custom DLQ fallback, no eligible destination and native INCR failure through
both rejection routes. Fallback selects the DLQ writer; absent/ineligible destinations
invoke neither writer. A noninteger native counter makes publication fail after mapping:
Reject keeps its existing true/no-throw result, publishes no payload, preserves the
counter and releases the consumer to receive its next source message. A true result
therefore does not guarantee forwarding. Source payloads remain unchanged.

All twelve unchanged approved cases passed initially on both frameworks. Four
independent .NET 10 mutations produced sixteen intended failures at the reviewed
assertion lines: wrong fallback writer (4), writer invocation without a destination
(4), propagated native errors (4), and retained in-flight state after native failure
(4). Each mutation retained eight unaffected passes. Diagnostic rebuilds/JIT settings
established exact assertion locations. Each byte-exact restoration returned twelve
passes. No test/helper, fixture, assertion or time-bound changes were needed.

Final focused runs used a clean optimized rebuild with ordinary JIT and passed twelve
cases per framework. Both unfiltered full suites ran once on separate fresh brokers:
**170 passed, zero failed, four existing skips each**. All 162 preceding identities
and outcomes were retained, with twelve new passes. Four library targets
(netstandard2.0/net8.0/net9.0/net10.0) built with zero warnings/errors. Initial and
final test dependency builds each reported 604 existing warnings, none in the new
file. Hosted CI was not run.

All 6,521 preceding C# sources remain byte-identical. Only the approved test was
added to compiled sources; it matches the reviewed snapshot and reuses the unchanged
approved helper. Dependency, lockfile and workflow hashes remain unchanged. Validation
used native arm64 Redis 8.10.2 pinned to
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`,
native Docker SDK runners and no host ports. All three owned brokers were removed
after saving logs; unrelated containers were untouched. TRX, logs, mutations,
restoration and source/protected-file verification, runtime settings and cleanup
records are retained in `/private/tmp/brighter-4170-redis-rejection-boundaries/`.

The next [four-case isolation draft](review/redis-rejection-writer-isolation/README.md)
checks both captured writer selections across three legacy/custom/composed endpoints,
in two creation orders, and repeated forwarding through both routes (48 forwards).
It reuses the unchanged helper and proposes three mutations with twelve intended
failures. The draft remains outside the compiled suite, unbuilt/unrun pending review.

Rejection endpoint isolation, fallback mapping failure, native failures after partial
progress, delayed retry mapping, concurrency, cancellation and broader lifecycle
coverage remain open alongside other gateway, scheduler, configuration and release
workstreams. These native-error cases establish neither atomic forwarding nor crash
redelivery. Whole-feature production readiness remains open.

## Redis rejection-writer isolation (2026-10-30)

The approved [four-case isolation batch](review/redis-rejection-writer-isolation/README.md)
passes focused and mutation validation without production changes. Sync/async × two
endpoint orders exercise 48 forwards across legacy/custom/composed writers, capturing
both rejection selections before settings are replaced. Exact native payloads,
independent counters/queues, unchanged source payloads and delivery order are checked;
the decoy is unused and repeated serialization does not accumulate earlier content.

All four cases pass initially and after clean optimized rebuilds on both frameworks.
Three independent .NET 10 mutations produce twelve intended native-string failures:
cache the first two selections (4), resolve live subscription settings (4), accumulate
serializer output (4). Every byte-exact restoration returns four passes. Diagnostic
builds/JIT settings verify the exact assertion line; final focused/full runs use
ordinary JIT. Approved test/helper, fixture, assertions and bounds are unchanged.

Both complete suites ran once on separate fresh brokers: **172 passed, two failed,
four existing skips each**. All four new cases pass. Both failures are prior async
RedisReaderFailureRecoveryTests cases (returnNull true/false), at Assert.Null(error)
on line 101, with ServiceStack.LicenseException reporting the 6,000 Redis requests/hour
free quota. All 174 prior identities remain; 172 prior outcomes are unchanged and two
prior passes fail. The full regression gate remains incomplete. No reruns or quota
bypasses were attempted. The earlier 170-pass/four-skip baseline remains historical
evidence, not proof that this expanded suite passes.

All four library targets build with zero warnings/errors. The standalone sandboxed
build initially stalled/exited nonzero; an escalated build succeeded. Initial/final
test dependency builds report 604 existing warnings, none in the new file. All 6,522
preceding C# files remain byte-identical; only the approved test is added and its
helper is unchanged. Dependency, lockfile and workflow hashes remain unchanged.
Native arm64 Redis 8.10.2 was pinned to
`redis@sha256:2c2dff791878316e3b083188be0d578423b24813cdda5de0e80ea4f7b3e6bfd6`;
native Docker SDK runners exposed no host ports. All three owned brokers were removed
after logs were saved; unrelated containers were untouched. Evidence is retained in
`/private/tmp/brighter-4170-redis-rejection-writer-isolation/`.

No repository license registration or session SERVICESTACK_LICENSE variable was found;
the runner passes no license variable. ServiceStack documents the [quota](https://servicestack.net/download)
and [license registration](https://docs.servicestack.net/register). A valid license
must be made available and passed to the runner before a licensed full-suite retry.
The user was asked for an existing license file location or secret name, not the key.
Hosted CI was not run.

The next [fallback recovery draft](review/redis-rejection-fallback-recovery/README.md)
contains four throw/null cases and proposes sixteen mutation failures. It is outside
compiled sources, unbuilt/unrun, with execution deferred until this regression gate
and its own review are complete. Delayed retry mapping, native failure windows,
concurrency, cancellation, other gateway integrations and wider release work remain
open. This batch and whole-feature production readiness are incomplete.

Licensed rerun preparation (2026-10-30): the local evidence directory now includes
`licensed-regressions.py` and `licensed-rerun-inputs.json`, pinning 6,523 current C#
sources and 245 optimized assembly/output files plus protected inputs. The script
accepts a user-supplied license file or SERVICESTACK_LICENSE, forwards the variable
by name to Docker, uses a new evidence directory and fresh brokers, preserves the
quota-failure runs and checks all prior outcomes plus the four new cases. Syntax
and missing-license preflight checks pass; missing input starts no Docker commands
and creates no result directory. No licensed retry, new application test or container
was run during preparation. License availability remains the blocking input.

Paced regression amendment (2026-10-30): the user explicitly requested rerunning the
unchanged tests without payment and confirmed smaller batches with real waiting
periods. The new local runner in `/private/tmp/brighter-4170-redis-paced-regressions/`
is active. Four batches contain 89 cases each, two per framework. Static coverage
verification schedules all 178 case identities exactly once per framework. The first
batch is no earlier than 21:24:52 UTC, leaving a full hour plus five seconds after
the prior full runs; later batches wait 3,605 real seconds after the preceding batch
finishes. Each uses a fresh owned broker, ordinary JIT and the unchanged optimized
assemblies, tests and timeouts. Input hashes are verified before every batch.

This is a user-approved change to the validation execution plan, not a change to
application behavior or quota enforcement. Combined results will establish batched
regression coverage and must not be reported as a passing single-process full run.
Expected combined counts are 174 passes/four existing skips per framework. The runner
retains TRX/logs, stops on any failure or input mismatch, and removes owned brokers.
`status.json` reports progress; successful completion creates `summary.json`.
At launch no batch has completed; the regression task remains unchecked. A license
key is not required for this paced plan. Host/Docker availability affects completion.

## Redis paced regression completion (2026-10-31)

The user-approved paced regression amendment is complete. Four disjoint batches of
89 cases each ran on fresh brokers with the unchanged optimized assemblies and
ordinary JIT. Each batch passed 87 cases with two existing skips. Combined results
for each of .NET 9 and .NET 10 are **174 passed, zero failed, four existing skips**.
Every one of the 178 identities ran exactly once per framework, preserving all 174
preceding outcomes and adding four isolation passes. Both formerly quota-failing
async reader-recovery cases pass on both frameworks. No batch reported a quota error.

Completion was recorded at 2026-10-31 03:11:08 UTC. Actual idle intervals before the
batches were 3,606, 11,279, 4,597 and 4,507 seconds (rounded), all above the 3,605-second
minimum. All four owned brokers and their runners were removed; cleanup succeeded
for every batch. No license was supplied and no quota enforcement or counters were
modified. Code, dependencies, tests and timeouts are unchanged. Independent verification
on 2026-10-31 confirms current source, protected-input and assembly hashes still match
the pinned inputs, as well as exact TRX coverage/outcomes and real-time spacing.

This clears the approved batched regression gate and completes rejection-writer
isolation. It does not turn the earlier failed unfiltered runs into passing runs or
establish cross-batch process-state interactions. Historical single-process evidence
remains 172 passes/two quota failures/four skips per framework. The preceding focused
passes, twelve intended mutation failures with exact restorations, and four clean
library builds remain applicable because the verified inputs were unchanged. Hosted
CI was not run.

The plan, TRX, logs, broker statistics, cleanup records, `summary.json` and independently
checked `final-verification.json` are retained in
`/private/tmp/brighter-4170-redis-paced-regressions/`. The task checklist and current
review now reflect completion. The subsequent four-case fallback recovery draft
remains outside compiled sources, unbuilt/unrun pending its own test review, and its
regression plan now follows the user's paced-validation preference. Fallback mapping
failure, delayed retry mapping, other gateway integrations and wider lifecycle/release
coverage remain open; whole-feature production readiness is not established.


## Redis fallback regression host interruption (2026-11-01)

The first paced .NET 9 batch stopped with 86 passes, two existing skips and one
failure in the existing synchronous rejection-boundary `no-channels` case. No
quota error was reported; both owned containers were removed. Later batches did
not run. The new fallback recovery cases were not part of this first batch.

The failed case spans 00:24:45–01:24:42 UTC in TRX wall timestamps but reports only
307 ms of execution. It fails when rereading an original source payload seeded with
a ten-minute expiry. Redis reports 37 expired keys. macOS power logs confirm repeated
sleep periods in this interval. This supports expiry during host suspension; it does
not establish a production regression. The failed TRX, native logs, and diagnosis
remain in `/private/tmp/brighter-4170-redis-rejection-fallback-recovery/paced` and
`sleep-interruption-diagnosis.json` in its parent directory.

After investigation, all four unchanged batches are rerunning from scratch under
`paced-awake/`, using identical verified assemblies, a temporary `caffeinate -i -s`
assertion and host-clock discontinuity detection. The lid must remain open; this
assertion cannot override lid closure. Real waits of at least 3,605 seconds remain.
No production, dependency, test, assertion, expiry or timeout changes were made.
Completion is still pending; the failed attempt is not counted as a passing gate.

The first sleep-protected rerun batch passed 87 cases/two existing skips, including
the previously failing unchanged case. Runner wall/monotonic durations both measure
96.45 seconds, with no clock discontinuity or quota error. Owned containers are
removed. Three scheduled batches remain; this is not a completed regression gate.


## Redis fallback recovery completion (2026-11-01)

The approved four sync/async × throw/null fallback cases pass initially and after
restoration on both frameworks. Four independent production-only mutations produce
sixteen failures at the reviewed assertions; each exact restoration returns four
passes. No production correction is needed. Final optimized focused runs pass four
cases per framework, and all four library targets build with zero warnings/errors.
The test dependency rebuild retains 604 existing warnings, none in the new test.

The user-approved paced regressions finish with **178 passes, zero failures and four
existing skips per framework**. Every one of the 182 cases runs exactly once per
framework; all 178 prior outcomes are retained and the four new cases pass. Real
idle periods before the batches exceed one hour. No quota errors or license were
needed; owned containers are removed. All 6,523 preceding C# files, eight protected
inputs, approved test/helper bytes and final regression assemblies retain their
recorded hashes. This establishes complete batched coverage; it does not claim a
single-process full-suite pass. The fallback batch is complete; delayed retries,
broader lifecycle coverage and the full feature's other release gates remain open.

Evidence: `/private/tmp/brighter-4170-redis-rejection-fallback-recovery`, with the
independent final result in `paced-awake/final-verification.json`. No hosted CI, commit,
push, deployment or publication is claimed.


## Redis scheduled retry dispatch focused validation (2026-11-01)


The user approved this batch. The test was adopted unchanged; both reused helpers
remain byte-identical. Initial and final ordinary optimized runs each pass four
cases on .NET 9 and .NET 10. Three independent production-only mutations produce
all ten reviewed assertion failures; each exact restoration returns four passes.
No production correction is needed. All 6,524 preceding C# files and eight protected
dependency/workflow inputs retain their snapshot hashes. Four Redis library targets
build with zero warnings/errors. Test dependency rebuilds retain 604 existing
warnings, with none in the new test. The owned focused broker is removed.

Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-dispatch/`. The complete
regression schedule and status are in `paced/plan.json` and `paced/status.json`.
The first batch is eligible at 2026-11-01T21:12:23Z, then each subsequent batch waits
at least 3,605 real seconds after the preceding batch finishes. Each framework has
an 89-case batch and a 97-case batch, with expected combined totals of 182 passes
and four existing skips. Temporary idle-sleep prevention and host clock-discontinuity
checks apply; the Mac lid must remain open. The batch is not complete until all
186 cases per framework and all prior outcomes are independently verified.


## Redis scheduled retry dispatch completion (2026-11-02)

Four approved sync/async × custom/default cases pass through native Redis and the
production in-memory scheduler, command processor, scheduler handler and outbox
mediator. They establish publication writer selection at dispatch, delayed native
visibility, next-message progress, native counter/recipient fan-out, expiry and
preservation of original native payloads. No production correction is needed.

Three production-only mutations produce ten intended assertion failures: two
custom-payload failures when the publication writer is ignored (two default cases
still pass), four next-receive failures with a retained in-flight entry, and four
empty-native-string failures. Each exact restoration returns four passes. Final
optimized focused runs pass four cases per framework; all four Redis library targets
build with zero warnings/errors. Approved tests/helpers and all 6,524 preceding C#
files and protected inputs are unchanged.

Complete paced regressions pass **182 cases, zero failures and four existing skips
per framework**. All 186 cases run exactly once per framework, preserving all 182
prior outcomes. Real idle intervals are at least 3,605 seconds, with no quota or
clock-discontinuity errors; owned containers are removed. This is complete batched
coverage, not a single-process full-suite pass. Evidence and independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-dispatch/paced/final-verification.json`.

This closes registered-publication dispatch characterization only. Per-subscription
delayed writer selection, queue-specific scheduled retry routing, callback failure
recovery, durable schedulers and the wider feature release gates remain open.
No hosted CI, commit, push, deployment or publication is claimed.


## Redis subscription-selected scheduled retries: final focused validation (2026-11-02)

The approved five-file Redis implementation passes all four native focused cases
on .NET 9 and .NET 10, including final ordinary optimized builds. The preceding
property-only implementation produced four intended counter assertion failures per
framework. Five independent mutations produce all eighteen reviewed assertion
failures; exact restoration returns four passes after every mutation. Four Redis
library targets build with zero warnings/errors. The test build retains 604 existing
warnings. The approved test/helpers, 6,520 other prior C# files and protected
inputs are unchanged. The focused broker is removed.

The complete paced regression schedule is running, with its first batch eligible at
2026-11-02T18:27:19.877436+00:00. Four disjoint native batches use real 3,605-second
idle intervals, temporary idle-sleep prevention and automatic owned cleanup. New
cases join the smaller prior batch to preserve free-quota headroom. Expected coverage
is 190 cases exactly once per framework: 186 passes and four existing skips, with
all 186 prior outcomes retained. These outcomes are pending, not yet claimed.
Keep the laptop lid open; idle-sleep prevention cannot prevent lid sleep.

Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-selection/focused-verification.json`.
Live schedule: `/private/tmp/brighter-4170-redis-scheduled-retry-selection/paced/status.json`.
Successful completion independently checks identities, outcomes, source/assembly
hashes, real idle gaps and cleanup before updating local completion reports.
Failure stops later batches and retains evidence. Whole-feature production readiness
is not established; multi-endpoint isolation, invalid configuration, scheduler
failure/cancellation/persistence and broader release gates remain open.


## Redis subscription-selected scheduled retries completion (2026-11-02)

The four approved sync/async × custom/default cases pass through native Redis and
the production in-memory scheduler, command processor, scheduler handler and outbox
mediator. Redis subscriptions capture an optional scheduled publication topic; the
registered publication selects its writer, native destination topic and single queue.
The original application topic/type and caller message are preserved. Null options
retain existing routing, and the public consumer constructor remains compatible.
Only five Redis production files change; shared Core and scheduler APIs are unchanged.

Initial behavior fails at the intended native source counter assertion on both
frameworks. Five independent production mutations then produce eighteen intended
assertion failures, with four passes after each exact restoration. Initial and final
optimized focused runs pass four cases per framework; all four Redis library targets
build with zero warnings/errors. The approved test/helpers, 6,520 other preceding C#
files and protected dependency/workflow inputs remain unchanged.

Complete paced regressions pass **186 cases, zero failures and four existing skips
per framework**. All 190 cases run exactly once per framework, preserving all 186
prior outcomes. Real idle intervals are at least 3,605 seconds, with no quota or
clock-discontinuity errors; owned containers are removed. This is complete batched
coverage, not a single-process full-suite pass. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-selection/paced/final-verification.json`.

This closes the reviewed single-endpoint subscription selection and queue-specific
routing batch. Shared-factory multi-endpoint isolation, invalid/missing configuration,
submission/callback failure recovery, cancellation, durable schedulers and broader
feature release gates remain open. No hosted CI, commit, push or deployment is claimed.


## Approved isolation batch: focused validation complete; regressions pending

Four cases pass initially and finally on .NET 9/.NET 10. All twelve intended
mutation failures and exact restorations are verified. Four Redis library targets
build without warnings/errors; all preceding production/test sources and protected
inputs remain unchanged. The approved test alone is added and the focused broker
is removed. Complete paced regressions are queued; no full regression pass is claimed.
Live evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced/status.json`.

First regression eligibility: 2026-11-02T23:45:51.175528+00:00.


## Isolation regression sleep interruption and unchanged rerun (2026-11-03)

The first paced regression attempt stopped during net9.0-batch-1: 91 passed,
four failed and two existing skips. Two new isolation cases saw an empty receive;
two existing reader/immediate-retry cases saw expired native payloads. The runner
reported 24,597 wall seconds versus 176 monotonic seconds and rejected the clock
discontinuity. macOS power logs confirm lid-closed sleep at 00:14 local time and
repeated maintenance sleep during the batch. Redis recorded 51 expired keys and
no quota error. Owned containers were removed. This is not a passing regression
result, and the original evidence remains under
`/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced/`.

On the user's continuation, the complete unchanged suite was restarted after the
required real idle interval had already elapsed. Source inventory, all pinned source,
dependency and assembly hashes, and approved tests/helpers match. No timeout, TTL,
dependency, assertion, license or clock setting was changed. The new run retains
3,605-second gaps between all batches and temporary idle-sleep prevention. The lid
must stay open because caffeinate cannot override clamshell sleep.

Current status: `/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced-awake/status.json`.
The failed attempt is preserved alongside the rerun. A clean rerun must establish
all 194 identities exactly once per framework, 190 passes/four existing skips and
all 190 preceding outcomes before completion is claimed. Focused/mutation checks
and four-target builds remain valid; the complete regression gate remains pending.


## Redis scheduled retry endpoint isolation completion (2026-11-03)

Four approved sync/async × endpoint-order cases pass through native Redis and the
production in-memory scheduler, command processor, handler and outbox mediator.
Two subscriptions share a factory, registry, scheduler and native topic while their
captured aliases retain independent default/composed writers and destination queues.
Repeated deliveries preserve current payloads and all caller snapshots. No permanent
production change is needed.

Three independent production-only mutations produce twelve intended assertion
failures: wrong queue with a cached consumer selector, wrong framing with a reused
publication writer, and stale decoded body with a cached default representation.
Each exact restoration returns four passes. Initial and final optimized focused
checks pass four cases per framework. All four Redis library targets build with zero
warnings/errors. Approved test/helpers, all 6,526 preceding C# files and protected
inputs are unchanged; only the approved test is added.

Complete paced regressions pass **190 cases, zero failures and four existing skips
per framework**. All 194 identities run exactly once, preserving all 190 prior
outcomes. Real idle intervals are at least 3,605 seconds. No quota or clock-discontinuity
errors are reported in this rerun and owned containers are removed. The earlier
sleep-interrupted failure is retained separately under `paced/`; it is not erased
or counted as a successful run. This establishes complete
batched coverage, not a single-process full-suite pass. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-isolation/paced-awake/final-verification.json`.

Invalid/missing configuration, scheduling/dispatch failure recovery, cancellation,
durable persistence, concurrency and broader feature release gates remain open.
Whole-feature production readiness, hosted CI, commits, pushes and deployment are
not claimed.


## Redis scheduled retry writer recovery: focused validation (2026-11-04)

The user approved the four cases, persistent-failure helper and named mutation plan.
All four cases pass initially and in final optimized builds on .NET 9 and .NET 10.
The premature-settlement mutation produces four failures at the outstanding-message
assertion; bypassing the publication writer produces four failures at the failed-write
assertion. Exact restoration returns four passes after each mutation. No permanent
production change is needed. All four Redis library targets build with zero warnings
or errors; the test build retains 604 existing warnings.

The approved test/helper are unchanged, and independent verification preserves all
6,527 preceding C# files and 281 build/dependency/workflow inputs. The focused broker
is removed. Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-writer-recovery/focused-verification.json`
and `launch-verification.json` in the same directory.

Complete paced regressions are queued in four disjoint batches; their outcomes are
pending. Each framework is expected to cover 198 identities exactly once, with 194
passes and four existing skips, preserving all 194 preceding outcomes. The four new
cases measure 277 Redis commands per framework. An allowance of 516 commands puts
the smaller preceding batches at estimated totals of 5,174 and 5,202, under the 5,500
allocation threshold; no extra batch is required. No tests, dependencies, timeout
bounds or license settings are changed to achieve this allocation.

The first batch is eligible at `2026-11-04T16:16:16.683922+00:00`. Real 3,605-second
idle intervals remain required after focused work and between batches. The launcher
uses temporary idle-sleep prevention; keep the lid open during test execution.
Live state: `/private/tmp/brighter-4170-redis-scheduled-retry-writer-recovery/paced/status.json`.
The runner stops on unexpected results and retains evidence. Successful completion
independently verifies outcomes, exact identities, source/assembly hashes, waits and
cleanup before updating completion reports. This is batched coverage; no passing
single-process full suite or whole-feature production readiness is claimed.


## Redis scheduled retry writer recovery completion (2026-11-04)

Four approved sync/async × throwing/null cases pass through native Redis and the
production in-memory scheduler, command processor, handler and outbox mediator.
Failed callbacks leave exactly one outstanding message with its revised payload,
metadata and selected publication. Native counters, payloads and queues remain
unchanged. Explicit recovery sends one composed/default payload to the selected
queue and settles the outbox while preserving the caller and original native records.
No permanent production change is needed. This does not establish automatic retries
or exactly-once delivery across arbitrary failures.

Two independent production-only mutations produce eight intended assertion failures:
premature settlement loses the pending message, and bypassing the selected writer
fails the required failed-attempt assertion. Exact restoration returns four passes
after each mutation. Initial and final optimized focused checks pass four cases per
framework. Four Redis library targets build with zero warnings/errors. All 6,527
preceding C# files and 281 protected build/dependency/workflow inputs are unchanged;
only the approved test and writer helper are added.

Complete paced regressions pass **194 cases, zero failures and four existing skips
per framework**. All 198 identities run exactly once, preserving all 194 prior
outcomes. Real idle intervals are at least 3,605 seconds, no quota or host clock
errors are reported, and owned containers are removed. This establishes complete
batched coverage, not a single-process full-suite pass. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-writer-recovery/paced/final-verification.json`.

Invalid/missing registration, submission and native-send failures, cancellation,
durable persistence, concurrency and broader transport/release gates remain open.
Whole-feature production readiness, hosted CI, commits, pushes and deployment are
not claimed.


## Scheduled Redis retry registration: reproduced failure and initial fix (2026-11-06)

All four approved sync/async × missing-alias/wrong-type cases reproduce native retry
acceptance followed by an unhandled ConfigurationException from ProducerRegistry
escaping the production InMemoryScheduler callback. Each child process exits 134;
the parent xUnit process survives and fails at its exit assertion. This occurs on
both .NET 9 and .NET 10. Evidence is under
`/private/tmp/brighter-4170-redis-scheduled-retry-registration/initial-red-*.trx` and
`red-verification.json`.

The message-callback branch now logs the original exception with its scheduler ID
and guarantees existing generation-aware cleanup in finally. All four initial green
cases pass per framework: retained message data, direct dispatch errors, scheduler-ID
reuse, later healthy dispatch and explicit recovery through corrected configuration.
Request callbacks and public signatures are unchanged.

Unchanged full baselines pass 1,716 Core cases/seven existing skips and 167 InMemory
cases on each framework. A sandbox pipe failure and then a temporary output-location
failure were retained as setup evidence; repository-local output resolves the Core
architecture checks without changing them. The fixture's ambiguous subscription type
and nullable project context were corrected without changing assertions or behavior.

Mutation validation and final optimized/build/regression checks are in progress.
Rethrowing the callback and omitting failed-job cleanup each produce four intended
parent assertion failures with exact restoration and four restored passes. The
log-suppression mutation's first build had an unused-variable error; it is retained
as compilation evidence, not behavioral RED. Its catch binding is corrected and
validation resumes without rerunning completed mutations. No complete Redis
regression result or whole-feature production readiness is claimed yet.


## Scheduled Redis retry registration: final focused validation (2026-11-06)

All four approved cases pass on both frameworks in final optimized builds. All twelve
named mutation failures are verified, with exact restorations and four passing cases
after each. Eight Core/Redis library target builds pass with zero warnings/errors.
Full post-fix Core suites retain 1,716 passes/seven existing skips and InMemory retains
167 passes on each framework; every baseline identity and outcome is preserved.
All 6,528 other preceding C# files remain unchanged, and fixture/project changes match
the approved scope plus the recorded non-behavioral compilation corrections.
The focused native broker is removed.

The complete Redis regression schedule has 4 disjoint paced
batches, first eligible at `2026-11-06T08:53:15.366933+00:00`. Expected coverage is 202
identities exactly once per framework, with 198 passes/four existing skips and every
preceding outcome retained. These results remain pending. Allocation uses measured
native command counts plus a conservative allowance; real 3,605-second idle gaps,
source/assembly pinning, owned cleanup and stop-on-failure are enforced. Child
processes isolate callback failures and do not change the quota/pacing contract.

Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-registration/focused-verification.json`.
Live schedule: `/private/tmp/brighter-4170-redis-scheduled-retry-registration/paced/status.json`.
Successful completion independently verifies all identities/outcomes, input hashes,
real waits and cleanup before writing completion reports. Keep the laptop lid open
for native runs; idle-sleep prevention cannot prevent lid sleep. This establishes no
unrun CI or whole-feature production readiness.


## Scheduled Redis retry registration failure containment completion (2026-11-06)

Four approved sync/async × missing-alias/wrong-event-type cases reproduce an accepted
native retry followed by an unhandled registry ConfigurationException terminating
its child process with exit code 134, on both frameworks. The parent xUnit process
survives and preserves the actual callback diagnostics. The initial ambiguous
RedisSubscription type and nullable-context build issues were corrected without
changing assertions or behavior; the failed build and original reviewed files are
retained separately from behavioral RED.

InMemoryScheduler's message-callback branch now logs dispatch exceptions with the
scheduler ID and always performs generation-aware cleanup. Direct invalid-registry
clearing still throws to callers. The failed message remains in the outbox, the same
job ID accepts later healthy work, and a correctly configured registry explicitly
recovers the original payload to its selected native queue. Only InMemoryScheduler.cs
changes production behavior; request callbacks and public signatures are unchanged.

Three independent mutations produce twelve intended parent assertion failures:
rethrowing the callback error, omitting failed-job cleanup, and suppressing the error
log. Exact restorations each return four passes. Initial/final optimized native checks
pass four cases per framework. All eight Core/Redis library target builds have zero
warnings/errors. Full Core regressions retain 1,716 passes/seven existing skips and
InMemory retains 167 passes on each framework, with every baseline outcome preserved.
The initial Core output-location failure is retained as setup evidence; moving build
output under the ignored repository bin directory restores the architecture checks
without altering tests.

Complete paced Redis regressions pass **198 cases, zero failures and four existing
skips per framework**, covering each of 202 identities exactly once and preserving
all 198 preceding outcomes. Real idle intervals are at least 3,605 seconds, no quota
or host clock errors are reported, and owned containers are removed. This is complete
batched coverage, not a single-process full-suite claim. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-registration/paced/final-verification.json`.

All 6,528 other preceding C# files remain unchanged. Additions are the four-case test
and four child-fixture sources, one fixture project and one test-project reference.
The compiled fixture matches its recorded effective contents after the two routine
compilation corrections; no package versions or test semantics changed.

This establishes message-callback containment and explicit in-process outbox recovery.
Request-callback failures, submission/native-send errors, cancellation, durable
persistence, broader transport checks and final release gates remain open. No hosted
CI, commit, push, deployment or whole-feature production readiness is claimed.


## In-memory scheduled request failure containment (2026-11-07)

The [approved request callback package](review/inmemory-scheduled-request-failures/README.md)
reproduces four real timer crashes on each framework, covering sync/async Send and
Publish. The scoped Core fix logs the original dispatch exception and scheduler ID,
then guarantees existing generation-aware cleanup. Direct dispatch still throws;
the same scheduler ID accepts and completes later healthy work. Failed local
requests are not retained or retried automatically.

All four initial/final focused cases pass on both frameworks. Three named mutations
produce twelve intended failures and exact restorations return twelve passes.
Full Core suites pass 1,720 cases/seven existing skips; full InMemory suites pass
167 cases/no skips per framework. Every baseline identity/outcome is preserved.
Four Core library targets build with zero warnings/errors. Approved test sources
are unchanged; only `InMemoryScheduler.cs` changes production behavior.

This closes the local request-handler exception boundary identified in the previous
message-callback audit. Submission/native-send recovery, cancellation, restart
persistence, broader transport coverage and final release checks remain open.
Evidence: `/private/tmp/brighter-4170-inmemory-scheduled-request-failures/`.

## PostgreSQL native wire mapping — 2026-11-07

[Native integration evidence](review/postgres-wire-mapping/README.md): 28 focused cases,
76 intended mutation failures/restorations and 317 passing full-project tests preserve
all 289 prior outcomes. The test project targets .NET 10; all four library targets
build with zero warnings/errors. Only the four existing PostgreSQL gateway files change,
with two default adapter exports added. Schema/outbox/build inputs remain unchanged.
Whole-feature readiness and the final cross-gateway matrix remain open.

## SQL Server initial wire mapping — 2026-11-07

[Integration evidence](review/mssql-wire-mapping/README.md): twelve focused cases per
framework, twenty intended mutation failures with exact restorations, and full suites
of 380 passes/four existing skips on .NET 9 and .NET 10 preserve all 372 preceding
outcomes. All four gateway targets build with zero warnings/errors. Existing public
constructors, queue schema and outbox serialization remain unchanged. Only the five
intended existing MsSql gateway files change, with a publication and default adapter
exports added.

Validation uses the repository's native ARM64 Azure SQL Edge fixture. Concurrent
SQL/RocketMQ execution first caused documented memory-allocation errors; stopping the
owned RocketMQ services resolved that contention. A separate amd64 SQL Server 2022
exit-139 failure recurred after isolation and remains unresolved. Stable repository
fixture results do not resolve that environment finding. Preserve it as a final release
gate along with remaining scheduler/rejection/lifecycle combinations. No commits or pushes.


## Local finalization checkpoint — 2026-11-07

No commits or pushes are authorized. Review-after remains active for #4170. Full
production readiness is not established by this checkpoint.

RocketMQ's initial reduced-memory fixture reproduced native receive deadlines in an
SDK-only probe. Broker pop logs establish direct-memory exhaustion. Increasing only
local direct-memory bounds and restoring original polling yields three successful
SDK-only deliveries and six unchanged regression-probe passes on each framework.
The complete unchanged net9.0 baseline passes 103 cases/two skips. Reusing its retained
native topic log contaminates net10.0 with older IDs; that failed run is preserved.
Fresh owned brokers are now used per native run, and the fresh net9.0 baseline also
passes 103/two. Mapping acceptance and mutations remain pending.

RabbitMQ Async scheduled retry selection is implemented in an isolated checkout.
Four cases per framework first fail to obtain selected dispatch, then pass with
captured publication selection, a private message copy and explicit destination queue
routing. The test's OpenTelemetry initialization was corrected using the existing
Quartz test pattern. Full baseline fixture TLS/native-delay omissions are corrected
without excluding cases; complete baselines, mutations and final regressions are
running. Sync parity is in progress.

Kafka public default-adapter tests reproduce mutable body and byte-array header
aliasing on both frameworks. Independent copies fix both cases in the isolated
checkout, with two focused passes per framework. Complete native regressions are
running against owned Kafka/schema-registry containers. These focused results alone
do not establish native compatibility or permit integration without final checks.

Redis's first quota-paced batch passes 102 cases/two existing skips on each framework.
The remaining disjoint batches retain real 3,605-second minimum idle intervals. This
is partial coverage toward 207 identities per framework, not a completed full matrix.

Evidence roots: `/private/tmp/brighter-4170-rocketmq-wire-mapping/`,
`/private/tmp/brighter-4170-rmq-scheduled-selection/`,
`/private/tmp/brighter-4170-rmq-sync-scheduled-selection/`,
`/private/tmp/brighter-4170-kafka-default-body-ownership/`, and
`/private/tmp/brighter-4170-redis-scheduled-retry-boundaries/paced/`.

## Integrated-workspace confirmation — 2026-11-08

Locked restores and Release rebuilds from the main workspace pass for RabbitMQ
Async/Sync, Kafka, Hangfire, TickerQ and SQL Server test projects. Their focused
native checks pass on both frameworks: 6, 4, 2, 2, 6 and 12 cases respectively.
Each project's source/configuration test inventory matches its validated isolated
inputs. The full regression counts and actual RED/mutation evidence are recorded
in the corresponding review packages. No commits or pushes were made.

Evidence: `/private/tmp/brighter-4170-validated-integration/main-verification.json`.
Subsequent packages require their own integration confirmation; this checkpoint does
not establish full-issue readiness or durable scheduler restart.

The subsequent SQL scheduled-retry and PostgreSQL forwarding packages also pass
main-workspace confirmation. The latest SQL focus has 16 cases on each framework;
PostgreSQL has 24 cases on its explicitly declared .NET 10 test target. An initial
SQL restore reported premature MSBuild worker exits; its log is retained separately.
A single-worker restore/rebuild and the unchanged native assertions pass.

### Quartz durable restart checkpoint — 2026-11-08

The [native process-restart probe](review/quartz-durable-restart/README.md) passes
four baseline and four restored cases, two intended corruption failures and
both full 53-case Quartz suites. Production source is restored exactly. This closes
the selected PostgreSQL JobStoreTX/System.Text.Json process-restart gate only.
TickerQ durable restart, AWS callback recovery and the remaining transport/final
release gates remain open. No commits or pushes.

### RocketMQ rejection selection checkpoint — 2026-11-08

The [native rejection package](review/rocketmq-rejection-selection/README.md)
produces twelve actual RED and GREEN cases per framework. Full regressions preserve
all 118 prior identities/outcomes and add twelve passes: 128 passes/two existing
skips each. All four library targets build; guarded local integration is complete.
Legacy acknowledgement after failed forwarding remains unchanged; recovery uses
a subsequent message. Main-workspace locked restore/rebuild and all twelve native cases pass on both
frameworks, with no host-clock discontinuity.

### TickerQ durable restart checkpoint — 2026-11-08

The [native SQLite restart probe](review/tickerq-durable-restart/README.md) passes
eight baseline/restored cases, both intended corruption checks and both full
54-case suites, preserving prior identities/outcomes. Production code/dependencies
are unchanged. The recorded Hangfire, Quartz and TickerQ provider-specific restart
gates are complete. AWS callback-pump recovery and final transport/release gates
remain open; this is not a whole-issue production-ready claim.

### RabbitMQ unsupported-delay fallback checkpoint — 2026-11-09

The [fallback package](review/rmq-destination-delay-fallback/README.md) fixes native
missing-exchange failures for explicit queues when delay support is disabled.
Eight actual RED failures and eight passing default controls precede the fix.
Full Async suites pass 566 cases/two existing skips and Sync 299 cases/one existing
skip on both frameworks, with all prior outcomes retained and all library targets
built. Suspended attempts are excluded and retained separately. Guarded local
integration and main-workspace native checks pass (ten Async/eight Sync each).
No commits or pushes; remaining whole-issue release gates stay open.

### MQTT consumer and scheduled writer selection checkpoint — 2026-11-09

The [consumer writer package](review/mqtt-consumer-writers/README.md) adds eighteen
native actual RED/GREEN cases per framework for immediate retry, invalid-message
and dead-letter selection, throwing/null writers and recovery. The
[scheduled retry package](review/mqtt-scheduled-selection/README.md) adds four actual
RED/GREEN cases each plus four intended selector-omission failures. Both full final
suites pass 121 cases/four existing skips; every preceding outcome remains. All
library targets build, mutations are restored exactly and guarded integration is
complete. Main-workspace locked rebuild and all 22 native cases pass on both
frameworks without clock discontinuity. Protocol ACK ownership and the
legacy logged/true-result rejection failure contract remain unchanged.

### AWS automatic callback pump recovery checkpoint — 2026-11-09

The [callback recovery package](review/aws-callback-pump/README.md) validates eight
SQS/SNS, sync/async-business-send, throw/null cases per SDK/framework through the
actual Floci scheduler and ServiceActivator Proactor. Failed business sends retain
outbox identity/body; the callback is settled, and explicit outbox recovery delivers
with selected mapping and fresh traceparent/tracestate/baggage. Sixteen intended
body-corruption failures precede exact production restoration. All four complete
AWS suites pass 627 cases, preserving all 619 prior outcomes plus eight new cases.
Four test/helper files are integrated locally; no permanent production change.
S3 endpoint setup failures and clock-discontinuous attempts remain separate from
certified continuous runs. Main-workspace locked restore/rebuild and all eight new native cases pass for
each SDK/framework, with no host-clock discontinuity. Hosted AWS IAM,
durable outbox and automatic failed-business redelivery are not claimed.

### Hosted Azure access recheck — 2026-11-09

The read-only Service Bus namespace list still fails with AADSTS9002313; Azure CLI
requires interactive authentication (`az login --scope https://management.core.windows.net//.default`).
A sandbox-only session-cache permission failure preceded the authorized external
check and is not service behavior. No namespace, queue or paid capacity was created.
ASB hosted scheduling, DLQ and lost-lock acceptance remain unexecuted.

### Kafka dead-letter and scheduled retry selection checkpoint — 2026-11-09

The [dead-letter package](review/kafka-dead-letter-selection/README.md) reproduces
and fixes eight cases per framework: selected replacement/composed mapping plus
throw/null failure retaining the source offset and redelivery recovery. The
[scheduled retry package](review/kafka-scheduled-selection/README.md) reproduces
four wrong-format callbacks each, then verifies subscription capture, typed lookup,
original native destination, fresh dispatch telemetry, local selector isolation and
original-offset-only settlement. Omitting the selector gives four intended failures.
After exact restoration, both full suites pass 412 cases, preserving all 400 prior
outcomes. All library targets build and guarded local integration is complete.
Main-workspace restore/rebuild and fourteen combined native/ownership cases
pass on both frameworks without clock discontinuity. No commits or pushes. The historical native
assignment/close stall remains unresolved; no speculative shutdown change was made.

### Executable telemetry example verification — 2026-11-09

Re-reading the certified full TRXs finds sixteen existing tests whose names contain
`non_cloud_events_telemetry` in each AWS SDK/framework suite and four in each RabbitMQ
Async framework suite; all pass. These are the compiled examples linked from
[usage](usage.md#move-telemetry-outside-cloudevents), covering custom attributes/body,
trace restoration and real application pump/settlement where specified by each test.
They ran as part of the complete 627-case AWS and 566-pass/two-skip RabbitMQ matrices;
no extra test execution or broader binary-format guarantee is implied.
Both ADRs now describe the gateway-wide typed composition design and transport-specific
ownership/failure semantics. Documentation remains local and unpublished. The ASB
queue-retry bypass and other release gates remain explicit in the completion plan.

### GCP rejection writer selection — 2026-11-09

The [rejection selection package](review/gcp-rejection-selection/README.md) reproduces
24 ignored-writer failures per framework, then passes all 24 native pull/stream,
sync/async, shared-route/fallback and throw/null recovery cases. A topic-only cache
mutation causes twelve intended shared-route failures; exact restoration passes.

Both pre-change available Pub/Sub suites pass 429 cases/55 skips. Each final suite
has 452 passes, one failure and 55 skips, retaining all preceding identities plus
24 new passing cases. The changed outcome is the historical generated async ordered-
nack missing-ID assertion at line 158. The failure is retained and not retried away.
This is scoped rejection validation, not a clean full-suite or production-ready claim.
All library targets build; guarded local integration is complete and main-workspace
restore/rebuild and all 24 native cases pass on both frameworks without clock discontinuity. No hosted, Firestore, Spanner or remote CI coverage is claimed.

### Final local build and cleanup checkpoint — 2026-11-09

Complete solution builds pass in Release (22,073 warnings/zero errors) and Debug
(20,517 warnings/zero errors). These are successful builds, not warning-free builds.
All 7,244 recorded source/build inputs remain identical throughout; clock continuity
is verified. `git diff --check` passes. The command used `--locked-mode` for restore,
but not every repository project has a package lock file, so this flag alone is not
claimed as a repository-wide locked dependency guarantee.

Two duplicate PostgreSQL test PackageReference declarations were removed. Before/after
resolved targets and library identities/hashes compare exactly equal; no package
version or test source changes were made. The final solution builds include the cleanup.
All newly integrated AWS, RabbitMQ, Kafka, MQTT and GCP acceptance cases pass from the
main workspace. GCP's full matrix retains the separately recorded async ordered-nack
failure on both frameworks; no passing full GCP regression is claimed.

Eleven task-owned fixtures are stopped, with broker logs and containers retained;
unrelated services remain running. Compact [build/source evidence](review/final-build-verification.json)
and [main-workspace results](review/main-workspace-verification.json) are saved locally.
No commits, pushes, remote CI, deployment or publication occurred. Whole-issue release
gates remain explicit in the [completion plan](completion-plan.md).

## ASB queue retry writer selection — 2026-11-09

Both native consumer-created retry scheduling and the global Azure retry adapter
now capture an explicit writer. Mapping follows current trace propagation and
preserves custom binary body/metadata, while the scheduler restores retry and
original identity, session, partition and handled count. Null retains the old raw
binary body path. Writer throw/null leaves scheduling and source completion untouched;
recovery schedules once before completion. Public constructor signatures are unchanged.

Twelve ignored-writer assertion failures per framework establish actual behavioral
RED, including a second baseline with the corrected telemetry fixture. Both final
local inventories pass **536 cases**, retaining all **524** prior identity/outcome
pairs and adding twelve cases. No failures/skips or clock discontinuities. The
inventory covers 66 test classes using local SDK-I/O/static checks; 80 native-service
classes remain outside this inventory. It is not a replacement for the historical
native/emulator 628-case baseline, nor a hosted-delivery claim.

Setup failures (internal constants, timeout argument and ambient baggage) and the
SDK session/partition ordering failure are retained separately. Clearing the mapped
session before restoring source controls resolves the SDK conflict without changing
the legacy no-writer path. Four production files and two new test files were copied
with before/after hash guards. Tests, inventory and compact results are retained in
[the review package](review/asb-retry-writer-selection/README.md).

Hosted scheduling/DLQ/lost-lock acceptance and the retained session-lock finding remain
open. Nothing committed or pushed. Evidence:
`/private/tmp/brighter-4170-asb-retry-writer-selection/`.

Post-integration verification: twelve main-workspace retry-writer cases and all
thirteen Azure scheduler tests pass per framework. Complete Release and Debug
solution builds pass with zero errors, including both changed libraries on
netstandard2.0, net8.0, net9.0 and net10.0. All 7,246 recorded source/build
inputs remain unchanged through both builds; clocks are continuous. Repository
warnings remain (22,073 Release; 821 Debug).
No native fixtures were started for this package. No commits, pushes or remote CI.

## GCP scheduled publication selection — 2026-11-09

Four native cases exercise synchronous/asynchronous delayed publication through
real InMemoryScheduler callbacks, CommandProcessor, typed producer lookup and outbox
submission. Default/composed payload, dispatch tracing/state/baggage, native ordering,
application identity and acknowledgement all pass. The default-writer bypass mutation
fails both custom body checks and preserves both default controls. Exact source
restoration and all four restored cases pass per framework; no production change.

Full Pub/Sub matrices retain all 508 prior identities plus four new passing cases:
.NET 9 has 457 passes/55 skips; .NET 10 has 456 passes/one known generated ordered-nack
failure/55 skips. The .NET 9 historical failure passes on this run without a product
correction; it remains unresolved. All runs have continuous clocks. The one new test
is integrated with hash guards. See [the review package](review/gcp-scheduled-publication/README.md).
No commits or pushes. Separate passive RPC diagnostics are investigating the retained
failure under the existing generated full-suite conditions.

## Public signatures and generated ordered-nack diagnosis — 2026-11-09

The [98-file public declaration audit](review/public-surface-audit/README.md)
finds no removed/changed covered public/protected signatures versus HEAD, with
68 additions. This syntax audit is scoped separately from compiled binary and
default-wire behavioral evidence. GCP's integrated scheduled-publication tests
rebuild and pass four cases per framework in the main workspace.

The [generated ordered-nack full-suite diagnostic](review/gcp-generated-nack-diagnostic/README.md)
reproduces the exact async failure on .NET 9: 456 passes, one failure, 55 skips.
Its native trace proves successful deadline-zero RPC with no native redelivery
within the unchanged bound. A concurrent 60-second lease RPC prevents attributing
this particular trace solely to the independently reproduced emulator stream/ack
stall. Preserve the failure and leave that distinction open. No test assertions
or production behavior changed.

## Fixed GCP lease control and Kafka diagnostic outcome — 2026-11-09

The [GCP raw lease-order control](review/gcp-ordered-nack-lease-control/README.md)
completes eight observations on the pinned emulator. One-stream redelivery succeeds
with either completed lease/nack order; both two-stream variants miss the unchanged
30-second bound on both frameworks. This supports the already isolated emulator
stream-membership boundary, without claiming the exact concurrent server order in
the generated-test trace or dispositioning separate historical shutdown findings.
No production or assertion changes occur. Both owned fixtures are stopped.

The [Kafka native close diagnostic](review/kafka-native-close-diagnostic/README.md)
completes its one planned full .NET 9 suite: 412 passes, zero failures/skips, continuous
clocks, both commit/revoke strategies exercised. Native debug output is retained in
the TRX; the diagnostic source was restored before execution. The archived close
stall is not reproduced and remains unresolved. Both owned fixtures are stopped.

Current usage/external-documentation text no longer labels validated RabbitMQ
scheduler selection, AWS callback-pump recovery or Kafka partition/retry packages
unimplemented. Historical package outcomes remain in the validation/review records.
All local targets in seven current guidance/ADR files resolve.

## Compiled API compatibility — 2026-11-09

All [68 compiled API comparisons](review/binary-api-audit/README.md) pass across
the 18 changed production assemblies and their declared target frameworks versus
Git HEAD, with no unresolved references or compatibility suppressions. Parameter
names are included. The separate all-attributes run reports only 360 compiler
async-state-machine metadata differences; all are retained and classified. A
reversed Core comparison correctly rejects the two missing wire contracts.
Source and binary hashes are recorded. This closes the scoped public API check,
not native runtime, environment or lifecycle gates.

## Final local checkpoint after delayed-publication integration — 2026-11-09

RocketMQ passes 136 cases/two existing skips per framework, retaining all 130 prior
outcomes plus eight new cases. Main confirmation passes all eight on both frameworks;
GCP main confirmation passes four on both. All temporary production mutations and
diagnostic source edits are restored.

Complete main-workspace Release and Debug builds pass with zero errors and unchanged
7,248 source/build inputs. Warning counts are 22,073 and 4,107; neither new scheduling
test file emits warnings. Final Release output hashes match all 68 API comparison
records after rechecking the 24 regenerated binaries. All issue-owned containers are
stopped; existing unrelated services remain untouched. No commit, push, remote CI,
paid resource creation, deployment or publication occurs.

The [completion plan](completion-plan.md) now records four concrete remaining gates:
native ASB retry scheduling, ASB DLQ/lost-lock coverage after Azure authentication,
cause-supported disposition of retained ASB/Kafka/GCP lifecycle findings, and supported
x86-64 SQL Server 2022 runtime coverage. The feature remains not production-ready.


## Original GCP shutdown diagnostic follow-up — 2026-11-09

The [bounded native-input diagnostic](review/gcp-original-shutdown-diagnostic/README.md)
passes all four original unsettled/buffered shutdown cases on each framework, then
passes those four again in one fixed full .NET 9 matrix. All twelve observations
record a successful nack for the last original delivery and recovery through the
replacement subscriber with the same native/application IDs and a fresh ack ID.
The historical shutdown failure is not reproduced and remains unresolved.

The full matrix has **456 passes, one failure and 55 existing skips**, all 512 prior
identities retained, with continuous clocks (559.83 seconds). The failing existing
ordered-nack async/default-client diagnostic observes successful native nack but no
subsequent native redelivery within thirty seconds. Its exact trace is retained in
the review directory. Near-simultaneous ack, lease-extension and nack completions do
not prove server execution order. This does not establish a new Brighter defect.

All 6,614 main production/test sources are unchanged; 974 relevant isolated inputs
match main after temporary instrumentation is restored. Build: zero errors, 3,316
existing warnings. All three owned emulators are stopped. No production correction,
assertion change, dependency update, commit or push occurs. Complete evidence is in
`/private/tmp/brighter-4170-gcp-original-shutdown-diagnostic/`; compact results,
per-case native traces, scripts and verification are retained in the review folder.

A read-only Azure check still fails with AADSTS9002313 and requires interactive
login. Only local Docker contexts are configured; supported x64 SQL Server 2022
coverage remains unavailable. The [Kafka source follow-up](review/kafka-native-close-diagnostic/README.md)
locates the indefinite native assignment queue wait without establishing its cause.
All outstanding release gates remain explicit in the completion plan.


## Original GCP async disposal result now observed — 2026-11-09

The [validated amendment](review/gcp-disposal-result/README.md) adds one await to
each of the two original async shutdown tests after its unchanged timeout assertion.
A scoped temporary production mutation proves the gap: both original tests pass
with a faulted initial disposal; both amended tests fail at the new await on each
framework. Production is restored exactly, and both cases pass afterward.

The first full matrices experience a 44.3-second wall/monotonic divergence, correlated
with a logged host idle-sleep event. Their complete results are retained separately.
The justified repeat inhibits idle sleep for the runner's lifetime and retains all
512 case identities per framework: **457 passed, zero failed, 55 skipped on .NET 9**;
**456 passed, one retained generated async ordered-nack failure, 55 skipped on .NET 10**.
Both clocks remain continuous. All four original shutdown cases pass in both matrices.

Main-workspace integration changes only the two awaits. Its build succeeds with
zero errors and 3,320 warnings; both changed cases pass on each framework from that
build. Verification covers all 6,614 existing production/test source files and
confirms exact production restoration. Owned fixtures are stopped; the sleep
inhibitor is released. No commit, push or dependency change occurs. Historical
lifecycle findings, Azure authentication and supported x64 SQL Server validation
remain open. Complete evidence: `/private/tmp/brighter-4170-gcp-disposal-result/`.


## GCP emulator version comparison — 2026-11-09

The unchanged [raw protocol control](review/gcp-emulator-comparison/README.md)
reproduces the ordered-redelivery stall on both pinned emulator 0.8.36 and official
Google Cloud CLI 592.0.0's emulator 0.8.37. Each of four combinations runs once per
framework/image: sixteen observations, twelve timely redeliveries and four timeouts.
Every timeout follows adding a stream and acknowledging the second message before
nacking the first. A separately recorded repeated acknowledgement releases the
first message in all four cases, preserving its native ID/body with a new ack ID.
Those recoveries do not turn the original timeouts into passes.

The baseline image is amd64; the candidate is native arm64. This exact-image
comparison establishes reproduction on the newer native image, without isolating
JAR version from all other image differences. Build: zero warnings/errors. All
four runs have continuous clocks, all owned fixtures are stopped, and all 6,618
source/build/protocol inputs remain unchanged during the matrix. Full traces and
compact evidence are retained in the review; complete artifacts are under
`/private/tmp/brighter-4170-gcp-emulator-comparison/`.

The tested update does not resolve this control. No project dependency/fixture,
production or test source is changed; no hosted service or Redis is rerun. The
upstream report is updated locally and remains unpublished. Historical shutdown,
ASB/Kafka lifecycle, native ASB acceptance and supported x64 SQL Server gates remain
open. No commit or push occurs.


## Kafka evidence availability and live capture — 2026-11-10

The [live-capture package](review/kafka-live-capture/README.md) preserves four healthy
consumer traces from the prior 412-pass diagnostic TRX and validates capture before
a test host exits. The original cancellation-recovery directory/raw failed stack
is no longer available locally; the earlier interpretation remains historical,
without recovered raw evidence. This does not clear the failure.

A diagnostic-only Linux startup hook redirects native stderr to a private mounted
file. The unchanged existing .NET 10 RoundRobin/CooperativeSticky cases run once:
both pass. While the runner is live, 21,462 bytes of native output are observed and
a nonterminating stack report captures 29 threads, nine with librdkafka frames.
Internal native symbols and queue memory are not supplied by that report. It is a
healthy capture validating the collection method, not a reproduced hang or a full
regression. A failing full-memory/native-thread capture remains required.

The hook and its final comment/license-only amendment build with zero errors and
warnings. All 6,616 source/build inputs remain unchanged during validation, and both
owned broker/schema fixtures are verified stopped. No product/test implementation,
dependency, hosted resource, commit or push changes. The Kafka release gate remains
open, along with the other lifecycle and external acceptance requirements.


## Kafka full native capture validated; fixed suite remains inconclusive — 2026-11-10

The [full-capture package](review/kafka-full-capture/README.md) validates live native
stderr, a 439,771,136-byte ELF memory core, process maps and native task names/states
on the existing .NET 9 commit/revoke cases. Both pass after nonterminating capture.
The healthy report contains 28 threads, eight with librdkafka frames. The exact
packaged library retains `.symtab`; all forty recorded native frames resolve to
containing function ranges, including Kafka main/queue/broker functions. There is
no DWARF type/variable reconstruction and no claim of decoded failing queue state.

A sixty-second diagnostic watchdog is verified against healthy close sequences
and an explicit uncompleted-event control, then used in one full .NET 9 run.
The suite passes all **412 cases, zero failures/skips**, preserving the exact prior
case identities, in 899.577 monotonic seconds. Clocks remain continuous. All four
instrumented consumers close in 5–13 ms; the watchdog never triggers. This is a
passing diagnostic attempt, not a reproduction or fix of the historical stall.
The only memory core collected is clearly labelled healthy validation.

Both fixtures are verified stopped. All 6,616 source/build inputs and recorded
read-only diagnostic binaries remain unchanged. The memory core stays in
`/private/tmp/brighter-4170-kafka-full-capture/`; compact results, native traces,
thread reports, scripts, checksums and TRXs are retained in the review. No product
or test source, assertion, dependency, commit or push changes. Another identical
green run is not a release criterion. The original missing raw stack and unresolved
cause remain explicit limitations. Azure authentication still fails AADSTS9002313;
external ASB and supported x64 SQL Server acceptance remain open.
