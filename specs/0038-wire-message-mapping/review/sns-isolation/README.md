# SNS publication writer isolation

Status: approved and validated. All four cases pass initially and after byte restoration on both frameworks. Each named mutation case fails at the mapped-topic assertion in its own .NET 9 process. Both complete AWS V4 Release suites pass with 495 tests, zero failures and zero skips per framework.

[Test](When_sns_publications_select_different_wire_writers_should_keep_their_mapping_isolated.cs)

Four cases cover sync/async factory and send paths, with default-first and custom-first creation and send orders. One factory creates two producers for distinct SNS topics. Each topic has a separate native SNS-to-SQS envelope subscription. One publication uses default mapping; the other composes the existing tenant-header writer with its own default writer.

Native assertions check the real destination, the mapped topic ARN, application ID, payload, subject, tenant attribute presence or absence, CloudEvents subject, serialized bag/count and unchanged source messages and configuration. Cleanup disposes producers and reaps the topics and queues. This uses existing doubles and needs no request type or logging registration.

Named mutation: in `SnsMessagePublisher`, add a static nullable writer cache and replace constructor selection with `_wireMessageWriter = s_cachedWriter ??= wireMessageWriter ?? new DefaultSnsWireMessageWriter(topicArn);`. Leave `_topicArn` and the tests unchanged. Reusing the first resolved writer across publications must fail at mapped `topic` attribute equality, while native delivery still reaches the correct topic. Run each theory case in a fresh test process for mutation validation so both creation orders start with an empty cache. Setup errors, missing deliveries and timeouts do not count as RED.

The approved sequence is complete: exact draft copy, initial GREEN on both frameworks, four isolated mutation failures on .NET 9, byte-for-byte publisher restoration, restored GREEN and both full Release suites. Every preceding test and outcome is retained, with exactly four new passing cases. Artifacts are under `/private/tmp/brighter-4170-sns-isolation/`. The task emulator is stopped. No lasting production change, commit or push was made.

This establishes per-publication selection, not simultaneous converter execution or scheduled mapping support.
