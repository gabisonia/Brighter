# RabbitMQ Sync invalid-writer native delivery-tag amendment

Status: approved one-line correction copied unchanged and validated. All fourteen
cases pass on .NET 9 and .NET 10 initially after amendment and after mutation
restoration. Nine mutations produce 22 intended failures; both complete Sync suites
pass 251 cases with one existing skip and retain every preceding outcome. No
production behavior changes to satisfy the corrected assertion. The original test
and both incorrect-assertion runs remain recorded.

The [fourteen-case batch](../rmq-sync-invalid-writer/README.md) adds the subscription
selection, captures it per consumer and passes it to the invalid-forward publisher.
Both targets initially fail compilation on the missing setting. After implementing
that setting, the ten recovery cases fail at line 192: they incorrectly expect no
native delivery-tag header. The two isolation and two native dead-letter cases pass.
This is a test-contract error, not evidence that the writer's conflicting tag leaks.

`Message.DeliveryTag` stores its value in `Message.Header.Bag`; Sync
`RmqMessageCreator.SetMessageMetadata` sets that property from the broker delivery.
`RmqMessagePublisher.PublishMessage` removes the writer-supplied tag, then adds the
native message delivery tag. `HeaderNames.DELIVERY_TAG` aliases the same key. Keeping
this established gateway behavior is part of preserving native controls.

The [amended test](When_rmq_sync_selects_an_invalid_writer_should_preserve_mapping_and_recover_from_failures.cs)
changes exactly one assertion, at line 192: inspect the native header as bytes,
decode it, and require equality with `message.DeliveryTag.ToString()`. The strategy
still writes `writer-owned-tag`, so the new assertion requires gateway replacement
with the current native value. No case, payload, identity, metadata, recovery,
settlement, delay or persistence expectation is removed. The writer helper and
other two tests remain byte-for-byte unchanged.

## Validation after approval

Copy this single amended file over the compiled test. Run all fourteen cases on
.NET 9 and .NET 10. Retain the initial missing-setting errors and both runs of the
incorrect assertion. Once green, execute all nine originally approved mutations,
requiring 22 intended failures at the same assertion lines: this one-line change
does not shift them. The header-cleanup mutation still fails at line 191 because
it leaves the writer-owned delay header. Restore production bytes and normal
optimization, run all fourteen cases again, then both complete Sync suites,
expecting 251 passes and one existing skip each with every preceding outcome
retained by exact test name. Build all four library targets, verify approved files,
production/dependency scope and saved Git state, update documentation and stop the
task-owned brokers.

The native-header correction does not change the public setting or its contract.
No commit, push or review-gear change is authorized. Full #4170 implementation and
production readiness remain open.
