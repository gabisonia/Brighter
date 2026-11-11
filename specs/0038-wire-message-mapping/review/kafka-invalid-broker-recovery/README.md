# Kafka invalid-channel broker failure and recovery

Status: approved, implemented and validated. Both cases reproduce false success
at False, line 125, on .NET 9 and .NET 10. Both pass after the fix and after mutation
restoration. Eight mutations produce 16 intended failures. Both final full Kafka
suites pass 357 cases with no failures/skips, retaining all 355 prior outcomes.
Only KafkaMessageConsumer and KafkaMessageProducer change. No commit or push.

The [test](When_kafka_broker_rejects_invalid_forwarding_should_keep_source_uncommitted_and_recover_after_redelivery.cs)
combines sync/async rejection with an actual Kafka broker rejection. A dedicated
invalid topic limits records to 1,024 bytes. A native preflight first confirms that
a 4,096-byte record receives MsgSizeTooLarge. The source topic accepts the
small default record. No broker shutdown, shared configuration or external service
is used. The [selected writer](KafkaBrokerRejectedWireWriter.cs) initially returns
a valid native message with an oversized body; it does not throw or return null.
The normal producer size limit is larger than this record, so the topic's broker
limit supplies the failure. The helper owns no broker or acknowledgement behavior.

Reject/RejectAsync must return false on this negative delivery outcome, preserve
the original payload/identity/topic/key/count/tenant and native acknowledgement
offset, and publish no invalid record. Disposal flushes any buffered source offsets;
a native observer must still see an unset group commit. After reducing the selected
writer's output to its small mapped format, a fresh consumer in the same group must
receive the original record and successfully forward it, committing the source only
after successful delivery. The native invalid record must contain the selected
body/key/wire identity. The writer is reused across both consumer lifetimes.

The case uses unique one-partition topics/group, bounded receives and native calls,
a 90-second token and finally cleanup. It uses public factories and broker APIs;
no mock, handler, registration or dependency is added. Both approved recovery and
successful-forwarding suites must remain green.

Before the fix, sync sends enqueued delivery and async sends could convert
ProduceException into a negative confirmation without throwing. Reject methods
acknowledged without observing those confirmations. Both initial cases reproduced
this at False, line 125, on each framework.

## Validation after approval

Copy both files unchanged into MessagingGateway/TestDoubles. Preserve the initial
results on both frameworks. Any fix must wait for the actual forwarding outcome
before source acknowledgement without changing normal publication/outbox confirmation
semantics. Ensure waits are bounded and failed or missing confirmations cannot be
reported as success. Reuse existing confirmation contracts; do not add a public API
solely for testing. Default/dead-letter failure paths and cancellation remain separate.

| Mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Report failed broker delivery as successful rejection | 2 | False, line 125 |
| Store source acknowledgement despite failed delivery | 2 | Equal unset committed offset, line 147 |
| Omit failed source offset restoration | 2 | True, line 136 |
| Clear source binary body on forwarding failure | 2 | Equal, line 132 |
| Replace recovered native body with original bytes | 2 | Equal, line 157 |
| Replace recovered native key with source key | 2 | Equal, line 155 |
| Replace recovered native ID with source identity | 2 | Equal, line 156 |
| Omit successful recovered rejection acknowledgement | 2 | Equal committed source offset, line 158 |

Require 16 intended assertion failures across eight independent mutations. Preserve
false results/source data except the named defect. Failure acknowledgement must be
flushed on disposal before observation. Recovered-output mutations must apply only
to the successful second forward, preserving the initial oversized broker failure.
Compilation failures, preflight failures, missing deliveries and unrelated timeouts
are not mutation proof. Restore source bytes and optimized builds, rerun both
focused suites and both full Kafka suites (357 cases each, retaining all 355).
Build all library/test targets and verify scope, dependencies, warnings, file identity,
HEAD/stash, links and whitespace. Preserve logs/TRX and stop task-owned services.
No commit or push. Global #4170 production readiness remains open; forwarding and
source acknowledgement are not atomic, and later offsets in the same partition,
concurrent operations, cancellation and other transports need separate coverage.
Reactor/Proactor currently ignore false rejection results for unacceptable messages;
a subsequent acknowledged offset can skip the failed source record. This production
pump gap is separate from the explicit consumer-recreation recovery tested here.


## Implementation and evidence

KafkaMessageProducer has internal per-send confirmation overloads that reuse
PublishConfirmationResult. Normal public send/delay signatures are unchanged and
continue through the same publication and queued external-confirmation behavior.
The existing external notification is enqueued before the internal confirmation
releases the rejecting consumer. The internal callback belongs to the individual
send, so a late callback cannot satisfy a later attempt merely because it has the
same source ID. No public test hook or new dependency is added.

KafkaMessageConsumer waits up to five seconds for that operation's confirmation;
the async path also links caller cancellation. Negative confirmations and wait/send
exceptions use the existing false-result/source-offset restoration path. Successful
rejection stores the source acknowledgement only after confirmed persistence.
The same forwarding helpers serve explicit invalid and dead-letter routes. This
batch proves selected invalid-channel size-rejection recovery; default/dead-letter
failures, cancellation, missing-confirmation timeout and producer initialization
failure remain separately unverified. Source inspection also shows initialization
errors return a null producer, after which rejection takes the no-producer path and
acknowledges. That separate failure path remains unresolved. Forwarding and
acknowledgement are not atomic.

The initial draft used the nonexistent enum spelling MessageSizeTooLarge. The first
qualification-only attempt also failed to compile. The installed SDK documentation
identifies Confluent.Kafka.ErrorCode.MsgSizeTooLarge; line 82 was corrected to that
member. The assertion's intended broker-size error and all line numbers remain
unchanged. Original approved bytes and both compile logs are preserved, and neither
compile error counts as behavioral RED. The helper is unchanged; compiled test and
helper match the reviewed files exactly after this mechanical spelling correction.

The initial implementation passed two focused cases on both frameworks, all eight
mutations and both full 357-case suites. Review then moved the internal confirmation
after external-notification enqueue. Both suites completed before the requested
stop reached their containers; no container was stopped by those attempts. The
pre-ordering results are preserved separately and are not the final evidence.
All eight mutations, optimized builds, focused runs and both full suites were then
repeated against the final ordering, again producing 16 intended mutation failures
and 357 final passes per framework. Every prior outcome is retained by exact name.

Evidence is under `/private/tmp/brighter-4170-kafka-invalid-broker-recovery/`, including
compile logs, original approved test, RED/green/restored/full TRX, mutation logs/TRX,
source snapshots, manifests, builds and verification.json. All four library targets
and both test targets build with no new warning diagnostics. The other 1,280
production files and all 268 dependency entries are unchanged; no production file
is added. HEAD and stash are unchanged. Task-owned Kafka/schema services are stopped
after verification.

The subsequent [four offset-barrier cases](../kafka-failed-forward-offset-barrier/README.md)
are now implemented and validated: sequential later acknowledgements cannot commit
past a failed forward, including at disposal. Eight mutations produce 32 intended
failures, and both full suites pass 361 cases. The conservative limit remains for
the consumer lifetime in that batch. The subsequent
[same-consumer recovery batch](../kafka-failed-forward-same-consumer-recovery/README.md)
now releases the matching limit and retains later acknowledgements. The subsequent
[multiple-failure batch](../kafka-multiple-failed-forwards/README.md) retains every
outstanding failure in a partition and verifies partial recovery. Partition isolation,
concurrent commits/rebalance and automatic pump retry remain open. Kafka and global #4170 are not production ready.
