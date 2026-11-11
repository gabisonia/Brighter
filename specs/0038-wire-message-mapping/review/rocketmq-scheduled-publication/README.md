# RocketMQ delayed publication selection

Review-after #4170 characterisation. Eight native cases cross sync/async,
native-delay/real scheduler callback, and default/composed writing. Use public
SDK producer and Brighter producer construction with typed registry, CommandProcessor,
InMemoryScheduler and outbox, then native receive/ack with distinct tags. Assert
no early delivery, correct mapping time, selected body, current telemetry, identity,
keys/tag/destination and completed outbox dispatch. Native topics use existing
provisioned DELAY topics; normal topics require callback dispatch.

Named RED mutation: replace RocketMqMessagePublisher's selected writer with the
default writer. The two native custom cases must fail immediate mapping count and the two callback
custom cases must fail selected native body comparison; four default controls pass. Restore source exactly, run both complete RocketMQ
suites and compare prior identities/outcomes. No hosted or arbitrary durable-store
claim. No commits or pushes.

The initial early-delivery fixture incorrectly assumed the SDK Receive completed
within SetAwaitDuration. All eight cases actually returned the correctly delayed
message from that pull. Retain the initial failures and correct the observation:
race that same receive against a one-second local timer, require empty if it returns
early, then await the outstanding receive without issuing a concurrent pull. Keep
the separate elapsed >= requested three-second delay assertion and all payload
assertions. No production defect or product change is claimed.

## Validated result — 2026-11-09

All eight corrected native-delay/callback cases pass on both frameworks. The named
selected-writer bypass produces the four intended custom failures and four default
passes on .NET 10. Production source is restored byte-for-byte. Both complete
RocketMQ suites pass **136 cases with two existing skips**, preserving all 130 prior
identities/outcomes and adding exactly eight passing cases per framework. Recorded
wall/monotonic clocks show no discontinuity.

Only the characterized test is integrated into the main workspace, guarded by the
review/compiled test equality and publisher source hashes. No production correction
is needed. Main-workspace confirmation and final solution builds are recorded below
when complete. Evidence remains under `/private/tmp/brighter-4170-rocketmq-scheduled-publication/`.

The integrated main-workspace project rebuilds successfully. All eight cases pass on both .NET 9 and .NET 10 after integration. The owned proxy, broker and nameserver are stopped; logs are retained.

Final main-workspace Release and Debug solution builds both succeed with zero errors and continuous clocks. All 7,248 recorded source/build inputs remain unchanged. The builds retain 22,073 and 4,107 warnings respectively; neither newly integrated GCP/RocketMQ test file emits a warning. `git diff --check` passes.
