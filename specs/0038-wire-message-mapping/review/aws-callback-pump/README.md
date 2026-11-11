# AWS automatic callback pump and outbox recovery

Review-after for #4170. Native validation and guarded local integration are complete. No commits
or pushes. Both SDK versions exercise the actual Floci scheduler, native callback
queue, ServiceActivator Proactor, generic JSON mapper and production scheduler
handlers. Sync/async business send, SQS/SNS destinations and throwing/null writers
are covered.

The expected existing contract acknowledges the callback after retaining failed
business output in the outbox. The test requires an empty callback queue after
visibility expiry, unchanged pending identity/body, no business delivery, and explicit
selected-writer outbox recovery with fresh producer telemetry and exactly one native
message. It does not claim automatic failed-business redelivery or durable outbox.

CHARACTERISE mutation: corrupt the callback business body immediately before the
AWS handler dispatches FireSchedulerMessage. Every case must fail at the pending-body
assertion. Restore both production handlers exactly, repeat focused checks and run
complete AWS V3/V4 suites on both frameworks; retain all prior identities/outcomes.

Fixture: pinned Floci 2.3.0 image, task-owned container, no hosted AWS credentials.
Evidence: `/private/tmp/brighter-4170-aws-callback-pump/`.

## Validated result

Eight cases per SDK/framework pass initially and after exact production restoration.
The body-corruption mutations produce sixteen intended assertion failures. Each
full AWS SDK v3/v4 suite passes 627 cases on .NET 9 and .NET 10, preserving every
619 preceding identity/outcome plus the eight callback cases. No production correction
is needed. Four test/helper files are integrated locally after source-hash guards.

S3 DNS setup failures are retained separately: complete runs use the loopback-wildcard
`localhost.localstack.cloud` endpoint required by virtual-hosted S3 bucket addresses.
Clock-discontinuous attempts are retained under `interrupted-20261108` and excluded
from certified outcomes; continuous reruns pass. Main-workspace locked restore/rebuild and all eight new native cases pass for
each SDK/framework, with no host-clock discontinuity.
This verifies the automatic callback pump and explicit business outbox recovery,
not automatic business-message redelivery, durable outbox or hosted AWS IAM behavior.
