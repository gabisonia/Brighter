# Historical first-increment boundary — superseded by full implementation

Status: superseded by the user's instruction to implement the entire feature.
The boundary below records what the completed first increment covers; it is no
longer the release completion criterion. All previously excluded gateway and
mapping paths are active work in [tasks.md](tasks.md). No commit or push is authorized.

## Supported paths

- Direct SQS/SNS and RabbitMQ Async publication.
- SQS and RabbitMQ Async subscription reading.
- RabbitMQ direct immediate and native delayed retry mapping.
- SQS/SNS publication returning through Brighter's in-memory scheduler.

The [release review](release-review.md) records the evidence and limitations for
these paths. The latest complete regressions pass 499 AWS cases and 512 RabbitMQ
cases on each framework; RabbitMQ retains two existing skips.

## Explicit exclusions

- AWS native scheduler targets (`UseMessageTopicAsTarget`) serialize their own
  requests and do not consult publication writers. Custom mapping is unsupported
  on this path. Arbitrary dispatch-time converters require a different execution
  path than an already serialized AWS target.
- `RmqSubscription.RequeueWireMessageWriter` does not configure scheduler-backed
  retries or invalid-message forwarding. Those paths retain existing mapping.
- Other scheduler implementations have no new end-to-end wire-mapping evidence
  from this increment.
- AWS SDK v3, RabbitMQ Sync, Kafka, Azure Service Bus, GCP Pub/Sub, MQTT, Redis,
  RocketMQ, SQL Server and PostgreSQL integration remain global follow-up work.

Approval does not waive the repository's test-review workflow. New behavior still
requires its concrete test review before execution. Local validation does not
establish remote CI success or real AWS service validation.
