---
id: 0087-configurable-wire-message-readers
title: "Configure wire message readers per subscription"
status: Proposed
author:
  - "gabisonia"
created: 2026-10-20
summary: "Select native message readers per subscription, expose default parsing for composition, and retain delivery ownership at the consumer boundary."
tags:
  - "transports"
  - "message-mapping"
  - "api-design"
---

# 0087. Configure wire message readers per subscription

## Status

Proposed for upstream acceptance. This decision applies to all gateway packages in
issue #4170. Endpoint reader/writer integration is present across the local tree;
remaining dispatch combinations, native-environment findings and final release
validation are tracked in the
[completion plan](../../specs/0038-wire-message-mapping/completion-plan.md).
The [configuration review](../../specs/0038-wire-message-mapping/configuration-design-review.md)
chooses typed default composition as the issue's permitted alternative to a DSL.
Implementation evidence does not establish upstream acceptance or release readiness.

## Context

Legacy publishers can place application metadata outside Brighter's default wire
format. Application message mappers receive a Brighter message after native
parsing. They cannot recover fields that the gateway discarded during parsing.

The reader contract must support complete replacement and selective composition.
It must preserve the consumer's ability to settle each delivery when mapping fails.
See requirements 1–4 and 6–9 in the [wire mapping requirements](../../specs/0038-wire-message-mapping/requirements.md).

## Decision

Use the synchronous, contravariant `IAmAWireMessageReader<TWireMessage>` contract
in core. `ReadMessage` accepts native content and returns a fresh Brighter message.
The contract has no dependency on a broker SDK. Mapping performs in-memory
conversion; the consumer retains asynchronous broker operations.

### Configuration and native inputs

Configure readers on transport subscriptions before creating consumers. Factories
capture each subscription's reader independently. An unset reader uses the
existing format.

| Gateway | Subscription setting | Native input | Default adapter |
| --- | --- | --- | --- |
| AWS SDK v3/v4 | `WireMessageReader` | SDK SQS `Message` | `DefaultSqsWireMessageReader` |
| RabbitMQ Async/Sync | `WireMessageReader` | SDK `BasicDeliverEventArgs` | `DefaultRmqWireMessageReader` |
| Kafka | `WireMessageReader` | `ConsumeResult<string, byte[]>` | `DefaultKafkaWireMessageReader` |
| Azure Service Bus | `WireMessageReader` | `ServiceBusReceivedMessage` | `DefaultAzureServiceBusWireMessageReader` |
| GCP Pub/Sub | `PullWireMessageReader` / `StreamWireMessageReader` | `ReceivedMessage` / `GcpStreamMessage` | `DefaultGcpPubSubPullWireMessageReader` / `DefaultGcpPubSubStreamWireMessageReader` |
| MQTT | `WireMessageReader` | `MqttApplicationMessage` | `DefaultMqttWireMessageReader` |
| Redis | `WireMessageReader` | `string` | `DefaultRedisWireMessageReader` |
| RocketMQ | `WireMessageReader` | `MessageView` | `DefaultRocketMqWireMessageReader` |
| SQL Server | `WireMessageReader` | `string` | `DefaultMsSqlWireMessageReader` |
| PostgreSQL | `WireMessageReader` | `string` | `DefaultPostgresWireMessageReader` |

A custom SQS reader receives the original SQS delivery, including an SNS envelope
when present. It replaces envelope parsing as well as application mapping.
`DefaultSqsWireMessageReader(rawMessageDelivery: false)` exposes envelope parsing
for applications that only need to customize the parsed result.

Default readers support composition through their public contract. Parser helpers
remain internal to each gateway; applications do not inherit parser internals.
The public RabbitMQ conversion entry point remains available.

### Delivery ownership

The consumer applies transport metadata after a custom reader returns. A reader
cannot substitute a receipt handle or delivery tag to settle another message.

| Gateway | Consumer-owned metadata |
| --- | --- |
| SQS | Native receipt handle and delivery count resolved against broker metadata |
| RabbitMQ Async/Sync | Native delivery tag, redelivery flag, persistence and canonical handled count when present |
| Kafka | Native topic/partition/offset and commit/requeue bookkeeping |
| Azure Service Bus | Native received message, lock token, session and settlement lifecycle |
| GCP Pub/Sub | Native receipt/callback ownership and acknowledgement operations |
| MQTT | SDK protocol acknowledgement and the gateway receive buffer |
| Redis | Native payload key, queue/in-flight entry and delivery counter |
| RocketMQ | Native receipt and delivery attempt |
| SQL Server | Atomic read-and-delete and queue operations |
| PostgreSQL | Native row receipt ID, visibility and locking |

RabbitMQ attaches a canonical native handled count after custom mapping using
the default parser's rules. If that header is absent, the reader's count remains.
This preserves the count written by direct retries even when a replacement reader
only maps application fields.

Readers control application identity, body and application headers. Consumers
continue to own acknowledgement, rejection, retry routing and connection lifetime.

### Failure behavior

Reader failure policy follows the transport's existing receive contract. Gateways
with an invalid-message path create an unacceptable Brighter message retaining raw
payload and native settlement identity; they preserve valid and failed deliveries
in the same batch. MQTT retains its SDK-backed drop-and-continue policy for failed
mapping and does not manufacture a rejectable delivery or manual MQTT receipt.
The [gateway usage](../../specs/0038-wire-message-mapping/usage.md) and native tests
record each provider's behavior rather than promising uniform settlement semantics.

Default parser failures also retain native payload and delivery metadata,
including a canonical handled count when present. An
application identity or topic parsed before failure is retained where available.
A successfully parsed rejection envelope keeps its inner payload and metadata.

This decision does not make forwarding and source acknowledgement atomic.
Existing rejection behavior and duplicate-delivery possibilities remain.

### Reuse and concurrency

Readers must not mutate native inputs or cache per-delivery mutable state.
The SQS default reader creates a parser per call because the envelope parser
retains parsing state. The RabbitMQ default reader delegates to stateless parsing.
Each call produces its own Brighter message and header bag.

The reader contract does not select a writer for retries. RabbitMQ direct retries
use the separate subscription setting described in [ADR 0086](0086-configurable-wire-message-writers.md).
Applications must pair readers with a compatible retry representation.

## Consequences

Applications can consume legacy native messages without replacing the consumer's
delivery operations. Composition keeps a small header customization independent
of the complete default format.

A complete replacement reader assumes responsibility for application metadata,
including any trace fields required by downstream processing. Native types expose
provider-specific semantics, so implementations are intentionally transport-specific.

All gateway integrations use the same core contract with their native input types.
The local release review records provider-specific validation and unresolved findings;
this decision does not certify those runtime gates.

## Alternatives Considered

- Application message mappers execute too late to replace native parsing.
- A universal dictionary would discard provider types and envelope semantics.
- Inheriting parser internals would enlarge the supported API and couple custom
  readers to parsing implementation details.

## References

- [Implementation and validation evidence](../../specs/0038-wire-message-mapping/validation.md)
- [Configuration examples](../../specs/0038-wire-message-mapping/usage.md)
- [Remaining release work](../../specs/0038-wire-message-mapping/tasks.md)
