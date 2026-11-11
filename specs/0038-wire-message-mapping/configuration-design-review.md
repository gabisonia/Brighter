# Configuration design review

Local design decision for issue #4170; upstream ADR acceptance remains separate.
This review closes the API-choice question, not the remaining runtime release gates.

## Decision

Keep typed, endpoint-selected writer and reader objects, with public default adapters
for composition. Do not introduce a general mapping language in this change.
The issue prefers a DSL but explicitly permits alternative designs. This chooses that
alternative deliberately; interface availability alone is not evidence that a DSL
was delivered.

Ordinary applications configure nothing. An integration that changes one field calls
the default adapter and changes its returned representation. A complete replacement
owns its payload format and paired reader. Both are configured on the existing
publication/subscription before factories create endpoints. The gateway retains
native delivery operations and applies dispatch telemetry before writing.

## Concrete usability cases

| Integration need | Configuration and mapping work | Evidence and limitation |
| --- | --- | --- |
| Existing CloudEvents application | Leave mapping properties unset | Existing full gateway regressions preserve the default path; no new registration is required |
| Add or rename one native attribute | One writer class composing the public default adapter; assign it to the publication | Default-composition and endpoint-isolation tests cover repeated use; the application still supplies transport-specific value types |
| Put trace context in a JSON body | One complete writer and a paired reader; select both on their endpoints | Executable SQS/SNS/RabbitMQ examples and native pump tests cover traceparent, tracestate, baggage and settlement; rewriting assumes an explicitly compatible body format |
| Read a legacy format while retaining other default fields | Reader calls the default adapter and changes the resulting Message | Native receipt/ack metadata is reapplied by the consumer; a reader cannot supply a forged settlement identity |
| Requeue or forward through another format | Explicit outbound selection on the relevant consumer path, or a registered scheduled publication | A reader is not assumed to have an inverse. Remaining path integrations are tracked in the completion plan |

The executable examples are linked from [usage](usage.md#move-telemetry-outside-cloudevents).
Their tests run the production mapping boundary and native transport; the text does not
present pseudocode as executed evidence. The 2026-11-09 complete AWS/RabbitMQ regressions rerun the linked examples:
sixteen matching cases pass in each SDK/framework suite and four pass in each
RabbitMQ Async suite. This verifies those example paths, not every gateway or format.

## Why this alternative

A transport-neutral rename DSL needs rules for different native value types, byte
payloads, JSON envelopes, duplicate attributes, missing values, default precedence and
partial parse failures. SQS attributes, RabbitMQ properties, SQL text and an immutable
RocketMQ delivery are not interchangeable dictionaries. A language that silently
converts them would add behavior that the caller currently controls explicitly.

Typed composition keeps those conversions visible to the compiler and allows an
integration to use its existing serializer. It also reuses the default encoding
without requiring a copy of Brighter's parser. Public contracts describe stateless
reuse, independent outputs, caller preservation and conversion-only behavior. Native
controls remain outside the customization contract.

The cost is real: an advanced integration defines a class for each direction and
must understand its transport's field limits and its external format. Defaults and
complete worked examples reduce that burden; they do not eliminate it. A bare Func
adapter would shorten some declarations but would not resolve those conversion or
fallback decisions. Adding one now would expand the public API without addressing
the main source of integration work.

## Boundary for a future convenience layer

A future fluent API can return the existing writer/reader interfaces. It must specify
missing-field behavior, conversion failures, precedence, immutable native inputs and
body ownership before it can be a safe replacement for typed composition. It must
also leave native routing, receipts and locks under gateway control. This change
does not require applications to migrate to such a layer later.

## Review checks

- All gateway configurations use the same core interfaces; GCP exposes separate pull
  and stream readers because their native delivery types differ.
- The default application path needs no configuration changes.
- Existing public constructor signatures remain available.
- Custom code can compose public default adapters without copying private gateway code.
- Error handling and retry/forwarding selection are documented as distinct concerns.
- No DSL, upstream acceptance or completed release gate is claimed by this decision.

References: [issue #4170](https://github.com/BrighterCommand/Brighter/issues/4170),
[writer ADR](../../docs/adr/0086-configurable-wire-message-writers.md),
[reader ADR](../../docs/adr/0087-configurable-wire-message-readers.md),
[completion plan](completion-plan.md).
