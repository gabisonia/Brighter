# Wire message mapping requirements

Wire mapping applies to all supported gateways. Applications can replace native
formats or compose the default mapping while gateways retain delivery ownership.

## Behavior

1. Each gateway accepts a custom wire writer on its publication and a custom wire reader on its subscription. Selection is per endpoint, not global mutable state.
2. Omitted configuration preserves existing wire formats, parsing fallbacks, public constructors, and supported sync and async operations.
3. Applications can implement a complete mapping or reuse the default mapping and alter selected fields. Custom readers can consume messages that the default reader cannot parse.
4. Writers control application metadata and body representation. Readers restore Brighter headers and body before tracing extraction and the application mapper run.
5. Writing occurs after producer trace propagation, on actual dispatch from the outbox or scheduler. Mapping must not bake dispatch-time tracing into the deposited message.
6. Gateways retain transport operations: destination resolution, acknowledgement tokens, delivery counts, retry bookkeeping, confirmation correlation, scheduling, and connection management. A custom mapping must not accidentally disable these operations.
7. A writer creates a fresh native representation without mutating the source body or bag. Readers and writers shared across messages must not store per-message mutable state on an instance.
8. Outbound mapping failures fail the send and leave the outbox entry undispatched. Inbound mapping failures follow each gateway's invalid-message path while retaining the delivery identity needed for rejection or acknowledgement.
9. Preserve provider-specific limits and validation. Optional custom fields cannot silently overwrite gateway-owned control values or be silently dropped to fit a limit.
10. Apply the feature to all gateway rows below. Completion of AWS alone does not complete this specification.

## Shared contracts

These synchronous contracts perform in-memory conversion. Producers and consumers retain asynchronous I/O ownership.

```csharp
public interface IAmAWireMessageWriter<out TWireMessage>
{
    TWireMessage WriteMessage(Message message);
}

public interface IAmAWireMessageReader<in TWireMessage>
{
    Message ReadMessage(TWireMessage message);
}
```

The contracts belong in `Paramore.Brighter` and have no broker SDK dependencies. Concrete publication and subscription properties use the appropriate closed generic type. Keep the existing nongeneric base configuration types compatible; introduce typed configuration in gateways that currently accept only `Publication` where needed.

Use native SDK payload types where they separate content from delivery operations. Use a small gateway-specific carrier where the SDK combines payload with an operation or splits payload across arguments. For SQS, `SqsWireMessage` contains `Body` and `MessageAttributes`; `SqsMessageSender` adds queue URL, delay, and FIFO controls. Default writers can receive immutable transport configuration at construction, including the queue URL needed by the existing SQS topic attribute.

Default implementations should support composition. The default SQS writer is sealed; applications alter its fresh result rather than override private serialization helpers. Avoid making every existing parsing helper public. Existing static conversion exports should delegate to default implementations without breaking their callers.
