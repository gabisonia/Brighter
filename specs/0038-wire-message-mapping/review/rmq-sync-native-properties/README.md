# RabbitMQ Sync native-properties API amendment

Status: approved and implemented. All four amended files were copied unchanged
into compiled projects. Six cases pass on both frameworks; eight mutations produce
26 intended assertion failures. Both full Sync suites pass 219 cases and retain
the one existing skip. Original drafts and the failed first attempt remain saved.

## Finding

The [original endpoint proposal](../rmq-sync-endpoint-mapping/README.md) assumed
RabbitMQ Client 6 exposed a constructible `RabbitMQ.Client.Framing.BasicProperties`.
The installed client makes that class internal. The first implementation therefore
failed with CS0122 on all supported library targets. The replacement-writer helper
also uses that inaccessible constructor. The package's supported creation path is
`IModel.CreateBasicProperties()` on the publishing channel. No client upgrade is
proposed, and reflection into client internals is not an acceptable dependency.

## Revised contract

Keep `RmqPublication.WireMessageWriter`, `RmqSubscription.WireMessageReader` and
both public default adapters. Change the proposed outgoing value to:

```csharp
public sealed class RmqWireMessage
{
    public ReadOnlyMemory<byte> Body { get; set; }
    public Action<IBasicProperties>? ConfigureProperties { get; set; }
}
```

The publisher obtains fresh native properties from its existing channel, invokes
the optional callback once, then applies persistence and delivery-control headers
and publishes. This gives custom writers the full public native properties API
without a second connection, native-class construction or a parallel property DTO.
The callback must only populate the supplied properties, must not retain them and
runs during the send. Exceptions follow the existing send failure path; additional
writer-recovery coverage remains separate. A null callback leaves application
properties unset, while gateway-owned delivery controls still apply.

The default writer snapshots its application values and headers when WriteMessage
runs, and returns a callback that populates a fresh property object from those
values. Composing writers capture the default callback and invoke it before their
own adjustments. Return fresh output per call. Native retries continue to use the
default format; this batch does not select custom retry writers.

## Test amendment

Both test files and the reader helper are byte-identical to the original review.
Only the [writer helper](SyncEndpointWireWriter.cs) changes: it populates the native
properties supplied to the callback instead of constructing an inaccessible class.
It captures source ID, tenant and traceparent during WriteMessage. Assertions and
six cases remain unchanged, including default composition, replacement, sync/send
wrapper paths, persistence, delay/delivery-tag removal, dispatch-time tracing,
original confirmation identity, source preservation, reader metadata and ack.

The same eight named mutations in the original review remain required, with
26 intended assertion failures. Writer-control mutations apply after configuring
the native properties. Late-tracing mutation must still fail the native parent
comparison because the helper captures traceparent during WriteMessage.

## Reviewed validation plan (completed)

Copy these four reviewed files into the same compiled locations, retain the
recorded missing-API RED, implement the revised contract and run all six cases on
both frameworks. Apply and restore all eight mutations, rebuild the library on
all supported targets and run the full Sync suites against isolated RabbitMQ 4.2
with TLS and native-delay support. Compare against the existing clean baseline:
213 passed and one existing skip on each framework; expect six added passes.
Preserve prior WIP and stash; do not commit or push.

The failed attempt is saved under
`/private/tmp/brighter-4170-rmq-sync-endpoints/failed-attempt/`, with original
sources, build logs and baseline TRX alongside it. The first baseline attempt used
a reused task broker and encountered a durable/non-durable `mary` queue conflict;
it was stopped and its log retained. Both clean baselines passed on a fresh broker.

The subsequent [mixed reader-failure batch](../rmq-sync-reader-failures/README.md)
is implemented and validated separately. This amendment does
not complete RabbitMQ Sync parity or issue #4170.
