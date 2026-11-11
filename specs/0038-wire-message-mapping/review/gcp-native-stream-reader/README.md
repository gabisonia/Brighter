# GCP native streaming reader selection

Status: implemented and validated for the reviewed reader-selection behavior on
net9.0 and net10.0. Broader streaming regressions retain a pre-existing failure;
GCP and feature-wide production readiness remain open.

## Implemented contract

GcpPubSubSubscription.StreamWireMessageReader selects an
IAmAWireMessageReader<GcpStreamMessage>. Null preserves existing parsing through a
default adapter. Both public factory entry points carry the selection to the
stream consumer without changing its existing public constructor signature.

The reader owns the application ID, routing key, body and application metadata.
The gateway restores the actual GcpStreamMessage receipt and resolves delivery
count from native attributes, irrespective of the reader's receipt/count.

## Four native cases

The theory crosses sync/async factory and consumer operations with Nack/Requeue.
Each case creates its own topic and subscription using EmulatorOnly native SDK
clients. It uses one performer and a buffer of one, with no generated message pump
or handler registration. The helper records real callback envelopes and returns
a custom ID, topic, prefixed binary body and marker, plus a fabricated callback
receipt and HandledCount=777.

Assertions require one reader invocation per received delivery, preserved native
ID/bytes/attributes, actual callback identity and native wire count fallback 7.
No DLQ is configured, so populated broker delivery counters are outside this batch.

The first callback must remain pending until the consumer nacks or requeues it,
then complete with the native SDK's Nack reply within three seconds. The same
consumer must receive the same native message again with a distinct callback and
mapped message. Acknowledging that delivery must complete its callback with Ack.
A subsequently published marker must arrive through the stream and be acknowledged,
demonstrating continued delivery after callback completion.

This observes native redelivery and callback completion, not durable server-side
acknowledgement or an absence-of-redelivery guarantee. It does not treat an empty
stream receive as proof of settlement. Receive windows are twelve seconds with
three-second calls; operations share a ninety-second cancellation budget. Cleanup
deletes owned resources with bounded waits. Execution also needs an outer process
timeout because synchronous SDK setup and stream disposal are not token bounded.

## Validation

| Check | net9.0 | net10.0 |
| --- | --- | --- |
| Ignored reader before integration | 4 intended failures | 4 intended failures |
| Initial/restored focused cases | 4 passes each | 4 passes each |
| Previously selected regression matrix | 234 passes, 30 skips | 234 passes, 30 skips |
| Additional stream matrix | 138 passes, 25 skips | 137 passes, 1 failure, 25 skips |
| Combined unique outcomes | 364 passes, 55 skips | 363 passes, 1 failure, 55 skips |

All 260 outcomes from the preceding pull-reader batch are retained exactly, with
only four new cases in that selection. The additional stream matrix overlaps eight
purge cases; combined totals count those only once. These are selected Pub/Sub
results, not whole-project passes. Fragile=CI cases, Firestore, Spanner, Cloud
Storage and hosted-only behavior are excluded.

The pre-implementation net10.0 stream baseline has 134 passes, four failures and
25 skips. Failures concern ordered nack/redelivery, sync and async disposal with
buffered redelivery, and sync disposal with an unsettled message. The final
net10.0 stream run retains the async buffered-redelivery failure; all previously
passing cases still pass. The other three baseline failures passing later does
not establish that they are fixed. The remaining failure is
GcpStreamDisposeWithBufferedRedeliveryAsyncTests at its native message-ID assertion
(line 87), after prompt disposal and creation of a replacement channel. The replacement
channel returns an empty message after its twenty-second receive window. Root cause
is unresolved.

Eight mutations on net10.0 produce 28 intended failures:

| Mutation | Failures | Assertion |
| --- | --- | --- |
| Bypass reader | 4 | Invocation count |
| Retain fabricated receipt | 4 | Native callback identity |
| Restore native body | 4 | Mapped binary body |
| Trust reader count | 4 | Native wire count fallback |
| Invoke reader twice | 4 | Invocation count |
| Omit nack | 2 | Bounded callback completion |
| Omit requeue | 2 | Bounded callback completion |
| Omit acknowledgement | 4 | Bounded callback completion |

All mutations are restored; both focused suites pass afterwards. The test and
helper remain byte-for-byte identical to the approved draft. Four existing
production files change and one default adapter is added; 6,462 other existing
production/test C# files are unchanged. The shared parser count resolver preserves
existing native-counter and routed-rejection policy without parsing an intermediate
message. The existing public stream consumer constructor is unchanged.

The emulator CI filter includes the new category. Category checks pass four cases
per framework; hosted exclusion selects none. Normal final builds pass with
existing warnings. Regressions use preserved final binaries, independent of the
mutation builds. No dependency changes, commits or pushes are included.

Evidence: `/private/tmp/brighter-4170-gcp-stream-reader/` contains builds, raw TRX,
mutation assertions, source verification, filters and exact outcome comparison.
The initial sandbox build times out without output; the escalated build succeeds.

Mapping exceptions/nulls, isolation/concurrency, positive delivery counters,
rejection routing, cancellation and hosted stream behavior remain separate gates.
The next priority is reproducing and diagnosing the existing streaming redelivery
failures before claiming this transport is production-ready.
