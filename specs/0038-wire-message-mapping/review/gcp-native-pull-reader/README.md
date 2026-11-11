# GCP native pull reader selection

Status: implemented and validated on net9.0 and net10.0 against a native Pub/Sub
emulator. This completes the reviewed pull-reader selection batch, not GCP or
feature-wide production readiness.

## Contract and implementation

GcpPubSubSubscription.PullWireMessageReader selects an
IAmAWireMessageReader<ReceivedMessage>. Null retains default parsing through
DefaultGcpPubSubPullWireMessageReader. Both public consumer factory paths carry the
selection into their dedicated pull consumer. The existing public consumer constructor
is unchanged; an internal overload receives the selection from the factory.

Both receive paths call the selected reader once, then overwrite the receipt with
the native AckId and derive HandledCount from native attributes and DeliveryAttempt.
The default parser uses the same count resolver. This makes no extra broker calls
and allocates no intermediate message just to recover delivery metadata.

The sync and async cases publish binary bytes and three native attributes. The
reader returns a custom ID, routing key, prefixed body and bag value, while supplying
an invalid receipt and HandledCount=777. The gateway must retain native settlement
ownership and the wire HandledCount=7 fallback. The subscription has no DLQ policy,
so the native DeliveryAttempt is zero; populated counters are outside this focused
batch. Native body, ID and attributes remain unchanged.

## Native settlement evidence

After the first receive, a native RPC extends its deadline to 30 seconds. Consumer
Requeue releases it for a native observer within a five-second polling window
(with at most one three-second RPC overrun). The release therefore precedes natural
expiry. The observer verifies the native ID/body/attribute and releases it again.
The same consumer receives a second independently mapped message, using the receipt
from that delivery. No assumption requires GCP to change AckId between deliveries.

After acknowledging the second delivery, the test waits twelve seconds past its
ten-second deadline. A six-second native observation window must find a newly
published marker and no original message. The omitted-ack mutation detects the
original message returning in both modes. This is bounded emulator evidence, not
a guarantee against arbitrarily late hosted redelivery. Pull Nack remains a no-op;
the tested immediate-release operation is Requeue.

## Validation

| Check | net9.0 | net10.0 |
| --- | --- | --- |
| Ignored reader before integration | 2 intended failures | 2 intended failures |
| Initial implementation | 2 passes | 2 passes |
| Restored implementation and emulator CI selection | 2 passes | 2 passes |
| Complete selected Pub/Sub regression matrix | 230 passes, 30 existing skips | 230 passes, 30 existing skips |

Both original RED runs fail at the reader invocation assertion with only the
selection property added. No draft corrections or assertion changes are needed.
All 258 outcomes from the preceding writer batch are retained exactly, with two
new passes. The regression filter includes all eight streaming purge cases.

Eight named production mutations run on net10.0 and produce 14 intended failures:

| Mutation | Failures | Intended assertion |
| --- | --- | --- |
| Bypass sync reader | 1 | Reader invocation count |
| Bypass async reader | 1 | Reader invocation count |
| Keep reader receipt | 2 | Receipt equals native AckId |
| Restore native body | 2 | Prefixed mapped body |
| Trust reader count | 2 | Native wire fallback is 7 |
| Omit native release | 2 | Native delivery observed before expiry |
| Omit native acknowledgement | 2 | Original ID absent after deadline |
| Read twice | 2 | Reader invocation count |

The unaffected mode passes in each bypass run. All mutations are restored byte for
byte. Selected regressions use independent saved green binaries; their hashes remain
unchanged throughout mutation execution. Removing an obsolete parser comment does
not change behavior; restored focused checks and final workspace builds also pass.

The test and helper match the reviewed files exactly. Four existing production files
change and one default adapter is added. The other 6,459 pre-existing production/test
C# files remain unchanged. The workflow adds the reader category to emulator CI;
the hosted exclusion selects none. No new command, handler, pump or logging
registration is needed, and no dependencies change.

## Evidence and limits

Evidence: `/private/tmp/brighter-4170-gcp-reader/`, including builds, RED/green/restored
TRX files, all named mutation results, exact baseline comparisons, binary/source
hashes and the scoped CI diff. The baseline comes from the preceding validated
writer batch. The isolated native emulator publishes no host ports and is removed
after validation. Nothing is committed or pushed.

The selected matrix covers MessagingGateway tests except Stream/StreamOrdering
conformance, with all GcpPubSubPurge cases included. Fragile=CI cases remain excluded.
This is not a whole-project pass: Firestore, Spanner, Cloud Storage and hosted-only
suites were not executed. The separate ASB baseline remains 628 per framework.

Streaming readers, failed/null mappings, mixed batches, concurrency, subscription
isolation, populated delivery counters, rejection-routing metadata, scheduler paths
and broader release gates remain open. Overall production readiness is not established.
