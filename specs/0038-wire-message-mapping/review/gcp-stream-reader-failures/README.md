# GCP streaming reader failure recovery

Status: approved tests adopted unchanged; streaming reader recovery implemented.
Initial and restored focused tests pass on both frameworks, and all thirteen
approved mutations produce their intended failures. Full affected Pub/Sub runs
retain unresolved stream lifecycle failures; feature-wide production readiness
remains open.

## Contract

A configured streaming reader that throws or returns null must yield an
MT_UNACCEPTABLE message while preserving the native binary payload, native ID
when no application ID is supplied, source topic, handled count and actual SDK
callback receipt. Failure handling must not depend on successful default parsing.
The callback remains pending until an explicit settlement operation.

Once mapping recovers, nacking or requeuing that failed message must release its
callback with Nack. The same consumer must receive the same native message again,
apply the configured reader, and retain gateway ownership of receipt and count.
Acknowledgement must complete that new callback with Ack. A subsequently published
marker must also arrive and complete through the same consumer.

## Review files

- [Eight native cases](When_a_gcp_stream_reader_fails_should_preserve_the_delivery_and_recover.cs)
- [Recoverable mapping strategy](GcpRecoverableStreamWireReader.cs)

The theory crosses sync/async operations, exception/null results, and nack/requeue.
Every case creates its own native topic/subscription, configures the public factory,
and uses EmulatorOnly SDK clients. Buffer size and performer count are one. The
helper replaces only the application mapping strategy, not broker I/O. No generated
pump or logging generic is introduced.

The native body contains invalid UTF-8 bytes. The content-type attribute is malformed
to expose a fallback that reparses the same delivery through the default parser.
No application-ID attribute is supplied, so fallback identity must be the native
publication ID. The healthy reader returns a custom ID/topic/body and deliberately
incorrect receipt/count. Assertions inspect the recorded native inputs separately.

The initial receive must return without an exception and expose exactly one
unacceptable delivery. Redelivery must have a distinct callback envelope and mapped
message, with exactly two reader invocations. The fresh marker brings the invocation
count to three. Both recovery and marker callbacks must complete with Ack.

Callback checks use three-second bounds. Receive loops use twelve-second windows
and three-second receive calls; token-aware operations share a ninety-second budget.
Cleanup deletes only owned entities with fifteen-second bounds. Execution requires
an outer timeout because synchronous SDK setup and consumer disposal are not token
bounded. Callback completion is not proof of durable server acknowledgement.

## RED and mutation checks

Before correction, all eight cases fail at Assert.Null(error), line 108, on both
net9.0 and net10.0. Throwing readers surface InvalidOperationException; null-returning
readers cause NullReferenceException. No test or helper amendment is needed.

The correction catches reader exceptions/null results and creates an unacceptable
message directly from the native ID, topic and raw bytes. The normal gateway step
attaches the actual callback receipt and resolves native handled count. Failure is
logged without settling the callback. Neither fallback nor recovery parses the
malformed MIME attribute. Public constructors and dependencies are unchanged.

After correction all eight cases pass on both frameworks. Each of the following
temporary production mutations is then run independently on net10.0 and restored:

| Mutation | Required observation |
| --- | --- |
| Bypass the configured reader | Reader invocation or initial receive assertion fails |
| Remove exception/null recovery | Initial receive exception assertion fails |
| Use the default parser as failure fallback | Malformed MIME produces an initial receive exception |
| Mark fallback as a normal event | MT_UNACCEPTABLE assertion fails |
| Decode/re-encode fallback body as UTF-8 | Exact binary body assertion fails |
| Generate a fallback ID instead of using the native ID | Identity assertion fails |
| Remove/replace the fallback callback receipt | Receipt identity assertion fails |
| Discard native handled count | Count assertion fails |
| Automatically acknowledge the failed delivery | Pending callback assertion fails |
| Omit stream nack | Four nack cases fail bounded callback completion |
| Omit stream requeue nack | Four requeue cases fail bounded callback completion |
| Omit stream acknowledgement | Ack callback assertion fails |
| Stop using the configured reader after failure | Recovery invocation or mapped-content assertion fails |

All mutations produce assertion failures at the intended boundaries: eight cases
per mutation except omitted nack/requeue, which each fail their four applicable
cases while the other four pass. The total is 96 intended failures across thirteen
mutations. Restored focused runs pass all eight cases on both frameworks.

## Final validation

| Check | net9.0 | net10.0 |
| --- | --- | --- |
| Initial RED | 8 intended failures | 8 intended failures |
| Initial GREEN | 8 passes | 8 passes |
| Restored focused run | 8 passes | 8 passes |
| Full affected Pub/Sub matrix | 370 passes, 2 failures, 55 skips | 371 passes, 1 failure, 55 skips |

The combined filter is
`(FullyQualifiedName~Paramore.Brighter.Gcp.Tests.MessagingGateway.|Category=GcpPubSubPurge)&Fragile!=CI`.
Both runs retain all 419 prior case identities and add exactly the eight approved
cases, all passing. The filter combines the previous available and stream selections
without counting the eight overlapping purge cases twice.

The net9.0 failures are ordered async nack/redelivery (missing the second native ID)
and async disposal with an unsettled message (empty replacement receive). The net10.0
failure is sync disposal with an unsettled message (empty replacement receive).
All three passed in the immediately preceding saved full results. The earlier
net10.0 buffered-redelivery failure passes here; that does not establish a fix.

Three focused repetitions using preserved pre-change binaries pass all affected
cases: six net9.0 executions and three net10.0 executions. One current-build recheck
also passes all three cases. These controls do not reproduce the full-suite failures,
identify their cause, or prove the current implementation regression-free. Full-suite
lifecycle findings remain open; no shutdown or redelivery correction is included.

Library builds pass for netstandard2.0, net8.0, net9.0 and net10.0. The test project
build passes on both target frameworks with warnings. Only the stream
consumer changes among the 6,471 source files hashed after adopting the tests;
the other 6,470 remain unchanged. The adopted test/helper match the approved drafts
byte-for-byte. All temporary mutations are restored. There are no dependency or
public-constructor changes. XML receive documentation describes the new fallback.

The existing GcpNativeStreamReader category includes these cases in emulator CI;
remote CI has not been run. Firestore, Spanner, Cloud Storage, hosted-only and
Fragile=CI cases are outside the available Pub/Sub matrix. This is not a whole-project
or production-readiness pass.

Evidence is retained in `/private/tmp/brighter-4170-gcp-stream-failures/`: raw TRX/logs,
mutation results, pre-change control results, exact outcome comparisons and source
hash verification. The first sandboxed build stalls without output and is stopped;
the escalated build succeeds. Test execution uses isolated Pub/Sub emulators and
separate .NET SDK containers. Both emulators created for this batch are removed.
No commit or push is performed.

## Separate disposal investigation

The saved net10.0 stream regression fails at the original-ID assertion after a
twenty-second replacement-channel receive. Its total recorded duration is about
22.19 seconds. This confirms the observed empty receive, not the cause.

Source review identifies two observation gaps in the existing buffered-redelivery
tests: a two-second delay does not prove the redelivery was buffered, and Task.WhenAny
checks disposal task completion without awaiting its result. A faulted disposal
task can therefore satisfy the prompt-completion assertion. Neither observation
establishes an actual shutdown fault or message loss.

A separate diagnostic batch should observe callback arrival before shutdown, await
the disposal result, and distinguish SDK cancellation/Nack completion from native
redelivery. Do not increase the receive timeout or alter shutdown behavior without
that evidence. These eight mapping cases do not dispose with an unsettled callback
on their successful path and do not resolve the pre-existing disposal finding.

## Exclusions

Pull consumers, default-reader failures without a custom reader, mixed batches,
reader mutation of native inputs, cancellation, endpoint concurrency, populated
broker delivery counters, rejection/DLQ forwarding, scheduler paths and hosted
Pub/Sub behavior remain separate work.
