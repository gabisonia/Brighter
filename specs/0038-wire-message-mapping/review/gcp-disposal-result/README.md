# Observe the original async shutdown task result

Review-after #4170, test-first characterisation amendment. The two original async
unsettled/buffered shutdown cases compare Task.WhenAny with the disposal task but
never await that task. Completion within ten seconds therefore accepts a faulted
disposal. Preserve the timeout assertion and await the completed disposal before
creating the replacement channel. All other assertions and bounds stay unchanged.

Named mutation: throw InvalidOperationException("disposal-result-mutation") at
the end of GcpPubSubStreamMessageConsumer.DisposeAsync after native shutdown and
router disposal have finished, only on the first disposal for each unique
subscription. A temporary static dictionary scopes this fault to the original
consumer, leaving the replacement consumer's cleanup intact. Only the isolated
production copy is mutated.
First run the two original cases under the mutation to demonstrate the blind spot;
then add the awaits and require both cases to fail with this exact exception at
the newly awaited disposal. Restore production byte-for-byte and require both
cases to pass on .NET 9 and .NET 10. No simulated broker response is used.

Run the complete available Pub/Sub matrix once per framework on fresh pinned
emulators, retaining all 512 identities and all failures. Build serially; framework
regressions may run concurrently on independent fixtures. Integrate only the two
test awaits after verification, guarded against intervening edits, then confirm
the focused cases from a main-workspace build. No production correction or new
test identity is introduced, and existing lifecycle findings are not closed by
passing reruns. Stop owned fixtures, preserve evidence, and do not commit or push.

Evidence directory: `/private/tmp/brighter-4170-gcp-disposal-result/`.

The initial unscoped mutation also faults replacement-channel cleanup, so both old
tests fail there rather than detecting the original disposal fault. That unexpected
outcome is retained and is not counted as the intended RED. The scoped mutation
above distinguishes the intended observation without changing test operations.
Two earlier Docker name-collision setup failures ran no tests and are retained too.

## Characterisation result

With the scoped mutation, both original .NET 9 tests pass despite the original
consumer's disposal throwing. After adding the awaits, both tests fail on both
frameworks with the exact injected exception at the new awaited-disposal lines
(78 for unsettled, 82 for buffered), before replacement creation. These four
failures are the intended RED. Production is then restored exactly; both changed
tests pass on each framework. The isolated builds succeed with zero errors and
3,316 warnings.

## Full regressions and integration — complete, 2026-11-09

The first full matrices retain 512 identities each but record approximately 44.3
seconds of wall/monotonic clock divergence. The [host power log](host-sleep-wake.log)
records idle sleep from 21:16:32 to 21:17:19 local time. Those results are preserved
in [the interrupted-run summary](interrupted-regression-results.json) and the raw
`clock-discontinuity/` evidence directory; they are not the clean timing-validation
result. Both ordered-redelivery failures in that run precede the sleep event.

A justified repeat uses new fixtures and inhibits idle sleep only while the runner
exists. It retains all original assertions, timeouts, SDK versions and test IDs:

| Framework | Passed | Failed | Skipped | Wall / monotonic seconds |
| --- | ---: | ---: | ---: | --- |
| .NET 9 | 457 | 0 | 55 | 561.620 / 561.612 |
| .NET 10 | 456 | 1 | 55 | 558.220 / 558.213 |

All four original shutdown cases pass in each complete matrix. The .NET 10 failure
is the retained generated async ordered-nack redelivery assertion at line 158.
No new test identity is added or existing identity removed. These results do not
resolve historical shutdown failures or the separate ordered-redelivery finding.

Guarded integration adds only one `await disposing;` to each original async test.
The [exact patch](await-disposal.patch) is retained. A main-workspace rebuild has
zero errors and 3,320 warnings; both focused cases pass on each framework with
continuous clocks. [Source verification](final-verification.json) checks all 6,614
existing production/test source files: only those two tests differ, and production
matches its pre-mutation bytes. No dependency changes, commits or pushes occur.
All owned emulators are stopped and the runner's sleep inhibitor is released.

Azure remains blocked by AADSTS9002313; no alternate ASB test credentials are set.
The configured Docker contexts remain local, so supported x64 SQL Server coverage
is still unavailable. [Firebase release notes](https://firebase.google.com/support/release-notes/cli)
mention Pub/Sub emulator 0.8.37; the initial read-only registry lookup returned a
candidate ARM64 image [digest](emulator-candidate.json). The subsequent
[version comparison](../gcp-emulator-comparison/README.md) pulled that image,
verified its 0.8.37 JAR and reproduced the same ordered-nack protocol stall on
both frameworks. The candidate does not fix that control. No dependency or fixture
upgrade was made; the separate historical shutdown finding remains unresolved.
