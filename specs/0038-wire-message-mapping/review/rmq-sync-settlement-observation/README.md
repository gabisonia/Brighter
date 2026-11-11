# RabbitMQ Sync settlement observation amendment

Status: approved amendment copied unchanged and validated. All ten cases pass
initially and after restoration on .NET 9 and .NET 10. Six mutations produce 42
intended failures. Three additional acknowledgement-omission runs per framework
produce all 60 intended failures, with no misses. Both full suites retain exactly
237 passes and one existing skip. All 1,280 production files and the original
approved helper are unchanged in this observation increment. The subsequent
[fourteen invalid-writer cases](../rmq-sync-invalid-writer/README.md) are now validated
with their approved native-tag correction; see that batch for the later results.

The [original ten cases](../rmq-sync-invalid-forwarding/README.md) pass on both
frameworks. The omit-source-ack mutation initially produces nine intended failures
and one pass (explicit unacceptable reader, unbound-route recovery). A repeat of
the same mutation produces all ten intended failures. Both runs are retained;
the second result does not erase the false negative in the first.

The original test asked BasicGet once immediately after consumer disposal. The
consumer aborts its channel during disposal. Reading an empty queue at one instant
can miss a delivery whose requeue has not yet become observable. This is a plausible
explanation for the miss, not a proven broker trace of that delivery.

The [amended test](When_rmq_sync_forwards_invalid_mappings_should_preserve_content_and_recover_from_an_unbound_route.cs)
keeps all ten inputs and all payload, identity, mapping, metadata, routing and
settlement expectations. Only the two source-queue observations after disposal
change:

- Failed forwarding: poll BasicGet for up to two seconds until the original delivery
  appears, then keep the existing native-delivery assertions.
- Successful forwarding: poll for two seconds and fail if a source delivery appears
  at any point. An immediate empty response no longer ends the check.

Both loops use Stopwatch and 25-millisecond polling intervals. The tests still
observe the real broker through an independent channel. There are no mocks, new
helpers, production APIs, arbitrary sleeps before an assertion or weakened expected
outcomes. The observation remains bounded; it does not prove absence forever or
atomic publication and acknowledgement.

## Validation after approval

Copy the amended file over the compiled test, leaving the original review draft
and evidence intact. Rebuild and run all ten cases on both frameworks. Re-run the
six named mutations with these assertion locations:

| Mutation | Expected failures | Assertion |
| --- | ---: | --- |
| Replace configured-reader result with default parsing | 2, explicit reader only | Mapped ID, line 123 |
| Publish an empty forwarded body | 10 | Exact bytes, line 210 |
| Move source acknowledgement before forwarding | 5, unbound route only | Original native delivery, line 156 |
| Ignore mandatory BasicReturn | 5, unbound route only | Forwarding exception, line 140 |
| Omit source acknowledgement after successful forwarding | 10 | Source absent throughout observation, line 207 |
| Leave the message topic at the invalid route | 10 | Original topic, line 141 or 198 |

Require 42 intended failures across the six mutations. Then repeat the
omit-source-ack mutation three additional times on each framework, requiring all
ten cases to detect it in every run (60 additional intended failures). A build
error, timeout or unrelated exception is not evidence. Restore production bytes
and normal optimization, run the focused cases and both complete Sync suites,
retaining every preceding outcome by exact name. The amended file replaces an
existing test, so the full-suite case count must not increase.

Record the first false negative and every subsequent result. If a miss persists,
report it rather than retrying until a passing mutation result can be selected.
No commit, push or review-gear change is authorized by this batch.


## Recorded result

All prescribed mutations and repeats meet the exact failure counts and assertion
locations above. Restored normal builds and complete suites pass on both frameworks.
The unchanged case count and exact-name comparison confirm that the amendment
replaces the existing test rather than adding duplicate cases. Source manifests,
TRX, mutation logs and `verification.json` are retained under
`/private/tmp/brighter-4170-rmq-sync-settlement-observation/`.

The original false negative remains in the preceding batch's evidence. These runs
support the two-second observation under the tested environment; they do not prove
absence beyond that bound or resolve the earlier broker storage I/O finding.
HEAD and the saved stash are unchanged; no commit or push is made.
