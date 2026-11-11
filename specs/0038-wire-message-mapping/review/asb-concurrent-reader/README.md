# ASB concurrent reader — validated

Status: approved and validated. Both native cases pass on .NET 9 and .NET 10,
initially and after mutation restoration. All six named defects have intended failure
evidence, with the cache-isolation qualification below. Both full available suites
pass 590 cases, zero failures/skips, retaining all 588 previous case/outcome pairs.
No production correction was needed.

## Approved contract

Two distinct consumers, one queue and one topic subscription, share a safe reader.
Sync and async cases publish distinct messages through the native SDK. Dedicated
workers receive concurrently; a two-participant, 20-second barrier inside the reader
proves overlapping mapping calls. The helper records native deliveries by unique
message ID in a ConcurrentDictionary and creates fresh mapped messages and bags.

Mapped IDs, routes, bytes, subject, correlation and reader metadata must stay with
their respective deliveries. The helper supplies deliberately invalid lock tokens
and sequence numbers; the consumer must replace these with each delivery's native
values. Lock tokens must differ. Sequence numbers need not differ across entities.
The helper uses no shared mutable per-message mapping state and does not replace I/O.

Before acknowledging, native peeks at the exact sequence numbers must see both locked
deliveries with their original IDs. After each consumer acknowledges its own mapped
message, native peeks from those same sequence numbers poll for up to two seconds
and must observe removal. A locked message is still visible to peek, so temporary
inability to receive does not substitute for settlement evidence. Destinations are
isolated and contain no unrelated traffic. The omission mutation below validates
this bounded observation; it is not an unlimited absence guarantee.

Both receive tasks are awaited before disposing consumers and the shared reader.
Barrier waits and receives are bounded; async I/O uses a 90-second token. Sync calls
also rely on the external runner timeout. Existing ASBTestCommand is used only for
subscription configuration; no new handlers or logging registrations are introduced.

## Named production mutations

1. Ignore the configured reader: reader count or custom mapping fails.
2. Cache the first completed mapped Message across consumer instances under a lock,
   after the reader returns: distinct-message or identity isolation fails.
3. Cache the first completed mapped body across consumers while retaining separate
   message/header objects and native settlement metadata: per-delivery body fails.
4. Omit the gateway's native lock-token override: receive/dispatch rejects the invalid
   token, or the native-token assertion fails. This guards token ownership, not body
   isolation; unrelated broker failures do not count.
5. Omit the native sequence-number override: the exact native-sequence assertion fails.
6. Omit native completion during acknowledgement in both sync and async paths:
   post-acknowledgement native peek still finds the previously observed delivery.

Cache mutations must act after mapping completes, leaving the helper barrier intact.
Timeouts do not establish message/body isolation. Preserve native object ownership
for the body-only mutation to avoid unrelated SDK failures.

## Validation evidence and cache qualification

Five initial mutations produce ten intended failures. The initial shared-Message
cache produces the intended NotSame failure for sync, but async rejects the corrupted
lock during dispatch and returns no message. That async failure is retained as raw
evidence and is not counted as proof of the planned identity assertion.

To isolate the same cached-identity defect, the refined production mutation copies
the cached header into a fresh message before attaching each delivery's native lock
metadata, while retaining cached application identity and body. Both modes then fail
at the mapped-ID assertion. Tests and helper were not changed or weakened. The final
six-defect validation has twelve intended failures (ten plus the two refined identity
failures); the initial cache attempt is reported separately rather than hidden.

All mutations are restored. All 1,171 non-generated production C# files match their
pre-batch hashes. Approved test/helper match their compiled copies byte-for-byte.
All library targets build. Both full available suites pass 590 cases on their first
runs, zero failures/skips, with exact retention of all 588 previous outcomes.
Previous session-lock instability remains unresolved despite passing here.

Evidence: `/private/tmp/brighter-4170-asb-concurrent-reader/` contains initial/restored
matrices, initial mutations.json (including the rejected cache result), refined
cache-identity-verification.json, full regression TRXs, exact baseline comparisons
and source verification. Dedicated fixture containers are stopped after validation.
Nothing committed or pushed.

This covers concurrent mapping across two distinct consumers and subsequent individual
acknowledgements. Concurrent settlement, repeated receives on one consumer, nack,
sessions and reader failure recovery remain separate. Scheduled sends, cancellation,
session-lock instability, eight DLQ-blocked cases, 52 unavailable hosted classes and
broader production-readiness gates remain open. No commits or pushes are requested.
