# Redis subscription-selected scheduled retries

Status: approved and complete (2026-11-02) under the user-approved paced regression plan.
Both frameworks pass 186 cases/four existing skips, with all preceding outcomes retained.

Review [the four cases](When_a_redis_subscription_selects_scheduled_retry_mapping_should_preserve_its_source_and_target_only_its_queue.cs).
Reuse the existing `RedisPublicationWireWriter` and `RedisRecoverableRequeueWireWriter`
helpers unchanged. No package, project reference, lockfile or workflow changes.

## Proposed opt-in API

| Property | Type | Meaning when supplied |
| --- | --- | --- |
| `RedisSubscription.ScheduledRequeuePublicationTopic` | `RoutingKey?` | Select the registered publication used for positive-delay retries; captured when the consumer is created |
| `RedisMessagePublication.DestinationTopic` | `RoutingKey?` | Override native Redis storage/counter topic without changing the message's logical topic |
| `RedisMessagePublication.ChannelName` | `ChannelName?` | Deliver to this one queue rather than fan out through the native topic's recipient set |

All three default to null. Existing callers retain their current delayed-publication
behavior and public constructor compatibility. Immediate retries continue using
`RequeueWireMessageWriter`. Delayed writers remain on registered publications:
subscriptions select a stable publication name, not a delegate serialized into a job.
The selected publication must be registered with the original message's event type.
Its destination topic/queue are application configuration, not inbound message data.

This adds explicit per-subscription delayed mapping selection while keeping mapping
at dispatch, where producer trace propagation occurs. Two subscriptions can select
different retry publications with independent writers/destinations. This first batch
proves one configured endpoint per case; shared-factory multi-endpoint isolation,
invalid/missing configuration and concurrent changes still need separate coverage.

## Existing mechanism and implementation boundary

Brighter already stores a local producer lookup topic under
`Message.ProducerTopicHeaderName`. Both ordinary outbox dispatch paths use it while
retaining `Header.Topic`. Redis default serialization filters that local-only key.
Reuse this mechanism on a private copy of the scheduled message. Preserve application
ID, event type, handled count, topic, body and metadata; do not modify the caller's
message when adding the lookup key or when the later callback propagates tracing.
Do not use `MessageHeader.Copy()` without preserving its omitted/reset fields.

The callback's registered Redis producer must use the publication's destination
storage topic and queue. It still owns the native counter, expiry and native ID;
the selected writer only supplies the stored string. Preserve no-option behavior.
No shared scheduler/outbox API redesign is proposed. Serialization survival is
supported by the existing string-bag mechanism but durable scheduler runtime
coverage is not established by this in-memory batch.

## Four native cases

Sync/async × custom/default cases run through the real in-memory scheduler,
command processor, scheduler handler and outbox mediator. Source storage topic,
logical application topic and retry publication lookup topic are deliberately
three different values. Both ordinary and retry publications are registered with
a non-default event type; the ordinary writer is a decoy. A sibling queue subscribes
to both native/source and application topics so an accidental fan-out is observable.

The subscription selection is changed after consumer creation to the ordinary
publication: the consumer must retain its originally captured retry publication.
The original source is received, its body revised, and a five-second retry scheduled.
The source's complete JSON representation must remain unchanged immediately after
scheduling and after dispatch. No writer/native publication may run before the due
time; the same consumer must receive/acknowledge the next source message and finish
an empty receive before that time.

At dispatch, the source storage counter advances from 41 to 42 and only the source
queue receives native ID 42. The sibling queue stays empty. Custom payload must be
exactly `W:` plus the revised body; default payload must preserve application ID,
topic, event type, handled count 7 and marker, without the local producer selector.
Original native payloads remain unchanged; neither application nor lookup topic
gets a counter/payload. TTL and outbox settlement must be correct. The immediate
retry writer and ordinary publication writer are never invoked.

These assertions cover selection/routing and source preservation. Submission and
callback failure recovery, cancellation, durable persistence, cross-consumer mapping
isolation and dispatch-time telemetry remain separate work.

## Execution after approval and preceding regression completion

Copy the approved test unchanged only after the preceding paced gate finishes.
The three properties do not exist yet: record the compilation failure separately,
then add only nullable auto-properties with XML contracts. With selection/routing
still unwired, require four behavioral failures at the native source counter
assertion (expected 42, actual 41). The ordinary publication delivers to the observed
queue, allowing the test to reach an assertion instead of timing out.

Implement the minimum Redis subscription/factory/consumer/publication/producer
changes. Preserve old constructors and unaffected source files. Require four passes
on both frameworks, then validate independent .NET 10 production mutations:

| Mutation | Intended failure | Count |
| --- | --- | ---: |
| Ignore the captured scheduled-retry publication selection | Source native counter remains 41 instead of 42 | 4 |
| Ignore the publication destination topic during dispatch | Source native counter remains 41 instead of 42 | 4 |
| Ignore the configured queue and use recipient fan-out | Sibling queue must be empty after dispatch | 4 |
| Ignore the selected publication writer | Exact custom native string; default cases still pass | 2 |
| Add the lookup key to the caller's original message instead of a private copy | Exact source JSON immediately after scheduling | 4 |

Require 18 intended assertion failures, exact source restoration and four restored
passes after each mutation. Compiler, timing, setup and callback crashes do not count
as behavioral RED. Return semantic test/contract changes for review before running
them. Final focused checks use ordinary optimized builds on both frameworks.

Build all four Redis library targets. Complete disjoint native regression batches
must preserve all 186 preceding outcomes and add four passes: 186 passes/four skips,
190 cases exactly once per framework. This baseline assumes the pending preceding
gate succeeds; never claim its unrun outcomes. Keep the pinned Redis image/native SDK
runners, no host ports, real 3,605-second waits after focused tests/between regression
batches, temporary idle-sleep prevention and owned cleanup. Retain the 45-second
async token, one-second receives and external process/hang bounds. Keep the lid open.
If shared Core code changes become necessary, stop this local implementation plan
and review that wider scope before proceeding.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". This is the concrete
review-before [/test-first](../../../../.claude/commands/tdd/test-first.md) package.
Approval covers these four tests, the three opt-in properties and required Redis-only
wiring/docs, 18 reviewed mutation failures/restorations, native focused/paced tests,
four-target builds and cleanup/reports after the preceding gate completes.
No hosted calls, commits, deployment or publication are included.


## Approval recorded and initial execution queued

The user explicitly approved the four cases, three properties and Redis-only
implementation plan. Approval persists; no additional test-review approval is
required for this exact package. The runner in
`/private/tmp/brighter-4170-redis-scheduled-retry-selection/queued-red.py` waits for
the preceding independent regression verification and completion report, verifies
all pinned inputs, then waits at least one real hour after the preceding tests.
It adopts the unchanged test, observes the missing-property compile failure, adds
only the three approved nullable properties, and requires four intended native
counter failures per framework. It removes its owned broker and stops with evidence
ready for the approved functional implementation. Any unexpected result stops the
queue for investigation; it does not retry or change the reviewed assertions.

The queued RED phase completed on 2026-11-02 at 01:20:42 UTC and removed its broker.
The missing-property compilation result is separate from behavioral RED: all four
cases per framework failed at line 195, expecting native source counter 42 but seeing
41. The approved Redis-only wiring now passes all four initial focused cases per
framework. Validation records remain in that directory; hosted calls, commits and
deployment remain excluded.


## Redis subscription-selected scheduled retries: final focused validation (2026-11-02)

The approved five-file Redis implementation passes all four native focused cases
on .NET 9 and .NET 10, including final ordinary optimized builds. The preceding
property-only implementation produced four intended counter assertion failures per
framework. Five independent mutations produce all eighteen reviewed assertion
failures; exact restoration returns four passes after every mutation. Four Redis
library targets build with zero warnings/errors. The test build retains 604 existing
warnings. The approved test/helpers, 6,520 other prior C# files and protected
inputs are unchanged. The focused broker is removed.

The complete paced regression schedule is running, with its first batch eligible at
2026-11-02T18:27:19.877436+00:00. Four disjoint native batches use real 3,605-second
idle intervals, temporary idle-sleep prevention and automatic owned cleanup. New
cases join the smaller prior batch to preserve free-quota headroom. Expected coverage
is 190 cases exactly once per framework: 186 passes and four existing skips, with
all 186 prior outcomes retained. These outcomes are pending, not yet claimed.
Keep the laptop lid open; idle-sleep prevention cannot prevent lid sleep.

Evidence: `/private/tmp/brighter-4170-redis-scheduled-retry-selection/focused-verification.json`.
Live schedule: `/private/tmp/brighter-4170-redis-scheduled-retry-selection/paced/status.json`.
Successful completion independently checks identities, outcomes, source/assembly
hashes, real idle gaps and cleanup before updating local completion reports.
Failure stops later batches and retains evidence. Whole-feature production readiness
is not established; multi-endpoint isolation, invalid configuration, scheduler
failure/cancellation/persistence and broader release gates remain open.


## Redis subscription-selected scheduled retries completion (2026-11-02)

The four approved sync/async × custom/default cases pass through native Redis and
the production in-memory scheduler, command processor, scheduler handler and outbox
mediator. Redis subscriptions capture an optional scheduled publication topic; the
registered publication selects its writer, native destination topic and single queue.
The original application topic/type and caller message are preserved. Null options
retain existing routing, and the public consumer constructor remains compatible.
Only five Redis production files change; shared Core and scheduler APIs are unchanged.

Initial behavior fails at the intended native source counter assertion on both
frameworks. Five independent production mutations then produce eighteen intended
assertion failures, with four passes after each exact restoration. Initial and final
optimized focused runs pass four cases per framework; all four Redis library targets
build with zero warnings/errors. The approved test/helpers, 6,520 other preceding C#
files and protected dependency/workflow inputs remain unchanged.

Complete paced regressions pass **186 cases, zero failures and four existing skips
per framework**. All 190 cases run exactly once per framework, preserving all 186
prior outcomes. Real idle intervals are at least 3,605 seconds, with no quota or
clock-discontinuity errors; owned containers are removed. This is complete batched
coverage, not a single-process full-suite pass. Independent verification:
`/private/tmp/brighter-4170-redis-scheduled-retry-selection/paced/final-verification.json`.

This closes the reviewed single-endpoint subscription selection and queue-specific
routing batch. Shared-factory multi-endpoint isolation, invalid/missing configuration,
submission/callback failure recovery, cancellation, durable schedulers and broader
feature release gates remain open. No hosted CI, commit, push or deployment is claimed.
