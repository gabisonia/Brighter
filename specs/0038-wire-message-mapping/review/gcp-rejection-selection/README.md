# GCP rejection writer selection

Review-after for #4170. Twenty-four native cases cover pull/stream, sync/async,
shared invalid/dead-letter destination versus invalid fallback, composed mapping,
throwing/null writers and actual redelivery before successful recovery. Selections
must be captured at consumer creation and stay distinct even on the same topic.
Existing source settlement remains: a failed route releases/nacks the original.

Observe ignored-writer call-count RED with subscription-only scaffolding. Then
wire captured selections into route-specific cached publications, preserve public
constructors, restore any named mutations exactly and compare preceding/final
available Pub/Sub regressions. Historical streaming emulator findings remain open;
this package does not claim hosted, Firestore or Spanner validation. No commits/pushes.
Evidence: `/private/tmp/brighter-4170-gcp-rejection-selection/`.

## Result and remaining regression failure

All 24 cases fail on ignored-writer call counts before wiring, then pass on both
frameworks. Collapsing the producer cache to topic-only makes all twelve shared-route
cases fail while all twelve fallback controls pass; exact restoration returns GREEN.
This proves writer separation even when DLQ and invalid routes share one native topic.
Mapping throw/null releases the native source and recovery republishes its original
identity/body through the selected route.

The pre-change available Pub/Sub suites pass 429 cases/55 existing skips each.
Final suites retain all 484 preceding identities and add 24 passing cases, but have
452 passes/one failure/55 skips on each framework. The sole changed outcome is the
known generated async ordered-nack case at its missing-ID Assert.Contains (line 158).
No nack or stream callback implementation was changed. Existing native protocol/hosted
controls are relevant context, not proof of the cause of these particular failures.
These are not clean full-regression passes, and the lifecycle release gate stays open.

All four library targets build. Five source files and two tests/helpers are integrated
locally after original-main/final-isolated hash checks. Main-workspace restore/rebuild and all 24 new native cases pass on both
frameworks without host-clock discontinuity. An initial internal constructor XML-doc build error is retained as setup
evidence, not behavioral RED. No test exclusion, dependency change, hosted call,
commit or push was used. Compact results and source hashes are in [verification.json](verification.json).
