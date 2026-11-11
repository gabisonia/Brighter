# Kafka scheduled retry publication selection

Review-after under the full #4170 completion plan. No commits or pushes.
Four native cases exercise sync/async factory paths and selected custom/default
writers through an actual InMemoryScheduler callback, typed publication registry,
outbox and Kafka delivery. A conflicting fallback format exposes wrong selection.
The logical topic, payload, CloudEvents type and source application metadata must
survive; the selector stays local and the original offset alone is committed.
Existing resend header cleanup remains unchanged. The alias topic must remain empty.

Observe actual format-assertion RED before wiring the captured selector. After GREEN,
omit the selector under a named mutation, restore exactly, and run the full Kafka
matrix together with the eight dead-letter cases against the prior 400 outcomes.
Evidence: `/private/tmp/brighter-4170-kafka-scheduled-selection/`.

## Validated result

Eight dead-letter and four scheduled retry cases reproduce intended assertion RED
on each framework, then pass after wiring. The scheduled-selector omission produces
four intended format failures; production source is restored exactly. Both full
Kafka suites pass 412 cases with no failures/skips, preserving every preceding
400 identity/outcome plus the twelve new cases. All four library targets build.
Three source files and four tests/helpers are integrated after hash guards.
Main-workspace restore/rebuild and fourteen combined native/ownership cases
pass on both frameworks without clock discontinuity. The earlier native disposal stall remains
an unresolved separate finding; these passing suites do not establish its cause.

Combined evidence: `/private/tmp/brighter-4170-kafka-scheduled-selection/final-verification.json`.
