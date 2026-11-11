# Kafka dead-letter writer selection

Review-after for #4170; native validation and guarded local integration complete. No commits/pushes.
The dead-letter producer currently omits the selected-writer seam already used by
invalid-message forwarding. Eight cases adapt the existing native invalid-route
acceptance to DeliveryError/dead-letter routing: replacement/default composition,
throw/null mapping failures, exact native fields and source-offset commit/recovery
on both factory modes. Public constructors and existing settlement remain unchanged.

Observe behavioral RED with subscription scaffolding before wiring the factory
and lazy producer. Preserve all 400 prior test outcomes and build all supported
targets after correction. Scheduled retries remain a separately identified gap.

Evidence: `/private/tmp/brighter-4170-kafka-dead-letter-selection/`.

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
