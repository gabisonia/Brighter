# MQTT consumer-owned wire writers

Review-after for #4170. Native validation is complete and guarded local integration is done.
Source inspection finds all three consumer-created producers construct plain
Publication instances, so configured mappings cannot be selected for immediate
requeue or invalid/dead-letter forwarding.

Eighteen native cases cover public sync/async factories, three routes, composed
writer output, throwing/null writers and recovery. Subscription selection must be
captured before lazy producer creation. Native observers assert exact route,
identity, payload, metadata and inactive-writer isolation. Retry failures propagate
and permit retry of the same in-memory message. Rejection preserves the existing
logged/true-result semantics; recovery uses a subsequent native source delivery.

First add configuration scaffolding only and observe behavioral RED. Then wire the
captured selections into typed internal publications, preserve public constructors,
run full prior and final matrices on both frameworks and build all supported targets.
Scheduler-backed retry selection remains a separate path; this batch does not claim it.

Evidence: `/private/tmp/brighter-4170-mqtt-consumer-writers/`. No commits or pushes.

## Validated result

Both frameworks reproduce eighteen intended assertion failures before factory and
lazy producer wiring: wrong native payload, unexpected failure publication, or
missing retry error. Test setup compile/configuration errors are retained separately.
All eighteen cases pass after wiring. Full matrices preserve all 103 prior identities
and add eighteen passes: 117 passes/four existing skips per framework. All supported
library targets build. Public consumer constructors remain unchanged.

Three production files and two tests/helpers are integrated after source-hash guards.
MQTT protocol acknowledgement still belongs to the SDK; rejection writer failures
retain the existing logged/true result. No recovery of a lost original is promised.
Scheduled retry selection remains a separate required path.

Main-workspace locked restore/rebuild and the combined 22 native acceptance cases
pass on both frameworks with no host-clock discontinuity.
