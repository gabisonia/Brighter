# Redis writer failure recovery

Status: approved, implemented and validated on 2026-10-30.

Review [the four cases](When_a_redis_writer_fails_should_preserve_the_error_and_recover_without_native_publication.cs)
and [the recoverable writer](RedisRecoverableWireWriter.cs).

## Contract and behavior

Sync/async × throw/null cases configure a writer that fails once and then composes
the default writer. Writer exceptions must reach the caller as the original
instance. A null result must fail with InvalidOperationException before incrementing
the message counter, storing a payload or adding a queue entry. A failed mapping
must not silently succeed or substitute default/empty content.

An independent native Redis client registers the recipient queue before the failed
send. Immediately after failure, the topic counter and first payload key must be
absent and the queue empty. The same producer must then send a valid message whose
exact legacy envelope is stored at ID 1, with counter 1 and exactly one queue entry.
ID 2 remains absent. The selected writer runs twice and the failed source ID,
Unicode text body, topic and marker bag remain unchanged.

These observations cover mapping failures before native publication. They do not
establish atomicity after a Redis command succeeds, transaction/outbox behavior,
pooling failure recovery, retries, cancellation, concurrency or scheduler delivery.
Reader failures and consumer retry/rejection mapping remain separate. Existing
public signatures remain compatible; any evidenced correction must be confined
to validating the writer result before the first native mutation.

## Results

Both frameworks initially passed the two throwing cases and failed the two null
cases at `Assert.NotNull(error)`: null results silently succeeded. These failures
preceded the native-state assertions, so the initial run does not establish their
counter/storage/queue values. Both send paths now reject null with
InvalidOperationException before native mutation, preserving original writer errors.

All four unchanged approved cases pass on .NET 9 and .NET 10. The four reviewed
mutations produced fourteen intended assertion failures; each exact production
restoration returned four passes. Premature publication failed at the absent-counter
assertion, after satisfying the original mapping-error assertions.

Both complete suites passed 112 cases with four existing skips, retaining all 112
preceding identities/outcomes and adding four passes. All four library targets built
with zero warnings/errors. The initial dependency build reported 604 existing warnings;
none concern the new or changed files. The incremental green build was clean.

Of 6,509 preceding C# files, 6,507 remain byte-identical; only the producer and
publication XML documentation changed. The two new compiled test/helper files match
the approved snapshots exactly. Dependency, lockfile and workflow hashes are unchanged.
All three owned brokers were removed; unrelated containers were untouched. Logs,
TRX, mutation snapshots, source verification and cleanup evidence are retained in
`/private/tmp/brighter-4170-redis-writer-failure-recovery/`.

## Approved execution plan (completed)

After approval, adopt the test/helper unchanged and run both frameworks. Record
the actual null-result failure and the throwing-case outcomes before changing
production; setup/compiler failures do not count. Add only the minimum guard and
public contract documentation needed, preserving original writer exceptions. If
behavior is already green, use the reviewed mutations to establish behavioral RED.

Apply these independent production-only mutations on .NET 10 after green:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Ignore the configured writer and use the default writer | Non-null error in all cases | 4 |
| Suppress configured writer and null-result errors, returning normally without native publication | Non-null error in all cases | 4 |
| Store and fan out a default-encoded copy before invoking the configured writer | Absent native counter immediately after failure in all cases | 4 |
| Replace a null writer result with an empty native string instead of failing | Non-null error in both null cases | 2 |

Require fourteen intended assertion failures, with unaffected throwing cases passing
in the last mutation. The premature-publication mutation must preserve the required
mapping error, so it reaches the native counter assertion. Setup, timeout and
compilation failures do not count. Restore production bytes exactly after each
mutation and require four passes. Keep the approved assertions and bounds unchanged.

Finish with focused passes on both frameworks, all four Redis library targets built,
and both full suites preserving all 112 preceding identities/outcomes plus four
new passes. Expected complete matrices are 112 passes/four existing skips each if
the baseline remains unchanged. Run complete suites once on separate fresh ready
brokers; preserve failures and investigate without retrying regressions until green.

Use the recorded Redis image digest/platform, native Docker SDK runners, no host
ports, unique keys and explicit cleanup. Async sends have a 45-second token;
external process/hang limits bound sync/native calls and disposal. Save logs and
remove only owned containers. Existing Redis CI selection covers these cases;
no dependency/workflow changes or hosted services are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
four cases, the evidenced minimum correction and documentation, fourteen reviewed
mutation failures with exact restorations, native Docker validation, affected
regressions/builds, owned cleanup and local reports. Hosted calls, commits,
deployment and publication are excluded. The compiled test/helper match the approved
draft. The next [reader failure-recovery draft](../redis-reader-failure-recovery/README.md)
remains outside the compiled suite, unbuilt/unrun pending its own approval.
