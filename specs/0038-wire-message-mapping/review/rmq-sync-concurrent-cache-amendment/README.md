# Concurrent mapping: completed-properties cache mutation amendment

Status: approved and validated. The exact reviewed temporary patch produced four
`Assert.Equal` failures at native member line 174 on .NET 9. All production bytes
were restored. All eight focused cases then passed on both frameworks, and both
complete Sync suites passed 291 cases with one existing skip, retaining every prior
outcome. Normal builds pass for all four library targets and both test targets,
with no warnings in the affected files. This completes twelve named mutations and
34 intended failures for the concurrency batch. The eight tests and two helpers
are unchanged from their approved drafts. No production fix, commit or push was made.

The original shared-properties mutation initialized a static native properties
object under a lock, then let both mapping callbacks modify it concurrently. In
both attempts, at least one case failed at the operation-success assertion (line
154), before the approved native member-isolation assertion (line 174). Those runs
are preserved and do not satisfy the mutation gate. Concurrent mutation of the
shared headers is a plausible cause; the exact swallowed exception was not captured.

The [reviewable temporary production patch](shared-completed-properties.patch)
instead caches the first **completed** properties object per connection configuration
immediately at the two
native publish calls. Each mapper and gateway completes its own fresh properties
before the cache chooses the first result under a lock. Subsequent publishers reuse
that same completed object without further gateway mutation. A locked dictionary
keys the cache by the shared connection configuration, so each case exercises its
own pair of publishers without inheriting a preceding case’s cached topic. This keeps the shared
per-call data defect while avoiding concurrent writes to the shared dictionary.
The original tests, barriers and assertions are not modified.

After approval:

1. Apply only this temporary patch to the restored production source.
2. Build .NET 9 and run the four existing writer cases for operations 1 and 2,
   excluding publication operation 0, which uses the existing producer send lock.
3. Require all four cases to fail with `Assert.Equal` at native member line 174.
   An operation failure, exception, timeout or different assertion does not count.
4. Restore exact production bytes in a finally block, rebuild normal Release on
   all four library targets and both test targets, rerun all eight focused cases
   and both full Sync suites, retaining every prior outcome by exact name.
5. Verify the full twelve-mutation total of 34 intended failures, unchanged approved
   files/dependencies/HEAD/stash, warning and whitespace checks, and stop owned brokers.

This is a validation amendment, not a production fix or a claim that #4170 is
complete. Scheduler-backed retry, remaining transports and other completion gates
in [tasks](../../tasks.md) remain open. Nothing will be committed or pushed.

Approval is required by [the repository test workflow](../../../../.agent_instructions/testing.md):
“STOP after writing the test and ASK FOR APPROVAL.” The same workflow requires the
named mutation to be reviewed with the characterization test. This amendment
changes where the shared-state mutation is introduced, so it is presented for review.
