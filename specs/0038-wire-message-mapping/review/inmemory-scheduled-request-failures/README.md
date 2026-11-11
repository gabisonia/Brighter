# In-memory scheduled request failure containment

Status: approved and complete (2026-11-07). The four cases pass on both frameworks; full Core and InMemory regressions preserve every preceding outcome.

The completed scheduled Redis registration package contains failures in message
callbacks. Before this package, the `FireSchedulerRequest` branch dispatched before cleanup
without an exception boundary. These cases reproduce process termination on both
frameworks, then verify containment and cleanup after the request-boundary fix.

## Review files and installation

| Draft | Destination after approval |
| --- | --- |
| [Four parent cases](When_a_scheduled_request_handler_fails_should_preserve_the_host_and_allow_later_requests.cs) | `tests/Paramore.Brighter.Core.Tests/CommandProcessors/Scheduler/` |
| [Child scenario](Probe/Program.cs), timer observer, logger and `TestDoubles/` | New `tests/Paramore.Brighter.SchedulerRequestFailureProbe/` executable fixture |
| [Fixture project](Probe/Paramore.Brighter.SchedulerRequestFailureProbe.csproj) | Same fixture directory |
| [Core test project](Paramore.Brighter.Core.Tests.csproj.proposed) | One project reference added to the existing Core test project |
| [Core logging initializer](Initializer.cs.proposed) | Initialize the new closed request/handler/pipeline generics with the existing test logger |

All draft C# files stay outside compiled test directories until approval. The new
fixture references Core only, is not packable and adds no package dependencies or
versions. Its project paths resolve at the installation destination, not in this
review directory. The parent launches it using the parent runtime configuration and
dependency manifest, on the same framework.

## Four cases and contract

The cases cover synchronous and asynchronous `Send` and `Publish`. Each application
handler has a distinct request type. Commands use `Send`; events use `Publish` with
one registered application handler. Async handlers yield before throwing, so their
fault is observed through the awaited dispatch path.

1. A real `CommandProcessor` accepts a positive five-second delay through its public
   `Send`/`Publish` scheduling APIs, returning the configured scheduler ID before any
   application handler runs. The child emits its unique acceptance marker.
2. The real timer dispatches through `FireSchedulerRequestHandler`, request
   deserialization and the selected application handler. The process survives the
   application exception. The scheduler logs exactly one Error-or-higher entry with
   the scheduler ID and original dispatch exception. For `Send`, the exception is
   the original application exception instance. For `Publish`, the existing
   `AggregateException` contains that original instance as its sole flattened error.
3. The handler observes the original request ID, Unicode payload and failure flag
   exactly once. Direct dispatch through the same command processor still exposes
   that application exception (or the existing publish aggregate) to its caller.
4. A healthy request is accepted with the same scheduler ID under `OnConflict.Throw`.
   It runs once through the same scheduler and preserves its own ID and payload.
   There are exactly three observed application calls: failed timer dispatch,
   explicit direct dispatch, then healthy timer dispatch. No failed request is
   replayed during this observation window.
5. No additional scheduler error is logged for direct dispatch or healthy work.
   Public `ReScheduler` returns false after the healthy callback, proving that its
   completed entry is also released. The child emits its recovery marker and exits
   zero; the parent requires acceptance, zero exit and recovery.

The observer delegates to `TimeProvider.System.CreateTimer` with unchanged state,
due time and period. It signals only after the callback returns normally. It does
not catch callback exceptions or invoke scheduler internals. Each child configures
its own logging before initializing closed generics; it cannot replace the parent
test process logger. The parent initializer also registers the new closed generics
to follow the Core suite's existing logger initialization convention.

The child has a 45-second bound and the parent allows 70 seconds for startup and
exit. A stuck child tree is terminated. Native core dumps must be disabled by the
runner for expected crash cases. A child crash becomes an ordinary parent assertion
failure, preserving the rest of the test process.

## Proposed production scope

After behavioral RED, add exception logging and guaranteed existing
generation-aware cleanup to the request branch of `InMemoryScheduler.Execute`.
Extend the public scheduler remarks to explain request callback failures as well
as message callback failures. Keep direct command-processor exception behavior,
public signatures, scheduling conflict policy and the message branch unchanged.

This changes the shared Core timer boundary. It does not add automatic retry,
request persistence, an outbox for local handlers or recovery across restart.
`Post`/external publication, serialization failures before acceptance, cancellation,
concurrent replacement during a failing callback and native-send recovery remain
outside these four cases. No broker or Redis quota is involved in this package.

## Validation after approval

1. Snapshot the existing work and record full Core and InMemory baselines on .NET 9
   and .NET 10. Prior recorded counts are Core 1,716 passes/seven skips and InMemory
   167 passes per framework; compare actual test identities and outcomes as well.
2. Install the reviewed drafts and build through the Core test project using existing
   central versions. Run the four cases on both frameworks before any production edit.
   An intended RED must reach acceptance and fail the parent's exit assertion with
   an unhandled application failure through the real scheduler callback. For publish,
   diagnostics must retain the application failure inside the aggregate. Build/setup
   failures, missing dependencies and timeouts do not establish behavioral RED.
3. Apply the scoped request-branch fix and require all four passes on both frameworks.
4. Independently apply each named mutation below on .NET 10. Require the intended
   failure in all four cases, restore exact production bytes and require four passes
   after each restoration. These mutations also establish RED if a case unexpectedly
   passes before the fix; do not weaken tests or invent a different mutation.
5. Finish ordinary optimized focused runs, all Core library target builds and full
   Core/InMemory regressions on both frameworks. Expected expanded Core counts are
   1,720 passes/seven existing skips; InMemory remains 167 passes. Preserve every
   preceding identity and outcome. Keep source manifests and full diagnostics.

| Named production mutation | Required diagnostic after acceptance |
| --- | --- |
| Rethrow the caught request callback exception | Unhandled original dispatch failure and nonzero child exit |
| Omit failed-request cleanup while retaining successful cleanup | `A completed failed request must release its scheduler ID.` |
| Suppress the request callback error log | `Failed request callback must log its original dispatch exception and scheduler ID once.` |

No production mutation is retained. Return any semantic test/contract change for
review; retain compilation corrections separately. Completion of this package
closes only scheduled local-handler failure containment, not the whole feature.


## Verified completion (2026-11-07)

All four cases initially reached scheduling acceptance and then terminated their
child process with exit 134 on each framework. Send preserved the original
`InvalidOperationException`; Publish retained it inside `AggregateException`.
The captured stack reached the real `InMemoryScheduler.Execute` callback.

Only the request callback branch and public remarks in `InMemoryScheduler.cs`
change production behavior for this package. The branch logs the dispatch error
with the scheduler ID and runs the existing generation-aware cleanup in `finally`.
All four initial and final optimized cases pass on both frameworks. The three named
mutations produce twelve intended failures; each exact restoration returns four
passes. Direct-dispatch exception contracts and subsequent healthy execution pass.

Full Core suites pass **1,720 tests with seven existing skips per framework**;
InMemory suites pass **167 tests with no skips per framework**. Every preceding
identity/outcome is retained. Core builds for netstandard2.0, net8.0, net9.0 and
net10.0 succeed with zero warnings/errors. Core test builds retain analyzer warnings.
The initial sandbox attempt failed on MSBuild named-pipe access and is retained as
setup evidence; the elevated baseline succeeded without test changes.

The approved fixtures, four cases, project reference and logging initializer were
installed unchanged. All 6,669 other preceding source/project/build files are
unchanged. No package references or versions were added. No broker or quota wait
was needed. Evidence and independent comparisons are retained under
`/private/tmp/brighter-4170-inmemory-scheduled-request-failures/`.

This closes local Send/Publish handler-failure containment. Scheduler submission
and native-send recovery, cancellation, restart persistence, remaining transport
coverage and the final release audit remain open. No whole-feature production
readiness, hosted CI, commit, push or deployment is claimed.
