# Quartz callback completion

Status: approved and validated with the
[positive-delay amendment](../quartz-job-positive-delay/README.md). The original
zero-delay drafts and first-run scheduling failures are retained. The amended
test reaches the intended completion assertion: four failures per framework before
the fix, then five passes per framework afterward. Four named mutations produce
nine intended failures. Both full Quartz suites pass 53 cases, preserving all 48
baseline outcomes. The production correction adds only two early returns.

## Contract

The [test](When_quartz_dispatch_succeeds_should_complete_without_a_job_error.cs)
contains four successful-dispatch cases: a scheduled Brighter message or a
scheduled request posted through the command processor, each using sync or async
scheduling/dispatch. A fifth case protects rejection of a job with no message or
request payload.

Each case creates an isolated real Quartz scheduler, a real command processor and
the production scheduler handlers, mapper, outbox mediator and in-memory producer.
The public Quartz scheduler factory creates successful jobs. Quartz itself calls
the production job and reports its completion to the
[listener](QuartzCompletionListener.cs). The test waits at most 15 seconds for
that notification, shuts the scheduler down, and checks both the single delivered
message's identity/content and the absence of a job error. Observing a delivered
message alone would miss the defect under review.

The invalid-job case submits a real Quartz job without either payload key, checks
that nothing is delivered, and checks the reported underlying exception and its
existing message. It protects the guard against a fix that silently returns for
all jobs.

The [job factory](QuartzCompletionJobFactory.cs) supplies the real command
processor through Quartz's public factory API. It has instance state, with no
static processor shared between fixtures. The
[event](QuartzCompletionEvent.cs) is distinct from existing test requests. There
are no new handlers or logging-initializer registrations, dependencies, public
production APIs, mocks or generated conformance fixtures. Cleanup shuts down the
scheduler before disposing the mediator and mappers.

## Expected defect and minimal correction

Before the correction, `QuartzBrighterJob.Execute` awaited the appropriate handler
and then unconditionally threw the missing-payload exception. The contract expects four behavioral
failures at `Assert.Null(jobError)` after successful payload assertions, and one
passing invalid-job case. A compilation/setup error or timeout is not behavioral
RED. The proposed minimal correction is to return after either successful
dispatch while retaining the final error for an unrecognized job. Exceptions
from dispatch continue to propagate; this batch adds no retry policy or catch.

If initial results differ, retain the evidence. Compilation-only corrections may
qualify existing types or correct signatures without changing test behavior.
Return semantic test changes for review.

## Named mutations

After GREEN, apply each production mutation separately with test bytes unchanged:

| Mutation | Expected failures | Intended assertion |
| --- | ---: | --- |
| Restore fallthrough after both recognized payload branches | 4 | No job error after successful delivery |
| Omit dispatch from only the message branch, retaining its return | 2 message cases | Exactly one delivered message |
| Omit dispatch from only the request branch, retaining its return | 2 request cases | Exactly one delivered message |
| Replace the final missing-payload exception with a return | 1 invalid-job case | Job error must be present |

These four mutations should produce nine intended assertion failures on .NET 9.
The first also defines a characterisation mutation if the initial success cases
unexpectedly pass. Restore exact GREEN production bytes after each mutation and
confirm all five cases pass on .NET 9 and .NET 10.

## Work after approval

Capture the existing complete Quartz test names/outcomes on both frameworks before
copying the draft files. Copy the test to the Quartz test project root and its
three collaborators to `TestDoubles`. Run the initial cases, make only the
reproduced completion correction, validate the mutations and restored focused
cases, build all supported Quartz target frameworks, and run both complete Quartz
suites. Require every preceding case/outcome to remain, plus five passing cases.
If shared Core production code needs changing, include both full Core regressions.

This is a scheduler-completion prerequisite for issue #4170. It does not establish
native custom-wire mapping through Quartz, callback failure recovery, other
schedulers or any remaining gateway. Those completion gates remain open. No
commit, push, dependency upgrade or review-gear change is included.
