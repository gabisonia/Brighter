# Quartz completion fixture: positive delay

Status: approved and validated. The one-line amendment reproduces four completion
assertion failures per framework after successful delivery. Two early returns in
`QuartzBrighterJob.Execute` fix the defect. All five cases pass on both frameworks;
four mutations produce nine intended failures, and both full Quartz suites pass
53 cases without failures/skips. The compiled test matches this draft exactly;
its three collaborators match the original approved drafts. See the
[validation record](../../validation.md#quartz-successful-callback-completion-fix).

## Observed results

Both complete baseline Quartz suites pass all 48 cases, with no failures or skips.
The four original approved files compile without correction. On both .NET 9 and
.NET 10, all four new successful-dispatch cases fail during scheduling with
`ArgumentOutOfRangeException: Invalid at, it should be in the future`. The
missing-payload guard passes on each framework.

The fixture requests `TimeSpan.Zero`. Each scheduler delay overload computes an
absolute timestamp and calls an overload that reads the current time again.
The timestamp is already in the past at that comparison. The tests therefore
never reach job execution or the intended `Assert.Null(jobError)` assertion.
These failures do not establish the completion regression guard. All original
test files, source bytes and failure evidence are retained.

## Exact amendment

The [revised test](When_quartz_dispatch_succeeds_should_complete_without_a_job_error.cs)
changes one line:

```diff
-        var delay = TimeSpan.Zero;
+        var delay = TimeSpan.FromSeconds(1);
```

Scheduling occurs while Quartz is stopped. It starts after the job is registered
and still has the same 15-second completion timeout. Both public scheduling paths,
both dispatch modes, delivery assertions, job-completion assertions and the
missing-payload guard remain unchanged. This amendment isolates the completion
contract from the separately observed zero-delay timestamp rejection.

## Work after approval

Replace only the compiled completion test with this reviewed copy; retain the
three unchanged collaborators and the original draft. Rebuild and rerun all five
cases on both frameworks. Require behavioral RED at the completion assertion
before applying the two early returns in `QuartzBrighterJob.Execute`.

Complete the original four named mutations (nine intended failures), restore
exact GREEN source bytes, build every supported Quartz target, and run both full
Quartz suites. Each must retain the 48 baseline test names/outcomes and add five
passing cases, for 53 passes with no skips/failures. Shared Core regression is
required if shared production code changes. Return further semantic test changes
for review; retain any unexpected evidence.

Logs, TRX files, original source, source manifest and `initial-verification.json`
are in `/private/tmp/brighter-4170-quartz-completion/`. The production correction
is limited to the two validated early returns. No commit, push or review-gear
change is included.
