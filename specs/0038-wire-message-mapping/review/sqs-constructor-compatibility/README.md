# Preserve the legacy SQS constructor call

Status: approved, implemented and validated. The compiled test matches this draft.

The [draft](When_selecting_the_legacy_sqs_constructor_should_have_one_applicable_overload.cs)
checks the exported constructor surface for an existing three-argument call:

```csharp
new SqsMessageConsumer(connection, queueName, default)
```

Before the feature, the third argument binds to `int batchSize`. The added public
reader-taking overload accepts the same call with a different third-argument
type and optional remaining arguments.

The test uses only public constructors. It asserts one applicable constructor
shape for these positional arguments, with `int` as its third parameter.
This permits an assertion failure with the current API instead of relying only
on a test compilation failure. No private implementation is inspected and no
broker is required.

Expected RED: `Assert.Single` finds both public constructor candidates.
The proposed fix is to make only the new reader-taking constructor internal,
remove its internal XML documentation per repository style, and retain the
original public constructor and subscription/factory configuration.

Named mutation after GREEN: expose the new constructor publicly again.
The test must fail at `Assert.Single`, then pass after byte-for-byte restoration.

Separately compile the original call in a small external consumer project.
That project must compile against the corrected gateway on its supported target
frameworks. It must produce CS0121 with the conflicting public overload.
Keep that compilation probe outside the test project so it does not prevent
the runtime public-surface assertion from executing under mutation.

Run existing custom-reader factory coverage and both full AWS V4 suites after
the change. Existing reader functionality must remain reachable through
`SqsSubscription.WireMessageReader`. Build supported gateway targets and affected
dependents. Do not commit or push.

## Results

The initial test fails at `Assert.Single` on both frameworks. The external call
reports CS0121 on all four gateway targets. Making the new constructor internal
fixes both checks without changing its body or the original public constructor.
Custom-reader factory cases pass through both sync and async entry points.

The approved public-overload mutation fails at `Assert.Single` on .NET 9.
After byte-for-byte restoration, all three targeted cases pass per framework.
Both complete AWS V4 Release suites pass 489 tests with no skips or failures.
Their result sets contain every previous passing case plus the new test.

The full solution builds in Debug and Release after restoring missing dependency
assets. The external legacy-call project builds on netstandard2.0, net8.0,
net9.0 and net10.0. The three C# examples in the external documentation draft
also compile on those targets. See [validation.md](../../validation.md) for
logs and warning counts. Nothing was committed or pushed.
