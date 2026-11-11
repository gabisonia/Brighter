# Compiled public API compatibility — 2026-11-09

The installed .NET SDK 10.0.400 ApiCompat task compares all 18 changed production
assemblies against an isolated build of Git HEAD
`64ba97097199c708974195f55fbdca0ce3e8487c`. All **68 assembly/framework comparisons
pass**, with zero unresolved references and no compatibility suppressions. Every
baseline project builds successfully in its declared targets. The target set is
.NET Standard 2.0, .NET Framework 4.6.2 where declared by SQL Server, and .NET 8/9/10
where declared by each package; not every package declares every target.

The standard binary API checks run with parameter-name checking enabled, internals
excluded and strict equality disabled so new public APIs are allowed. Captured
assembly hashes identify the actual compared Release binaries. The 98-file source
audit is complementary, and recorded changed production source hashes remain intact.
This does not prove runtime wire encoding, deployment, arbitrary dependency-version
compatibility or overload resolution for every possible caller. Existing native
default-mapping and external-constructor compilation tests supply separate evidence.

## Additional attribute review

The optional all-attributes rule is also run with complete dependency resolution.
Its 360 diagnostics across 41 assembly/target combinations concern only
`System.Runtime.CompilerServices.AsyncStateMachineAttribute`: compiler-generated
state-machine type identity changes or removal when a method no longer uses async.
Those private generated types are implementation details, not stable caller APIs.
No other attribute difference is reported. The complete diagnostic classification
is retained in `attribute-review.json`; the standard check does not enable this
optional strict attribute-equality rule. No source, public signature, dependency,
test or suppression file is changed to make the comparisons pass.

A negative control reverses the .NET 10 Core comparison. It fails with CP0001 for
the new writer and reader contracts, and has zero unresolved references. This
establishes that the checker rejects missing exports. It does not modify binaries.

## Reproduction and setup corrections

`prepare-build.py` extracts HEAD into an isolated directory and builds the changed
projects. `compare.py` resolves each target's dependencies from its assets and
installed/downloaded reference packs, generates one SDK task invocation per pair,
and records exits, unresolved-reference counts and hashes. The SDK task expects
one comma-separated reference list per assembly; see the
[official task source](https://github.com/dotnet/sdk/blob/main/src/Compatibility/ApiCompat/Microsoft.DotNet.ApiCompat.Task/ValidateAssembliesTask.cs).
The earlier incorrectly grouped-reference attempt, the initial .NET Framework
dependency-target selection error and the missing .NET Standard reference pack
attempt are preserved under `/private/tmp/brighter-4170-binary-api-audit/`. Their
results are setup evidence, not accepted compatibility passes.

Full task project files and raw logs remain in that directory, including the
separate attribute-aware run. Nothing is committed, pushed or installed globally.

The final Release solution build regenerated 24 compared binaries. Those 24 pairs were checked again successfully; the other 44 retain identical hashes. All 68 recorded current hashes now match the final Release outputs. The pre-build comparisons remain archived separately.
