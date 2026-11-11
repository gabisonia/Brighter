# Kafka live native capture and evidence preservation

Validated 2026-11-10. The existing two commit/revoke cases pass while a diagnostic
startup hook writes native stderr directly to disk and `createdump` collects a
nonterminating stack report. This establishes a capture method, **not a reproduction
or fix of the historical native assignment/close stall**.

## Evidence availability correction

The original `/private/tmp/brighter-4170-kafka-requeue-cancellation-recovery/`
directory is no longer present. Its stack report and stalled-run console log were
not recovered from the available temporary files or retained issue-owned Docker
containers. Their recorded interpretation remains in the historical validation
notes, but the raw failed stack cannot currently be reexamined. The later passing
run cannot substitute for that missing evidence. Nothing in this continuation
deletes the old package or establishes why it disappeared.

The 2026-11-09 full-run TRX remains available and contains the native logs in
`ResultSummary/Output/StdOut`, rather than its small runner console log. Four complete
consumer traces are now preserved here, correlated with the two case strategies,
topics and groups. [summary.json](summary.json) includes their provenance and the
TRX hash. At millisecond log resolution the RoundRobin consumers close in 8/1 ms
and the CooperativeSticky consumers in 9/4 ms. These are healthy reference traces,
not latency guarantees or explanations of the earlier failure.

## Validated capture

[StartupHook.cs](StartupHook.cs) is a Linux-only diagnostic hook loaded through
`DOTNET_STARTUP_HOOKS` into the isolated `testhost.dll` process. It redirects native
file descriptor 2 to a private append-only capture file and records PID/runtime
metadata. It does not alter broker configuration, assignment strategy, production
code, test assertions or test sequencing. Redirecting stderr means native errors
must be read from that file as well as the TRX. The hook is not added to any product
or test project or normal environment configuration.

[validate.py](validate.py) reuses the previously compiled diagnostic assembly,
with native debug already enabled for the existing class. It runs only its two
.NET 10 cases once, to validate capture. While the test runner is still active,
21,462 bytes of native output are observed on disk. A `createdump --crashreportonly`
call then succeeds without terminating the host. Both cases subsequently pass.

The [healthy stack report](healthy-capture.crashreport.json) includes 29 threads,
nine with librdkafka frames, including native-only threads. Internal librdkafka
function names are absent from the packaged binary's frames; module offsets are
preserved. This report alone does not expose the native operation-queue contents.
It is explicitly a healthy capture and must never be labelled a hang dump.

The diagnostic container receives `SYS_PTRACE` for this collection only. Its
runner inhibits idle sleep for its lifetime, then releases the inhibitor and
stops both owned Kafka/schema fixtures. All 6,614 existing production/test C#
files and the two build/package props remain byte-identical. This batch does not
run a full regression, change dependencies, exercise hosted services or rerun Redis.

The hook builds with zero warnings/errors. After capture validation, only a license
header and explanatory-comment placement change; the final source also builds
with zero warnings/errors. The exact validated source snapshot and distinct
validated/final binary hashes are preserved. No executable statement changes.
No commits or pushes occur.

## Upstream comparison and next useful capture

[librdkafka 2.16.0's release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.16.0)
include an assignment teardown fix. The associated [issue #5573](https://github.com/confluentinc/librdkafka/issues/5573)
reports duplicate FETCH_STOP operations leading to an assertion failure;
[PR #5574](https://github.com/confluentinc/librdkafka/pull/5574) guards outstanding
partition stops. The historical Brighter record describes a blocked assignment
call, without that assertion signature. These sources identify a candidate to
check against a failing capture, not an established match or reason to upgrade.

A useful next failure capture needs the live native file, strategy/group/topic
correlation, every native thread's name/state, and a full native-memory dump before
the test host is stopped. Native addresses must be matched to the exact loaded
library. The capture method here supplies durable logs and validates native-thread
stack collection; a full memory dump and queue-state analysis remain unvalidated.
Do not infer that a five-minute managed hang report alone supplies those missing
details. Do not add a shutdown timeout or change the commit semaphore without a
reproduced cause.

Complete artifacts remain in `/private/tmp/brighter-4170-kafka-live-capture/`.
The preserved scripts use those fixed diagnostic paths and deliberately refuse
to overwrite an existing validation directory. A future run needs its own output
directory and must retain both its successes and failures. The Kafka release gate
remains open.

## Full-memory follow-up — 2026-11-10

The [subsequent package](../kafka-full-capture/README.md) validates a full ELF memory
core and native thread names/states, and resolves all forty native frames in that
healthy sample from the exact library's retained `.symtab`. Missing names in the
initial crash report therefore do not mean the binary lacks internal function
symbols. DWARF type/variable reconstruction remains unavailable. Its single full
.NET 9 diagnostic run passes all 412 cases with no captured stall; the original
failure remains unresolved.
