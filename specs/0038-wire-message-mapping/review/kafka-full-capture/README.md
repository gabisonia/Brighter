# Kafka full native memory and thread-state capture

This 2026-11-10 diagnostic extends the [live-stderr capture](../kafka-live-capture/README.md)
with a native memory dump, Linux thread names/states and internal function-symbol
resolution. Product/test code and dependencies are unchanged. The historical
assignment/close stall remains unresolved unless a failing capture establishes its
cause; healthy capture validation is not such evidence.

## Bounded execution

[run.py](run.py) uses the existing diagnostic test binaries, with debug output already
enabled only for `KafkaMessageConsumerCommitRevokeConcurrency`. First, the two .NET 9
cases validate capture while the host is healthy. Then one full .NET 9 run uses the
same binaries and the new watchdog. The previous full matrix has 412 identities;
the runner compares them at completion. It does not retry failures until green.

[capture.py](capture.py) observes native `Closing consumer` / `Consumer closed`
events. A close outstanding for sixty monotonic seconds triggers one capture of
`/proc/<pid>/maps`, all task names/status/wait channels and `createdump --full
--crashreport`, before the existing five-minute VSTest hang timeout. This is a
diagnostic threshold, not a change to Close, commit or test timeouts. A thirty-minute
outer budget bounds the runner. The watchdog observes only consumers with native
close logging; it does not claim coverage for every possible hang location.

Its event parser was checked against all four preserved healthy close sequences.
A synthetic uncompleted close triggers at sixty seconds, not before; a completion
clears the trigger. This verifies the diagnostic predicate without manufacturing
a product failure. It is not a new Kafka regression test.

## Healthy full-memory validation

Both existing cases pass after the nonterminating capture. The validation takes
18.602 seconds, with continuous wall/monotonic clocks. It records all four native
closes with none left pending. The capture contains:

- A 439,771,136-byte ELF `ET_CORE`, with 510 load segments, including 270 populated
  writable segments. The core stays outside the checkout; its path/hash are retained.
- A 28-thread report, including eight threads with librdkafka frames.
- Native task identities, including `rdk:main` and broker threads, with sampled
  Linux states and the process memory map.

These are healthy samples. Their sleeping threads are not evidence of deadlock,
and the presence of a memory dump does not mean queue contents have been decoded.
The diagnostic container alone receives `SYS_PTRACE`; idle sleep is inhibited only
while the runner owns the fixtures. Both Kafka/schema fixtures stop after each run.

## Native function resolution

The preceding crash report omitted internal function names. Inspecting the exact
packaged linux-arm64 `librdkafka.so` establishes that it retains `.symtab`, although
it has no `.debug_info`. [symbolicate.py](symbolicate.py) reads that symbol table
and resolves each frame only to functions whose declared ELF address range contains
the recorded module offset. Aliases are retained; it does not guess a nearest name.

All forty librdkafka frames in the healthy capture resolve, across eight threads.
They include `rd_kafka_thread_main`, queue-serving/wait functions and broker transport
functions. The script correlates thread IDs with the separately captured task names.
The exact loaded read-only library is hashed; no newly built or upgraded library
is substituted. This resolves the internal-function-name gap in the previous review.
DWARF source lines, local variables and typed queue fields remain unavailable.

## Preservation and limits

Scripts, compact reports, native logs, proc snapshots, results and binary hashes are
retained here. Complete artifacts, including the healthy memory core, remain in
`/private/tmp/brighter-4170-kafka-full-capture/`. The runner refuses to overwrite
an existing mode directory. [preserve.py](preserve.py) verifies the source and
diagnostic binary hashes before copying evidence into the review.

All 6,614 existing production/test C# files plus the build/package props are checked
for preservation. Redis and hosted brokers are not exercised. A read-only Azure
check still reports `AADSTS9002313`; native ASB acceptance remains blocked by
interactive authentication. No commits, pushes, dependency updates, assertion
changes or speculative shutdown changes are made.

## Full-run result

The one full .NET 9 run completes in **899.577 monotonic seconds: 412 passed,
zero failed, zero skipped**, retaining exactly the prior 412 test identities.
Wall and monotonic duration differ by only 0.039 seconds. All four instrumented
consumers close normally, in 5/13/7/12 ms at log resolution. No close remains pending,
no watchdog threshold is reached and no failing-memory dump is generated.

Both owned fixtures are verified stopped. All 6,616 source/build inputs and the
read-only diagnostic binary inputs remain unchanged. Complete results are in
[full/summary.json](full/summary.json); the healthy capture result is separately in
[validate/summary.json](validate/summary.json). The 440 MB healthy core stays in the
temporary evidence package, with its hash recorded in [native-core.json](native-core.json).

This is an inconclusive attempt to reproduce the historical stall. Native debug,
output redirection and stack collection can perturb scheduling; a passing run
under observation does not establish that the original failure cannot occur.
Another unchanged green repetition is not a release criterion. The capture method
is validated, but a failing native trace or deterministic cause-supported control
is still needed before changing shutdown or declaring that lifecycle gate complete.
