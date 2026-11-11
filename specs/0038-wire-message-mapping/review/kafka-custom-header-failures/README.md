# Kafka custom-header failures

Status: approved, implemented and validated. All sixteen cases reproduce exact
payload loss on both frameworks and pass after custom-header errors are contained
per entry. Ten mutations produce 144 intended assertion failures, with the sixteen
expected passing mutation cases retained. Both complete Kafka suites pass 347 cases
with no failures/skips, retaining all 331 prior outcomes. Only KafkaMessageCreator
changes in production; all approved assertions and their line numbers are unchanged.
The existing test warning suppression was extended. No commit or push was made.

The [test](When_kafka_custom_header_parsing_fails_should_preserve_the_delivery_and_continue_receiving.cs)
combines sync/async receive, unset/explicit default reader, bad-first/bad-last order,
and duplicate/null-valued custom headers. Each case publishes two native records
on a unique single-partition topic. The malformed record must remain unacceptable
while retaining its exact binary body, identity, topic, partition key, handled count,
trace, actual offset and independently readable headers before and after the error.
The valid neighboring record remains a valid event with its own payload and offset.
Both deliveries can be acknowledged; disposal flushes the partial commit batch and
a native observer verifies the final committed offset. Receive loops are bounded and
cleanup runs after assertion failures. No mock, helper, handler, registration or
new dependency is required.

Kafka permits repeated and null-valued headers. This test preserves the existing
Brighter default reader's rejection classification when those entries cannot be
represented by its string-valued dictionary mapping. It does not impose that policy
on custom readers. For duplicates, the first successfully read value is retained;
the duplicate still makes the delivery unacceptable. For null values, no new bag
representation is specified. Other readable entries must survive either error.

The approved runs confirmed that `ReadBagEntry` errors replaced the mapped delivery
with an empty failure placeholder: all sixteen cases failed exact body equality at
line 150 on both frameworks. The creator now contains each custom-header exception,
marks the existing delivery unacceptable and continues reading subsequent entries.
The protected virtual `ReadBagEntry` extension point remains unchanged.

## Validation after approval

Copy the file unchanged into MessagingGateway and run on both frameworks. After
implementation, prove these mutations independently, applying changes only to the
malformed delivery so valid-message assertions do not mask the intended failure.

| Mutation | Intended failures | Assertion |
| --- | ---: | --- |
| Clear malformed delivery body | 16 | Equal, line 150 |
| Clear malformed delivery identity | 16 | Equal, line 151 |
| Clear malformed delivery partition key | 16 | Equal, line 153 |
| Clear malformed delivery handled count | 16 | Equal, line 154 |
| Clear malformed delivery trace parent | 16 | Equal, line 155 |
| Omit default-reader native offset on malformed delivery | 8 unset-reader cases | True, line 156 |
| Remove readable tenant preceding the error | 16 | True, line 158 |
| Remove readable header following the error | 16 | True, line 160 |
| Treat malformed delivery as a valid event | 16 | Equal, line 148 |
| Remove first successfully read duplicate value | 8 duplicate cases | True, line 164 |

Require 144 intended failures across ten mutations. The explicit-default consumer
restores the native offset, so those eight cases must pass the offset mutation.
The eight null-valued cases must pass the duplicate-value mutation. Preserve strategy
selection and actual native delivery. Compile failures, missing deliveries or unrelated
exceptions/timeouts are not mutation proof.

Restore production bytes, rebuild optimized library targets and both test targets,
and rerun all sixteen focused cases on both frameworks. Run both complete Kafka
suites, expecting 347 passes each with all 331 prior outcomes retained. Verify source
and dependency scope, approved-file identity, warning diagnostics, documentation
links, whitespace, HEAD and stash. Preserve evidence and stop task-owned services.
No commit or push.

Broker failure/cancellation, rejection forwarding, other malformed headers, retry
mapping, transactions/batches, concurrency and the [global #4170 gates](../../tasks.md)
remain incomplete. This batch does not establish production readiness for the issue.


## Execution evidence

The first optimized .NET 9 run showed all sixteen empty-body failures, but some stack
traces exposed only cleanup locations. Preserved that TRX/log, then reran the original
failures with optimization/inlining disabled to verify line 150 in every case on
both frameworks. The initial temporary mutation guard needed legacy MessageType
warning suppression; applying that suppression inside the identity expression then
caused a separate syntax failure. Both failed build logs are retained. The harness
was corrected without changing the approved test or final production code. Neither
build error is counted as mutation proof. All ten mutations subsequently reached the
intended assertions, with the expected offset/duplicate-case passes.

Evidence is under `/private/tmp/brighter-4170-kafka-custom-header-failures/`.
The final warning check found CS0618 in the valid-event assertion at line 167.
Extended the existing compatibility suppression without changing any assertion or
line number. The original draft and pre-suppression build logs are retained. All
four library targets and both test targets rebuild without new warnings; all sixteen
focused cases pass again on both frameworks. Full-suite results precede this
warning-only adjustment. No production or behavioral test change followed those runs.
The subsequent [four invalid-message writer cases](../kafka-invalid-writer/README.md)
are implemented and validated: nine mutations produce 34 intended failures and both
complete suites pass 351 cases. Failure/recovery remains separate work.
