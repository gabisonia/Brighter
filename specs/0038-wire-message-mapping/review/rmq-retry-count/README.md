# RabbitMQ retry-count reconsumption

Status: approved and copied unchanged into the compiled test project.
Both cases initially fail at the retry-count assertion on .NET 9 and .NET 10.
The corrected attachment passes both successive retries. The named mutation
restores the expected failure, and byte-for-byte restoration passes again.
Full regression is combined with the approved count-precedence cases: both
complete Release suites pass with 502 passed, two existing skips and no failures.

- [Test](When_reconsuming_a_custom_rmq_retry_should_preserve_the_retry_count.cs)
- [Writer helper](RetryableLegacyRmqWireMessageWriter.cs)

Two cases use the public consumer factory and sync/async operations against a
real broker. An initial legacy delivery has no canonical handled-count header
and maps to count zero. The existing replacement reader understands only the
application fields. The new writer helper composes the existing legacy writer
and includes the application ID header that reader requires.

Each case increments the count, directly requeues, and receives the retry through
the same configured consumer twice. The resulting counts must be one and two.
The assertions also verify mapped application ID, body, tenant, content type,
persistence and fresh delivery tags. The original message remains unchanged by
requeue. Final acknowledgement and native queue observation verify settlement.
The writer does not supply a count: the gateway already writes the canonical
retry header.

Expected RED: the first reconsumed retry has count zero instead of one because
the consumer currently retains the replacement reader's count. Mapping-failure
messages, missing deliveries or unrelated exceptions do not qualify as RED.
This is a code-review prediction, not an observed runtime result.

After approval, apply the drafts unchanged and prove that failure on both
frameworks. The proposed correction attaches canonical native handled-count
metadata after custom reading, reusing the existing parser semantics for integer
and UTF-8 values. Preserve the default-reader path and existing source-compatible
constructors. Do not extend the public API to expose parser internals.

Named mutation after GREEN: remove only the consumer's new handled-count
attachment, leaving delivery-tag, redelivery and persistence attachment intact.
Both cases must again fail at the retry-count assertion. Restore the production
snapshot, verify GREEN and run both complete RabbitMQ Async suites.

This increment covers immediate direct retries. It does not establish native
delayed or scheduler-backed retry reconsumption, malformed-count behavior, or
precedence over a custom reader's nonzero count when no native count exists.
No commits or pushes are authorized.
