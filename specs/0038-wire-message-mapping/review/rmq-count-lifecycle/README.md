# RabbitMQ count preservation across parsing failure and native delay

Status: approved and copied unchanged into the compiled project. The predicted
parser-failure gap is reproduced and corrected. Both named mutations fail at
their intended count assertions, and byte restoration passes all 22 count cases
per framework. Both full Release suites pass with 512 passed, two existing skips
and zero failures per framework. Nothing was committed or pushed.

- [Default-parser failure test](When_rmq_default_parsing_fails_should_preserve_the_native_handled_count.cs)
- [Native delayed-retry test](When_reconsuming_a_custom_delayed_rmq_retry_should_preserve_the_retry_count.cs)

Both tests reuse existing public-factory fixtures and existing test helpers.
There is no new request type, helper, mapper registration or public API.

## Default-parser failure

Eight cases combine both consumer-factory paths, malformed content/body MIME
properties, and an unset versus explicitly configured default reader. Native
input supplies canonical handled count 7. The unacceptable result must preserve
that count, exact body bytes, parsed identity/topic and broker delivery metadata.
Rejection must settle the native source delivery.

Expected initial result: four unset-reader cases fail at the handled-count
assertion with actual zero; the four configured-default cases pass because the
consumer already attaches the native count. This is a code-review prediction.
No execution evidence exists yet.

Proposed fix: attach the canonical count using the existing parser in
`RmqMessageCreator.SetMessageMetadata`, after either successful parsing or failure
fallback. Preserve current parsing, payload, identity and settlement behavior.
No constructor or public API change is needed.

Named mutation after GREEN: remove only the new count attachment in
`RmqMessageCreator.SetMessageMetadata`, leaving the consumer's custom-reader
attachment intact. The four unset-reader cases must fail at their count
assertion, while the four configured-default cases pass.

## Native delayed-retry reconsumption

Two cases use the native delayed-message plugin and both factory paths. The
existing legacy reader and retryable writer process two successive retries with
requested one-second delays. Received handled counts must be one and two, while
application mapping and settlement remain intact.

This characterizes the shared direct-retry path. The existing native-delay
suite separately verifies delay duration and queue isolation; these cases
verify count preservation after consuming the delayed retries.

Named mutation: remove only the count attachment from
`RmqMessageConsumer.ReadMessage`. Both delayed cases must fail at their first
retry-count assertion, returning zero instead of one.

## Validation

After approval, apply both drafts unchanged, run them on .NET 9 and .NET 10,
implement the minimal correction after observing the intended failure, and
validate both named mutations separately. Restore production byte snapshots
after each mutation. Then verify all ten cases and run both complete RabbitMQ
Async suites, including TLS and the independent native-delay brokers.

Mapping/setup failures, timeouts and compilation errors do not count as RED.
Scheduler-backed retries and other transports remain outside this increment.
Nothing will be committed or pushed.
