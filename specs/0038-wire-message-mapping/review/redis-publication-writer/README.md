# Redis publication writer

Status: approved, adopted unchanged and validated on .NET 9 and .NET 10.

## Result

Both complete baselines pass 96 cases with four existing skips. After the expected
missing-adapter compilation failure, the contract/default adapter alone produces
four intended native payload failures and two legacy passes on each framework.
Selecting the configured writer in both producer paths makes all six cases pass.
The public property and reusable default adapter are implemented; counters, storage,
expiry and queue fan-out retain their existing code.

All five reviewed mutations produce the 26 intended assertion failures. Exact
production restorations return six passes after each. Both full suites pass 102
cases with four existing skips, preserving all 100 baseline identities/outcomes.
All four library targets build with zero warnings/errors. The test/helper and all
assertions/bounds remain byte-identical to their approval snapshots.

Async cleanup stack locations do not identify the original assertion, including
in an optimization-disabled diagnostic run. Assertion types and expected/actual
value signatures establish the intended failures. The evidence parser was corrected
for xUnit's typed collection display and ServiceStack's maximum TTL for a key without
expiry; neither correction changed tests or production behavior. Original outputs
are retained. Full suites run once per framework, without retrying regressions.

The initially missing ServiceStack package was restored in locked mode without
changing dependencies or lockfiles. All five owned Redis brokers are removed with
logs retained. Evidence: `/private/tmp/brighter-4170-redis-publication-writer/`,
including TRX/log/JSON records, snapshots, mutants, `final-verification.json` and
`cleanup.json`. Redis reader/failure/lifecycle and broader release gates remain open.

Review [the six cases](When_sending_with_a_redis_wire_writer_should_preserve_payload_and_native_delivery_controls.cs)
and [the configured writer](RedisPublicationWireWriter.cs).

## Contract and behavior

Propose an optional RedisMessagePublication.WireMessageWriter of type
IAmAWireMessageWriter<string> and a public reusable DefaultRedisWireMessageWriter.
The default adapter must preserve the current RedisMessagePublisher envelope,
creating a fresh legacy publisher per conversion because its StringWriter accumulates
state. Applications can replace the stored string or compose default framing.
The string is the native stored payload; counters, keys, expiry and queue fan-out
remain gateway responsibilities. No existing public signature needs changing.

Sync/async × legacy/custom/composed cases send two distinct messages through one
public producer. A separate native ServiceStack Redis client observes exact stored
strings, a counter of two, both recipient queues containing IDs 1 and 2 in order,
and positive expiry within 3000–3600 seconds of the configured one-hour lifetime.
The source header topic deliberately differs from the publication topic. Only the
source topic's storage and queues may be used; the publication's decoy queue stays
empty and its counter absent. The selected writer runs twice, and both source IDs,
text bodies, topics and marker bags remain unchanged.

Expected default strings are produced independently with a fresh existing public
RedisMessagePublisher for each message. This establishes compatibility with the
current codec, not a second independent specification of that codec. Unicode text
is included; arbitrary binary encoding is outside this batch. Native expiry uses
the nullable TimeSpan returned by ServiceStack's
[GetTimeToLive contract](https://reference.servicestack.net/api/ServiceStack.Caching/ICacheClientExtended/).

Reader integration, consumer retry/rejection republishing, mapping errors/nulls,
cancellation, scheduling, concurrency, pooling failures, transactions and delivery
after expiry remain separate. The helper is an application mapping strategy, not
an I/O mock. No internals are exposed for testing.

## Approved execution plan (completed)

After approval, capture both complete Redis suite baselines on fresh ready brokers
before changing source. Adopt these two files unchanged into MessagingGateway and
TestDoubles. The absent property/default adapter are expected to cause compilation
RED; that alone does not establish behavior. Add only those contract declarations
and the legacy default adapter, leaving producer selection unwired, then observe
four custom/composed payload failures and two legacy passes on each framework.
Wire the selected writer into both producer paths and require six passes.

Apply these production-only mutations independently on .NET 10:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Ignore the configured writer and retain the default writer | First native stored-payload equality in custom/composed cases | 4 |
| Default adapter returns raw message body instead of the legacy envelope | First native stored-payload equality in legacy/composed cases | 4 |
| Keep the publication topic instead of assigning the source header topic in both send paths | Source counter equality in all cases | 6 |
| Omit queue fan-out in both send paths | First recipient queue sequence equality in all cases | 6 |
| Omit expiry when storing messages in both send paths | First stored message TTL range in all cases | 6 |

Require 26 intended assertion failures, with unaffected cases passing. Setup,
compiler or timeout failures do not count. Restore production bytes exactly after
each mutation and require six passes; keep tests and bounds unchanged. Finish with
focused passes on both frameworks, full suites retaining all baseline identities
and outcomes plus six cases, and all four Redis library targets building
(netstandard2.0/net8.0/net9.0/net10.0). Preserve and report existing skips/failures;
do not retry behavioral regressions until green.

Use the repository Redis image with its resolved digest and platform recorded,
separate readiness-checked Docker brokers, native SDK runners and no host port
bindings. Unique keys and explicit cleanup avoid flushing shared databases. The
45-second async token is supplemented by external process and hang limits for
native calls, sync sends and disposal. Save logs and remove only owned containers.
Existing redis-ci runs the complete project with Fragile!=CI; these cases have no
Fragile exclusion. No dependency/workflow change or hosted service is proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
six cases, the scoped public writer contract and minimum implementation, the 26
reviewed mutation failures with exact restorations, native Docker baselines and
validation, affected builds/regressions, owned cleanup and local reports. Commits,
deployment, publication and hosted calls are excluded. The approved test/helper now
match their compiled copies byte-for-byte. The next
[Redis subscription reader draft](../redis-subscription-reader/README.md) remains
outside the compiled suite pending its own approval.
