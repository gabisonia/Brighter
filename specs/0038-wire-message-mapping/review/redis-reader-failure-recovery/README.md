# Redis reader failure recovery

Status: approved, implemented and validated on 2026-10-30.

Review [the four cases](When_a_redis_reader_fails_should_preserve_the_native_content_for_rejection_and_continue.cs)
and [the recoverable reader](RedisRecoverableWireReader.cs).

## Contract and behavior

Sync/async × throw/null cases configure a reader that fails once, then composes the
default reader for the next native delivery. A configured reader exception or null
result must become a rejectable MT_UNACCEPTABLE message, preserving the raw stored
string, native queue ID and source topic. It must not escape Receive, disappear as
an empty result, or fall back to default parsing. This follows the issue's inbound
failure requirement while preserving Redis's destructive queue-pop semantics.

An independent ServiceStack client seeds two source payload keys and queue IDs 1
and 2, plus the invalid-channel recipient registration. The first receive must
return one failure message with ID 1, the source topic and exact raw single-line
Unicode payload. ID 2 must remain queued. Explicit rejection with Unacceptable must
return success and publish one message on the configured invalid channel. Native
counter/list observations and decoding the stored envelope must preserve the failed
ID/body and rejection metadata. The source queue must still contain ID 2.

The same consumer must then read the valid second delivery with exact application
ID, body, topic and marker, acknowledge it and complete an empty receive. Source
stored strings remain unchanged and the configured reader sees exactly the two
original inputs. The fixture explicitly supplies the required request/pump types
and qualifies Brighter's subscription type to avoid the ServiceStack name collision.

The correction must stay at the configured-reader mapping boundary. Native connection
and Redis command failures retain their existing handling. Unconfigured legacy
parsing and sentinels remain unchanged. This does not establish arbitrary multi-line
or binary payload round trips, other malformed/default-parser cases, invalid-channel
failure recovery, automatic pump recovery, retry mapping, cancellation, concurrency
or crash redelivery. Redis cannot undo its destructive pop; no new broker settlement
or transaction guarantee is proposed.

## Results

Both frameworks reproduced four intended failures at `Assert.Null(receiveError)`:
the configured InvalidOperationException escaped Receive, while null results caused
NullReferenceException. No setup, fixture or assertion changes were needed.

Both receive paths now use a configured-reader mapping boundary that converts
throw/null into a failure with native ID, source topic and raw content. Unconfigured
default parsing remains outside that boundary; native connection and command calls
also remain outside it. Existing public constructor signatures are unchanged.
Subscription XML documentation describes the failure and destructive-pop contract.

All four unchanged approved cases pass on .NET 9 and .NET 10, including independent
native invalid-channel publication and recovery through the same consumer. All four
reviewed mutations produced their sixteen intended assertion failures; every exact
production restoration returned four passes. Final focused runs pass on both frameworks.

Both unfiltered complete suites passed 116 cases with four existing skips, retaining
all 116 preceding identities/outcomes and adding four passes. All four library
targets built with zero warnings/errors. Initial and green test dependency builds
each reported 604 existing warnings, none in the new or changed files.

Of 6,511 preceding C# files, 6,509 remain byte-identical; only the consumer and
subscription changed. The two compiled test/helper additions match their approved
snapshots. Dependency, lockfile and workflow hashes are unchanged. All three owned
brokers were removed; unrelated containers were untouched. TRX, logs, mutation
snapshots, source verification and cleanup evidence are retained in
`/private/tmp/brighter-4170-redis-reader-failure-recovery/`.

The next [reader error-boundary draft](../redis-reader-error-boundaries/README.md)
covers unconfigured malformed parsing/sentinels, explicit default-reader composition
failure and native command errors. It remains outside the compiled suite, unbuilt/unrun.

## Approved execution plan (completed)

After approval, adopt these files unchanged and run both frameworks. Record the
actual failures at the captured-receive-error assertion before implementing the
smallest configured-reader throw/null conversion needed. Preserve the native raw
string, queue ID and topic; keep existing public constructors compatible. Require
all four cases to pass, including native invalid-channel publication and same-consumer
recovery. If behavior is already green, use the reviewed mutations to establish RED.

Apply these independent production-only mutations on .NET 10 after green:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Fall back to legacy parsing after a configured throw/null result | First received message is MT_UNACCEPTABLE | 4 |
| Replace the mapping failure's native ID with Id.Empty | First failure ID equals native ID 1 | 4 |
| Replace the mapping failure's raw body with an empty body | First failure body equals the native stored string | 4 |
| Skip invalid-channel Send/SendAsync while returning successful rejection | Invalid-channel native counter equals one | 4 |

Require sixteen intended assertion failures. Setup, compiler and timeout failures
do not count. Restore production bytes exactly after each mutation and require four
passes. Keep the approved cases, assertions and time bounds unchanged.

Finish with focused passes on both frameworks, all four Redis library targets built,
and complete suites retaining all 116 preceding identities/outcomes plus four new
passes. Expected complete matrices are 116 passes/four existing skips each if the
baseline remains unchanged. Run full suites once on fresh ready brokers; retain
failures and investigate without retrying behavioral regressions until green.

Use the recorded Redis image digest/platform, native Docker SDK runners, no host
ports, unique keys and explicit cleanup. Async operations use a 45-second token;
receives request one second. External process/hang limits bound sync/native calls
and disposal. Save logs and remove only owned containers. Existing Redis CI selection
covers these cases; no dependency/workflow changes or hosted services are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
four cases, the evidenced minimum failure-boundary correction and documentation,
sixteen reviewed mutation failures with exact restorations, native Docker validation,
affected regressions/builds, owned cleanup and local reports. Hosted calls, commits,
deployment and publication are excluded. The compiled test/helper match the approved
draft. Further error-boundary and lifecycle tests require their own review.
