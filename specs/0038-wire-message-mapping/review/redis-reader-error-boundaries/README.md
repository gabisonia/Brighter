# Redis reader error boundaries

Status: approved, characterized and validated on 2026-10-30.

Review [the eight cases](When_a_redis_read_fails_should_preserve_legacy_and_native_error_boundaries.cs)
and [the recording default reader](RedisRecordingDefaultWireReader.cs).

## Contract and behavior

The configured-reader recovery boundary must preserve existing unconfigured parsing
and native command error handling. These eight cases cover sync/async × four paths:

| Path | Required observation |
| --- | --- |
| Unconfigured reader, malformed JSON header | The existing JsonException escapes Receive; no message is returned and the next native ID stays queued. |
| Configured reader composing the default parser, same malformed header | Receive returns a failure message preserving native ID 1, topic and the complete raw string; the next native ID stays queued. |
| Unconfigured reader, invalid envelope prefix | Receive retains the existing empty-message sentinel, with empty body; the next native ID stays queued. |
| Configured reader, native queue key has string type | Native WRONGTYPE produces ChannelFailureException with a RedisException inner error; the reader is never called and the native string remains intact. |

An independent native client seeds source data and inspects queue state. After each
first outcome, the same consumer reads a valid message with exact application ID,
topic and Unicode body, acknowledges it and reaches an empty receive. For the native
error case, the fixture first repairs only its owned wrong-type queue key. Stored
source strings remain unchanged. Recording inputs establish that the configured
parser receives only the original strings and never a synthetic native-error value.

The malformed payload is multiline to verify receive-side content preservation.
No rejection/forwarding round trip is asserted for that payload. Connection loss,
cancellation, timeout handling, other parser failures, retry mapping, concurrency,
automatic pump recovery and crash redelivery remain separate.

## Results

All eight approved cases passed initially on .NET 9 and .NET 10. No production
correction was needed. Four independent .NET 10 mutations produced the eight
intended assertion failures, with each mutation's other six cases passing. Every
byte-exact restoration returned eight passes; final focused runs pass on both
frameworks. The approved test/helper, cases, assertions and bounds are unchanged.

Both unfiltered full suites passed 124 cases with four existing skips, retaining
all 120 preceding identities/outcomes plus eight new passes. All four library
targets built with zero warnings/errors. The initial dependency build reported
604 existing warnings, none in the new files; the final restored build was clean.

All 6,513 preceding C# files remain byte-identical. The only compiled source
additions are the approved test/helper. Dependency, lockfile and workflow hashes
are unchanged. All three owned brokers were removed after logs were saved;
unrelated containers were untouched. TRX, logs, mutation snapshots, source
verification and cleanup evidence are retained in
`/private/tmp/brighter-4170-redis-reader-error-boundaries/`.

The next [immediate requeue-writer draft](../redis-immediate-requeue-writer/README.md)
covers ten sync/async legacy/custom/composed/throw/null cases with native retry
controls and same-consumer recovery. It remains unbuilt/unrun outside the compiled suite.

## Approved execution plan (completed)

Adopt the approved test/helper unchanged and run both frameworks. This is a
characterization batch: current behavior is expected to pass. Establish behavioral
RED with the following independent production-only .NET 10 mutations:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Move unconfigured default parsing inside the configured-reader catch boundary | Unconfigured malformed JSON must still raise JsonException | 2 |
| Bypass the configured-reader catch for explicit default composition | Configured malformed JSON receive error must be null | 2 |
| Replace unconfigured empty-message parser sentinels with mapping failures | Legacy sentinel IsEmpty must be true | 2 |
| Return an empty array from both Receive RedisException handlers | Native error must be ChannelFailureException | 2 |

Require eight intended assertion failures; each mutation's other six cases must
pass. Setup/compiler/timeout failures do not count. Restore production bytes exactly
after every mutation and require all eight passes. If initial behavior differs,
retain the evidence and confine any correction to these reviewed boundaries;
do not weaken cases, assertions or bounds.

Finish with focused passes on both frameworks, four Redis library targets built,
and both complete suites retaining all 120 preceding identities/outcomes plus eight
new passes. Expected complete matrices are 124 passes/four existing skips each.
Run each full suite once on a separate fresh ready broker; preserve and investigate
failures without retrying behavioral regressions until green.

Use the pinned Redis image and native Docker SDK runners, no host ports, unique
keys and owned cleanup. Async operations use a 45-second token; receives request
one second. External process/hang limits bound sync/native calls and disposal.
Save logs before removing owned containers. No dependency/workflow or public
signature changes are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
eight cases, any evidenced minimum boundary correction, eight reviewed mutation
failures with exact restoration, native Docker validation, affected full regressions,
four library builds, owned cleanup and local reports. Hosted calls, commits,
deployment and publication are excluded. The compiled test/helper match the approved
draft. Further retry and lifecycle tests require their own review.
