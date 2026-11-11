# Observe ordered GCP nack and redelivery

Status: approved and executed; native redelivery failure reproduced. The restoration gate remains unresolved; not characterized or production-ready.

## Failure to investigate

Existing generated two-message ordered-nack tests intermittently miss the nacked
message's ID at their final assertion (Reactor line 152, Proactor line 158). The
other message is received. Earlier pre-change controls also reproduced ordered
redelivery failure; no cause or correction is established. Shutdown observation
does not exercise this active-consumer sequence.

Review [the six cases](When_nacking_an_ordered_gcp_delivery_should_send_a_native_nack_and_recover_both_messages.cs),
[the passive RPC observer](GcpStreamRpcObserver.cs), and
[the forwarding stream reader](GcpObservedStreamReader.cs).
The matrix crosses sync/async operations with SDK-default, one and two subscriber
clients. Zero in the theory data means leave ClientCount unset, not create zero
clients. The trace records the configured count and process processor count.

## Behavior and evidence

Each case creates a real ordered native subscription and public factory-created
producer/channel. Two distinct messages use the same ordering key and are sent
sequentially through the selected sync/async API. The first delivered message is
selected by its actual ID; no assumption about which source arrives first is made.
Its body, native ordering key and pending receipt must be intact.

The public SDK ApiSettings.Interceptor hook forwards every request and response
unchanged. A forwarding IAsyncStreamReader observes successful StreamingPull
responses before the SDK processes them and clones their native ReceivedMessage
values, linking native message IDs to acknowledgement IDs. Unary observations record
request start, successful completion or failure. SDK-owned connections and their
shutdown path remain in use. There is no substituted broker response or native fault.

The test snapshots acknowledgement IDs already observed for the first native message,
then nacks through the public channel and immediately begins the same bounded receive/
ack pattern as the existing tests: 500-millisecond receives for up to thirty seconds,
acknowledging each arrival until both source IDs are observed. **It does not wait for
the native nack RPC before receiving or acknowledging the other message.** Such a wait
could hide the timing condition under investigation. Content and native ordering are
checked on every nonempty receive.

After that loop, the test first requires a successful deadline-zero ModifyAckDeadline
RPC started after Nack began, targeting the exact subscription and at least one
acknowledgement ID observed for the first native message. Then it requires the original
receipt to complete with Nack and both application IDs to appear. A snapshot may
contain multiple delivery IDs for the same native message; the test does not assume
that an at-least-once service delivers it only once. It does not claim to identify
which duplicate receipt the SDK selected if multiple copies were already present.

The trace can distinguish an absent native nack, a completed nack followed by missing
native redelivery, and a native redelivery that fails to reach the application. It
also records actual acknowledgement requests and their relationship to native delivery
IDs for subsequent diagnosis. The original generated tests remain unchanged and stay
in the full regression matrix. This diagnostic uses public factories and native setup;
it is not claimed to duplicate every provisioning detail of their harness.

The operation budget is ninety seconds. Disposal/delete are bounded, cleanup failures
are surfaced after successful behavior, and a process hang limit is required. The two
helpers only observe I/O and introduce no request/handler type or logger registrations.
Existing GcpNativeStreamLifecycle/Requires traits select emulator execution and exclude
hosted execution.

## Proposed execution and named mutations

Adopt the test and two helpers, preserving review copies, and run six cases on both
frameworks. If behavior is initially RED, retain full native/application traces and
establish its cause before a minimum correction. Observer/setup failures do not count
as product defects. Do not increase receive bounds, reduce default client count,
change SDK versions or serialize native nack/ack operations to make the test pass.

If initially green, use these named production-only mutations:

| Mutation | Required RED |
| --- | --- |
| Make the consumer's private Nack helper return without rejecting the receipt | Six missing correlated native nack assertions |
| Make that Nack helper call Accepted instead of Reject | Six missing correlated native nack assertions; native acknowledgement trace retained |
| Replace the gateway-owned receipt assignment in ReadWireMessage with a new GcpStreamMessage containing the same native message | Six missing correlated native nack assertions: the mapped receipt is disconnected from the SDK callback |

Expected total: eighteen intended failures. All mutations must fail at the correlated
RPC assertion, not setup, compilation or a different assertion. Restore exact production
source and require six passing cases after each. Finish with both restored focused
suites, all available Pub/Sub regressions and all four library targets. Record all
prior case identities, failures and skips; passing focused runs do not resolve earlier
intermittent findings or prove hosted-service behavior.

## Review boundary

The repository's [.agent_instructions/testing.md](../../../../.agent_instructions/testing.md)
requires "STOP after writing the test and ASK FOR APPROVAL". Approval covers this
concrete diagnostic, three mutations, regressions and an evidence-supported minimum
correction. No commit or publication is included. Shutdown/buffered-disposal failures,
broader concurrency, scheduler/rejection mapping, hosted checks and other gateway
integrations remain separate release requirements.

## Execution outcome

Initial: six passes on each framework. Three approved mutations: eighteen intended
native-nack assertion failures; exact production source restored after each.
Restorations: six passes, six passes, then five passes/one async default-client failure.
The failed restoration completes the correlated native nack and receipt reply but
never receives the first message again, including at the raw native stream boundary.
Full net9.0 independently reproduces this failure. Final focused runs pass six each,
but the captured failures remain unresolved. No product correction is made.

| Framework | Passed | Failed | Skipped | Total |
| --- | ---: | ---: | ---: | ---: |
| net9.0 | 413 | 3 | 55 | 471 |
| net10.0 | 415 | 1 | 55 | 471 |

The full matrices retain all 465 previous identities plus six new cases. All four
library targets build. All 6,481 pre-existing C# files remain byte-identical, and the
three adopted files still match these review copies. Owned containers are cleaned up.
See [the validation record](../../validation.md) and the evidence directory
`/private/tmp/brighter-4170-gcp-ordered-nack-observation/`.

The [native SDK control](../gcp-ordered-nack-native-control/README.md) is the next
concrete review draft. No new control is built or run yet. No commit or publication.
