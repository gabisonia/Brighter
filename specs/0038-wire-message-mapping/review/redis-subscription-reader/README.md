# Redis subscription reader

Status: approved, implemented and validated on .NET 9 and .NET 10, with fixture-only corrections described below.

## Result

The first build confirms the missing default adapter. The next build exposes an
ambiguous RedisSubscription name shared by Brighter and ServiceStack; qualifying
the Brighter type resolves it. Initial execution then stops at the existing
subscription constructor's configuration guard. Supplying its required requestType
and sync/async messagePumpType resolves that setup failure. These are the only
test changes: all six cases, assertions and bounds are unchanged, and the helper
matches its approval snapshot. Original and corrected snapshots are retained.

With those fixture corrections and consumer selection still unwired, both frameworks
show four intended first-ID failures and two legacy passes. The property/default
adapter and internal factory/consumer wiring now make all six cases pass. Existing
public constructor signatures and native delivery-control code remain compatible.

The four reviewed mutations produce all twenty intended assertion failures at
their specified source assertions. Exact production restorations return six passes
after each. Both complete suites pass 108 cases with four existing skips, retaining
all 106 preceding identities/outcomes plus six new passes. All four library targets
build without warnings/errors. All three owned brokers are removed with logs saved.

Evidence: `/private/tmp/brighter-4170-redis-subscription-reader/`, including approved
and corrected fixtures, setup/RED/green/mutation/full records, source hashes,
`final-verification.json` and `cleanup.json`. Configured-reader error handling and
wider Redis lifecycle/release gates remain open.

Review [the six cases](When_receiving_with_a_redis_wire_reader_should_preserve_mapping_and_acknowledgement.cs)
and [the configured reader](RedisSubscriptionWireReader.cs).

## Contract and behavior

Propose an optional RedisSubscription.WireMessageReader of type
IAmAWireMessageReader<string> and a reusable public DefaultRedisWireMessageReader
that composes the existing RedisMessageCreator. Both consumer factories must pass
the selection into each consumer; existing public constructor signatures remain
compatible through internal wiring. Null selection uses the default adapter. Its
legacy parsing and sentinel behavior must remain unchanged.

Sync/async × legacy/custom/composed cases seed two native stored strings and queue
IDs through a ServiceStack client. They receive through the corresponding public
consumer factory. The native topic differs from the application topic, and native
queue IDs 1 and 2 differ from the mapped message IDs. Assertions require each exact
application ID, Unicode text body, topic and marker. Custom input uses a simple
pipe-separated representation; composed input prefixes the existing envelope and
delegates decoding to the default reader. No Brighter producer encodes the input.

After the first receive, the native queue must contain only ID 2. A receive before
acknowledgement must raise ChannelFailureException without consuming that next ID.
Acknowledging the first mapped message must allow the second through the same
consumer. After acknowledging the second, an empty receive succeeds. Native stored
strings stay unchanged, and the configured reader sees exactly the two originals
in order, with no invocation for blocked/empty receives.

This checks the existing in-flight acknowledgement gate and destructive queue pop;
it does not add broker settlement or claim redelivery after a process crash. Reader
exceptions/nulls, malformed/default parser failures, retry/rejection republishing,
lost connections, cancellation, concurrency and scheduling remain separate. The
helper is a mapping strategy; tests use native Redis I/O and public exports only.

## Approved execution plan (completed with fixture corrections)

After approval, adopt these two files unchanged. Observe the expected missing
contract compilation failure, then add only the property/default adapter with
consumer selection unwired. Both frameworks must show four custom/composed mapped
identity failures and two legacy passes. Wire both factories and receive paths,
preserving the existing delivery controls, and require six passes.

Apply these independent production-only mutations on .NET 10 after green:

| Mutation | Required assertion failures | Count |
| --- | --- | ---: |
| Ignore configured readers and use the default reader in both receive paths | First mapped identity in custom/composed cases | 4 |
| Default reader replaces the decoded body with a fixed wrong body | First mapped body in legacy/composed cases | 4 |
| Replace each mapped application topic with the consumer's native topic | First mapped topic in all cases | 6 |
| Omit in-flight tracking after mapping in both receive paths | ChannelFailureException assertion for the receive before acknowledgement in all cases | 6 |

Require twenty intended assertion failures and unaffected cases passing. Use the
assertion type and expected/actual values to identify failures; async cleanup stack
locations alone may be misleading. Setup, timeout and compilation failures do not
count. Restore production bytes exactly after each mutation and require six passes.
Keep the approved cases, assertions and bounds unchanged.

Finish with focused passes on both frameworks, all four Redis library targets built,
and full suites retaining the preceding 106 identities/outcomes plus six cases.
Expected complete matrices are 108 passes/four existing skips each if the baseline
remains unchanged. Run full suites once on fresh ready brokers, retain any failures
and investigate without retrying behavioral regressions until green.

Use the already recorded Redis image digest/platform, native Docker SDK runners,
no host port bindings, unique keys and explicit cleanup. Async calls use a 45-second
token; receives request a one-second wait. External process/hang limits bound sync
I/O and disposal. Save logs and remove only owned containers. Existing redis-ci
selection includes these cases. No dependency/workflow changes are proposed.

## Approval boundary

The repository [testing instructions](../../../../.agent_instructions/testing.md)
require "STOP after writing the test and ASK FOR APPROVAL". Approval covers these
six cases, the scoped reader contract and minimum wiring, twenty reviewed mutation
failures with exact restorations, native Docker validation, affected regressions
and builds, owned cleanup and local reports. Hosted calls, commits, deployment and
publication are excluded. The corrected review fixture now matches its compiled
copy; the helper remains unchanged. The next
[Redis writer failure-recovery draft](../redis-writer-failure-recovery/README.md)
remains outside the compiled suite pending its own approval.
