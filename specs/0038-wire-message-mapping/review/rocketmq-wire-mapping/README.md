# RocketMQ endpoint wire mapping

Status: initial native integration validated locally on 2026-11-07; broader release gates remain open.
No commits or pushes.

The initial integration exposes publication-selected writing and subscription-selected
reading with public default adapters. A writer owns body bytes and application properties;
the gateway retains native topic, tag, keys, FIFO group and delivery timestamp. A reader
receives the actual SDK MessageView. After mapping, the consumer always attaches that
native receipt and resolves the broker delivery count. Custom mappings cannot forge a
settlement receipt or replace native retry semantics.

The existing public constructors and publisher method remain compatible. Defaults must
retain the current encoding and header precedence. Default reader composition must remain
available without constructing or mocking SDK MessageView objects. Native requeue changes
invisibility on the original broker delivery; it does not invoke the writer or serialize a
modified application message.

The first matrix contains four sync/async × complete/composed native round trips, each
with repeated payloads and native redelivery. It verifies dispatch-time trace context,
baggage, caller preservation, captured subscription selection, native routing/tag/keys,
authentic receipts, normalized broker delivery counts, acknowledgement and empty receipt
after settlement. Four writer failure cases must fail before publication and recover on
the same producer. Four reader failure cases must return raw rejectable deliveries with
authentic receipts, settle through rejection and recover on the same consumer.

Named production mutations bypass the selected writer, bypass the selected reader, omit
native receipt replacement, omit broker delivery count normalization and propagate reader
failures. Each requires the matching assertion to fail and exact restoration to pass.
Full native regressions on both test frameworks and all supported library builds remain
mandatory. FIFO/delayed sends, scheduled callbacks, rejection forwarding and remaining
lifecycle combinations remain open in the completion plan until separately reconciled.

The fixture uses the repository's RocketMQ 5.5.0 topology on a private network without
host ports. Native topics are provisioned explicitly. Its reduced JVM heap settings are
local fixture configuration only, recorded with evidence; no repository dependency,
test coverage or protocol is changed to obtain a pass. Large native fixtures run
sequentially because concurrent SQL/RocketMQ execution exhausted Docker VM memory.

The composed writer deliberately modifies the returned default body array in place.
Caller preservation must therefore cover both the property dictionary and mutable
byte ownership, not only replacing the carrier's Body property. This is part of the
fresh independent representation contract and is checked through native publication.

Native setup successfully provisions all 116 required topics. The first ten-minute
setup wait expired while the admin process was still making progress; a longer wait
resumed without restarting the broker or duplicating native tests. All topic commands
then completed with exit zero. Native baselines use the saved pre-integration assemblies.

The first complete unchanged .NET 9 baseline has 72 passes, 31 failures and two existing
skips. One failure is the temporary runner's assembly-directory layout: its repository
file test could not locate the real compose file by walking parents. A nested mount now
places assemblies below the read-only checkout. The initial missing mount-point error
was corrected before adapter tests executed; neither setup error is counted as a RED test.

The other baseline failures include native receive deadlines and missed redeliveries.
Reducing the isolated proxy's minimum polling time from 5,000 ms to 100 ms does not fix
them: unchanged six-case probes yield zero passes/six failures on .NET 9 and one pass/
five failures on .NET 10. The repository's polling settings remain unchanged. A standalone
RocketMQ.Client 5.2.1 probe, without Brighter, receives an empty queue successfully, then
times out on all three receives after successful native sends. Proxy logs report a
timeout waiting for the broker. This isolates a native broker/client environment problem;
it does not yet establish its cause or permit skipping the affected tests.

A separate public default-adapter ownership test reproduces body aliasing on both
frameworks. Copying the source body into an independent array fixes both runs. The test
checks raw bytes as well as the cached textual value, and independent properties/output
arrays. The matrix now contains thirteen new cases; full native acceptance remains open.

The broker's `pop.log` subsequently identifies the native timeout cause:
`OutOfDirectMemoryError`, with 267,714,560 bytes allocated against the temporary
268,435,456-byte direct-memory ceiling. The SDK-only probe reproduces the same error.
The isolated fixture now retains its bounded heap but allows 1 GiB broker direct
memory and 512 MiB proxy direct memory. The original 5,000 ms polling setting is
restored. Repository broker settings and dependencies are unchanged.

After provisioning native routes and restarting the owned proxy to clear its earlier
negative route lookup, the SDK-only probe receives and acknowledges all three messages.
Both unchanged six-case regression probes pass. Complete unchanged baselines and the
thirteen-case integration/mutation matrix are running; these probe passes alone do
not complete native acceptance. Logs and fixture settings remain under
`/private/tmp/brighter-4170-rocketmq-wire-mapping/`.

The corrected unchanged .NET 9 full suite passes 103 cases with two existing skips.
Reusing that broker for .NET 10 produces 26 failures, including receipt of message
IDs from the preceding run. Native topic history is retained across consumer groups;
the first suite's successful consumption does not empty the log for later groups.
That failed run remains under `baseline-direct-memory-full-net10.0.*`.

Each subsequent native run recreates only the task-owned ephemeral broker/proxy and
provisions the same 116 topic definitions through the native admin SDK. The isolated
.NET 9 baseline again passes 103 cases/two existing skips; .NET 10 is running. New
mapping cases additionally use a unique native tag/filter per case, preserving every
payload, receipt, count and settlement assertion while isolating retained history.
This changes test provisioning only, not gateway behavior or default broker settings.

## Completed initial integration — 2026-11-07

Fresh native baselines pass 103 cases/two existing skips on each framework.
Final suites pass 116 cases/two existing skips per framework, preserving all prior
identities/outcomes and adding thirteen mapping cases. Six mutations produce 25
intended assertion failures; each mutation is exactly restored and its cases pass
again. All four library targets build. The whole-source comparison confirms exactly
five expected existing gateway files changed during this package.

The new factory tests also required `requestType: typeof(Message)` to satisfy the
existing subscription contract. This was a setup correction; mapping, receipt,
delivery count and settlement assertions remain intact. A cached-image fixture
restart initially attempted a registry pull and timed out before test execution;
restarts now explicitly use `--pull never --no-build` with the same pinned digest.
All failure evidence is retained. Consumer-created forwarding writer selection and
remaining lifecycle/release reconciliation are separate open gates.

Final evidence: `/private/tmp/brighter-4170-rocketmq-wire-mapping/final-verification.json`.
