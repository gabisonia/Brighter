# SNS wire attributes during overlapping sends

Status: approved and validated. Both cases pass on .NET 9 and .NET 10; the named mutation fails both .NET 9 cases at the intended distinct-ID assertion. Both complete AWS V4 Release suites pass with 491 tests, zero failures and zero skips per framework.

- [Test](When_sns_sends_overlap_should_preserve_each_messages_wire_attributes.cs)
- [Overlap helper](OverlappingSnsWireMessageWriter.cs)

Two cases use the public SNS producer factory and sync/async sends. They compose
the default SNS writer with the existing tenant-header writer, then publish two
different messages through one producer. A native SNS-to-SQS subscription exposes
the resulting SNS envelopes independently of Brighter's reader.

The helper holds the first mapped result before publication until the second
mapping finishes. This makes the two output lifetimes overlap without relying
on thread scheduling. It does not claim simultaneous execution inside the
default converter. The helper follows the existing SQS overlap fixture and
needs no request type, mapper registration or logging initializer.

The test checks two distinct Brighter IDs, each envelope's body and subject,
topic ARN, tenant header, CloudEvents values and serialized handled count.
Source messages and bags must remain unchanged. Finally blocks release the
mapping gate, join send tasks, dispose the producer and reap native resources.

This is a characterization test; no production change is expected.

Named mutation in `DefaultSnsWireMessageWriter`: add one instance-level
`Dictionary<string, MessageAttributeValue>` and make `BuildMessageAttributes`
clear and refill it from the freshly built dictionary before returning that
shared dictionary. Do not change the test/helper, body or subject mapping.

The second conversion then overwrites the first result before either send uses
it. Both native envelopes should expose the second Brighter ID, causing the
distinct-ID count assertion to fail with one instead of two. A timeout, transport
error or unrelated assertion does not establish RED.

The approved sequence is complete: initial GREEN on both frameworks, mutation
RED on .NET 9, byte-for-byte writer restoration, restored GREEN on both frameworks
and both complete AWS V4 Release suites. All 489 preceding cases and outcomes
are retained with exactly two new passing cases. The task emulator is stopped.
Logs, TRX files and the original writer snapshot are under
`/private/tmp/brighter-4170-sns-overlap/`.
No public API change, commit or push is authorized.
