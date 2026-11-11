# Kafka default reader content-type compatibility correction

Status: approved and applied. All four cases pass initially and after mutation
restoration on both frameworks. The charset mutation fails at the intended exact
ContentType assertion, completing ten verified mutations. Both complete Kafka suites
pass 269 cases with no failures/skips; all 265 baseline outcomes are retained.
The original failed expectation and logs remain preserved. Production behavior was
not changed to satisfy the amended expectation.

The approved default-reader case expected `application/octet-stream` without a
charset. Its first .NET 9 run returned `application/octet-stream; charset=utf-8`.
`KafkaMessageCreator.SuccessMessage` passes the parsed ContentType to `MessageBody`,
whose byte-array constructor calls `SetCharacterEncoding` with its existing UTF-8
default. The same ContentType is retained by the header. The new reader delegates
to that creator, so removing the charset in production would change existing behavior.

The [amended reader test](When_the_default_kafka_reader_maps_a_native_delivery_should_preserve_content_telemetry_and_offsets.cs)
changes one expectation at line 90 to:

```csharp
Assert.Equal(new ContentType("application/octet-stream") { CharSet = "utf-8" }, first.Header.ContentType);
```

This keeps exact ContentType equality, including media type and charset. It does
not reduce the assertion to checking only the media type. All other assertions
remain intact. The file also includes the already applied, behavior-neutral
qualification of `Paramore.Brighter.PartitionKey`, needed because the test namespace
contains a child namespace named `PartitionKey`. The original approved files and
initial missing-type/namespace and reader-assertion evidence remain preserved.

After approval, copy this amended reader test into the compiled project and run
all four cases on .NET 9 and .NET 10. Complete the original nine named mutations
plus one additional mutation: clear `result.Header.ContentType.CharSet` after the
default reader maps the native message. Select the single reader case and require
`Assert.Equal` at line 90. This guards the corrected compatibility expectation.
The completed total is ten intended assertion failures. Restore production bytes,
build all supported targets, rerun the four focused cases and both complete Kafka
suites, expecting 269 passes each while retaining all 265 baseline outcomes.

No Core or legacy creator behavior is to be changed to satisfy this expectation.
The [original batch](../kafka-default-wire-mapping/README.md) and
[remaining completion gates](../../tasks.md) retain their scope. No commit or push
is authorized. Review is required by
[testing.md](../../../../.agent_instructions/testing.md):
“STOP after writing the test and ASK FOR APPROVAL.”
