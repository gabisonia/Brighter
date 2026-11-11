# RabbitMQ publish-sequence ownership

Status: approved and copied unchanged into the compiled test project.
Both cases pass on .NET 9 and .NET 10. The named mutation fails both cases at
the expected header-type assertion; byte-for-byte restoration passes again.
Both complete RabbitMQ Async Release suites pass with 490 passed, two existing
skips and zero failures per framework. Every prior test and outcome is retained,
with exactly the two new passing cases added. No lasting production change was
needed; nothing was committed or pushed.

- [Test](When_a_wire_writer_supplies_a_publish_sequence_should_preserve_sdk_confirmation_controls.cs)
- [Writer helper](ConflictingConfirmationRmqWireMessageWriter.cs)

Two cases create producers through the public factory and send through sync and
async entry points. The writer returns a legacy body, ID and tenant header, plus
the string `writer-sequence` in `Constants.PublishSequenceNumberHeader`.

An independent native reader must observe sequence number `1` as an AMQP long.
Each producer has a fresh channel and sends one message. The confirmation must
still identify the original Brighter message and topic. Existing assertions also
verify custom body/properties, gateway persistence and unchanged source data.

The pinned RabbitMQ.Client 7.2.2
[publish implementation](https://github.com/rabbitmq/rabbitmq-dotnet-client/blob/v7.2.2/projects/RabbitMQ.Client/Impl/Channel.BasicPublish.cs)
overwrites this header when publisher confirmations and tracking are enabled.
The test verifies that the Brighter configuration reaches that SDK behavior.
It does not require a second, competing header implementation in Brighter.

Named RED mutation: in `RmqMessageGateway.ConnectToBrokerAsync`, change only
`publisherConfirmationTrackingEnabled: true` to `false`. Keep publisher
confirmations enabled and retain the producer's confirmation event subscription.
Both cases must fail at the native sequence-header type/value assertion.
A timeout or unrelated failure is not sufficient mutation evidence.

Restore the gateway from its byte snapshot, verify both cases on .NET 9 and
.NET 10, then run both complete RabbitMQ Async suites with TLS and native delay.
Do not alter the test or helper during mutation.

The helper composes the existing test writer and adds no request type or mapper.
There is no new public API. No production change is expected if the SDK controls
already satisfy the contract. Nothing will be committed or pushed.
