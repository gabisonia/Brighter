# RabbitMQ Sync invalid mapping forwarding and recovery

Status: approved tests executed unchanged. All ten pass on both frameworks without
production changes; final full suites pass 237 cases with one existing skip each.
Six mutation runs produce 42 intended failures, but an earlier omit-source-ack run
missed one case (nine failures/one pass). The repeat does not erase that gap. The
[settlement-observation amendment](../rmq-sync-settlement-observation/README.md)
has since been approved and validated: all six mutations and six additional
acknowledgement-omission runs detect every intended case, with both full suites
retaining 237 passes/one existing skip. See that amendment for current compiled
test bytes; the original review draft and false-negative evidence remain here.

The [ten native cases](When_rmq_sync_forwards_invalid_mappings_should_preserve_content_and_recover_from_an_unbound_route.cs)
cover five mapping outcomes with a working route and with an initially unbound
invalid-message route. The outcomes are an unset default reader, an explicit default
reader, a throwing reader, a null-returning reader and a reader that explicitly
returns an unacceptable mapped message. The [reader helper](SyncInvalidDeliveryWireReader.cs)
provides the three custom outcomes; it has no handler or request registrations.

An independent RabbitMQ Client 6 publisher confirms a malformed binary delivery.
Its application-topic header differs from its native routing key. Default parsing
retains the parsed topic; throw/null recovery uses the native topic. The explicit
unacceptable result supplies its own ID, topic, binary body and marker, which must
survive mapping and forwarding. Deliberately conflicting custom delivery controls
must be replaced by the broker's tag, persistence, redelivery and handled count.

The public factory creates the consumer with a validated invalid queue. For the
recovery cases the test removes its binding after receiving. Reject must report
ChannelFailureException and restore the message topic. After disposing the consumer,
an independent native BasicGet must find the original delivery with its original
bytes, ID, routing key, properties, malformed header and count. The invalid queue
must remain empty. The test requeues that delivery, restores the binding and uses
a new consumer to complete rejection.

For both paths, confirmed forwarding must preserve the mapped bytes and identity,
original-topic and rejection metadata, persistence and handled count. The source
must be settled, the original message topic restored and exactly one invalid copy
present. Queue/exchange cleanup is confined to unique resources owned by each case.
The tests join the existing RMQ collection and use no mocks, dependencies or new
public APIs. They extend existing rejection tests with mapping-failure inputs and
native reconsumption after failed forwarding.

## Named mutations

Apply each independently to production code with unchanged tests after approval.
These are characterisation guards if the first run is already green.

| Mutation | Expected failures | Intended assertion |
| --- | ---: | --- |
| Replace a successfully returned configured-reader result with default parsing | 2, explicit unacceptable results only | Custom mapped ID, line 121 |
| Publish an empty body from ForwardToInvalidChannel while retaining its header | 10 | Exact forwarded binary body, line 195 |
| Move source acknowledgement before ForwardToInvalidChannel | 5, unbound-route cases only | Original native delivery remains available, line 148 |
| Ignore the mandatory BasicReturn after confirmation | 5, unbound-route cases only | Forwarding reports ChannelFailureException, line 138 |
| Omit source acknowledgement after successful forwarding | 10 | Source queue is empty after disposal, line 192 |
| Leave the message topic at the invalid route in the forwarding finally block | 10 | Original topic restored, line 139 or 190 |

Expected: 42 intended assertion failures across six mutations. A compile error,
timeout or unrelated exception does not count. Diagnostic compilation may preserve
precise assertion locations; final builds and restored runs use normal optimization.

After approval, copy both files unchanged into MessagingGateway and TestDoubles,
run all ten cases on .NET 9 and .NET 10, correct any demonstrated defect, validate
all six mutations, and build the Sync library's four supported targets. Run both
complete Sync suites and compare every outcome with the preceding batch. Do not
commit or push. Custom invalid-writer selection, writer/callback/outbox recovery,
retry formats, concurrency and scheduler paths remain separate #4170 work.
