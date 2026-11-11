# Kafka default body ownership

Review-after for issue #4170; no commits or pushes. This scoped package is validated and integrated locally.

The shared writer contract requires independent output suitable for composition.
The earlier Kafka extraction explicitly retained the old read-only convention for
body arrays. That convention makes the mutable `Message<string, byte[]>.Value`
surprising to compose: modifying one mapped body also modifies the source and another
mapping. This package strengthens that boundary without changing wire bytes, native
routing, keys, headers or confirmation behavior. Application-supplied header builders
retain their documented ownership responsibilities.

The public-adapter test maps one source twice, modifies the first native body and
requires the source raw bytes and second body to retain `original-body`, with distinct
arrays. Both .NET 9 and .NET 10 reproduce the intended RED: the source becomes
`Xriginal-body`. Evidence is in
`/private/tmp/brighter-4170-kafka-default-body-ownership/ownership-red_*.trx`.

The body copy in `DefaultKafkaWireMessageWriter` passes on both frameworks in an
isolated checkout. A second public test reproduces the same aliasing for default
byte-array headers on both frameworks (`original-header` becomes `Xriginal-header`).
Copying those arrays in `KafkaDefaultMessageHeaderBuilder` fixes the second case.
Both focused cases now pass on both frameworks. Default wire bytes remain identical.

Both full native suites pass 400 cases, preserving all 398 prior identities/outcomes
and adding the two ownership cases. All four library targets build. Both source fixes
are integrated after matching their original main-checkout hashes and completing
RocketMQ's whole-source integrity check. The focused tests need no broker; they do not establish native Kafka behavior.

## Batch and transaction applicability

The current `KafkaMessageProducer` implements the single-message synchronous and
asynchronous producer contracts and their confirmation contracts. It exposes no
Brighter bulk-send or transaction lifecycle. Both publisher paths invoke the
selected writer before calling Confluent `Produce` or `ProduceAsync`; SDK batching
therefore receives already-mapped individual records. There is no separate Brighter
batch serializer to integrate.

`TransactionalId` and `EnableIdempotence` are passed into native producer configuration,
but the gateway does not call `InitTransactions`, `BeginTransaction`, `CommitTransaction`
or `AbortTransaction`, and does not expose these operations. This review does not
claim transactional dispatch works. Adding a transaction lifecycle is a separate
capability, not required to preserve mapping on existing exported send paths.
Rebalance, cancellation, confirmation, scheduler and the retained disposal-stall
finding still require their own evidence and disposition.
