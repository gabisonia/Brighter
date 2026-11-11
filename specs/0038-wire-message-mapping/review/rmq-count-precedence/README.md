# RabbitMQ handled-count precedence

Status: approved and copied unchanged into the compiled test project.
All ten cases pass on both frameworks. Each named mutation fails only its
intended cases at the handled-count assertion. Full regression together with
the two retry-cycle cases passes on both frameworks: 502 passed, two existing
skips and no failures.

- [Test](When_native_rmq_count_headers_vary_should_preserve_count_precedence.cs)
- [Reader helper](CountedLegacyRmqWireMessageReader.cs)

Ten native-broker cases exercise both public consumer factory paths. The helper
composes the existing legacy reader and sets its returned handled count to 99.
It adds no request type, mapper registration or public API.

| Native canonical count | Expected returned count | Contract |
| --- | --- | --- |
| Absent | 99 | Preserve the reader's count |
| AMQP integer 7 | 7 | Native count overrides the reader |
| UTF-8 string `7` | 7 | Reuse the existing text parser |
| UTF-8 string `not-a-count` | 0 | Reuse the existing malformed-value fallback |
| AMQP long 7 | 0 | Reuse the existing unsupported-type fallback |

The tests also assert legacy application mapping and broker delivery metadata,
then acknowledge the result and check the source queue independently. They do
not directly call parser internals.

These are characterization cases for the approved retry-count correction.
No production behavior change is proposed.

Named mutations, applied separately:

1. Remove the new count-attachment condition and assignment from
   `RmqMessageConsumer.ReadMessage`. Eight cases with native headers must fail
   at the final handled-count assertion, returning 99. Both missing-header cases
   must still pass.
2. Replace that condition and assignment with an unconditional assignment from
   `RmqMessageCreator.ReadHandledCount(delivery.BasicProperties.Headers ??
   new Dictionary<string, object?>()).Result`. The two missing-header cases
   must fail at the handled-count assertion, returning zero instead of 99.
   The other eight cases must pass.

After each mutation, restore the production snapshot byte-for-byte. Verify all
ten cases on .NET 9 and .NET 10 and run both complete RabbitMQ Async suites.
Do not count setup errors, mapping failures or timeouts as mutation evidence.

This increment does not cover a null native header dictionary, reader failures
with a count header, negative or overflowing counts, or delayed reconsumption.
No commits or pushes are authorized.
