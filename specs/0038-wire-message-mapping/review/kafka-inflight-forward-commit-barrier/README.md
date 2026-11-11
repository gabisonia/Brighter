# In-flight Kafka rejection commit barrier — proposed tests

Status: approved and executed. All four cases demonstrated the expected premature commit on both frameworks. A candidate fix then exposed an incorrect test expectation about source-offset metadata after success. See [the proposed amendment](../kafka-inflight-forward-offset-amendment/README.md). The amended batch is now complete: both full Kafka suites pass 381 cases; see its final results. The original draft remains historical evidence.

The preceding assignment-observation batch passed 377 Kafka tests on each of .NET 9 and .NET 10. This batch addresses a separate interval: a rejection has entered its configured writer, but forwarding has not yet succeeded or failed.

## Observable contract

Four cases cover synchronous/asynchronous consumers and successful/failed first mapping. Two source records occupy offsets 0 and 1 of a fresh partition. A gated strategy writer holds rejection of offset 0 while the caller acknowledges offset 1. A native consumer queries the broker's committed offset before the gate is released: it must be 0, preserving the unresolved record.

After successful forwarding, closing the consumer must leave commit 2 and exactly one invalid-topic record with the configured key, wire ID and mapped binary body. After a mapping failure, rejection must return false, commit must remain 0, and the invalid topic must be empty; retrying the same delivery must recover to the same final state. Source topic, partition key, body, ID and offset remain intact.

The helper implements the public writer strategy; it does not replace Kafka. The gate is always released and the rejection task joined before consumer disposal. Commit observation is bounded to three seconds, with one-second native queries; the gate itself has a thirty-second emergency bound. A slow broker may exhaust the existing five-second forwarding confirmation timeout: this is an infrastructure failure, not acceptable behavioral RED, and must be diagnosed before changing either test or production code.

## Approval scope and validation

Copy the two C# files unchanged into the Kafka test project's MessagingGateway and TestDoubles directories, respectively. Build and run the four cases on .NET 9 and .NET 10. Expected behavioral RED is committed offset 2 instead of 0 while the writer is gated. If green initially, apply the named missing-in-flight-barrier mutation below and follow the characterisation workflow.

After approval and RED, implement only the required barrier behavior. Run these named production mutations independently, restore production after each, and require failures on the corresponding behavioral assertions:

1. Missing in-flight barrier: record protection only after forwarding fails — gated commit assertion.
2. Premature barrier release: release before invoking the writer — gated commit assertion.
3. Lost later acknowledgement: discard the retained high acknowledgement — final commit 2 assertion.
4. Release after failure: remove the failed record's protection — failure/retry commit assertions (if another commit attempt is required to expose this mutation, report the coverage gap rather than changing the approved test).
5. Report success after mapping failure — false rejection assertion.
6. Omit source topic restoration — source delivery assertion.
7. Substitute original key, ID or body for configured mapped values, separately — native invalid record assertions.

Run the full Kafka suites on both frameworks, preserving all existing 377 cases, and build every library target. Record concrete results and restore every mutation before completion. No commit or push is included.

## Limits

This does not prove safety for an acknowledgement already committed before rejection starts, rebalance during the gated operation, late acknowledgements from a previous assignment, two simultaneous rejections of the same record, broker-confirmation loss, or automatic pump recovery. It tests writer completion rather than a broker delivery callback held in flight.

The repository's `.agent_instructions/testing.md` requires: “STOP after writing the test and ASK FOR APPROVAL” and “DO NOT run tests without approval.” These files remain outside compiled projects pending that review.
