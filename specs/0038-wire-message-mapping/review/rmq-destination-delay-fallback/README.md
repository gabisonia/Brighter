# RabbitMQ destination fallback without a delay provider

Validated and integrated locally under review-after for #4170. No commits or pushes.
When DestinationQueue is set and a positive delay is requested without native delay
support or a scheduler, both clients now preserve the existing immediate-send
fallback. The publisher chooses the private delayed exchange only when SupportDelay
is enabled. Native delayed routing and default-publication behavior remain covered.

Eight queue-targeted cases failed before correction across Async/Sync and .NET 9/10;
eight default-publication controls passed. Async received native 404 NOT_FOUND;
Sync confirmation failed with the same missing exchange in the native broker log.
A separate initial broker-starting connection failure is retained as setup evidence.

Full regression results on each framework: Async 566 passes/two existing skips;
Sync 299 passes/one existing skip. All preceding identities/outcomes remain; all
four library targets build. Suspended .NET 10 attempts are retained separately and
excluded; continuous reruns passed. Main-workspace locked rebuild and combined
native selections pass ten Async and eight Sync cases per framework. Source hashes
were checked before integration. This closes the fallback defect only.

Evidence: `/private/tmp/brighter-4170-rmq-destination-delay-fallback/` and
`/private/tmp/brighter-4170-validated-integration/main-verification.json`.
