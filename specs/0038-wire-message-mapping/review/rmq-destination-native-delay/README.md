# RabbitMQ explicit destination with native delay

Review-after for #4170. Validation is complete and the guarded source/test integration is local; no commits
or pushes. The preceding scheduled-retry package is already integrated locally.

Two sync/async send cases for each client target one native queue while a sibling
shares the ordinary topic binding. With the new DestinationQueue option, both clients
reproduce immediate delivery despite a positive native delay on both frameworks:
eight intended timing failures. The fix uses the existing queue-specific delayed
exchange and provisions its destination binding in Create mode. Zero-delay queue
routing and null/default publication routing retain their prior paths.

The native probes require the requested delay, selected native destination, original
identity, confirmation, wire payload and negative broker delay header; the sibling
must remain empty. The original source message retains its logical topic. The owned
fixtures use the repository's native-delay plugin and existing package versions.

Initial focused passes and the .NET 9 full pass are retained. Overnight regression
runs span host suspension; their timestamps and failures are retained, not certified.
The rerun detects wall/monotonic divergence explicitly. A later all-green run also
hit that guard and was excluded. Final validation used a task-scoped keep-awake
process and separate owned brokers for the Async and Sync matrices. Both complete matrices pass without host-clock discontinuity: 562 passes/two
existing skips for Async and 295 passes/one existing skip for Sync on each framework,
preserving all preceding identities/outcomes. All four library targets build for
each client. Original main-workspace source bytes matched before integration.

Evidence: `/private/tmp/brighter-4170-rmq-destination-native-delay/`.
This package does not close unrelated scheduler, recovery or whole-release gates.
