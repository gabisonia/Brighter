# MQTT scheduled retry publication selection

Validated and integrated locally under review-after for #4170. No commits or pushes.
Four native cases per framework reproduce wrong callback wire formats before the
fix and pass afterwards. Omitting the alias stamp produces four intended format
failures; the mutation is restored exactly. Real InMemoryScheduler callbacks,
typed publication lookup, outbox and native MQTT delivery preserve the selected
custom/default writer, logical destination, identity, body and fresh dispatch span.
The original caller message remains unchanged; subscription changes after consumer
creation do not replace the captured selection.

Full MQTT suites pass 121 cases/four existing skips on .NET 9 and .NET 10, retaining
all 121 preceding identities/outcomes plus the four new cases. All supported library
targets build and source hashes guard local integration. Setup failures (legacy
registry type selection, overlapping runner restart and shared-build metadata output
collision) are retained separately and are not behavioral RED evidence.

This includes the preceding eighteen consumer retry/rejection cases. Protocol ACK
remains SDK-owned; rejection forwarding failure still logs and returns true.
Evidence: `/private/tmp/brighter-4170-mqtt-scheduled-selection/`.

Main-workspace locked restore/rebuild and the combined 22 native acceptance cases
pass on both frameworks with no host-clock discontinuity.
