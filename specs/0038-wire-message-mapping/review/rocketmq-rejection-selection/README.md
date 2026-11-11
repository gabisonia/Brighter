# RocketMQ rejection writer selection

Review-after for #4170. Twelve intended native RED failures are verified on each framework; all GREEN and full regression checks pass.
No commits or pushes. The suite covers sync/async consumers, selected dead-letter
and invalid forwarding, writer throw/null and recovery with subsequent messages.

Each case uses a unique native tag, independently observed source/target routes,
and the pinned broker/SDK fixture from initial integration. Configured writer
selection must be captured before lazy forwarding producer creation. Native data
must retain identity, body, tenant, rejection metadata and trace fields. Existing
source acknowledgement after a forwarding failure is retained: recovery is with a
fresh subsequent message, not recovery of an acknowledged original.

Initial scaffolding added properties only. Each framework produced eight intended
nonempty-destination failures and four native body mismatches. A first observation
attempt omitted the forwarding tag after receipt; it was stopped and retained as
setup evidence, not RED. Tests now explicitly set that native forwarding control
and independently observe both destinations. Full suites preserve all prior 118 identities and add twelve passing cases per
framework: 128 passes and two existing skips. All four library targets build.
Three production files and the native test are integrated after source-hash checks.
Main-workspace locked restore/rebuild and all twelve native cases pass on both
frameworks, with no host-clock discontinuity.

Evidence: `/private/tmp/brighter-4170-rocketmq-rejection-selection/`; owned fixture
configuration and logs remain under `/private/tmp/brighter-4170-rocketmq-wire-mapping/`.
