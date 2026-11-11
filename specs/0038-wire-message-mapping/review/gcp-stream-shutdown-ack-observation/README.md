# Shutdown acknowledgement observation amendment

Status: approved, adopted and validated. All four named mutations reach their
intended assertions. Focused suites pass on both frameworks; full regressions
retain all cases but report one ordered-nack failure per framework.

The approved premature-native-ack mutation causes the held-delivery cases to fail
at the missing-shutdown-nack assertion before checking forbidden acknowledgements.
The native traces show successful unwanted Acknowledge requests. Buffered cases
fail at their bounded redelivery observation as expected. These are genuine failures,
but the held-delivery assertion differs from the approved mutation expectation.

[The amended test](When_stopping_a_gcp_stream_should_release_the_observed_native_delivery.cs)
moves the existing forbidden-ack assertion immediately before the existing
shutdown-nack assertion. No assertion, bound, operation or case is added or removed.
The compiled test now matches this approved review copy exactly.

The approved execution plan was to adopt this ordering and rerun the premature-ack mutation: four held
cases must fail at Assert.DoesNotContain and four buffered cases at their arrival
assertion. Restore production sources and confirm all eight cases on both frameworks.
Recheck the other three approved mutations and full affected Pub/Sub regressions.
No production fix or timeout increase is proposed.

Evidence: `/private/tmp/brighter-4170-gcp-shutdown-observation/`, especially
`mutation-premature-ack.trx` and its native RPC traces. Those original-batch artifacts are preserved separately from the amendment results
recorded below.

Review is required by `.agent_instructions/testing.md`: "STOP after writing the test
and ASK FOR APPROVAL", and the original approved diagnostic README requires review
of unexpected mutation outcomes. This amendment changes which assertion reports the
held-delivery defect; it does not establish a production shutdown defect.

## Approved execution

The amendment changes only the position of the forbidden-Ack assertion. Its four
held premature-Ack cases fail at Assert.DoesNotContain (line 157), and four buffered
cases fail at their bounded arrival assertion (line 136). The other mutations are
rechecked: missing shutdown produces eight missing-nack assertions (line 158),
missing cancellation produces eight disposal timeout assertions (line 156), and
missing consumer Accept produces four acknowledgement timeouts (line 192), with
four native-pull cases passing. All 28 failures reach their intended assertions.
Every mutation is restored and followed by eight passing net10.0 cases. Final
focused suites pass eight cases on each framework. All library targets build.

The full affected Pub/Sub matrix has **379 passes, one failure and 55 skips** on
each framework. All 435 prior case identities are retained, including eight passing
shutdown diagnostics. The net9.0 failure is the ordered async nack case at line 158;
the net10.0 failure is its sync counterpart at line 152. Both assertions check the
nacked message's redelivery. The sync counterpart is newly observed in this batch;
its cause is not established. The existing disposal cases pass this run, which does
not close their previous intermittent failures.

All four library targets build with zero warnings/errors. All 6,472 existing src/tests
C# files are hashed: only the approved assertion ordering changes. Every mutated
production file is byte-restored and the compiled test matches this review copy.
All three batch-owned emulators are removed. No production correction, dependency
change or timeout relaxation is made. This completes the scoped diagnostic amendment;
it does not establish GCP or feature-wide production readiness.
Evidence is preserved separately in
`/private/tmp/brighter-4170-gcp-shutdown-ack-observation/`; the preceding batch's
artifacts are not overwritten.

## Follow-up status

The [factory shutdown diagnostics](../gcp-factory-stream-shutdown/README.md) have since
been approved and validated: eight focused cases per framework, 24 intended mutation
failures and 388 full-suite passes with 55 skips per framework. Those passing runs do
not establish the cause or correction of prior intermittent findings. The
[pull-reader recovery draft](../gcp-pull-reader-failures/README.md) is now pending review.
