# SDK trace for the native session-renewal control

The repository pins Azure.Messaging.ServiceBus 7.21.0. Its installed NuGet metadata
identifies commit `4e4c19469fe598b9f28a73d065514106985e7560`. The .NET 9 test output SDK
DLL and the installed net8.0 package DLL have identical SHA-256 hashes:
`8b43506eea82c852639e81754d1553dcd29816e8ee9a1f208e0cad1f82f0a8b7`.
Source was read at that commit, not inferred from the current main branch.

## What the source establishes

1. [AmqpReceiver.OnReceiverLinkClosed](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpReceiver.cs#L1688)
   records the link error, converting it to a session-lock-loss exception as needed.
   When the session link is closed, it starts CloseAsync to release resources.
2. [AmqpReceiver.CloseAsync](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpReceiver.cs#L1641)
   sets the transport closed flag before awaiting link cleanup.
3. [ServiceBusReceiver.CompleteMessageAsync](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Receiver/ServiceBusReceiver.cs#L568)
   checks IsDisposed before calling the transport completion operation. IsDisposed
   reads the transport closed flag. Therefore a call after that cleanup begins can
   throw ObjectDisposedException before reaching the stored session-lock-loss error.
4. [ServiceBusSessionReceiver.SessionLockedUntil](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Receiver/ServiceBusSessionReceiver.cs#L51)
   exposes the transport expiry. [AmqpReceiver](https://github.com/Azure/azure-sdk-for-net/blob/4e4c19469fe598b9f28a73d065514106985e7560/sdk/servicebus/Azure.Messaging.ServiceBus/src/Amqp/AmqpReceiver.cs#L1324)
   initializes expiry from link properties and updates it after a successful native
   renewal response.

## What remains inference

This is a concrete SDK path consistent with both recorded mutation failures. It does
not show the reason for the broker's link closure in those executions: the preserved
TRXs contain the resulting disposed-receiver exception, not the causal link event.
Session expiry is plausible under the disabled-renewal control but is not established
solely by that exception. No Brighter implementation change is justified by this trace.

The [proposed observation amendment](review/asb-native-session-expiry-observation/README.md)
requires the real receiver's expiry to advance, independently of the exception that
would occur after loss of the session. It retains successful completion/removal and
all mapping checks. This provides a stronger direct renewal assertion without claiming
to reproduce the earlier in-memory instability or prove hosted broker compatibility.

No tests ran during this source investigation. Baseline remains 620 cases per framework.
Downloaded source evidence is in `/private/tmp/asb-7.21-ServiceBusReceiver.cs`,
`/private/tmp/asb-7.21-ServiceBusSessionReceiver.cs` and `/private/tmp/asb-7.21-AmqpReceiver.cs`.

## Validated follow-up

The [expiry-observation amendment](review/asb-native-session-expiry-observation/README.md)
is now validated on native emulator queue/topic paths. Disabling renewal fails the
expiry-advance assertion in both cases, independent of settlement exception type.
Both expanded suites pass 622 cases. The historical link-closure cause remains an
inference; the trace is not a reproduction of that event.
