#region Licence

/* The MIT License (MIT)
Copyright © 2026 gabisonia

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using Paramore.Brighter.Gcp.Tests.MessagingGateway;

if (args.Length != 2 || args[0] != "--project" || args[1] != "brighter-pubsub-validation")
    throw new InvalidOperationException("Explicit --project brighter-pubsub-validation is required.");
if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")))
    throw new InvalidOperationException("Remove PUBSUB_EMULATOR_HOST before hosted execution.");

Console.WriteLine($"HOSTED BUFFERED SHUTDOWN PROJECT {args[1]} PROCESSORS {Environment.ProcessorCount}");
foreach (var useAsync in new[] { false, true })
foreach (var nativeRecovery in new[] { false, true })
{
    Console.WriteLine($"CASE START async={useAsync} buffered=True clients=1 nativeRecovery={nativeRecovery}");
    try
    {
        var observation = new GcpHostedBufferedShutdownObservationTests(new HostedTraceOutput());
        await observation.When_stopping_a_gcp_stream_should_release_the_observed_native_delivery(
            useAsync, true, nativeRecovery);
        Console.WriteLine($"RESULT async={useAsync} buffered=True clients=1 nativeRecovery={nativeRecovery} passed=True");
    }
    catch (Exception exception)
    {
        Console.WriteLine($"RESULT async={useAsync} buffered=True clients=1 nativeRecovery={nativeRecovery} passed=False error={exception}");
        throw;
    }
}
