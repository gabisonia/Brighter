#region Licence

/* The MIT License (MIT)
Copyright © 2026 Irakli Gabisonia

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

using Azure.Messaging.ServiceBus;
using System.Diagnostics;
using System.Text.Json;

var connection = Environment.GetEnvironmentVariable("BrighterTestsASBConnectionString")
    ?? throw new InvalidOperationException("Local emulator connection missing.");
var results = new List<object>();
var failures = 0;
foreach (var queue in new[] { false, true })
{
    for (var iteration = 0; iteration < 4; iteration++)
    {
        var clock = Stopwatch.StartNew();
        var phase = "setup";
        var id = Guid.NewGuid().ToString();
        var body = new byte[] { 0, 255, 128, 65 };
        string? observedId = null;
        long? sequence = null;
        long? deadSequence = null;
        string? error = null;
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await using var client = new ServiceBusClient(connection);
            await using var sender = client.CreateSender(queue ? "wire-mapping-queue" : "wire-mapping-topic");
            var deadOptions = new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter, ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete };
            await using var deadLetters = queue ? client.CreateReceiver("wire-mapping-queue", deadOptions) : client.CreateReceiver("wire-mapping-topic", "wire-observer", deadOptions);
            await using var observer = queue ? client.CreateReceiver("wire-mapping-queue") : client.CreateReceiver("wire-mapping-topic", "wire-observer");
            await using var receiver = queue ? client.CreateReceiver("wire-mapping-queue") : client.CreateReceiver("wire-mapping-topic", "wire-observer");
            var sent = new ServiceBusMessage(new BinaryData(body)) { MessageId = id, Subject = "native-failed-subject", CorrelationId = "native-failed-correlation", ContentType = "application/octet-stream" };
            sent.ApplicationProperties["reader-failure"] = "deliberate";
            sent.ApplicationProperties["source-only"] = "unchanged";
            phase = "send";
            await sender.SendMessageAsync(sent, timeout.Token);
            phase = "receive";
            var received = (await receiver.ReceiveMessagesAsync(1, TimeSpan.FromSeconds(10), timeout.Token)).Single();
            sequence = received.SequenceNumber;
            if (received.MessageId != id || !received.Body.ToArray().SequenceEqual(body)) throw new Exception("Original native identity/body mismatch.");
            phase = "dead-letter";
            await receiver.DeadLetterMessageAsync(received, "Unacceptable", "wire mapping failed", timeout.Token);
            phase = "receive-DLQ";
            var dead = await deadLetters.ReceiveMessageAsync(TimeSpan.FromSeconds(10), timeout.Token);
            observedId = dead?.MessageId;
            deadSequence = dead?.SequenceNumber;
            if (dead is null) throw new Exception("Native DLQ receive returned null after successful dead-letter settlement.");
            if (dead.MessageId != id) throw new Exception("Native DLQ identity mismatch.");
            if (!dead.Body.ToArray().SequenceEqual(body) || dead.Subject != sent.Subject || dead.CorrelationId != sent.CorrelationId || dead.ContentType != sent.ContentType) throw new Exception("Native DLQ payload/metadata mismatch.");
            if (dead.DeadLetterReason != "Unacceptable" || dead.DeadLetterErrorDescription != "wire mapping failed" || (string)dead.ApplicationProperties["source-only"] != "unchanged" || (string)dead.ApplicationProperties["reader-failure"] != "deliberate") throw new Exception("Native DLQ rejection metadata mismatch.");
            var healthyId = Guid.NewGuid().ToString();
            var healthyBody = new byte[] { 128, 0, 255, 66 };
            phase = "healthy-send";
            await sender.SendMessageAsync(new ServiceBusMessage(new BinaryData(healthyBody)) { MessageId = healthyId }, timeout.Token);
            phase = "healthy-receive";
            var healthy = (await receiver.ReceiveMessagesAsync(1, TimeSpan.FromSeconds(10), timeout.Token)).Single();
            if (healthy.MessageId != healthyId || !healthy.Body.ToArray().SequenceEqual(healthyBody)) throw new Exception("Healthy native delivery mismatch.");
            phase = "healthy-complete";
            await receiver.CompleteMessageAsync(healthy, timeout.Token);
            phase = "healthy-peek";
            ServiceBusReceivedMessage? remaining = null;
            for (var peek = 0; peek < 20; peek++)
            {
                remaining = await observer.PeekMessageAsync(healthy.SequenceNumber, timeout.Token);
                if (remaining is null) break;
                await Task.Delay(100, timeout.Token);
            }
            if (remaining is not null) throw new Exception("Completed healthy delivery remains visible.");
            phase = "complete";
        }
        catch (Exception ex)
        {
            failures++;
            error = ex.GetType().Name + ": " + ex.Message;
        }
        var record = new { queue, iteration, phase, expectedId = id, observedId, sequence, deadSequence, error, elapsedMs = clock.ElapsedMilliseconds };
        results.Add(record);
        Console.WriteLine(JsonSerializer.Serialize(record));
    }
}
File.WriteAllText(args[0], JsonSerializer.Serialize(new { framework = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription, sdk = typeof(ServiceBusClient).Assembly.GetName().Version?.ToString(), failures, results }, new JsonSerializerOptions { WriteIndented = true }));
return failures == 0 ? 0 : 1;
