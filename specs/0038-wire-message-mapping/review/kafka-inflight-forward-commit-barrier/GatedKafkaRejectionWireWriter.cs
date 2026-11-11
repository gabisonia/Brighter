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

#nullable enable

using System;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Paramore.Brighter.MessagingGateway.Kafka;

namespace Paramore.Brighter.Kafka.Tests.TestDoubles;

internal sealed class GatedKafkaRejectionWireWriter : IAmAWireMessageWriter<Confluent.Kafka.Message<string, byte[]>>, IDisposable
{
    private int _writeCount;
    public ManualResetEventSlim Entered { get; } = new(false);
    public ManualResetEventSlim Release { get; } = new(false);
    public bool FailFirstWrite { get; set; }
    public int WriteCount => Volatile.Read(ref _writeCount);

    public Confluent.Kafka.Message<string, byte[]> WriteMessage(Message message)
    {
        if (Interlocked.Increment(ref _writeCount) == 1)
        {
            Entered.Set();
            if (!Release.Wait(TimeSpan.FromSeconds(30)))
                throw new TimeoutException("The test did not release the rejection writer.");
            if (FailFirstWrite)
                throw new InvalidOperationException("The first rejection mapping failed.");
        }
        return new Confluent.Kafka.Message<string, byte[]>
        {
            Key = "mapped-" + message.Id.Value,
            Value = Encoding.UTF8.GetBytes("external:" + Convert.ToBase64String(message.Body.Memory.ToArray())),
            Headers = new Headers { { HeaderNames.MESSAGE_ID, Encoding.UTF8.GetBytes("wire-" + message.Id.Value) } }
        };
    }

    public void Dispose()
    {
        Entered.Dispose();
        Release.Dispose();
    }
}
