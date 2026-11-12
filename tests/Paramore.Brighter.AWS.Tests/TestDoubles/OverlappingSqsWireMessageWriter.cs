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

#nullable enable

using System;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.MessagingGateway.AWSSQS;

namespace Paramore.Brighter.AWS.Tests.TestDoubles;

internal sealed class OverlappingSqsWireMessageWriter(
    IAmAWireMessageWriter<SqsWireMessage> writer) : IAmAWireMessageWriter<SqsWireMessage>, IDisposable
{
    private readonly TaskCompletionSource _firstMapped = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ManualResetEventSlim _releaseFirst = new(false);
    private int _callCount;

    public Task FirstMapped => _firstMapped.Task;

    public SqsWireMessage WriteMessage(Message message)
    {
        var wireMessage = writer.WriteMessage(message);
        if (Interlocked.Increment(ref _callCount) == 1)
        {
            _firstMapped.TrySetResult();
            if (!_releaseFirst.Wait(TimeSpan.FromSeconds(15)))
                throw new TimeoutException("The second mapping did not release the first send.");
        }
        else
        {
            _releaseFirst.Set();
        }

        return wireMessage;
    }

    public void ReleaseFirstMapping() => _releaseFirst.Set();

    public void Dispose() => _releaseFirst.Dispose();
}
