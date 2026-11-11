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

using Quartz;
using Quartz.Listener;

namespace SchedulerPersistenceProbe;

/// <summary>Observes completion from the native scheduler after its restored job executes.</summary>
internal sealed class ProbeCompletionListener : JobListenerSupport
{
    private readonly TaskCompletionSource<JobExecutionException?> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    /// <inheritdoc />
    public override string Name => nameof(ProbeCompletionListener);
    /// <summary>Gets completion and any actual scheduler job failure.</summary>
    public Task<JobExecutionException?> Completion => _completion.Task;
    /// <inheritdoc />
    public override Task JobWasExecuted(IJobExecutionContext context, JobExecutionException? jobException,
        CancellationToken cancellationToken = default)
    {
        _completion.TrySetResult(jobException);
        return Task.CompletedTask;
    }
}
