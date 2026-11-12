#region License
/* The MIT License (MIT)
Copyright © 2025 Aboubakr Nasef <aboubakrnasef@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System.Text.Json;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.Scheduler.Events;
using TickerQ.Utilities.Base;

namespace Paramore.Brighter.MessageScheduler.TickerQ
{
    /// <summary>
    /// The TickerQ Message scheduler Job
    /// </summary>
    /// <param name="processor"></param>
    public class BrighterTickerQSchedulerJob(IAmACommandProcessor processor)
    {
        /// <summary>Dispatches a scheduled message no earlier than its native scheduled time.</summary>
        /// <param name="tickerContext">The native job context containing the serialized message and scheduled time.</param>
        /// <param name="cancellationToken">Cancels waiting for the scheduled time.</param>
        /// <returns>A task representing the wait and message dispatch.</returns>
        [TickerFunction(nameof(FireSchedulerMessageAsync))]
        public async Task FireSchedulerMessageAsync(TickerFunctionContext<string> tickerContext, CancellationToken cancellationToken)
        {
            var schedulerRequest = JsonSerializer.Deserialize<FireSchedulerMessage>(tickerContext.Request, JsonSerialisationOptions.Options)!;
            await WaitUntilScheduledAsync(tickerContext.ScheduledFor, cancellationToken);
            await processor.SendAsync(schedulerRequest);
        }

        /// <summary>Dispatches a scheduled request no earlier than its native scheduled time.</summary>
        /// <param name="tickerContext">The native job context containing the serialized request and scheduled time.</param>
        /// <param name="cancellationToken">Cancels waiting for the scheduled time.</param>
        /// <returns>A task representing the wait and request dispatch.</returns>
        [TickerFunction(nameof(FireSchedulerRequestAsync))]
        public async Task FireSchedulerRequestAsync(TickerFunctionContext<string> tickerContext, CancellationToken cancellationToken)
        {
            var schedulerRequest = JsonSerializer.Deserialize<FireSchedulerRequest>(tickerContext.Request, JsonSerialisationOptions.Options)!;
            await WaitUntilScheduledAsync(tickerContext.ScheduledFor, cancellationToken);
            await processor.SendAsync(schedulerRequest);
        }
        private static async Task WaitUntilScheduledAsync(DateTime scheduledFor, CancellationToken cancellationToken)
        {
            var remaining = scheduledFor - DateTime.UtcNow;
            while (remaining > TimeSpan.Zero)
            {
                await Task.Delay(remaining, cancellationToken);
                remaining = scheduledFor - DateTime.UtcNow;
            }
        }
    }
}
