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

using System.Diagnostics;
using Paramore.Brighter.TickerQ.Tests.TestDoubles;
using Paramore.Brighter.TickerQ.Tests.TestDoubles.Fixtures;

namespace Paramore.Brighter.TickerQ.Tests;

/// <summary>Verifies that native near-due execution does not dispatch Brighter work early.</summary>
[Collection("Scheduler")]
public class TickerQNotBeforeDispatchTests
{
    /// <summary>Both native job types respect a positive delay inside the immediate-dispatch window.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_tickerq_dispatches_near_due_work_should_wait_until_the_scheduled_time(bool useRequest, bool useAsync)
    {
        // Arrange
        BaseTickerQFixture fixture = useRequest
            ? useAsync ? new TickerQRequestAsyncTestFixture() : new TickerQRequestTestFixture()
            : new TickerQMessageTestFixture();
        try
        {
            await Task.Delay(250);
            var request = new MyEvent();
            var message = new Message(new MessageHeader(request.Id, fixture.RoutingKey, MessageType.MT_EVENT),
                new MessageBody("not-before-payload"));
            var delay = TimeSpan.FromMilliseconds(750);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            Assert.Empty(fixture.InternalBus.Stream(fixture.RoutingKey));

            // Act
            var elapsed = Stopwatch.StartNew();
            if (useRequest)
            {
                if (useAsync)
                    await fixture.SchedulerFactory.CreateAsync(fixture.Processor).ScheduleAsync(request, RequestSchedulerType.Post, delay, timeout.Token);
                else
                    fixture.SchedulerFactory.CreateSync(fixture.Processor).Schedule(request, RequestSchedulerType.Post, delay);
            }
            else
            {
                var scheduler = fixture.SchedulerFactory.Create(fixture.Processor);
                if (useAsync)
                    await Assert.IsAssignableFrom<IAmAMessageSchedulerAsync>(scheduler).ScheduleAsync(message, delay, timeout.Token);
                else
                    Assert.IsAssignableFrom<IAmAMessageSchedulerSync>(scheduler).Schedule(message, delay);
            }
            while (!fixture.InternalBus.Stream(fixture.RoutingKey).Any())
                await Task.Delay(5, timeout.Token);
            var observedAfter = elapsed.Elapsed;

            // Assert
            var delivered = Assert.Single(fixture.InternalBus.Stream(fixture.RoutingKey));
            Assert.Equal(request.Id, delivered.Id);
            Assert.True(observedAfter >= delay, $"Dispatched after {observedAfter}, before {delay} elapsed.");
        }
        finally
        {
            await fixture.DisposeAsync();
        }
    }
}
