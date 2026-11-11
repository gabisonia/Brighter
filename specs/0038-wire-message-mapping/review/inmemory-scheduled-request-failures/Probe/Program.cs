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

using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Paramore.Brighter.Logging;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using Paramore.Brighter.SchedulerRequestFailureProbe.TestDoubles;
using Polly.Registry;

namespace Paramore.Brighter.SchedulerRequestFailureProbe;

/// <summary>Runs a scheduled request failure inside an independent process.</summary>
public static class Program
{
    /// <summary>Exercises callback containment and subsequent dispatch.</summary>
    /// <param name="args">Async selection, publish selection and scenario token.</param>
    /// <returns>Zero for success, one for a scenario assertion or setup failure.</returns>
    public static async Task<int> Main(string[] args)
    {
        try
        {
            Require(args.Length == 3, "Expected async, publish and scenario token arguments.");
            var useAsync = bool.Parse(args[0]);
            var publish = bool.Parse(args[1]);
            if (useAsync && publish)
                await RunAsync<AsyncScheduledFailureEvent>(useAsync, publish, args[2]);
            else if (useAsync)
                await RunAsync<AsyncScheduledFailureCommand>(useAsync, publish, args[2]);
            else if (publish)
                await RunAsync<SyncScheduledFailureEvent>(useAsync, publish, args[2]);
            else
                await RunAsync<SyncScheduledFailureCommand>(useAsync, publish, args[2]);
            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine(error);
            return 1;
        }
    }

    private static async Task RunAsync<TRequest>(bool useAsync, bool publish, string token)
        where TRequest : class, IFailureProbeRequest, new()
    {
        //Arrange
        using var logs = new SchedulerFailureLogs();
        using var logging = LoggerFactory.Create(builder => builder.AddProvider(logs));
        ApplicationLogging.LoggerFactory = logging;
        InitializeLoggers<TRequest>();
        InitializeLoggers<FireSchedulerRequest>();
        var observations = new RequestFailureObservations();
        var time = new ObservedSystemTimeProvider();
        var jobId = "request-failure-job-" + token;
        var schedulerFactory = new InMemorySchedulerFactory
        {
            TimeProvider = time, GetOrCreateRequestSchedulerId = _ => jobId, OnConflict = OnSchedulerConflict.Throw
        };
        var subscribers = new SubscriberRegistry();
        subscribers.RegisterAsync<FireSchedulerRequest, FireSchedulerRequestHandler>();
        subscribers.Register<SyncScheduledFailureCommand, SyncScheduledFailureCommandHandler>();
        subscribers.Register<SyncScheduledFailureEvent, SyncScheduledFailureEventHandler>();
        subscribers.RegisterAsync<AsyncScheduledFailureCommand, AsyncScheduledFailureCommandHandler>();
        subscribers.RegisterAsync<AsyncScheduledFailureEvent, AsyncScheduledFailureEventHandler>();
        CommandProcessor? processor = null;
        var handlers = new SimpleHandlerFactory(
            type => type == typeof(SyncScheduledFailureCommandHandler)
                ? new SyncScheduledFailureCommandHandler(observations)
                : type == typeof(SyncScheduledFailureEventHandler)
                    ? new SyncScheduledFailureEventHandler(observations)
                    : throw new InvalidOperationException("Unexpected sync handler: " + type),
            type => type == typeof(FireSchedulerRequestHandler)
                ? new FireSchedulerRequestHandler(processor!)
                : type == typeof(AsyncScheduledFailureCommandHandler)
                    ? new AsyncScheduledFailureCommandHandler(observations)
                    : type == typeof(AsyncScheduledFailureEventHandler)
                        ? new AsyncScheduledFailureEventHandler(observations)
                        : throw new InvalidOperationException("Unexpected async handler: " + type));
        processor = new CommandProcessor(subscribers, handlers, new InMemoryRequestContextFactory(),
            new PolicyRegistry(), new ResiliencePipelineRegistry<string>(), schedulerFactory);
        await using var scheduler = (InMemoryScheduler)schedulerFactory.CreateSync(processor);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var failed = new TRequest { ShouldFail = true, Value = "failed-π-" + token };

        //Act
        var acceptedId = await ScheduleAsync(processor, failed, useAsync, publish, TimeSpan.FromSeconds(5), timeout.Token);

        //Assert
        Require(acceptedId == jobId && observations.Calls.IsEmpty,
            "A positive-delay request must be accepted before its handler runs.");
        Console.WriteLine("PROBE_ACCEPTED:" + token);
        await time.WaitForNextCallbackAsync(timeout.Token);
        var failures = logs.Entries.Where(entry => entry.Level >= LogLevel.Error).ToArray();
        Require(failures.Length == 1 && IsExpectedError(failures[0].Error, observations.Failure, publish)
            && failures[0].Text.Contains(jobId, StringComparison.Ordinal),
            "Failed request callback must log its original dispatch exception and scheduler ID once.");
        var first = observations.Calls.ToArray();
        Require(first.Length == 1 && first[0].Id == failed.Id && first[0].Value == failed.Value && first[0].ShouldFail,
            "The failed callback must invoke its application handler once with the original request data.");

        //Act
        Exception? directError = null;
        try
        {
            await DispatchAsync(processor, failed, useAsync, publish, timeout.Token);
        }
        catch (Exception error) { directError = error; }

        //Assert
        Require(IsExpectedError(directError, observations.Failure, publish),
            "Direct dispatch must still expose the original application failure to its caller.");

        //Arrange
        var healthy = new TRequest { ShouldFail = false, Value = "healthy-λ-" + token };
        Exception? reuseError = null;
        string? reusedId = null;

        //Act
        try
        {
            reusedId = await ScheduleAsync(processor, healthy, useAsync, publish, TimeSpan.FromSeconds(1), timeout.Token);
        }
        catch (Exception error) { reuseError = error; }

        //Assert
        Require(reuseError is null && reusedId == jobId,
            "A completed failed request must release its scheduler ID.");
        await time.WaitForNextCallbackAsync(timeout.Token);
        var calls = observations.Calls.ToArray();
        Require(calls.Length == 3 && calls[1].Id == failed.Id && calls[1].Value == failed.Value && calls[1].ShouldFail
            && calls[2].Id == healthy.Id && calls[2].Value == healthy.Value && !calls[2].ShouldFail,
            "The same scheduler must dispatch later valid request data once without replaying the failed request.");
        Require(logs.Entries.Count(entry => entry.Level >= LogLevel.Error) == 1,
            "Direct failure and healthy work must not add scheduler callback failure logs.");
        Require(!scheduler.ReScheduler(jobId, TimeSpan.FromSeconds(5)),
            "A completed healthy request must also release its scheduler entry.");
        Console.WriteLine("PROBE_RECOVERED:" + token);
    }

    private static Task<string> ScheduleAsync<TRequest>(CommandProcessor processor, TRequest request,
        bool useAsync, bool publish, TimeSpan delay, CancellationToken cancellationToken)
        where TRequest : class, IRequest
    {
        if (useAsync)
            return publish
                ? processor.PublishAsync(delay, request, cancellationToken: cancellationToken)
                : processor.SendAsync(delay, request, cancellationToken: cancellationToken);
        return Task.FromResult(publish ? processor.Publish(delay, request) : processor.Send(delay, request));
    }

    private static async Task DispatchAsync<TRequest>(CommandProcessor processor, TRequest request,
        bool useAsync, bool publish, CancellationToken cancellationToken) where TRequest : class, IRequest
    {
        if (useAsync && publish)
            await processor.PublishAsync(request, cancellationToken: cancellationToken);
        else if (useAsync)
            await processor.SendAsync(request, cancellationToken: cancellationToken);
        else if (publish)
            processor.Publish(request);
        else
            processor.Send(request);
    }

    private static bool IsExpectedError(Exception? actual, Exception original, bool publish)
        => publish
            ? actual is AggregateException aggregate && aggregate.Flatten().InnerExceptions.Count == 1
                && ReferenceEquals(aggregate.Flatten().InnerExceptions[0], original)
            : ReferenceEquals(actual, original);

    private static void InitializeLoggers<TRequest>() where TRequest : class, IRequest
    {
        RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<TRequest>).TypeHandle);
        RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<TRequest>).TypeHandle);
        RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<TRequest>).TypeHandle);
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
            throw new InvalidOperationException(message);
    }
}
