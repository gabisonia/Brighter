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

using System.Collections.Specialized;
using System.Text.Json;
using System.Transactions;
using Paramore.Brighter;
using Paramore.Brighter.Extensions;
using Paramore.Brighter.MessageMappers;
using Paramore.Brighter.MessageScheduler.Quartz;
using Paramore.Brighter.Observability;
using Paramore.Brighter.Scheduler.Events;
using Paramore.Brighter.Scheduler.Handlers;
using ParamoreBrighter.Quartz.Tests.TestDoubles;
using Polly.Registry;
using Quartz;
using Quartz.Impl.Matchers;

namespace ParamoreBrighter.Quartz.Tests;

[Collection("Scheduler")]
public class QuartzJobCompletionTests : IAsyncLifetime
{
    private readonly InternalBus _bus = new();
    private readonly RoutingKey _topic = new($"completion-{Guid.NewGuid():N}");
    private readonly QuartzCompletionListener _listener = new();
    private IScheduler _quartz = null!;
    private QuartzSchedulerFactory _schedulerFactory = null!;
    private IAmACommandProcessor _processor = null!;
    private MessageMapperRegistry _mappers = null!;
    private OutboxProducerMediator<Message, CommittableTransaction> _mediator = null!;

    public async Task InitializeAsync()
    {
        //Arrange
        _quartz = await SchedulerBuilder.Create(new NameValueCollection
            { ["quartz.scheduler.instanceName"] = $"completion-{Guid.NewGuid():N}" })
            .Build().GetScheduler();
        _schedulerFactory = new QuartzSchedulerFactory(_quartz);
        var publications = new[]
        {
            new Publication { Topic = _topic, RequestType = typeof(QuartzCompletionEvent) }
        };
        var producers = new InMemoryProducerRegistryFactory(_bus, publications, InstrumentationOptions.None).Create();
        _mappers = new MessageMapperRegistry(
            new SimpleMessageMapperFactory(_ => new CloudEventJsonMessageMapper<QuartzCompletionEvent>()),
            new SimpleMessageMapperFactoryAsync(_ => new CloudEventJsonMessageMapper<QuartzCompletionEvent>()));
        _mappers.Register<QuartzCompletionEvent, CloudEventJsonMessageMapper<QuartzCompletionEvent>>();
        _mappers.RegisterAsync<QuartzCompletionEvent, CloudEventJsonMessageMapper<QuartzCompletionEvent>>();
        _mediator = new OutboxProducerMediator<Message, CommittableTransaction>(producers,
            new ResiliencePipelineRegistry<string>().AddBrighterDefault(), _mappers,
            new EmptyMessageTransformerFactory(), new EmptyMessageTransformerFactoryAsync(), null,
            new FindPublicationByPublicationTopicOrRequestType());
        var subscribers = new SubscriberRegistry();
        subscribers.RegisterAsync<FireSchedulerMessage, FireSchedulerMessageHandler>();
        subscribers.RegisterAsync<FireSchedulerRequest, FireSchedulerRequestHandler>();
        _processor = new CommandProcessor(subscribers,
            new SimpleHandlerFactoryAsync(type => type == typeof(FireSchedulerMessageHandler)
                ? new FireSchedulerMessageHandler(_processor)
                : new FireSchedulerRequestHandler(_processor)),
            new InMemoryRequestContextFactory(), new PolicyRegistry(), new ResiliencePipelineRegistry<string>(),
            _mediator, _schedulerFactory);
        _quartz.JobFactory = new QuartzCompletionJobFactory(_processor);
        _quartz.ListenerManager.AddJobListener(_listener, GroupMatcher<JobKey>.AnyGroup());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_quartz_dispatch_succeeds_should_complete_without_a_job_error(bool useRequest, bool useAsync)
    {
        //Arrange
        var request = new QuartzCompletionEvent { Value = "scheduled-completion" };
        var source = new Message(new MessageHeader(request.Id, _topic, MessageType.MT_EVENT),
            new MessageBody("scheduled-message-body"));
        var delay = TimeSpan.FromSeconds(1);

        //Act
        if (useRequest)
        {
            if (useAsync)
                await _schedulerFactory.CreateAsync(_processor).ScheduleAsync(request, RequestSchedulerType.Post, delay);
            else
                _schedulerFactory.CreateSync(_processor).Schedule(request, RequestSchedulerType.Post, delay);
        }
        else
        {
            var scheduler = _schedulerFactory.Create(_processor);
            if (useAsync)
                await ((IAmAMessageSchedulerAsync)scheduler).ScheduleAsync(source, delay);
            else
                ((IAmAMessageSchedulerSync)scheduler).Schedule(source, delay);
        }
        await _quartz.Start();
        var jobError = await _listener.Completion.WaitAsync(TimeSpan.FromSeconds(15));
        await _quartz.Shutdown(waitForJobsToComplete: true);

        //Assert
        var delivered = Assert.Single(_bus.Stream(_topic));
        Assert.Equal(request.Id, delivered.Id);
        if (useRequest)
        {
            using var json = JsonDocument.Parse(delivered.Body.Value);
            Assert.Equal(request.Value, json.RootElement.GetProperty("data").GetProperty("value").GetString());
        }
        else
        {
            Assert.Equal(source.Body.Value, delivered.Body.Value);
        }
        Assert.Null(jobError);
    }

    [Fact]
    public async Task When_quartz_job_has_no_payload_should_report_a_job_error()
    {
        //Arrange
        var job = JobBuilder.Create<QuartzBrighterJob>().WithIdentity($"missing-{Guid.NewGuid():N}").Build();
        var trigger = TriggerBuilder.Create().ForJob(job).StartNow().Build();

        //Act
        await _quartz.ScheduleJob(job, trigger);
        await _quartz.Start();
        var jobError = await _listener.Completion.WaitAsync(TimeSpan.FromSeconds(15));
        await _quartz.Shutdown(waitForJobsToComplete: true);

        //Assert
        Assert.Empty(_bus.Stream(_topic));
        Assert.NotNull(jobError);
        var cause = Assert.IsType<InvalidOperationException>(jobError.GetBaseException());
        Assert.Equal("No message or request found in the job, something is wrong with this job scheduler", cause.Message);
    }

    public async Task DisposeAsync()
    {
        if (_quartz is { IsShutdown: false })
            await _quartz.Shutdown(waitForJobsToComplete: true);
        _mediator?.Dispose();
        _mappers?.Dispose();
    }
}
