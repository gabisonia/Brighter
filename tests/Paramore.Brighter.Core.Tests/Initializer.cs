using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Paramore.Brighter.Core.Tests.CommandProcessors.TestDoubles;
using Paramore.Brighter.Core.Tests.MessageDispatch.TestDoubles;
using Paramore.Brighter.Core.Tests.Sweeping.TestDoubles;
using Paramore.Brighter.Logging;
using Serilog;
using Paramore.Brighter.SchedulerRequestFailureProbe.TestDoubles;

namespace Paramore.Brighter.Core.Tests
{
    sealed class Initializer
    {
        [ModuleInitializer]
        public static void InitializeTestLogger()
        {
            var logger = new LoggerConfiguration().MinimumLevel.Debug().WriteTo.TestCorrelator().WriteTo.Sink(CapturingLogSink.Instance).CreateLogger();
            ApplicationLogging.LoggerFactory = new LoggerFactory().AddSerilog(logger);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<SyncScheduledFailureCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<SyncScheduledFailureCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<SyncScheduledFailureCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<SyncScheduledFailureEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<SyncScheduledFailureEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<SyncScheduledFailureEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<AsyncScheduledFailureCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<AsyncScheduledFailureCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<AsyncScheduledFailureCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<AsyncScheduledFailureEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<AsyncScheduledFailureEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<AsyncScheduledFailureEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<ResilienceContextProbe>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<ResilienceContextProbe>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<ResilienceContextProbe>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<RequeueFailureCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<RequeueFailureCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<RequeueFailureCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<RequeueFailureEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<RequeueFailureEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<RequeueFailureEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<UnhandledReceivedCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<UnhandledReceivedCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<UnhandledReceivedCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<UnhandledReceivedEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<UnhandledReceivedEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<UnhandledReceivedEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<UnhandledNestedEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<UnhandledNestedEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<UnhandledNestedEvent>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<ClaimCheckDeliveryCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<ClaimCheckDeliveryCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<ClaimCheckDeliveryCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandler<RetainedClaimCheckDeliveryCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(RequestHandlerAsync<RetainedClaimCheckDeliveryCommand>).TypeHandle);
            RuntimeHelpers.RunClassConstructor(typeof(PipelineBuilder<RetainedClaimCheckDeliveryCommand>).TypeHandle);
        }
    }
}
