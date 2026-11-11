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

using System.Text.Json;
using Amazon.Scheduler.Model;
using Amazon.IdentityManagement.Model;
using Paramore.Brighter.AWSScheduler.V4.Tests.Helpers;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessageScheduler.AWS.V4;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;

namespace Paramore.Brighter.AWSScheduler.V4.Tests;

[Trait("Category", "AWS")]
public class AwsSchedulerDestinationSelectionTests
{
    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task When_scheduling_with_distinct_destinations_should_honor_the_selected_target(
        bool useAsync, bool useSns, bool useNativeTarget)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var clients = new AWSClientFactory(connection);
        using var sqs = clients.CreateSqsClient();
        using var sns = clients.CreateSnsClient();
        using var schedules = clients.CreateSchedulerClient();
        using var iam = clients.CreateIdentityClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var suffix = Guid.NewGuid().ToString("N");
        var businessName = $"Business-{suffix}";
        var callbackName = $"Callback-{suffix}";
        var groupName = $"Mapping-{suffix}";
        var scheduleId = $"Message-{suffix}";
        var roleName = $"Mapping-{suffix}";
        var queues = new List<string>();
        var topics = new List<string>();
        var groupCreated = false;
        var roleCreated = false;

        try
        {
            string businessDestination;
            string callbackDestination;
            if (useSns)
            {
                businessDestination = (await sns.CreateTopicAsync(businessName, timeout.Token)).TopicArn;
                topics.Add(businessDestination);
                callbackDestination = (await sns.CreateTopicAsync(callbackName, timeout.Token)).TopicArn;
                topics.Add(callbackDestination);
            }
            else
            {
                businessDestination = (await sqs.CreateQueueAsync(businessName, timeout.Token)).QueueUrl;
                queues.Add(businessDestination);
                callbackDestination = (await sqs.CreateQueueAsync(callbackName, timeout.Token)).QueueUrl;
                queues.Add(callbackDestination);
            }

            await schedules.CreateScheduleGroupAsync(new CreateScheduleGroupRequest { Name = groupName }, timeout.Token);
            groupCreated = true;
            var role = await iam.CreateRoleAsync(new CreateRoleRequest
            {
                RoleName = roleName,
                AssumeRolePolicyDocument = """
                    {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                    "Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}
                    """
            }, timeout.Token);
            roleCreated = true;
            var factory = new AwsSchedulerFactory(connection, role.Role.Arn)
            {
                MakeRole = OnMissingRole.Assume,
                UseMessageTopicAsTarget = useNativeTarget,
                SchedulerTopicOrQueue = new RoutingKey(callbackName),
                Group = new SchedulerGroup { Name = groupName, MakeSchedulerGroup = OnMissingSchedulerGroup.Assume },
                GetOrCreateMessageSchedulerId = _ => scheduleId
            };
            var scheduler = factory.Create(null!);
            var message = new Message(new MessageHeader(new Id("business-message"), new RoutingKey(businessName),
                MessageType.MT_EVENT, correlationId: new Id("business-correlation"),
                traceParent: new TraceParent("00-11111111111111111111111111111111-2222222222222222-01"),
                traceState: new TraceState("vendor=deposit")), new MessageBody("business payload"));
            message.Header.Baggage.Add("stage", "deposit");
            message.Header.Bag["tenant"] = "tenant-a";
            var before = JsonSerializer.Serialize(message, JsonSerialisationOptions.Options);
            var at = DateTimeOffset.UtcNow.AddHours(1);

            //Act
            var id = useAsync
                ? await ((IAmAMessageSchedulerAsync)scheduler).ScheduleAsync(message, at, timeout.Token)
                : ((IAmAMessageSchedulerSync)scheduler).Schedule(message, at);
            var stored = await schedules.GetScheduleAsync(new GetScheduleRequest
            {
                Name = id, GroupName = groupName
            }, timeout.Token);

            //Assert
            Assert.Equal(scheduleId, id);
            Assert.Equal(useSns ? "arn:aws:scheduler:::aws-sdk:sns:publish"
                : "arn:aws:scheduler:::aws-sdk:sqs:sendMessage", stored.Target.Arn);
            using var input = JsonDocument.Parse(stored.Target.Input);
            Assert.Equal(useNativeTarget ? businessDestination : callbackDestination,
                input.RootElement.GetProperty(useSns ? "TopicArn" : "QueueUrl").GetString());
            var body = input.RootElement.GetProperty(useSns ? "Message" : "MessageBody").GetString();
            if (useNativeTarget)
            {
                Assert.Equal("business payload", body);
                Assert.Equal("business-message", input.RootElement.GetProperty("MessageAttributes")
                    .GetProperty(HeaderNames.Id).GetProperty("StringValue").GetString());
            }
            else
            {
                var callback = JsonSerializer.Deserialize<FireAwsScheduler>(body!, JsonSerialisationOptions.Options);
                Assert.NotNull(callback);
                Assert.Equal(scheduleId, callback.Id.Value);
                Assert.Equal(useAsync, callback.Async);
                Assert.NotNull(callback.Message);
                Assert.Equal(message.Id, callback.Message.Id);
                Assert.Equal(message.Header.Topic, callback.Message.Header.Topic);
                Assert.Equal(message.Header.CorrelationId, callback.Message.Header.CorrelationId);
                Assert.Equal("business payload", callback.Message.Body.Value);
                Assert.Equal("tenant-a", callback.Message.Header.Bag["tenant"]);
                Assert.Equal(message.Header.TraceParent, callback.Message.Header.TraceParent);
                Assert.Equal(message.Header.TraceState, callback.Message.Header.TraceState);
                Assert.Equal("stage=deposit", callback.Message.Header.Baggage.ToString());
            }
            Assert.Equal(before, JsonSerializer.Serialize(message, JsonSerialisationOptions.Options));
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            try
            {
                await schedules.DeleteScheduleAsync(new DeleteScheduleRequest
                {
                    Name = scheduleId, GroupName = groupName
                }, cleanup.Token);
            }
            catch (Amazon.Scheduler.Model.ResourceNotFoundException)
            {
            }
            finally
            {
                try
                {
                    if (groupCreated)
                        await schedules.DeleteScheduleGroupAsync(new DeleteScheduleGroupRequest { Name = groupName }, cleanup.Token);
                }
                finally
                {
                    try
                    {
                        foreach (var topic in topics)
                            await sns.DeleteTopicAsync(topic, cleanup.Token);
                        foreach (var queue in queues)
                            await sqs.DeleteQueueAsync(queue, cleanup.Token);
                    }
                    finally
                    {
                        if (roleCreated)
                            await iam.DeleteRoleAsync(new DeleteRoleRequest { RoleName = roleName }, cleanup.Token);
                    }
                }
            }
        }
    }
}
