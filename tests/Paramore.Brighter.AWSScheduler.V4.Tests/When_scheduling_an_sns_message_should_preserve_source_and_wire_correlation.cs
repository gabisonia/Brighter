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
using Amazon.IdentityManagement.Model;
using Amazon.Scheduler.Model;
using Paramore.Brighter.AWSScheduler.V4.Tests.Helpers;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessageScheduler.AWS.V4;
using Paramore.Brighter.MessagingGateway.AWSSQS.V4;

namespace Paramore.Brighter.AWSScheduler.V4.Tests;

[Trait("Category", "AWS")]
public class AwsNativeSchedulerSourcePreservationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_scheduling_an_sns_message_should_preserve_source_and_wire_correlation(
        bool useAsync, bool hasCorrelationId)
    {
        //Arrange
        var connection = GatewayFactory.CreateFactory();
        var clients = new AWSClientFactory(connection);
        using var sns = clients.CreateSnsClient();
        using var schedules = clients.CreateSchedulerClient();
        using var iam = Paramore.Brighter.MessageScheduler.AWS.V4.AWSClientFactoryExtensions.CreateIdentityClient(clients);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var suffix = Guid.NewGuid().ToString("N");
        var name = $"Source-{suffix}";
        string? topicArn = null;
        var groupCreated = false;
        var roleCreated = false;
        try
        {
            topicArn = (await sns.CreateTopicAsync(name, timeout.Token)).TopicArn;
            await schedules.CreateScheduleGroupAsync(new CreateScheduleGroupRequest { Name = name }, timeout.Token);
            groupCreated = true;
            var role = await iam.CreateRoleAsync(new CreateRoleRequest
            {
                RoleName = name,
                AssumeRolePolicyDocument = """
                    {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                    "Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}
                    """
            }, timeout.Token);
            roleCreated = true;
            var factory = new AwsSchedulerFactory(connection, role.Role.Arn)
            {
                MakeRole = OnMissingRole.Assume, UseMessageTopicAsTarget = true,
                Group = new SchedulerGroup { Name = name, MakeSchedulerGroup = OnMissingSchedulerGroup.Assume },
                GetOrCreateMessageSchedulerId = _ => name
            };
            var scheduler = factory.Create(null!);
            var correlation = hasCorrelationId ? new Id("original-correlation") : Id.Empty;
            var original = new Message(new MessageHeader(new Id("original-message"), new RoutingKey(topicArn),
                MessageType.MT_EVENT, correlationId: correlation,
                traceParent: new TraceParent("00-11111111111111111111111111111111-2222222222222222-01"),
                traceState: new TraceState("vendor=deposit")), new MessageBody("original payload"));
            original.Header.Bag["tenant"] = "tenant-a";
            original.Header.Baggage.Add("stage", "deposit");
            var before = JsonSerializer.Serialize(original, JsonSerialisationOptions.Options);
            var at = DateTimeOffset.UtcNow.AddHours(1);

            //Act
            var id = useAsync
                ? await ((IAmAMessageSchedulerAsync)scheduler).ScheduleAsync(original, at, timeout.Token)
                : ((IAmAMessageSchedulerSync)scheduler).Schedule(original, at);
            var stored = await schedules.GetScheduleAsync(new GetScheduleRequest
            {
                Name = id, GroupName = name
            }, timeout.Token);

            //Assert
            Assert.Equal(name, id);
            Assert.Equal("arn:aws:scheduler:::aws-sdk:sns:publish", stored.Target.Arn);
            using var input = JsonDocument.Parse(stored.Target.Input);
            Assert.Equal(topicArn, input.RootElement.GetProperty("TopicArn").GetString());
            Assert.Equal("original payload", input.RootElement.GetProperty("Message").GetString());
            var attributes = input.RootElement.GetProperty("MessageAttributes");
            Assert.Equal("original-message", attributes.GetProperty(HeaderNames.Id).GetProperty("StringValue").GetString());
            Assert.True(attributes.TryGetProperty(HeaderNames.CorrelationId, out var wireCorrelation));
            Assert.Equal("String", wireCorrelation.GetProperty("DataType").GetString());
            var wireCorrelationValue = wireCorrelation.GetProperty("StringValue").GetString();
            Assert.False(string.IsNullOrWhiteSpace(wireCorrelationValue));
            if (hasCorrelationId)
                Assert.Equal("original-correlation", wireCorrelationValue);
            Assert.Equal(correlation, original.Header.CorrelationId);
            Assert.Equal(before, JsonSerializer.Serialize(original, JsonSerialisationOptions.Options));
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            try
            {
                await schedules.DeleteScheduleAsync(new DeleteScheduleRequest { Name = name, GroupName = name }, cleanup.Token);
            }
            catch (Amazon.Scheduler.Model.ResourceNotFoundException)
            {
            }
            finally
            {
                try
                {
                    if (groupCreated)
                        await schedules.DeleteScheduleGroupAsync(new DeleteScheduleGroupRequest { Name = name }, cleanup.Token);
                }
                finally
                {
                    try
                    {
                        if (topicArn is not null)
                            await sns.DeleteTopicAsync(topicArn, cleanup.Token);
                    }
                    finally
                    {
                        if (roleCreated)
                            await iam.DeleteRoleAsync(new DeleteRoleRequest { RoleName = name }, cleanup.Token);
                    }
                }
            }
        }
    }
}
