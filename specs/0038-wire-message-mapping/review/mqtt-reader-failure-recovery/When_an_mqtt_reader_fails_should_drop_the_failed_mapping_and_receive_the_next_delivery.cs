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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.MQTT;
using Paramore.Brighter.MQTT.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.MQTT.Tests.MessagingGateway;

[Trait("Category", "MQTT")]
[Collection("MQTT")]
public class MqttReaderFailureRecoveryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_an_mqtt_reader_fails_should_drop_the_failed_mapping_and_receive_the_next_delivery(
        bool useAsync, bool returnNull)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var prefix = "brighter-reader-recovery-" + token;
        var topic = new RoutingKey("source-" + token);
        var host = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883");
        var failed = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT), new MessageBody("failed"));
        var goodId = Id.Random();
        var goodBody = new byte[] { 0, 255, 1, 128 };
        var good = new Message(new MessageHeader(goodId, topic, MessageType.MT_EVENT), new MessageBody(goodBody));
        good.Header.Bag["source-marker"] = "marker-" + token;
        var failedWire = JsonSerializer.SerializeToUtf8Bytes(failed, JsonSerialisationOptions.Options);
        var goodWire = JsonSerializer.SerializeToUtf8Bytes(good, JsonSerialisationOptions.Options);
        var reader = new MqttRecoverableWireReader(failedWire, returnNull);
        var subscription = new MqttSubscription(new SubscriptionName("sub-" + token),
            new ChannelName("channel-" + token), topic, requestType: typeof(MyCommand),
            messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
        {
            WireMessageReader = reader
        };
        var factory = new MqttMessageConsumerFactory(new MqttMessagingGatewayConsumerConfiguration
        {
            Hostname = host, Port = port, TopicPrefix = prefix,
            ClientID = "consumer-" + token, CleanSession = true
        });
        await using var consumer = useAsync
            ? (MqttMessageConsumer)factory.CreateAsync(subscription)
            : (MqttMessageConsumer)factory.Create(subscription);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var publisher = new MqttFactory().CreateMqttClient();
        await publisher.ConnectAsync(new MqttClientOptionsBuilder().WithTcpServer(host, port)
            .WithClientId("native-" + token).WithCleanSession().Build(), timeout.Token);

        //Act
        await publisher.PublishAsync(new MqttApplicationMessageBuilder().WithTopic(prefix + "/" + topic)
            .WithPayload(failedWire).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).Build(), timeout.Token);
        await reader.FailedDeliverySeen.Task.WaitAsync(TimeSpan.FromSeconds(10), timeout.Token);
        await publisher.PublishAsync(new MqttApplicationMessageBuilder().WithTopic(prefix + "/" + topic)
            .WithPayload(goodWire).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).Build(), timeout.Token);
        var received = useAsync
            ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
            : consumer.Receive(TimeSpan.FromSeconds(10));
        var actual = Assert.Single(received);

        //Assert
        Assert.NotNull(actual);
        Assert.Equal(goodId, actual.Id);
        Assert.Equal(goodBody, actual.Body.Memory.ToArray());
        Assert.Equal(topic, actual.Header.Topic);
        Assert.Equal("marker-" + token, actual.Header.Bag["source-marker"]);
        Assert.Equal(2, reader.Calls);
        var remaining = useAsync
            ? await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(250), timeout.Token)
            : consumer.Receive(TimeSpan.FromMilliseconds(250));
#pragma warning disable CS0618
        Assert.Equal(MessageType.MT_NONE, Assert.Single(remaining).Header.MessageType);
#pragma warning restore CS0618
    }
}
