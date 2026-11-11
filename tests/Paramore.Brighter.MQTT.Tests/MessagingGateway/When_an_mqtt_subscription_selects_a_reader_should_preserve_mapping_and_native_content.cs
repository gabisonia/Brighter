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
public class MqttSubscriptionWireReaderTests
{
    [Theory]
    [InlineData(false, "legacy")]
    [InlineData(true, "legacy")]
    [InlineData(false, "custom")]
    [InlineData(true, "custom")]
    [InlineData(false, "composed")]
    [InlineData(true, "composed")]
    public async Task When_an_mqtt_subscription_selects_a_reader_should_preserve_mapping_and_native_content(
        bool useAsync, string mode)
    {
        //Arrange
        var host = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883");
        var token = Guid.NewGuid().ToString("N");
        var prefix = "brighter-reader-" + token;
        var topic = new RoutingKey("source-" + token);
        var nativeTopic = prefix + "/" + topic;
        var mappedTopic = new RoutingKey("mapped-" + token);
        var id = Id.Random();
        var body = new byte[] { 0, 255, 1, 128 };
        var source = new Message(new MessageHeader(id, topic, MessageType.MT_EVENT), new MessageBody(body));
        source.Header.Bag["source-marker"] = "marker-" + token;
        var wireBody = JsonSerializer.SerializeToUtf8Bytes(source, JsonSerialisationOptions.Options);
        var expectedBody = mode == "legacy" ? body
            : new byte[] { mode == "custom" ? (byte)82 : (byte)67, 0 }
                .Concat(mode == "custom" ? wireBody : body).ToArray();
        var reader = new MqttProbeWireReader(mode, id, mappedTopic);
        var subscription = new MqttSubscription(new SubscriptionName("sub-" + token),
            new ChannelName("channel-" + token), topic, requestType: typeof(MyCommand),
            messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
        {
            WireMessageReader = mode == "legacy" ? null : reader
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
        await publisher.PublishAsync(new MqttApplicationMessageBuilder().WithTopic(nativeTopic)
            .WithPayload(wireBody).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).Build(), timeout.Token);
        var received = useAsync
            ? await consumer.ReceiveAsync(TimeSpan.FromSeconds(10), timeout.Token)
            : consumer.Receive(TimeSpan.FromSeconds(10));
        var actual = Assert.Single(received);

        //Assert
        Assert.Equal(expectedBody, actual.Body.Memory.ToArray());
        Assert.Equal(mode == "legacy" ? topic : mappedTopic, actual.Header.Topic);
        Assert.Equal(id, actual.Id);
        Assert.Equal(mode == "legacy" ? 0 : 1, reader.Calls);
        if (mode != "custom")
            Assert.Equal("marker-" + token, actual.Header.Bag["source-marker"]);
        if (mode != "legacy")
        {
            Assert.Equal("mapped", actual.Header.Bag["reader-marker"]);
            Assert.Equal(wireBody, reader.Before);
            Assert.Equal(wireBody, reader.After);
            Assert.Equal(nativeTopic, reader.NativeTopic);
            Assert.Equal(MqttQualityOfServiceLevel.AtLeastOnce, reader.NativeQuality);
        }
    }
}
