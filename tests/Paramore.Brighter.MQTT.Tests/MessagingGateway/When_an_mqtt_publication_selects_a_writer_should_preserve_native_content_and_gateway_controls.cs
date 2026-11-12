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
using MQTTnet.Packets;
using MQTTnet.Protocol;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.MessagingGateway.MQTT;
using Paramore.Brighter.MQTT.Tests.TestDoubles;
using Paramore.Brighter.Observability;
using Xunit;

namespace Paramore.Brighter.MQTT.Tests.MessagingGateway;

[Trait("Category", "MQTT")]
[Collection("MQTT")]
public class MqttPublicationWireWriterTests
{
    [Theory]
    [InlineData(false, "legacy")]
    [InlineData(true, "legacy")]
    [InlineData(false, "custom")]
    [InlineData(true, "custom")]
    [InlineData(false, "composed")]
    [InlineData(true, "composed")]
    public async Task When_an_mqtt_publication_selects_a_writer_should_preserve_native_content_and_gateway_controls(
        bool useAsync, string mode)
    {
        //Arrange
        var host = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883");
        var token = Guid.NewGuid().ToString("N");
        var prefix = "brighter-wire-" + token;
        var topic = new RoutingKey("source-" + token);
        var expectedTopic = prefix + "/" + topic;
        var writerTopic = "writer-" + token;
        var body = new byte[] { 0, 255, 1, 128 };
        var id = Id.Random();
        var message = new Message(new MessageHeader(id, topic, MessageType.MT_EVENT),
            new MessageBody(body.ToArray()));
        message.Header.Bag["source-marker"] = "marker-" + token;
        var legacyBody = JsonSerializer.SerializeToUtf8Bytes(message, JsonSerialisationOptions.Options);
        var expectedBody = mode == "legacy" ? legacyBody
            : new byte[] { mode == "custom" ? (byte)87 : (byte)67, 0 }
                .Concat(mode == "custom" ? body : legacyBody).ToArray();
        var writer = new MqttProbeWireWriter(mode, writerTopic);
        Publication publication = mode == "legacy"
            ? new Publication { Topic = topic }
            : new MqttPublication { Topic = topic, WireMessageWriter = writer };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var observer = new MqttFactory().CreateMqttClient();
        var received = new TaskCompletionSource<(string Topic, byte[] Body, MqttQualityOfServiceLevel Quality)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        observer.ApplicationMessageReceivedAsync += args =>
        {
            var native = args.ApplicationMessage;
            received.TrySetResult((native.Topic, native.PayloadSegment.ToArray(), native.QualityOfServiceLevel));
            return Task.CompletedTask;
        };
        await observer.ConnectAsync(new MqttClientOptionsBuilder()
            .WithTcpServer(host, port).WithClientId("observer-" + token).WithCleanSession().Build(), timeout.Token);
        foreach (var observedTopic in new[] { expectedTopic, writerTopic, topic.Value })
        {
            await observer.SubscribeAsync(new MqttTopicFilter
            {
                Topic = observedTopic,
                QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce
            }).WaitAsync(timeout.Token);
        }
        var publisher = new MqttMessagePublisher(new MqttMessagingGatewayProducerConfiguration
        {
            Hostname = host,
            Port = port,
            TopicPrefix = prefix,
            ClientID = "producer-" + token,
            CleanSession = true
        });
        await using var producer = new MqttMessageProducer(publisher, publication, InstrumentationOptions.None);

        //Act
        if (useAsync)
            await producer.SendAsync(message, timeout.Token);
        else
            producer.Send(message);
        var actual = await received.Task.WaitAsync(timeout.Token);

        //Assert
        Assert.Equal(expectedBody, actual.Body);
        Assert.Equal(expectedTopic, actual.Topic);
        Assert.Equal(MqttQualityOfServiceLevel.AtLeastOnce, actual.Quality);
        Assert.Equal(mode == "legacy" ? 0 : 1, writer.Calls);
        Assert.Equal(body, message.Body.Memory.ToArray());
        Assert.Equal(id, message.Id);
        Assert.Equal(topic, message.Header.Topic);
        Assert.Single(message.Header.Bag);
        Assert.Equal("marker-" + token, message.Header.Bag["source-marker"]);
    }
}
