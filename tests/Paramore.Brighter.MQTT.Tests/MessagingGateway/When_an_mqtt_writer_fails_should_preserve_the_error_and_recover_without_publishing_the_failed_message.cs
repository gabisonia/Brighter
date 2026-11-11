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
using System.Collections.Concurrent;
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
public class MqttWriterFailureRecoveryTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_an_mqtt_writer_fails_should_preserve_the_error_and_recover_without_publishing_the_failed_message(
        bool useAsync, bool returnNull)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var host = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883");
        var prefix = "brighter-writer-recovery-" + token;
        var topic = new RoutingKey("source-" + token);
        var failedId = Id.Random();
        var failedBody = new byte[] { 0, 255, 1, 128 };
        var failed = new Message(new MessageHeader(failedId, topic, MessageType.MT_EVENT), new MessageBody(failedBody));
        failed.Header.Bag["source-marker"] = "failed-" + token;
        var good = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT), new MessageBody("good-" + token));
        var goodWire = JsonSerializer.SerializeToUtf8Bytes(good, JsonSerialisationOptions.Options);
        var writer = new MqttRecoverableWireWriter(returnNull);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var observer = new MqttFactory().CreateMqttClient();
        var observed = new ConcurrentQueue<byte[]>();
        var firstDelivery = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        observer.ApplicationMessageReceivedAsync += args =>
        {
            var bytes = args.ApplicationMessage.PayloadSegment.ToArray();
            observed.Enqueue(bytes);
            firstDelivery.TrySetResult(bytes);
            return Task.CompletedTask;
        };
        await observer.ConnectAsync(new MqttClientOptionsBuilder().WithTcpServer(host, port)
            .WithClientId("observer-" + token).WithCleanSession().Build(), timeout.Token);
        await observer.SubscribeAsync(new MqttTopicFilter
        {
            Topic = prefix + "/" + topic, QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce
        }).WaitAsync(timeout.Token);
        var publisher = new MqttMessagePublisher(new MqttMessagingGatewayProducerConfiguration
        {
            Hostname = host, Port = port, TopicPrefix = prefix,
            ClientID = "producer-" + token, CleanSession = true
        });
        await using var producer = new MqttMessageProducer(publisher,
            new MqttPublication { Topic = topic, WireMessageWriter = writer }, InstrumentationOptions.None);

        //Act
        var error = await Record.ExceptionAsync(async () =>
        {
            if (useAsync)
                await producer.SendAsync(failed, timeout.Token);
            else
                producer.Send(failed);
        });

        //Assert
        Assert.NotNull(error);
        if (returnNull)
            Assert.IsType<InvalidOperationException>(error);
        else
            Assert.Same(writer.Failure, error);
        if (useAsync)
            await producer.SendAsync(good, timeout.Token);
        else
            producer.Send(good);
        var first = await firstDelivery.Task.WaitAsync(TimeSpan.FromSeconds(10), timeout.Token);
        Assert.Equal(goodWire, first);
        await Task.Delay(TimeSpan.FromMilliseconds(250), timeout.Token);
        Assert.Single(observed);
        Assert.Equal(2, writer.Calls);
        Assert.Equal(failedId, failed.Id);
        Assert.Equal(failedBody, failed.Body.Memory.ToArray());
        Assert.Equal(topic, failed.Header.Topic);
        Assert.Single(failed.Header.Bag);
        Assert.Equal("failed-" + token, failed.Header.Bag["source-marker"]);
    }
}
