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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using MQTTnet.Packets;
using Paramore.Brighter.MessagingGateway.MQTT;
using Paramore.Brighter.MQTT.Tests.TestDoubles;
using Xunit;

namespace Paramore.Brighter.MQTT.Tests.MessagingGateway;

/// <summary>Verifies selected consumer writers through native retry and forwarding delivery.</summary>
[Trait("Category", "MQTT")]
[Collection("MQTT")]
public class MqttConsumerWriterSelectionTests
{
    /// <summary>Captures the selected writer and preserves native routing and failure recovery.</summary>
    [Theory]
    [InlineData(false, 0, 0)]
    [InlineData(false, 0, 1)]
    [InlineData(false, 0, 2)]
    [InlineData(false, 1, 0)]
    [InlineData(false, 1, 1)]
    [InlineData(false, 1, 2)]
    [InlineData(false, 2, 0)]
    [InlineData(false, 2, 1)]
    [InlineData(false, 2, 2)]
    [InlineData(true, 0, 0)]
    [InlineData(true, 0, 1)]
    [InlineData(true, 0, 2)]
    [InlineData(true, 1, 0)]
    [InlineData(true, 1, 1)]
    [InlineData(true, 1, 2)]
    [InlineData(true, 2, 0)]
    [InlineData(true, 2, 1)]
    [InlineData(true, 2, 2)]
    public async Task When_consumers_republish_should_use_captured_writers_and_preserve_recovery(bool useAsync, int route, int behavior)
    {
        // Arrange
        var name = "consumer-writers-" + Guid.NewGuid().ToString("N");
        var source = name + "/source";
        var dead = name + "/dead";
        var invalid = name + "/invalid";
        var topic = new RoutingKey("logical-business");
        var destination = (route == 0 ? source : route == 1 ? dead : invalid) + "/" + topic.Value;
        var host = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "localhost";
        var port = int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var selected = new MqttConsumerSelectedWireWriter(behavior);
        var unused = new MqttConsumerSelectedWireWriter(0);
        var replacement = new MqttConsumerSelectedWireWriter(0);
        var subscription = new MqttSubscription(new SubscriptionName(name), new ChannelName(source), topic, requestType: typeof(Command),
            messagePumpType: useAsync ? MessagePumpType.Proactor : MessagePumpType.Reactor)
        {
            DeadLetterRoutingKey = new RoutingKey(dead), InvalidMessageRoutingKey = new RoutingKey(invalid),
            RequeueWireMessageWriter = route == 0 ? selected : unused,
            DeadLetterWireMessageWriter = route == 1 ? selected : unused,
            InvalidMessageWireMessageWriter = route == 2 ? selected : unused
        };
        var factory = new MqttMessageConsumerFactory(new MqttMessagingGatewayConsumerConfiguration
        { Hostname = host, Port = port, TopicPrefix = source, ClientID = name + "-consumer", CleanSession = true });
        await using var consumer = (useAsync ? (MqttMessageConsumer)factory.CreateAsync(subscription) : (MqttMessageConsumer)factory.Create(subscription));
        subscription.RequeueWireMessageWriter = replacement;
        subscription.DeadLetterWireMessageWriter = replacement;
        subscription.InvalidMessageWireMessageWriter = replacement;
        using var native = new MqttFactory().CreateMqttClient();
        var deliveries = new ConcurrentQueue<MqttApplicationMessage>();
        var initialSeen = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var observe = 0;
        native.ApplicationMessageReceivedAsync += args =>
        {
            if (Volatile.Read(ref observe) == 0 && args.ApplicationMessage.Topic == source + "/" + topic.Value)
                initialSeen.TrySetResult(true);
            else if (args.ApplicationMessage.Topic == destination)
                deliveries.Enqueue(new MqttApplicationMessage
                { Topic = args.ApplicationMessage.Topic, PayloadSegment = args.ApplicationMessage.PayloadSegment.ToArray() });
            return Task.CompletedTask;
        };
        await native.ConnectAsync(new MqttClientOptionsBuilder().WithTcpServer(host, port)
            .WithClientId(name + "-native").WithCleanSession().Build(), timeout.Token);
        await native.SubscribeAsync(new MqttTopicFilter { Topic = name + "/#", QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce }).WaitAsync(timeout.Token);
        async Task<Message> ReceiveNativeSource()
        {
            var outgoing = new Message(new MessageHeader(Id.Random(), topic, MessageType.MT_EVENT), new MessageBody("consumer-payload"));
            outgoing.Header.Bag["tenant"] = "native-tenant";
            var wire = new DefaultMqttWireMessageWriter().WriteMessage(outgoing);
            wire.Topic = source + "/" + topic.Value;
            wire.QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce;
            await native.PublishAsync(wire, timeout.Token);
            while (true)
            {
                timeout.Token.ThrowIfCancellationRequested();
                var batch = useAsync ? await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(100), timeout.Token) : consumer.Receive(TimeSpan.FromMilliseconds(100));
                var found = batch.FirstOrDefault(message => message.Id == outgoing.Id);
                if (found != null) return found;
            }
        }
        var received = await ReceiveNativeSource();
        await initialSeen.Task.WaitAsync(timeout.Token);
        Volatile.Write(ref observe, 1);
        async Task Republish(Message message)
        {
            bool result;
            if (route == 0) result = useAsync ? await consumer.RequeueAsync(message, TimeSpan.Zero, timeout.Token) : consumer.Requeue(message);
            else
            {
                var reason = new MessageRejectionReason(route == 1 ? RejectionReason.DeliveryError : RejectionReason.Unacceptable, "consumer-rejection");
                result = useAsync ? await consumer.RejectAsync(message, reason, timeout.Token) : consumer.Reject(message, reason);
            }
            Assert.True(result);
        }

        // Act
        var error = await Record.ExceptionAsync(() => Republish(received));

        // Assert
        if (behavior != 0)
        {
            if (route == 0)
            {
                Assert.NotNull(error);
                if (behavior == 1) Assert.IsType<FormatException>(error);
                else Assert.IsType<InvalidOperationException>(error);
            }
            else Assert.Null(error);
            await Task.Delay(250, timeout.Token);
            Assert.Empty(deliveries);
            Assert.Equal(1, selected.Calls);
            selected.Recover = true;
            if (route != 0) received = await ReceiveNativeSource();
            await Republish(received);
        }
        else Assert.Null(error);
        while (deliveries.IsEmpty) await Task.Delay(10, timeout.Token);
        var delivered = Assert.Single(deliveries);
        Assert.Equal(destination, delivered.Topic);
        var payload = Encoding.UTF8.GetString(delivered.PayloadSegment);
        Assert.StartsWith("selected:", payload);
        var mapped = new DefaultMqttWireMessageReader().ReadMessage(new MqttApplicationMessage
        { Topic = delivered.Topic, PayloadSegment = Encoding.UTF8.GetBytes(payload[9..]) });
        Assert.Equal(received.Id, mapped.Id);
        Assert.Equal("consumer-payload", mapped.Body.Value);
        Assert.Equal("native-tenant", mapped.Header.Bag["tenant"].ToString());
        if (route != 0)
        {
            Assert.Equal("logical-business", mapped.Header.Bag["originalTopic"].ToString());
            Assert.Equal(route == 1 ? "DeliveryError" : "Unacceptable", mapped.Header.Bag["rejectionReason"].ToString());
        }
        Assert.Equal(behavior == 0 ? 1 : 2, selected.Calls);
        Assert.Equal(0, unused.Calls);
        Assert.Equal(0, replacement.Calls);
        await Task.Delay(250, timeout.Token);
        Assert.Single(deliveries);
    }
}
