#region Licence
/* The MIT License (MIT)
Copyright © 2017 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Paramore.Brighter.JsonConverters;
using Paramore.Brighter.Logging;
using Paramore.Brighter.Observability;
using ServiceStack.Redis;

namespace Paramore.Brighter.MessagingGateway.Redis
{
    public partial class RedisMessageConsumer : RedisMessageGateway, IAmAMessageConsumerSync, IAmAMessageConsumerAsync, IHaveAMessagingSystem
    {
        /// <inheritdoc />
        public MessagingSystem MessagingSystem => MessagingSystem.Redis;

        
        /* see RedisMessageProducer to understand how we are using a dynamic recipient list model with Redis */

        private static readonly ILogger s_logger = ApplicationLogging.CreateLogger<RedisMessageConsumer>();
        private static readonly DefaultRedisWireMessageReader s_defaultWireMessageReader = new();
        private static readonly DefaultRedisWireMessageWriter s_defaultRequeueWireMessageWriter = new();
        private static readonly DefaultRedisWireMessageWriter s_defaultRejectionWireMessageWriter = new();
        private readonly IAmAWireMessageReader<string>? _wireMessageReader;
        private readonly IAmAWireMessageWriter<string> _requeueWireMessageWriter;
        private readonly IAmAWireMessageWriter<string> _invalidMessageWireMessageWriter;
        private readonly IAmAWireMessageWriter<string> _deadLetterWireMessageWriter;
        private const string QUEUES = "queues";

        private readonly ChannelName _queueName;
        private readonly RedisMessagingGatewayConfiguration _redisConfiguration;
        private readonly RoutingKey? _deadLetterRoutingKey;
        private readonly RoutingKey? _invalidMessageRoutingKey;
        private readonly IAmAMessageScheduler? _scheduler;
        private readonly RoutingKey? _scheduledRequeuePublicationTopic;
        private RedisMessageProducer? _requeueProducer;
        private bool _requeueProducerInitialized;
        private object? _requeueProducerLock;

        private readonly Dictionary<string, string> _inflight = new();
        private Lazy<RedisMessageProducer?>? _deadLetterProducer;
        private Lazy<RedisMessageProducer?>? _invalidMessageProducer;

        /// <summary>
        /// Creates a consumer that reads from a List in Redis via a BLPOP (so will block).
        /// </summary>
        /// <param name="redisMessagingGatewayConfiguration">Configuration for our Redis client etc.</param>
        /// <param name="queueName">Key of the list in Redis we want to read from</param>
        /// <param name="topic">The topic that the list subscribes to</param>
        /// <param name="deadLetterRoutingKey">The routing key for the dead letter queue, if using Brighter-managed DLQ</param>
        /// <param name="invalidMessageRoutingKey">The routing key for the invalid message queue, if using Brighter-managed invalid message handling</param>
        public RedisMessageConsumer(
            RedisMessagingGatewayConfiguration redisMessagingGatewayConfiguration,
            ChannelName queueName,
            RoutingKey topic,
            IAmAMessageScheduler? scheduler = null,
            RoutingKey? deadLetterRoutingKey = null,
            RoutingKey? invalidMessageRoutingKey = null)
            : this(redisMessagingGatewayConfiguration, queueName, topic, scheduler, deadLetterRoutingKey,
                invalidMessageRoutingKey, null, null, null, null, null)
        {
        }

        internal RedisMessageConsumer(
            RedisMessagingGatewayConfiguration redisMessagingGatewayConfiguration,
            ChannelName queueName,
            RoutingKey topic,
            IAmAMessageScheduler? scheduler,
            RoutingKey? deadLetterRoutingKey,
            RoutingKey? invalidMessageRoutingKey,
            IAmAWireMessageReader<string>? wireMessageReader,
            IAmAWireMessageWriter<string>? requeueWireMessageWriter,
            IAmAWireMessageWriter<string>? invalidMessageWireMessageWriter,
            IAmAWireMessageWriter<string>? deadLetterWireMessageWriter,
            RoutingKey? scheduledRequeuePublicationTopic)
            : base(redisMessagingGatewayConfiguration, topic)
        {
            _wireMessageReader = wireMessageReader;
            _requeueWireMessageWriter = requeueWireMessageWriter ?? s_defaultRequeueWireMessageWriter;
            _invalidMessageWireMessageWriter = invalidMessageWireMessageWriter ?? s_defaultRejectionWireMessageWriter;
            _deadLetterWireMessageWriter = deadLetterWireMessageWriter ?? s_defaultRejectionWireMessageWriter;
            _queueName = queueName;
            _redisConfiguration = redisMessagingGatewayConfiguration;
            _scheduler = scheduler;
            _scheduledRequeuePublicationTopic = scheduledRequeuePublicationTopic;
            _deadLetterRoutingKey = deadLetterRoutingKey;
            _invalidMessageRoutingKey = invalidMessageRoutingKey;

            // LazyThreadSafetyMode.None: message pumps are single-threaded per consumer, so no
            // thread-safety mode is needed. None does not cache exceptions, allowing the factory
            // to retry on the next .Value access after a transient failure.
            if (_deadLetterRoutingKey != null)
                _deadLetterProducer = new Lazy<RedisMessageProducer?>(CreateDeadLetterProducer, LazyThreadSafetyMode.None);
            if (_invalidMessageRoutingKey != null)
                _invalidMessageProducer = new Lazy<RedisMessageProducer?>(CreateInvalidMessageProducer, LazyThreadSafetyMode.None);
        }

        /// <summary>
        /// Acknowledge the message, removing it from the queue 
        /// </summary>
        /// <remarks>
        /// This a 'do nothing operation' as with Redis we pop the message from the queue to read;
        /// this allows us to have competing consumers, and thus a message is always 'consumed' even
        /// if we fail to process it.
        /// The risk with Redis is that we lose any in-flight message if we kill the service, without allowing
        /// the job to run to completion. Brighter uses run to completion if shut down properly, but not if you
        /// just kill the process.
        /// If you need the level of reliability that unprocessed messages that return to the queue don't use Redis.
        /// </remarks>
        /// <param name="message"></param>
        public void Acknowledge(Message message)
        {
            Log.AcknowledgingMessage(s_logger, message.Id.Value);
            _inflight.Remove(message.Id.Value);
        }
        
        /// <summary>
        /// Acknowledge the message, removing it from the queue 
        /// </summary>
        /// <remarks>
        /// This a 'do nothing operation' as with Redis we pop the message from the queue to read;
        /// this allows us to have competing consumers, and thus a message is always 'consumed' even
        /// if we fail to process it.
        /// The risk with Redis is that we lose any in-flight message if we kill the service, without allowing
        /// the job to run to completion. Brighter uses run to completion if shut down properly, but not if you
        /// just kill the process.
        /// If you need the level of reliability that unprocessed messages that return to the queue don't use Redis.
        /// This is async over sync as the underlying operation does not block
        /// </remarks>
        /// <param name="message"></param>
        public Task AcknowledgeAsync(Message message, CancellationToken cancellationToken = default(CancellationToken))
        {
            Acknowledge(message);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Nacks the specified message. For Redis (LPOP-based), this is a no-op because the message
        /// has already been destructively removed from the list and cannot be returned.
        /// </summary>
        /// <param name="message">The message.</param>
        public void Nack(Message message)
        {
            // No-op for Redis: LPOP is destructive, cannot un-pop
        }

        /// <summary>
        /// Nacks the specified message. For Redis (LPOP-based), this is a no-op because the message
        /// has already been destructively removed from the list and cannot be returned.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cancellationToken">Cancel the nack operation</param>
        public Task NackAsync(Message message, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Dispose of the Redis consumer
        /// </summary>
        /// <remarks>
        /// Free up our RedisMangerPool, connections not held open between invocations of Receive, so you can create
        /// a consumer and keep it for program lifetime, disposing at the end only, without fear of a leak
        /// </remarks> 
        public void Dispose()
        {
            _requeueProducer?.Dispose();
            if (_deadLetterProducer?.IsValueCreated == true)
                (_deadLetterProducer.Value as IDisposable)?.Dispose();
            if (_invalidMessageProducer?.IsValueCreated == true)
                (_invalidMessageProducer.Value as IDisposable)?.Dispose();

            DisposePool();
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc cref="IAsyncDisposable"/>
        public async ValueTask DisposeAsync()
        {
            if (_requeueProducer != null) await _requeueProducer.DisposeAsync();

            if (_deadLetterProducer?.IsValueCreated == true && _deadLetterProducer.Value is IAsyncDisposable deadLetterAsync)
                await deadLetterAsync.DisposeAsync();
            else if (_deadLetterProducer?.IsValueCreated == true)
                (_deadLetterProducer.Value as IDisposable)?.Dispose();

            if (_invalidMessageProducer?.IsValueCreated == true && _invalidMessageProducer.Value is IAsyncDisposable invalidAsync)
                await invalidAsync.DisposeAsync();
            else if (_invalidMessageProducer?.IsValueCreated == true)
                (_invalidMessageProducer.Value as IDisposable)?.Dispose();

            await DisposePoolAsync().ConfigureAwait(false);
            GC.SuppressFinalize(this);
        }
        
        /// <summary>
        /// Clear the queue
        /// </summary>
        public void Purge()
        {
            Log.PurgingChannel(s_logger, _queueName);
            
            using var client = GetClient();
            if (client == null)
                throw new ChannelFailureException("RedisMessagingGateway: No Redis client available");
            
            //This kills the queue, not the messages, which we assume expire
            client.RemoveAllFromList(_queueName);
        }
        
        /// <summary>
        /// Clear the queue
        /// </summary>
        /// <param name="cancellationToken">The cancellation token</param>
        public async Task PurgeAsync(CancellationToken cancellationToken = default(CancellationToken))
        { 
            Log.PurgingChannel(s_logger, _queueName);
            
            await using var client = await GetClientAsync(cancellationToken);
            if (client == null)
                throw new ChannelFailureException("RedisMessagingGateway: No Redis client available");

            //This kills the queue, not the messages, which we assume expire
            await client.RemoveAllFromListAsync(_queueName, token: cancellationToken);
        }

        /// <summary>
        /// Get the next message off the Redis list, within a timeout
        /// </summary>
        /// <param name="timeOut">The period to await a message. Defaults to 1s.</param>
        /// <returns>The message read from the list</returns>
        public Message[] Receive(TimeSpan? timeOut = null)
        {
            Log.RetrievingNextMessage(s_logger, _queueName, Topic);

            if (_inflight.Any())
            {
                Log.UnackedMessageInFlight(s_logger, _queueName);
                throw new ChannelFailureException($"Unacked message still in flight with id: {_inflight.Keys.First()}");   
            }
            
            if (timeOut == null || timeOut.GetValueOrDefault().TotalSeconds < 1)
            {
                timeOut = TimeSpan.FromSeconds(1);
            }
            
            try
            {
                var client = GetClient();
                if (client == null)
                    throw new ChannelFailureException("RedisMessagingGateway: No Redis client available");
                
                EnsureConnection(client);
                (string? msgId, string rawMsg) redisMessage = ReadMessage(client, timeOut.Value);
                if (redisMessage.msgId == null || string.IsNullOrEmpty(redisMessage.rawMsg))
                    return [];
                
                var message = MapMessage(redisMessage.msgId, redisMessage.rawMsg);
#pragma warning disable CS0618 // Preserve the legacy message type for transport compatibility.
                if (message.Header.MessageType != MessageType.MT_NONE && message.Header.MessageType != MessageType.MT_UNACCEPTABLE)
#pragma warning restore CS0618
                {
                    _inflight.Add(message.Id.Value, redisMessage.msgId);
                }
                
                return [message];
            }
            catch (TimeoutException te)
            {
                Log.CouldNotConnectToRedisClient(s_logger, timeOut.Value.TotalMilliseconds.ToString(CultureInfo.CurrentCulture));
                throw new ChannelFailureException($"Could not connect to Redis client within {timeOut.Value.TotalMilliseconds.ToString(CultureInfo.InvariantCulture)} milliseconds", te);
            }
            catch (RedisException re)
            {
                Log.CouldNotConnectToRedis(s_logger, re.Message);
                throw new ChannelFailureException("Could not connect to Redis client - see inner exception for details", re);
            }
        }

        /// <summary>
        /// Get the next message off the Redis list, within a timeout
        /// </summary>
        /// <param name="timeOut">The period to await a message. Defaults to 1s.</param>
        /// <param name="cancellationToken">Cancel the receive operation</param>
        /// <returns>The message read from the list</returns>
        public async Task<Message[]> ReceiveAsync(TimeSpan? timeOut = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            Log.RetrievingNextMessage(s_logger, _queueName, Topic);

            if (_inflight.Any())
            {
                Log.UnackedMessageInFlight(s_logger, _queueName);
                throw new ChannelFailureException($"Unacked message still in flight with id: {_inflight.Keys.First()}");   
            }

            timeOut ??= TimeSpan.FromSeconds(1);
            try
            {
                await using IRedisClientAsync? client = await GetClientAsync(cancellationToken);
                if (client == null)
                    throw new ChannelFailureException("RedisMessagingGateway: No Redis client available");
                
                await EnsureConnectionAsync(client);
                (string? msgId, string rawMsg) redisMessage = await ReadMessageAsync(client, timeOut.Value);
                if (redisMessage.msgId == null || string.IsNullOrEmpty(redisMessage.rawMsg))
                    return [];
                
                var message = MapMessage(redisMessage.msgId, redisMessage.rawMsg);

#pragma warning disable CS0618 // Preserve the legacy message type for transport compatibility.
                if (message.Header.MessageType != MessageType.MT_NONE && message.Header.MessageType != MessageType.MT_UNACCEPTABLE)
#pragma warning restore CS0618
                {
                    _inflight.Add(message.Id.Value, redisMessage.msgId);
                }

                return [message];
            }
            catch (TimeoutException te)
            {
                Log.CouldNotConnectToRedisClient(s_logger, timeOut.Value.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
                throw new ChannelFailureException($"Could not connect to Redis client within {timeOut.Value.TotalMilliseconds.ToString(CultureInfo.InvariantCulture)} milliseconds", te);
            }
            catch (RedisException re)
            {
                Log.CouldNotConnectToRedis(s_logger, re.Message);
                throw new ChannelFailureException("Could not connect to Redis client - see inner exception for details", re);
            }
        }

        /// <summary>
        /// Reject the message, routing it to a DLQ or invalid message channel if configured
        /// </summary>
        /// <remarks>
        /// Writer failures propagate and retain the original topic and in-flight entry for retry.
        /// Native publication failures are logged and release the in-flight entry; the original
        /// Redis delivery has already been destructively removed.
        /// </remarks>
        /// <param name="message">The message to reject</param>
        /// <param name="reason">The <see cref="MessageRejectionReason"/> that explains why we rejected the message</param>
        public bool Reject(Message message, MessageRejectionReason? reason = null)
        {
            if (_deadLetterProducer == null && _invalidMessageProducer == null)
            {
                if (reason != null)
                    Log.NoChannelsConfiguredForRejection(s_logger, message.Id.Value, reason.RejectionReason.ToString());

                _inflight.Remove(message.Id.Value);
                return true;
            }

            var rejectionReason = reason?.RejectionReason ?? RejectionReason.None;
            var originalTopic = message.Header.Topic;
            RedisMessageProducer? producer;
            try
            {
                producer = PrepareRejection(message, reason);
            }
            catch (Exception ex)
            {
                Log.ErrorSendingToRejectionChannel(s_logger, ex, message.Id.Value, rejectionReason.ToString());
                _inflight.Remove(message.Id.Value);
                return true;
            }

            if (producer != null)
            {
                var redisMessage = MapRejectionMessage(message, originalTopic);
                try
                {
                    producer.SendMapped(message, redisMessage);
                    Log.MessageSentToRejectionChannel(s_logger, message.Id.Value, rejectionReason.ToString());
                }
                catch (Exception ex)
                {
                    Log.ErrorSendingToRejectionChannel(s_logger, ex, message.Id.Value, rejectionReason.ToString());
                }
            }
            else
            {
                Log.NoChannelsConfiguredForRejection(s_logger, message.Id.Value, rejectionReason.ToString());
            }

            _inflight.Remove(message.Id.Value);
            return true;
        }

        /// <summary>
        /// Reject the message asynchronously, routing it to a DLQ or invalid message channel if configured
        /// </summary>
        /// <remarks>
        /// Writer failures propagate and retain the original topic and in-flight entry for retry.
        /// Native publication failures are logged and release the in-flight entry; the original
        /// Redis delivery has already been destructively removed.
        /// </remarks>
        /// <param name="message">The message to reject</param>
        /// <param name="reason">The <see cref="MessageRejectionReason"/> that explains why we rejected the message</param>
        /// <param name="cancellationToken">The cancellation token</param>
        public async Task<bool> RejectAsync(Message message, MessageRejectionReason? reason = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_deadLetterProducer == null && _invalidMessageProducer == null)
            {
                if (reason != null)
                    Log.NoChannelsConfiguredForRejection(s_logger, message.Id.Value, reason.RejectionReason.ToString());

                _inflight.Remove(message.Id.Value);
                return true;
            }

            var rejectionReason = reason?.RejectionReason ?? RejectionReason.None;
            var originalTopic = message.Header.Topic;
            RedisMessageProducer? producer;
            try
            {
                producer = PrepareRejection(message, reason);
            }
            catch (Exception ex)
            {
                Log.ErrorSendingToRejectionChannel(s_logger, ex, message.Id.Value, rejectionReason.ToString());
                _inflight.Remove(message.Id.Value);
                return true;
            }

            if (producer != null)
            {
                var redisMessage = MapRejectionMessage(message, originalTopic);
                try
                {
                    await producer.SendMappedAsync(message, redisMessage, cancellationToken);
                    Log.MessageSentToRejectionChannel(s_logger, message.Id.Value, rejectionReason.ToString());
                }
                catch (Exception ex)
                {
                    Log.ErrorSendingToRejectionChannel(s_logger, ex, message.Id.Value, rejectionReason.ToString());
                }
            }
            else
            {
                Log.NoChannelsConfiguredForRejection(s_logger, message.Id.Value, rejectionReason.ToString());
            }

            _inflight.Remove(message.Id.Value);
            return true;
        }

        /// <summary>
        /// Requeues the specified message.
        /// </summary>
        /// <param name="message">The message to requeue</param>
        /// <param name="delay">Delay before the message becomes visible again</param>
        /// <returns>True if the message was requeued</returns>
        public bool Requeue(Message message, TimeSpan? delay = null)
        {
            delay ??= TimeSpan.Zero;

            if (delay > TimeSpan.Zero)
            {
                if (_scheduler == null)
                {
                    throw new ConfigurationException(
                        $"RedisMessageConsumer: delay of {delay} was requested for requeue but no scheduler is configured; configure a scheduler via MessageSchedulerFactory.");
                }

                _inflight.TryGetValue(message.Id.Value, out string? removedMsgId);
                _inflight.Remove(message.Id.Value);
                try
                {
                    EnsureRequeueProducer();
                    _requeueProducer!.SendWithDelay(PrepareScheduledRequeue(message), delay);
                    return true;
                }
                catch
                {
                    if (removedMsgId != null)
                        _inflight[message.Id.Value] = removedMsgId;
                    throw;
                }
            }

            using var client = GetClient();
            if (client == null)
                throw new ChannelFailureException("RedisMessagingGateway: No Redis client available");

            if (_inflight.TryGetValue(message.Id.Value, out string? msgId))
            {
                var redisMsg = MapRequeueMessage(message);
                StoreMessage(client, redisMsg, long.Parse(msgId));
                client.AddItemToList(_queueName, msgId);
                _inflight.Remove(message.Id.Value);
                return true;
            }
            else
            {
                Log.MessageNotFoundInFlight(s_logger, message.Id.Value);
                return false;
            }
        }

        /// <summary>
        /// Requeues the specified message.
        /// </summary>
        /// <param name="message">The message to requeue</param>
        /// <param name="delay">Delay before the message becomes visible again</param>
        /// <param name="cancellationToken">Cancel the requeue operation</param>
        /// <returns>True if the message was requeued</returns>
        public async Task<bool> RequeueAsync(Message message, TimeSpan? delay = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            delay ??= TimeSpan.Zero;

            if (delay > TimeSpan.Zero)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (_scheduler == null)
                {
                    throw new ConfigurationException(
                        $"RedisMessageConsumer: delay of {delay} was requested for requeue but no scheduler is configured; configure a scheduler via MessageSchedulerFactory.");
                }

                _inflight.TryGetValue(message.Id.Value, out string? removedMsgId);
                _inflight.Remove(message.Id.Value);
                try
                {
                    EnsureRequeueProducer();
                    await _requeueProducer!.SendWithDelayAsync(PrepareScheduledRequeue(message), delay, cancellationToken);
                    return true;
                }
                catch
                {
                    if (removedMsgId != null)
                        _inflight[message.Id.Value] = removedMsgId;
                    throw;
                }
            }

            await using var client = await GetClientAsync(cancellationToken);
            if (client == null)
                throw new ChannelFailureException("RedisMessagingGateway: No Redis client available");

            if (_inflight.TryGetValue(message.Id.Value, out string? msgId))
            {
                var redisMsg = MapRequeueMessage(message);
                await StoreMessageAsync(client, redisMsg, long.Parse(msgId));
                await client.AddItemToListAsync(_queueName, msgId, cancellationToken);
                _inflight.Remove(message.Id.Value);
                return true;
            }
            else
            {
                Log.MessageNotFoundInFlight(s_logger, message.Id.Value);
                return false;
            }
        }
        
        private string MapRequeueMessage(Message message) =>
            _requeueWireMessageWriter.WriteMessage(message)
            ?? throw new InvalidOperationException("The Redis requeue wire message writer returned null.");

        private Message PrepareScheduledRequeue(Message message)
        {
            if (_scheduledRequeuePublicationTopic == null)
                return message;

            var header = CopyScheduledHeader(message.Header);
            header.Bag[Message.ProducerTopicHeaderName] = _scheduledRequeuePublicationTopic.Value;
            var body = new MessageBody(message.Body.Memory,
                message.Body.ContentType == null ? null : new System.Net.Mime.ContentType(message.Body.ContentType.ToString()),
                message.Body.CharacterEncoding);
            return new Message(header, body) { Persist = message.Persist };
        }

        private static MessageHeader CopyScheduledHeader(MessageHeader source)
        {
            var baggage = new Baggage();
            foreach (var entry in source.Baggage)
                baggage.Add(entry.Key, entry.Value!);

            return new MessageHeader(source.MessageId, source.Topic,
#pragma warning disable CS0618 // Preserve the legacy message type for transport compatibility.
                source.MessageType,
#pragma warning restore CS0618
                source: source.Source, type: source.Type, timeStamp: source.TimeStamp,
                correlationId: source.CorrelationId, replyTo: source.ReplyTo,
                contentType: new System.Net.Mime.ContentType(source.ContentType.ToString()),
                partitionKey: source.PartitionKey, dataSchema: source.DataSchema, subject: source.Subject,
                handledCount: source.HandledCount, workflowId: source.WorkflowId, jobId: source.JobId,
                delayed: source.Delayed, traceParent: source.TraceParent, traceState: source.TraceState,
                baggage: baggage)
            {
                Bag = new Dictionary<string, object>(source.Bag, source.Bag.Comparer),
                DataRef = source.DataRef,
                SpecVersion = source.SpecVersion
            };
        }

        private void EnsureRequeueProducer()
        {
            LazyInitializer.EnsureInitialized(ref _requeueProducer, ref _requeueProducerInitialized,
                ref _requeueProducerLock, () => new RedisMessageProducer(
                    _redisConfiguration,
                    new RedisMessagePublication { Topic = Topic })
                {
                    Scheduler = _scheduler
                });
        }

        private RedisMessageProducer? CreateDeadLetterProducer()
        {
            if (_deadLetterRoutingKey == null) return null;

            try
            {
                return new RedisMessageProducer(_redisConfiguration,
                    new RedisMessagePublication { Topic = _deadLetterRoutingKey });
            }
            catch (Exception e)
            {
                Log.ErrorCreatingDlqProducer(s_logger, e, _deadLetterRoutingKey.Value);
                return null;
            }
        }

        private RedisMessageProducer? CreateInvalidMessageProducer()
        {
            if (_invalidMessageRoutingKey == null) return null;

            try
            {
                return new RedisMessageProducer(_redisConfiguration,
                    new RedisMessagePublication { Topic = _invalidMessageRoutingKey });
            }
            catch (Exception e)
            {
                Log.ErrorCreatingInvalidMessageProducer(s_logger, e, _invalidMessageRoutingKey.Value);
                return null;
            }
        }

        private RedisMessageProducer? PrepareRejection(Message message, MessageRejectionReason? reason)
        {
            RefreshMetadata(message, reason);
            var (routingKey, shouldRoute, isFallingBackToDlq) = DetermineRejectionRoute(
                reason?.RejectionReason ?? RejectionReason.None, _invalidMessageProducer != null, _deadLetterProducer != null);
            if (!shouldRoute)
                return null;

            message.Header.Topic = routingKey!;
            if (isFallingBackToDlq)
                Log.FallingBackToDlq(s_logger, message.Id.Value);

            return routingKey == _invalidMessageRoutingKey ? _invalidMessageProducer?.Value : _deadLetterProducer?.Value;
        }

        private string MapRejectionMessage(Message message, RoutingKey originalTopic)
        {
            var writer = message.Header.Topic == _invalidMessageRoutingKey
                ? _invalidMessageWireMessageWriter
                : _deadLetterWireMessageWriter;
            try
            {
                return writer.WriteMessage(message)
                    ?? throw new InvalidOperationException("The Redis rejection wire message writer returned null.");
            }
            catch
            {
                message.Header.Topic = originalTopic;
                throw;
            }
        }

        private static void RefreshMetadata(Message message, MessageRejectionReason? reason)
        {
            message.Header.Bag["originalTopic"] = message.Header.Topic.Value;
            message.Header.Bag["rejectionTimestamp"] = DateTimeOffset.UtcNow.ToString("o");
#pragma warning disable CS0618 // Preserve the legacy message type for transport compatibility.
            message.Header.Bag["originalMessageType"] = message.Header.MessageType.ToString();
#pragma warning restore CS0618

            if (reason == null) return;

            message.Header.Bag["rejectionReason"] = reason.RejectionReason.ToString();
            if (!string.IsNullOrEmpty(reason.Description))
                message.Header.Bag["rejectionMessage"] = reason.Description ?? string.Empty;
        }

        private (RoutingKey? routingKey, bool foundProducer, bool isFallingBackToDlq) DetermineRejectionRoute(
            RejectionReason rejectionReason,
            bool hasInvalidProducer,
            bool hasDeadLetterProducer)
        {
            switch (rejectionReason)
            {
                case RejectionReason.Unacceptable:
                    if (hasInvalidProducer)
                        return (_invalidMessageRoutingKey, true, false);
                    if (hasDeadLetterProducer)
                        return (_deadLetterRoutingKey, true, true);
                    return (null, false, false);

                case RejectionReason.DeliveryError:
                case RejectionReason.None:
                default:
                    if (hasDeadLetterProducer)
                        return (_deadLetterRoutingKey, true, false);
                    return (null, false, false);
            }
        }

        // Virtual to allow testing to simulate client failure
        protected virtual IRedisClient? GetClient()
        {
            try
            {
                return Pool.Value.GetClient();
            }
            catch (TimeoutException te)
            {
                throw new ChannelFailureException("RedisMessagingGateway: Timeout on getting client from pool", te);
            }
            catch(RedisException re)
            {
                throw new ChannelFailureException("RedisMessagingGateway: Error on getting client from pool", re);
            }
            catch(ObjectDisposedException ode)
            {
                throw new ChannelFailureException("RedisMessagingGateway: Connection pool has been disposed", ode);
            }
        }

        // Virtual to allow testing to simulate client failure
        protected virtual async Task<IRedisClientAsync?> GetClientAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                return await Pool.Value.GetClientAsync(cancellationToken);
            }
            catch (TimeoutException te)
            {
                throw new ChannelFailureException("RedisMessagingGateway: Timeout on getting client from pool", te);
            }
            catch(RedisException re)
            {
                throw new ChannelFailureException("RedisMessagingGateway: Error on getting client from pool", re);
            }
            catch(ObjectDisposedException ode)
            {
                throw new ChannelFailureException("RedisMessagingGateway: Connection pool has been disposed", ode);
            }
        }
            
        private void EnsureConnection(IRedisClient client)
        {
            Log.CreatingQueue(s_logger, _queueName);
            //what is the queue list key
            var key = Topic + "." + QUEUES;
            //subscribe us 
            client.AddItemToSet(key, _queueName);
        }
        
        private async Task EnsureConnectionAsync(IRedisClientAsync client)
        {
            Log.CreatingQueue(s_logger, _queueName);
            //what is the queue list key
            var key = Topic + "." + QUEUES;
            //subscribe us 
            await client.AddItemToSetAsync(key, _queueName);
        }

        private Message MapMessage(string nativeId, string rawMessage)
        {
            if (_wireMessageReader == null)
                return s_defaultWireMessageReader.ReadMessage(rawMessage);

            try
            {
                return _wireMessageReader.ReadMessage(rawMessage) ?? CreateMappingFailure(nativeId, rawMessage);
            }
            catch (Exception)
            {
                return CreateMappingFailure(nativeId, rawMessage);
            }
        }

        private Message CreateMappingFailure(string nativeId, string rawMessage)
        {
            return new Message(MessageHeader.FailureMessageHeader(Topic, new Id(nativeId)), new MessageBody(rawMessage));
        }

        private (string? msgId, string rawMsg) ReadMessage(IRedisClient client, TimeSpan timeOut)
        {
            var msg = string.Empty;
            var latestId = client.BlockingRemoveStartFromList(_queueName, timeOut);
            if (latestId != null)
            {
                var key = Topic + "." + latestId;
                msg = client.GetValue(key);
                Log.ReceivedMessageFromQueue(s_logger, _queueName, Topic, JsonSerializer.Serialize(msg, JsonSerialisationOptions.Options));
            }
            else
            {
                Log.TimeoutWithoutReceivingMessage(s_logger, _queueName, Topic);
            }
            return (latestId, msg);
        }
        
        private async Task<(string? msgId, string rawMsg)> ReadMessageAsync(IRedisClientAsync client, TimeSpan timeOut)
        {
            var msg = string.Empty;
            string? latestId = null;
            try
            {
                // Give the server-side BLPOP timeout a chance to fire first; if the client cancels first
                // a freshly-pushed item can be delivered to the lingering BLPOP and lost.
                using var cts = new CancellationTokenSource(timeOut + TimeSpan.FromSeconds(1));
                latestId = await client.BlockingRemoveStartFromListAsync(_queueName, timeOut, cts.Token);
                if (latestId != null)
                {
                    var key = Topic + "." + latestId;
                    msg = await client.GetValueAsync(key);
                    Log.ReceivedMessageFromQueue(s_logger, _queueName, Topic, JsonSerializer.Serialize(msg, JsonSerialisationOptions.Options));
                }
            }
            catch (OperationCanceledException)
            {
                Log.TimeoutWithoutReceivingMessage(s_logger, _queueName, Topic);
            }
            catch (RedisException re) when (re.InnerException is OperationCanceledException)
            {
                Log.TimeoutWithoutReceivingMessage(s_logger, _queueName, Topic);
            }
            
            return (latestId, msg);
        }

        private static partial class Log
        {
            [LoggerMessage(LogLevel.Information, "RedisMessageConsumer: Acknowledging message {Id}")]
            public static partial void AcknowledgingMessage(ILogger logger, string id);

            [LoggerMessage(LogLevel.Debug, "RedisMessageConsumer: Purging channel {ChannelName}")]
            public static partial void PurgingChannel(ILogger logger, ChannelName channelName);
            
            [LoggerMessage(LogLevel.Debug, "RedisMessageConsumer: Preparing to retrieve next message from queue {ChannelName} with routing key {Topic}")]
            public static partial void RetrievingNextMessage(ILogger logger, ChannelName channelName, RoutingKey topic);
            
            [LoggerMessage(LogLevel.Error, "RedisMessageConsumer: Preparing to retrieve next message from queue {ChannelName}, but have unacked or not rejected message")]
            public static partial void UnackedMessageInFlight(ILogger logger, ChannelName channelName);
            
            [LoggerMessage(LogLevel.Error, "Could not connect to Redis client within {Timeout} milliseconds")]
            public static partial void CouldNotConnectToRedisClient(ILogger logger, string timeout);
            
            [LoggerMessage(LogLevel.Error, "Could not connect to Redis: {ErrorMessage}")]
            public static partial void CouldNotConnectToRedis(ILogger logger, string errorMessage);
            
            [LoggerMessage(LogLevel.Debug, "RedisMessagingGateway: Creating queue {ChannelName}")]
            public static partial void CreatingQueue(ILogger logger, ChannelName channelName);
            
            [LoggerMessage(LogLevel.Information, "Redis: Received message from queue {ChannelName} with routing key {Topic}, message: {Request}")]
            public static partial void ReceivedMessageFromQueue(ILogger logger, ChannelName channelName, RoutingKey topic, string request);
            
            [LoggerMessage(LogLevel.Debug, "RedisMessageConsumer: Time out without receiving message from queue {ChannelName} with routing key {Topic}")]
            public static partial void TimeoutWithoutReceivingMessage(ILogger logger, ChannelName channelName, RoutingKey topic);
            
            [LoggerMessage(LogLevel.Error, "Expected to find message id {MessageId} in-flight but was not")]
            public static partial void MessageNotFoundInFlight(ILogger logger, string messageId);

            [LoggerMessage(LogLevel.Warning, "RedisMessageConsumer: No DLQ or invalid message channels configured for message {MessageId}, rejection reason: {RejectionReason}")]
            public static partial void NoChannelsConfiguredForRejection(ILogger logger, string messageId, string rejectionReason);

            [LoggerMessage(LogLevel.Information, "RedisMessageConsumer: Message {MessageId} sent to rejection channel, reason: {RejectionReason}")]
            public static partial void MessageSentToRejectionChannel(ILogger logger, string messageId, string rejectionReason);

            [LoggerMessage(LogLevel.Warning, "RedisMessageConsumer: Falling back to DLQ for message {MessageId}")]
            public static partial void FallingBackToDlq(ILogger logger, string messageId);

            [LoggerMessage(LogLevel.Error, "RedisMessageConsumer: Error sending message {MessageId} to rejection channel, reason: {RejectionReason}")]
            public static partial void ErrorSendingToRejectionChannel(ILogger logger, Exception ex, string messageId, string rejectionReason);

            [LoggerMessage(LogLevel.Error, "RedisMessageConsumer: Error creating DLQ producer for routing key {RoutingKey}")]
            public static partial void ErrorCreatingDlqProducer(ILogger logger, Exception ex, string routingKey);

            [LoggerMessage(LogLevel.Error, "RedisMessageConsumer: Error creating invalid message producer for routing key {RoutingKey}")]
            public static partial void ErrorCreatingInvalidMessageProducer(ILogger logger, Exception ex, string routingKey);
        }
    }
}

