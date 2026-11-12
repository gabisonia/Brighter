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
using System.Diagnostics;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Paramore.Brighter.Gcp.Tests.TestDoubles;

internal sealed class GcpStreamRpcObserver(Action<string> trace) : Interceptor
{
    public ConcurrentQueue<ReceivedMessage> Deliveries { get; } = new();
    public ConcurrentQueue<(long Started, long Finished, string Method, object Request)> Completed { get; } = new();

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var started = Stopwatch.GetTimestamp();
        var snapshot = Snapshot(request);
        trace($"{DateTimeOffset.UtcNow:O} RPC START {context.Method.Name} {snapshot}");
        var response = continuation(request, context);
        RecordSuccess(started, context.Method.Name, snapshot);
        return response;
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var started = Stopwatch.GetTimestamp();
        var snapshot = Snapshot(request);
        trace($"{DateTimeOffset.UtcNow:O} RPC START {context.Method.Name} {snapshot}");
        var call = continuation(request, context);
        return new AsyncUnaryCall<TResponse>(ObserveAsync(call.ResponseAsync, started, context.Method.Name, snapshot),
            call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
    }

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        var call = continuation(context);
        return new AsyncDuplexStreamingCall<TRequest, TResponse>(call.RequestStream,
            new GcpObservedStreamReader<TResponse>(call.ResponseStream, ObserveDelivery),
            call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
    }

    private void ObserveDelivery<T>(T response)
    {
        if (response is not StreamingPullResponse streaming)
            return;
        foreach (var delivery in streaming.ReceivedMessages)
        {
            var snapshot = delivery.Clone();
            Deliveries.Enqueue(snapshot);
            trace($"{DateTimeOffset.UtcNow:O} NATIVE DELIVERY {snapshot}");
        }
    }

    private async Task<T> ObserveAsync<T>(Task<T> response, long started, string method, object request)
    {
        try
        {
            var result = await response.ConfigureAwait(false);
            RecordSuccess(started, method, request);
            return result;
        }
        catch (Exception exception)
        {
            trace($"{DateTimeOffset.UtcNow:O} RPC FAILURE {method} {request} {exception}");
            throw;
        }
    }

    private void RecordSuccess(long started, string method, object request)
    {
        Completed.Enqueue((started, Stopwatch.GetTimestamp(), method, request));
        trace($"{DateTimeOffset.UtcNow:O} RPC SUCCESS {method} {request}");
    }

    private static object Snapshot(object request) => request switch
    {
        ModifyAckDeadlineRequest deadline => deadline.Clone(),
        AcknowledgeRequest ack => ack.Clone(),
        _ => request
    };
}
