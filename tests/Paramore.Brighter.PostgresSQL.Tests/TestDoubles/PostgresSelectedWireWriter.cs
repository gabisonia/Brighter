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
using System.Text.Json;
using System.Threading;
using Paramore.Brighter.MessagingGateway.Postgres;

namespace Paramore.Brighter.PostgresSQL.Tests.TestDoubles;

/// <summary>Builds complete or composed PostgreSQL JSON payloads for native mapping tests.</summary>
public sealed class PostgresSelectedWireWriter(bool compose) : IAmAWireMessageWriter<string>
{
    private int _calls;
    /// <summary>Gets the number of attempted conversions.</summary>
    public int Calls => Volatile.Read(ref _calls);
    /// <inheritdoc />
    public string WriteMessage(Message message)
    {
        Interlocked.Increment(ref _calls);
        if (compose)
        {
            using var content = JsonDocument.Parse(new DefaultPostgresWireMessageWriter().WriteMessage(message));
            return JsonSerializer.Serialize(new { composed = content.RootElement });
        }
        return JsonSerializer.Serialize(new
        {
            id = message.Id.Value, topic = message.Header.Topic.Value, body = message.Body.Value,
            trace = message.Header.TraceParent.Value, state = message.Header.TraceState.Value,
            baggage = message.Header.Baggage.ToString(), marker = message.Header.Bag["marker"].ToString(),
            handled = message.Header.HandledCount
        });
    }
}
