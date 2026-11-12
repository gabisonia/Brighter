#region Licence

/* The MIT License (MIT)
Copyright © 2026 Irakli Gabisonia

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
using System.Collections.Generic;
using Paramore.Brighter.MessagingGateway.RMQ.Sync;

namespace Paramore.Brighter.RMQ.Sync.Tests.TestDoubles;

internal sealed class SyncRecoverablePublicationWireWriter(int failureMode) : IAmAWireMessageWriter<RmqWireMessage>
{
    public bool FailOnWrite { get; set; } = true;

    public RmqWireMessage WriteMessage(Message message)
    {
        var fail = FailOnWrite && message.Id.Value == "failed-publication";
        if (fail && failureMode == 1) throw new FormatException("Publication mapping unavailable.");
        if (fail && failureMode == 2) return null!;
        var wire = new DefaultRmqWireMessageWriter().WriteMessage(message);
        var configure = wire.ConfigureProperties;
        wire.ConfigureProperties = properties =>
        {
            configure?.Invoke(properties);
            if (fail && failureMode == 3) throw new InvalidOperationException("Native properties unavailable.");
            var headers = properties.Headers ??= new Dictionary<string, object>();
            headers["publication-profile"] = "recoverable-writer";
        };
        return wire;
    }
}
