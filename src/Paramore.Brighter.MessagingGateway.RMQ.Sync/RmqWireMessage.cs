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

using System;
using RabbitMQ.Client;

namespace Paramore.Brighter.MessagingGateway.RMQ.Sync;

/// <summary>
/// Carries a message body and configuration for native RabbitMQ properties.
/// </summary>
/// <remarks>
/// The gateway controls destination, persistence and delivery bookkeeping. Return fresh
/// configuration and body data for each mapping so concurrent sends do not share mutable output.
/// </remarks>
public sealed class RmqWireMessage
{
    /// <summary>
    /// Gets or sets the body to publish.
    /// </summary>
    public ReadOnlyMemory<byte> Body { get; set; }

    /// <summary>
    /// Gets or sets the callback that fills the native application properties and headers.
    /// </summary>
    /// <remarks>
    /// The gateway creates fresh properties on its channel and invokes this callback once during the send,
    /// before applying delivery controls. Only populate the supplied properties; do not retain them.
    /// Null leaves application properties unset. Exceptions follow the send failure path.
    /// </remarks>
    public Action<IBasicProperties>? ConfigureProperties { get; set; }
}
