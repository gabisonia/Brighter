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

using System.Collections.Generic;

namespace Paramore.Brighter.MessagingGateway.RocketMQ;

/// <summary>Contains mapped body bytes and application properties for a RocketMQ publication.</summary>
/// <remarks>The gateway owns topic, tag, keys, message group and delivery timestamp.</remarks>
public sealed class RocketMqWireMessage
{
    /// <summary>Creates an independent mapped payload.</summary>
    /// <param name="body">The bytes to publish.</param>
    public RocketMqWireMessage(byte[] body) => Body = body;

    /// <summary>Gets or sets the mapped body bytes.</summary>
    public byte[] Body { get; set; }

    /// <summary>Gets the application properties to publish with the body.</summary>
    public Dictionary<string, string> Properties { get; } = new();
}
