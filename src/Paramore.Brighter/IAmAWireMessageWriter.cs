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

namespace Paramore.Brighter;

/// <summary>
/// Converts a Brighter message to a transport's wire representation.
/// </summary>
/// <typeparam name="TWireMessage">The native payload type defined by the messaging gateway.</typeparam>
/// <remarks>
/// Implementations must not mutate the source message. A configured writer can be called concurrently
/// and must return an independent wire representation for each call. The gateway owns delivery operations.
/// </remarks>
public interface IAmAWireMessageWriter<out TWireMessage>
{
    /// <summary>
    /// Maps the message body and application metadata to the transport's wire representation.
    /// </summary>
    /// <param name="message">The message, including trace context available at dispatch time.</param>
    /// <returns>A fresh wire representation ready for the gateway to send.</returns>
    TWireMessage WriteMessage(Message message);
}
