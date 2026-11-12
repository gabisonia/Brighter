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
/// Converts a transport's wire representation to a Brighter message.
/// </summary>
/// <typeparam name="TWireMessage">The native delivery type defined by the messaging gateway.</typeparam>
/// <remarks>
/// Implementations must not mutate the native delivery. A configured reader can be called concurrently
/// and must return an independent message for each call. The gateway owns delivery operations and metadata.
/// </remarks>
public interface IAmAWireMessageReader<in TWireMessage>
{
    /// <summary>
    /// Maps the native body and application metadata to a Brighter message.
    /// </summary>
    /// <param name="message">The native delivery received by the gateway.</param>
    /// <returns>A fresh Brighter message to which the gateway can attach delivery metadata.</returns>
    Message ReadMessage(TWireMessage message);
}
