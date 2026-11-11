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

using System.Threading;
using System.Threading.Tasks;

namespace Paramore.Brighter.RMQ.Async.Tests.TestDoubles;

internal sealed class QuartzWireCommandMapper : IAmAMessageMapper<QuartzWireCommand>, IAmAMessageMapperAsync<QuartzWireCommand>
{
    public IRequestContext? Context { get; set; }

    public Message MapToMessage(QuartzWireCommand request, Publication publication) =>
        new(new MessageHeader(request.Id, publication.Topic!, MessageType.MT_COMMAND), new MessageBody(request.Payload));

    public QuartzWireCommand MapToRequest(Message message) => new() { Id = message.Id, Payload = message.Body.Value };

    public Task<Message> MapToMessageAsync(QuartzWireCommand request, Publication publication,
        CancellationToken cancellationToken = default) => Task.FromResult(MapToMessage(request, publication));

    public Task<QuartzWireCommand> MapToRequestAsync(Message message, CancellationToken cancellationToken = default) =>
        Task.FromResult(MapToRequest(message));
}
