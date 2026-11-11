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

using System.Linq;
using Paramore.Brighter.MessagingGateway.AWSSQS;
using Xunit;

namespace Paramore.Brighter.AWS.Tests.MessagingGateway;

[Trait("Category", "AWS")]
public class SqsLegacyConstructorCompatibilityTests
{
    [Fact]
    public void When_selecting_the_legacy_sqs_constructor_should_have_one_applicable_overload()
    {
        //Arrange
        var constructors = typeof(SqsMessageConsumer).GetConstructors();

        //Act
        var candidates = constructors.Where(constructor =>
        {
            var parameters = constructor.GetParameters();
            return parameters.Length >= 3
                && parameters[0].ParameterType == typeof(AWSMessagingGatewayConnection)
                && parameters[1].ParameterType == typeof(string)
                && !parameters[2].ParameterType.IsByRef
                && parameters.Skip(3).All(parameter => parameter.IsOptional);
        });

        //Assert
        var legacyConstructor = Assert.Single(candidates);
        Assert.Equal(typeof(int), legacyConstructor.GetParameters()[2].ParameterType);
    }
}
