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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Paramore.Brighter.Redis.SchedulerFailureProbe;
using ServiceStack.Redis;
using Xunit;

namespace Paramore.Brighter.Redis.Tests.MessagingGateway;

[Trait("Category", "Redis")]
[Collection("RedisMessagingGateway")]
public class RedisScheduledRetryRegistrationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_scheduled_redis_publication_is_missing_should_preserve_the_host_and_allow_recovery(
        bool useAsync, bool wrongType)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var assemblyDirectory = Path.GetDirectoryName(typeof(RedisScheduledRetryRegistrationTests).Assembly.Location)!;
        var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true,
            WorkingDirectory = assemblyDirectory
        };
        foreach (var argument in new[]
        {
            "exec", "--runtimeconfig", Path.Combine(assemblyDirectory, "Paramore.Brighter.Redis.Tests.runtimeconfig.json"),
            "--depsfile", Path.Combine(assemblyDirectory, "Paramore.Brighter.Redis.Tests.deps.json"),
            typeof(Program).Assembly.Location, useAsync.ToString(), wrongType.ToString(), token
        })
            start.ArgumentList.Add(argument);
        start.Environment["DOTNET_DbgEnableMiniDump"] = "0";
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(70));
        using var child = new Process { StartInfo = start };
        var started = false;
        try
        {
            //Act
            started = child.Start();
            Assert.True(started);
            var stdout = child.StandardOutput.ReadToEndAsync();
            var stderr = child.StandardError.ReadToEndAsync();
            await child.WaitForExitAsync(timeout.Token);
            var output = await stdout;
            var error = await stderr;

            //Assert
            Assert.Contains("PROBE_ACCEPTED:" + token, output);
            Assert.True(child.ExitCode == 0,
                $"Scheduled callback child exited {child.ExitCode}.\nSTDOUT:\n{output}\nSTDERR:\n{error}");
            Assert.Contains("PROBE_RECOVERED:" + token, output);
        }
        finally
        {
            if (started && !child.HasExited)
            {
                child.Kill(entireProcessTree: true);
                using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await child.WaitForExitAsync(stop.Token);
            }
            var host = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
            var port = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
            using IRedisClient observer = new RedisClient(host, port);
            foreach (var route in new[] { "source", "application", "retry", "wrong", "health" })
                foreach (var suffix in new[] { ".1", ".42", ".nextid", ".queues" })
                    observer.Remove("registration-" + route + "-" + token + suffix);
            foreach (var queue in new[] { "queue", "sibling", "health-queue" })
                observer.Remove("registration-" + queue + "-" + token);
        }
    }
}
