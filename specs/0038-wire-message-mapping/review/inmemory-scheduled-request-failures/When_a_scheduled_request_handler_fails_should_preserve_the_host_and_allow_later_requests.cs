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
using Paramore.Brighter.SchedulerRequestFailureProbe;
using Xunit;

namespace Paramore.Brighter.Core.Tests.CommandProcessors.Scheduler;

/// <summary>Verifies real request timer failures without terminating the parent test runner.</summary>
public class InMemoryScheduledRequestFailureTests
{
    /// <summary>Requires callback containment, cleanup and subsequent healthy request execution.</summary>
    /// <param name="useAsync">Whether to use asynchronous scheduling and application dispatch.</param>
    /// <param name="publish">Whether to publish an event instead of sending a command.</param>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task When_a_scheduled_request_handler_fails_should_preserve_the_host_and_allow_later_requests(
        bool useAsync, bool publish)
    {
        //Arrange
        var token = Guid.NewGuid().ToString("N");
        var assemblyDirectory = Path.GetDirectoryName(typeof(InMemoryScheduledRequestFailureTests).Assembly.Location)!;
        var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true,
            WorkingDirectory = assemblyDirectory
        };
        foreach (var argument in new[]
        {
            "exec", "--runtimeconfig", Path.Combine(assemblyDirectory, "Paramore.Brighter.Core.Tests.runtimeconfig.json"),
            "--depsfile", Path.Combine(assemblyDirectory, "Paramore.Brighter.Core.Tests.deps.json"),
            typeof(Program).Assembly.Location, useAsync.ToString(), publish.ToString(), token
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
        }
    }
}
