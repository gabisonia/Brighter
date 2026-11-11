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

using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

internal static class StartupHook
{
    // Linux open flags select append-only writes; mode 0600 limits capture-file access.
    public static void Initialize()
    {
        if (!OperatingSystem.IsLinux() || !Environment.GetCommandLineArgs().Any(
                argument => argument.EndsWith("testhost.dll", StringComparison.Ordinal)))
            return;

        string directory = Environment.GetEnvironmentVariable("BRIGHTER_NATIVE_CAPTURE")
            ?? throw new InvalidOperationException("An explicit diagnostic capture directory is required.");
        int pid = Environment.ProcessId;
        string path = Path.Combine(directory, $"native-stderr-{pid}.txt");
        int descriptor = open(path, 1 | 64 | 1024, 384);
        if (descriptor < 0)
            throw new InvalidOperationException($"Cannot open native capture: {Marshal.GetLastPInvokeError()}");
        try
        {
            if (dup2(descriptor, 2) != 2)
                throw new InvalidOperationException($"Cannot redirect native stderr: {Marshal.GetLastPInvokeError()}");
        }
        finally
        {
            if (descriptor != 2) close(descriptor);
        }

        byte[] marker = Encoding.UTF8.GetBytes($"NATIVE CAPTURE pid={pid} utc={DateTimeOffset.UtcNow:O}\n");
        if (write(2, marker, (nuint)marker.Length) != marker.Length)
            throw new InvalidOperationException("Cannot write native capture marker.");
        File.WriteAllText(Path.Combine(directory, $"testhost-{pid}.json"), JsonSerializer.Serialize(new
        {
            pid, path, runtime = RuntimeEnvironment.GetRuntimeDirectory(), utc = DateTimeOffset.UtcNow
        }));
    }

    [DllImport("libc", SetLastError = true)]
    private static extern int open(string path, int flags, int mode);
    [DllImport("libc", SetLastError = true)]
    private static extern int dup2(int oldDescriptor, int newDescriptor);
    [DllImport("libc", SetLastError = true)]
    private static extern int close(int descriptor);
    [DllImport("libc", SetLastError = true)]
    private static extern nint write(int descriptor, byte[] bytes, nuint count);
}
