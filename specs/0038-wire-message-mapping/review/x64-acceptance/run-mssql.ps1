. "$PSScriptRoot/common.ps1"
$directory = "$PSScriptRoot/mssql-native"
New-Item -ItemType Directory -Path $directory -Force | Out-Null
$name = 'brighter-4170-x64-mssql-20261111-native'
$owned = $false
$result = [ordered]@{ head = $Head; utc = [DateTime]::UtcNow.ToString('o'); image = $SqlImage; frameworks = @(); host = $env:PROCESSOR_ARCHITECTURE }
try {
    $existing = DockerCommand @('ps', '-a', '--format', '{{.Names}}')
    if ($existing -contains $name) { throw 'Fixture name already exists; refusing reuse' }
    DockerCommand @('image', 'inspect', $SqlImage, '--format', '{{.Architecture}} {{.Os}} {{.Id}}') | Set-Content "$directory/image.txt"
    $env:ACCEPT_EULA = 'Y'; $env:MSSQL_PID = 'Developer'; $env:MSSQL_MEMORY_LIMIT_MB = '2048'
    # Several existing cases intentionally use the repository's constant connection.
    # Bind the dedicated fixture to that endpoint; Docker refuses an occupied port.
    $env:MSSQL_SA_PASSWORD = 'Password123!'
    DockerCommand @('run', '-d', '--pull', 'never', '--platform', 'linux/amd64', '--name', $name, '--label', 'brighter.validation=4170-x64', '-e', 'ACCEPT_EULA', '-e', 'MSSQL_PID', '-e', 'MSSQL_MEMORY_LIMIT_MB', '-e', 'MSSQL_SA_PASSWORD', '-p', '127.0.0.1:11433:1433', $SqlImage) | Out-Null
    $owned = $true
    WaitSql $name
    $port = ((DockerCommand @('port', $name, '1433/tcp')) -split ':')[-1]
    $result.port = $port
    DockerCommand @('exec', $name, 'uname', '-m') | Set-Content "$directory/container-architecture.txt"
    $env:SQLCMDPASSWORD = $env:MSSQL_SA_PASSWORD
    DockerCommand @('exec', '-e', 'SQLCMDPASSWORD', $name, '/opt/mssql-tools18/bin/sqlcmd', '-S', 'localhost', '-U', 'sa', '-C', '-b', '-Q', 'SELECT @@VERSION') | Set-Content "$directory/sql-version.txt"
    $dotnet9 = Join-Path $env:TEMP 'brighter-x64-dotnet/dotnet.exe'
    $dotnet10 = 'C:/Program Files/dotnet/dotnet.exe'
    '<RunSettings><RunConfiguration><TestSessionTimeout>840000</TestSessionTimeout></RunConfiguration></RunSettings>' | Set-Content "$directory/acceptance.runsettings" -Encoding UTF8
    foreach ($fw in @('net9.0', 'net10.0')) {
        $env:Sql__TestsBrighterConnectionString = "Server=127.0.0.1,$port;Database=BrighterTests;User Id=sa;Password=$env:MSSQL_SA_PASSWORD;Application Name=Brighter4170X64;Connect Timeout=60;Encrypt=false"
        # Build with SDK 10 for the declared multi-target project; execute the .NET 9 testhost with its own muxer.
        $build = RunBounded $dotnet10 @('build', 'tests/Paramore.Brighter.MSSQL.Tests/Paramore.Brighter.MSSQL.Tests.csproj', '-c', 'Release', '-f', $fw, '--verbosity', 'minimal') "$directory/$fw-build.log" 900
        if ($build.exitCode -ne 0) { throw "Build failed for $fw; inspect retained build log" }
        $hostDotnet = if ($fw -eq 'net9.0') { $dotnet9 } else { $dotnet10 }
        $assembly = (Join-Path (Get-Location) "tests/Paramore.Brighter.MSSQL.Tests/bin/Release/$fw/Paramore.Brighter.MSSQL.Tests.dll")
        $run = RunBounded $hostDotnet @('vstest', $assembly, "/Logger:trx;LogFileName=$fw.trx", "/ResultsDirectory:$directory", "/Settings:$directory/acceptance.runsettings") "$directory/$fw-test.log" 900
        $result.frameworks += @{ framework = $fw; head = $Head; run = $run; build = $build }
        SaveJson $result "$directory/results.json"
        Write-Output "$fw complete; exit $($run.exitCode)"
    }
} catch { $result.error = "$_"; Write-Output "SQL acceptance error: $_" }
finally {
    if ($owned) { $result.container = StopOwned $name $directory }
    Remove-Item Env:Sql__TestsBrighterConnectionString -ErrorAction SilentlyContinue
    Remove-Item Env:MSSQL_SA_PASSWORD -ErrorAction SilentlyContinue
    Remove-Item Env:SQLCMDPASSWORD -ErrorAction SilentlyContinue
    SaveJson $result "$directory/results.json"
}
if ($result.error) { exit 1 }
