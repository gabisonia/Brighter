. "$PSScriptRoot/common.ps1"
$directory = "$PSScriptRoot/asb-sdk"
New-Item -ItemType Directory -Path $directory -Force | Out-Null
$source = "$PSScriptRoot/../asb-sdk-dlq/probe"
$probe = Join-Path $env:TEMP 'brighter-4170-x64-sdk-probe-20261111'
if (Test-Path $probe) { throw 'Probe directory exists; refusing overwrite' }
New-Item -ItemType Directory -Path $probe | Out-Null
foreach ($file in @('Program.cs', 'Probe.csproj', 'packages.lock.json')) { Copy-Item "$source/$file" "$probe/$file" }
Copy-Item "$PSScriptRoot/../asb-sdk-dlq/Config.json" "$directory/Config.json"
$results = @()
$dotnet10 = 'C:/Program Files/dotnet/dotnet.exe'
$restore = RunBounded $dotnet10 @('restore', "$probe/Probe.csproj", '--locked-mode') "$directory/restore.log" 300 $probe
if ($restore.exitCode -ne 0) { throw 'Locked SDK-only restore failed' }
$build = RunBounded $dotnet10 @('build', "$probe/Probe.csproj", '-c', 'Release', '--no-restore') "$directory/build.log" 300 $probe
if ($build.exitCode -ne 0) { throw 'SDK-only build failed' }
SaveJson @{ head = $Head; sourceFiles = @(Get-ChildItem $source -File | Where-Object Name -in @('Program.cs','Probe.csproj','packages.lock.json') | Get-FileHash -Algorithm SHA256 | Select-Object Path,Hash); copiedWithoutEdits = $true; experiment = 'One cold sample per framework; unchanged four topic and four queue cycles; 300 second outer deadline; architecture/backend hypothesis only' } "$directory/provenance.json"
$pull = RunBounded $Docker @('pull', '--platform', 'linux/amd64', $AsbImage) "$directory/image-pull.log" 600
if ($pull.exitCode -ne 0) { throw 'Pinned amd64 emulator image pull failed' }
DockerCommand @('image', 'inspect', $AsbImage, '--format', '{{.Architecture}} {{.Os}} {{.Id}} {{json .RepoDigests}}') | Set-Content "$directory/emulator-image.txt"
foreach ($fw in @('net9.0', 'net10.0')) {
    $suffix = $fw.Replace('.', '-')
    $network = "brighter-4170-x64-sdk-$suffix-20261111"
    $db = "$network-sql"; $broker = "$network-broker"; $owned = @(); $networkOwned = $false
    $outcome = [ordered]@{ head = $Head; framework = $fw; utc = [DateTime]::UtcNow.ToString('o'); sqlImage = $SqlImage; emulatorImage = $AsbImage; hostArchitecture = 'AMD64'; sqlEmulated = $false; emulatorEmulated = $false; containers = @() }
    try {
        $networks = DockerCommand @('network', 'ls', '--format', '{{.Name}}')
        if ($networks -contains $network) { throw 'Fixture network exists; refusing reuse' }
        # Docker allocates a free subnet; no existing network is changed.
        DockerCommand @('network', 'create', '--label', 'brighter.validation=4170-x64', $network) | Out-Null
        $networkOwned = $true
        DockerCommand @('network', 'inspect', $network) | Set-Content "$directory/$fw-network.json"
        $env:ACCEPT_EULA = 'Y'; $env:MSSQL_PID = 'Developer'; $env:MSSQL_MEMORY_LIMIT_MB = '2048'
        $env:MSSQL_SA_PASSWORD = 'Local-' + [Guid]::NewGuid().ToString('N') + '!'
        $env:SQL_SERVER = 'sql'; $env:SQL_WAIT_INTERVAL = '15'
        $fixtureEnv = @('-e','ACCEPT_EULA','-e','MSSQL_PID','-e','MSSQL_MEMORY_LIMIT_MB','-e','MSSQL_SA_PASSWORD','-e','SQL_SERVER','-e','SQL_WAIT_INTERVAL')
        DockerCommand (@('run','-d','--pull','never','--platform','linux/amd64','--name',$db,'--network',$network,'--network-alias','sql','--label','brighter.validation=4170-x64') + $fixtureEnv + @($SqlImage)) | Out-Null
        $owned += $db
        WaitSql $db
        DockerCommand @('exec', $db, 'uname', '-m') | Set-Content "$directory/$fw-sql-architecture.txt"
        $env:SQLCMDPASSWORD = $env:MSSQL_SA_PASSWORD
        DockerCommand @('exec', '-e', 'SQLCMDPASSWORD', $db, '/opt/mssql-tools18/bin/sqlcmd', '-S', 'localhost', '-U', 'sa', '-C', '-b', '-Q', 'SELECT @@VERSION') | Set-Content "$directory/$fw-sql-version.txt"
        DockerCommand (@('run','-d','--pull','never','--platform','linux/amd64','--name',$broker,'--network',$network,'--label','brighter.validation=4170-x64','-p','127.0.0.1::5672','-v',"${directory}/Config.json:/ServiceBus_Emulator/ConfigFiles/Config.json:ro") + $fixtureEnv + @($AsbImage)) | Out-Null
        $owned += $broker
        $watch = [Diagnostics.Stopwatch]::StartNew(); $ready = $false
        while ($watch.Elapsed.TotalSeconds -lt 120) {
            $logs = DockerCommand @('logs',$broker)
            if (($logs -join "`n") -match 'Emulator Service is Successfully Up|Emulator is running') { $ready = $true; break }
            if ((DockerCommand @('inspect',$broker,'--format','{{.State.Running}}')) -ne 'true') { throw 'Emulator exited before readiness' }
            Start-Sleep -Seconds 2
        }
        if (!$ready) { throw 'Emulator readiness exceeded 120 seconds' }
        $port = ((DockerCommand @('port',$broker,'5672/tcp')) -split ':')[-1]
        $env:BrighterTestsASBConnectionString = "Endpoint=sb://127.0.0.1:$port;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
        Remove-Item Env:BrighterTestsASBNameSpace -ErrorAction SilentlyContinue
        $hostDotnet = if ($fw -eq 'net9.0') { Join-Path $env:TEMP 'brighter-x64-dotnet/dotnet.exe' } else { $dotnet10 }
        $assembly = "$probe/bin/Release/$fw/Probe.dll"
        $outcome.assemblySha256 = (Get-FileHash $assembly -Algorithm SHA256).Hash
        $outcome.run = RunBounded $hostDotnet @($assembly,"$directory/$fw-probe.json") "$directory/$fw.log" 300 $probe
        $outcome.probe = Get-Content "$directory/$fw-probe.json" -Raw | ConvertFrom-Json
        Write-Output "$fw SDK-only comparison: $($outcome.probe.failures) failures"
    } catch { $outcome.error = "$_"; Write-Output "ASB comparison error: $_" }
    finally {
        [array]::Reverse($owned)
        foreach ($name in $owned) { $outcome.containers += StopOwned $name $directory }
        $outcome.networkRetained = $networkOwned
        $results += $outcome; SaveJson $results "$directory/results.json"
        Remove-Item Env:BrighterTestsASBConnectionString -ErrorAction SilentlyContinue
        Remove-Item Env:MSSQL_SA_PASSWORD -ErrorAction SilentlyContinue
        Remove-Item Env:SQLCMDPASSWORD -ErrorAction SilentlyContinue
    }
    if ($outcome.error) { break }
}
