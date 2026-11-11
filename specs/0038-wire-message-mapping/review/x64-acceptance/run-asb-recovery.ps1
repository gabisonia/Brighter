. "$PSScriptRoot/common.ps1"
$directory = "$PSScriptRoot/asb-recovery-isolated"
if (Test-Path $directory) { throw 'Evidence directory exists; refusing overwrite' }
New-Item -ItemType Directory -Path $directory | Out-Null
$project = 'tests/Paramore.Brighter.AzureServiceBus.Tests/Paramore.Brighter.AzureServiceBus.Tests.csproj'
$dotnet10 = 'C:/Program Files/dotnet/dotnet.exe'
$scenarios = @()
foreach ($queue in @($false,$true)) {
    foreach ($async in @($false,$true)) {
        foreach ($nullResult in @($false,$true)) {
            $scenarios += [PSCustomObject]@{ index = $scenarios.Count; useQueue = $queue; useAsync = $async; returnNull = $nullResult }
        }
    }
}
$configuration = Get-Content "$PSScriptRoot/../asb-sdk-dlq/Config.json" -Raw | ConvertFrom-Json
$namespace = $configuration.UserConfig.Namespaces[0]
$queueTemplate = $namespace.Queues[0] | ConvertTo-Json -Depth 12
$topicTemplate = $namespace.Topics[0] | ConvertTo-Json -Depth 12
$namespace.Queues = @(); $namespace.Topics = @()
foreach ($scenario in $scenarios) {
    $queue = $queueTemplate | ConvertFrom-Json; $queue.Name = "recovery-queue-$($scenario.index)"
    $topic = $topicTemplate | ConvertFrom-Json; $topic.Name = "recovery-topic-$($scenario.index)"
    $namespace.Queues += $queue; $namespace.Topics += $topic
}
SaveJson $configuration "$directory/Config.json"
SaveJson @{ head = $Head; experiment = 'Original approved Brighter reader-recovery cases, one process and fresh entities per theory row. Distinguish Brighter-path failure and cross-case entity contamination; no production/test edits or weakened assertions.'; scenarios = $scenarios } "$directory/plan.json"
DockerCommand @('info','--format','{{.OSType}} {{.Architecture}}') | Set-Content "$directory/host.txt"
DockerCommand @('ps','--format','{{.Names}}|{{.Image}}|{{.Ports}}') | Set-Content "$directory/containers-before.txt"
foreach ($image in @($SqlImage,$AsbImage)) {
    DockerCommand @('image','inspect',$image,'--format','{{.Architecture}} {{.Os}} {{.Id}}') | Add-Content "$directory/images.txt"
}
$results = @()
foreach ($fw in @('net9.0','net10.0')) {
    $build = RunBounded $dotnet10 @('build',$project,'-c','Release','-f',$fw,'--verbosity','minimal') "$directory/$fw-build.log" 900
    if ($build.exitCode -ne 0) { throw "ASB build failed: $fw" }
    $suffix = $fw.Replace('.','-')
    $network = "brighter-4170-recovery-isolated-$suffix-20261111"
    $db = "$network-sql"; $broker = "$network-broker"; $owned = @()
    $outcome = [ordered]@{ head = $Head; framework = $fw; sqlImage = $SqlImage; emulatorImage = $AsbImage; build = $build; cases = @(); containers = @() }
    try {
        if ((DockerCommand @('network','ls','--format','{{.Name}}')) -contains $network) { throw 'Fixture network exists; refusing reuse' }
        DockerCommand @('network','create','--label','brighter.validation=4170-recovery-isolated',$network) | Out-Null
        DockerCommand @('network','inspect',$network) | Set-Content "$directory/$fw-network.json"
        $env:ACCEPT_EULA='Y'; $env:MSSQL_PID='Developer'; $env:MSSQL_MEMORY_LIMIT_MB='2048'
        $env:MSSQL_SA_PASSWORD='Local-'+[Guid]::NewGuid().ToString('N')+'!'
        $env:SQL_SERVER='sql'; $env:SQL_WAIT_INTERVAL='15'
        $fixtureEnv=@('-e','ACCEPT_EULA','-e','MSSQL_PID','-e','MSSQL_MEMORY_LIMIT_MB','-e','MSSQL_SA_PASSWORD','-e','SQL_SERVER','-e','SQL_WAIT_INTERVAL')
        DockerCommand (@('run','-d','--pull','never','--platform','linux/amd64','--name',$db,'--network',$network,'--network-alias','sql','--label','brighter.validation=4170-recovery-isolated')+$fixtureEnv+@($SqlImage)) | Out-Null
        $owned += $db; WaitSql $db
        DockerCommand (@('run','-d','--pull','never','--platform','linux/amd64','--name',$broker,'--network',$network,'--label','brighter.validation=4170-recovery-isolated','-p','127.0.0.1::5672','-v',"${directory}/Config.json:/ServiceBus_Emulator/ConfigFiles/Config.json:ro")+$fixtureEnv+@($AsbImage)) | Out-Null
        $owned += $broker
        $watch=[Diagnostics.Stopwatch]::StartNew(); $ready=$false
        while($watch.Elapsed.TotalSeconds -lt 120) {
            if (((DockerCommand @('logs',$broker)) -join "`n") -match 'Emulator Service is Successfully Up|Emulator is running') { $ready=$true; break }
            if ((DockerCommand @('inspect',$broker,'--format','{{.State.Running}}')) -ne 'true') { throw 'Emulator exited before readiness' }
            Start-Sleep -Seconds 2
        }
        if (!$ready) { throw 'Emulator readiness exceeded 120 seconds' }
        $port=((DockerCommand @('port',$broker,'5672/tcp')) -split ':')[-1]
        $env:BrighterTestsASBConnectionString="Endpoint=sb://127.0.0.1:$port;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
        Remove-Item Env:BrighterTestsASBNameSpace -ErrorAction SilentlyContinue
        $hostDotnet = if ($fw -eq 'net9.0') { Join-Path $env:TEMP 'brighter-x64-dotnet/dotnet.exe' } else { $dotnet10 }
        $assembly=Join-Path (Get-Location) "tests/Paramore.Brighter.AzureServiceBus.Tests/bin/Release/$fw/Paramore.Brighter.AzureServiceBus.Tests.dll"
        $outcome.assemblySha256=(Get-FileHash $assembly -Algorithm SHA256).Hash
        foreach ($scenario in $scenarios) {
            $index=$scenario.index
            $env:BrighterTestsASBWireQueue="recovery-queue-$index"
            $env:BrighterTestsASBWireTopic="recovery-topic-$index"
            $env:BrighterTestsASBWireSubscription='wire-observer'
            $filter="FullyQualifiedName~AsbReaderFailureRecoveryTests&DisplayName~useQueue: $($scenario.useQueue)&DisplayName~useAsync: $($scenario.useAsync)&DisplayName~returnNull: $($scenario.returnNull)"
            $run=RunBounded $hostDotnet @('vstest',$assembly,"/TestCaseFilter:$filter","/Logger:trx;LogFileName=$fw-case-$index.trx","/ResultsDirectory:$directory",'/Blame:CollectHangDump;TestTimeout=90s;HangDumpType=mini') "$directory/$fw-case-$index.log" 150
            [xml]$trx=Get-Content "$directory/$fw-case-$index.trx" -Raw
            $rows=@($trx.TestRun.Results.UnitTestResult)
            if ($rows.Count -ne 1) { throw "Theory filter did not execute exactly one case: $($rows.Count)" }
            $row=$rows[0]
            $outcome.cases += @{ scenario=$scenario; name=$row.testName; outcome=$row.outcome; message=$row.Output.ErrorInfo.Message; stack=$row.Output.ErrorInfo.StackTrace; run=$run }
            SaveJson $outcome "$directory/$fw-results.json"
            Write-Output "$fw case $index ($($scenario.useQueue),$($scenario.useAsync),$($scenario.returnNull)): $($row.outcome)"
        }
    } catch { $outcome.error="$_"; Write-Output "Recovery execution error: $_" }
    finally {
        [array]::Reverse($owned)
        foreach ($name in $owned) { $outcome.containers += StopOwned $name $directory }
        $results += $outcome; SaveJson $results "$directory/results.json"; SaveJson $outcome "$directory/$fw-results.json"
        foreach ($variable in @('BrighterTestsASBConnectionString','BrighterTestsASBWireQueue','BrighterTestsASBWireTopic','BrighterTestsASBWireSubscription','MSSQL_SA_PASSWORD')) { Remove-Item "Env:$variable" -ErrorAction SilentlyContinue }
    }
    if ($outcome.error) { break }
}
