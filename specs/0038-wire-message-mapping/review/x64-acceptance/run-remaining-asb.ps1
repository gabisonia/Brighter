. "$PSScriptRoot/common.ps1"
$meta=Get-Content "$PSScriptRoot/docker-remaining/workspace.json" -Raw|ConvertFrom-Json
$work=$meta.workspace; $out="$($meta.output)/asb"
New-Item -ItemType Directory -Force $out|Out-Null
$project="$work/tests/Paramore.Brighter.AzureServiceBus.Tests/Paramore.Brighter.AzureServiceBus.Tests.csproj"
$dotnet10='C:/Program Files/dotnet/dotnet.exe'
$static=@(Get-ChildItem "$work/tests/Paramore.Brighter.AzureServiceBus.Tests/MessagingGateway" -File -Filter '*.cs'|Where-Object{[IO.File]::ReadAllText($_.FullName).Contains('[Collection("ASB wire mapping")]')}|ForEach-Object{[regex]::Match([IO.File]::ReadAllText($_.FullName),'public class (\w+)').Groups[1].Value}|Where-Object{$_ -ne 'AsbReaderFailureRecoveryTests'}|Sort-Object)
$managedFilter=(@($static+'AsbReaderFailureRecoveryTests')|ForEach-Object{"FullyQualifiedName!~.$_."}) -join '&'
SaveJson @{ staticClasses=$static; managedFilter=$managedFilter; reusedRecovery='asb-recovery-isolated: 6 pass/2 fail net9; 8 pass net10'; originalLostLockIncluded=$true; fixtureOnlyAdapter=$true } "$out/inventory.json"
$all=@()
foreach($fw in @('net9.0','net10.0')) {
    $build=RunBounded $dotnet10 @('build',$project,'-c','Release','-f',$fw,'--verbosity','minimal') "$out/$fw-build.log" 900 $work
    if($build.exitCode -ne 0){throw "ASB isolated build failed: $fw"}
    $hostDotnet=if($fw -eq 'net9.0'){Join-Path $env:TEMP 'brighter-x64-dotnet/dotnet.exe'}else{$dotnet10}
    $assembly="$work/tests/Paramore.Brighter.AzureServiceBus.Tests/bin/Release/$fw/Paramore.Brighter.AzureServiceBus.Tests.dll"
    $batches=@([PSCustomObject]@{label='managed';classes=@();filter=$managedFilter;seconds=1200})
    for($i=0;$i -lt $static.Count;$i+=10){$batches += [PSCustomObject]@{label="wire-$i";classes=@($static[$i..([Math]::Min($i+9,$static.Count-1))]);filter='';seconds=600}}
    foreach($batch in $batches) {
        $label="$fw-$($batch.label)"; $directory="$out/$label"
        if(Test-Path $directory){throw 'Refusing evidence overwrite'}
        New-Item -ItemType Directory $directory|Out-Null
        $config=Get-Content "$PSScriptRoot/../asb-sdk-dlq/Config.json" -Raw|ConvertFrom-Json
        $ns=$config.UserConfig.Namespaces[0];$qt=$ns.Queues[0]|ConvertTo-Json -Depth 12;$tt=$ns.Topics[0]|ConvertTo-Json -Depth 12
        $ns.Queues=@();$ns.Topics=@()
        foreach($index in 0..9){$q=$qt|ConvertFrom-Json;$q.Name="wire-q-$index";$t=$tt|ConvertFrom-Json;$t.Name="wire-t-$index";$ns.Queues += $q;$ns.Topics += $t}
        foreach($pair in @(@('Session','wire-session','session-observer'),@('Renewal','wire-renewal','renewal-observer'),@('PartialSettlement','wire-partial-session','partial-session-observer'),@('PartialAbandonment','wire-partial-abandon','partial-abandon-observer'),@('BudgetRecovery','wire-budget','budget-observer'))) {
            $q=$qt|ConvertFrom-Json;$q.Name="$($pair[1])-queue";$q.Properties.RequiresSession=$true;$q.Properties.LockDuration='PT5S'
            $t=$tt|ConvertFrom-Json;$t.Name="$($pair[1])-topic";$t.Subscriptions[0].Name=$pair[2];$t.Subscriptions[0].Properties.RequiresSession=$true;$t.Subscriptions[0].Properties.LockDuration='PT5S'
            $ns.Queues += $q;$ns.Topics += $t
            Set-Item "Env:BrighterTestsASB$($pair[0])Queue" $q.Name
            Set-Item "Env:BrighterTestsASB$($pair[0])Topic" $t.Name
            Set-Item "Env:BrighterTestsASB$($pair[0])Subscription" $pair[2]
        }
        SaveJson $config "$directory/Config.json"
        $network='brighter-4170-rem-'+$label.Replace('.','-');$db="$network-sql";$broker="$network-broker";$owned=@()
        $result=[ordered]@{head=$Head;framework=$fw;batch=$batch.label;build=$build;cases=@();runs=@();containers=@()}
        try {
            if((DockerCommand @('network','ls','--format','{{.Name}}')) -contains $network){throw 'Network already exists'}
            DockerCommand @('network','create','--label','brighter.validation=4170-remaining',$network)|Out-Null
            $env:ACCEPT_EULA='Y';$env:MSSQL_PID='Developer';$env:MSSQL_MEMORY_LIMIT_MB='2048';$env:MSSQL_SA_PASSWORD='Local-'+[Guid]::NewGuid().ToString('N')+'!';$env:SQL_SERVER='sql';$env:SQL_WAIT_INTERVAL='15'
            $fe=@('-e','ACCEPT_EULA','-e','MSSQL_PID','-e','MSSQL_MEMORY_LIMIT_MB','-e','MSSQL_SA_PASSWORD','-e','SQL_SERVER','-e','SQL_WAIT_INTERVAL')
            DockerCommand (@('run','-d','--pull','never','--platform','linux/amd64','--name',$db,'--network',$network,'--network-alias','sql','--label','brighter.validation=4170-remaining')+$fe+@($SqlImage))|Out-Null
            $owned += $db;WaitSql $db
            DockerCommand (@('run','-d','--pull','never','--platform','linux/amd64','--name',$broker,'--network',$network,'--label','brighter.validation=4170-remaining','-p','127.0.0.1::5672','-p','127.0.0.1::5300','-v',"${directory}/Config.json:/ServiceBus_Emulator/ConfigFiles/Config.json:ro")+$fe+@($AsbImage))|Out-Null
            $owned += $broker
            $watch=[Diagnostics.Stopwatch]::StartNew();$ready=$false
            while($watch.Elapsed.TotalSeconds -lt 120){if(((DockerCommand @('logs',$broker))-join "`n") -match 'Emulator Service is Successfully Up|Emulator is running'){$ready=$true;break};Start-Sleep -Seconds 2}
            if(!$ready){throw 'Emulator readiness deadline exceeded'}
            $port=((DockerCommand @('port',$broker,'5672/tcp'))-split ':')[-1];$adminPort=((DockerCommand @('port',$broker,'5300/tcp'))-split ':')[-1]
            $env:BrighterTestsASBConnectionString="Endpoint=sb://127.0.0.1:$port;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
            $env:BrighterTestsASBAdministrationConnectionString="Endpoint=sb://127.0.0.1:$adminPort;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
            Remove-Item Env:BrighterTestsASBNameSpace -ErrorAction SilentlyContinue
            $result.managementHealth=(Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$adminPort/health" -TimeoutSec 15).StatusCode
            $classes=if($batch.label -eq 'managed'){@('managed')}else{$batch.classes}
            $index=0
            foreach($class in $classes) {
                $env:BrighterTestsASBWireQueue="wire-q-$index";$env:BrighterTestsASBWireTopic="wire-t-$index";$env:BrighterTestsASBWireSubscription='wire-observer'
                $filter=if($class -eq 'managed'){$batch.filter}else{"FullyQualifiedName~.$class."}
                $run=RunBounded $hostDotnet @('vstest',$assembly,"/TestCaseFilter:$filter","/Logger:trx;LogFileName=$class.trx","/ResultsDirectory:$directory",'/Blame:CollectHangDump;TestTimeout=120s;HangDumpType=mini') "$directory/$class.log" $batch.seconds $work
                $result.runs += @{class=$class;run=$run}
                [xml]$trx=Get-Content "$directory/$class.trx" -Raw
                foreach($row in @($trx.TestRun.Results.UnitTestResult)){$result.cases += @{name=$row.testName;outcome=$row.outcome;message=$row.Output.ErrorInfo.Message;stack=$row.Output.ErrorInfo.StackTrace}}
                SaveJson $result "$directory/results.json"
                Write-Output "$label $class : $(@($trx.TestRun.Results.UnitTestResult|Where-Object outcome -eq Passed).Count) passed, $(@($trx.TestRun.Results.UnitTestResult|Where-Object outcome -eq Failed).Count) failed"
                $index++
            }
        } catch {$result.error="$_";Write-Output "$label setup/execution error: $_"}
        finally {
            [array]::Reverse($owned);foreach($name in $owned){$result.containers += StopOwned $name $directory}
            SaveJson $result "$directory/results.json";$all += $result;SaveJson $all "$out/results.json"
        }
    }
}
