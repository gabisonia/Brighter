param([switch]$PassiveOnly,[string]$OutputName='kafka',[string[]]$Frameworks=@('net9.0','net10.0'),[string]$FilterManifest)
. "$PSScriptRoot/common.ps1"
$meta=Get-Content "$PSScriptRoot/docker-remaining/workspace.json" -Raw|ConvertFrom-Json
$diagnostics=Get-Content "$PSScriptRoot/docker-remaining/diagnostics.json" -Raw|ConvertFrom-Json
$images=Get-Content "$PSScriptRoot/docker-remaining/images/manifest.json" -Raw|ConvertFrom-Json
$out="$($meta.output)/$OutputName";$work=$meta.workspace;$hook=$diagnostics.hook
$selection=if($FilterManifest){Get-Content $FilterManifest -Raw|ConvertFrom-Json}else{$null}
if(Test-Path $out){throw 'Refusing Kafka evidence overwrite'}
New-Item -ItemType Directory $out|Out-Null
$brokerImage=($images|Where-Object name -eq kafka).id;$schemaImage=($images|Where-Object name -eq schema).id
$net9Image=($images|Where-Object name -eq net9).id
$net10Image=(Get-Content "$PSScriptRoot/docker-remaining/images/net10-cached-image.json" -Raw|ConvertFrom-Json).Id
SaveJson @{head=$Head;brokerImage=$brokerImage;schemaImage=$schemaImage;frameworks=$Frameworks;passiveOnly=$PassiveOnly.IsPresent;hypothesis='Native x64 lifecycle acceptance with direct native stderr and a 60-second pending-close trigger. Passive control removes the demonstrated intrusive healthy-reference pause; no green-seeking repetitions.';productionChanges=@();assertionsChanged=$false;schemaNetwork='Shares owned broker network namespace so hardcoded localhost:8081 resolves locally without publishing host ports'} "$out/plan.json"
$all=@();$env:MSSQL_SA_PASSWORD='nonexistent-local-secret'
foreach($fw in $Frameworks) {
    $directory="$out/$fw";New-Item -ItemType Directory $directory|Out-Null
    if($selection){
        $escaped=[Security.SecurityElement]::Escape([string]$selection.filter)
        "<RunSettings><RunConfiguration><TestCaseFilter>$escaped</TestCaseFilter></RunConfiguration></RunSettings>"|Set-Content "$directory/selection.runsettings" -Encoding UTF8
    }
    $prefix="brighter-4170-rem-$OutputName-"+$fw.Replace('.','-');$broker="$prefix-broker";$schema="$prefix-schema";$test="$prefix-tests";$owned=@()
    $result=[ordered]@{head=$Head;framework=$fw;cases=@();captures=@();containers=@();pendingCloses=@{};completedCloses=0}
    try {
        $existing=DockerCommand @('ps','-a','--format','{{.Names}}');foreach($name in @($broker,$schema,$test)){if($existing -contains $name){throw 'Fixture name exists'}}
        DockerCommand @('run','-d','--pull','never','--platform','linux/amd64','--name',$broker,'--label','brighter.validation=4170-remaining',
            '-e','KAFKA_NODE_ID=1','-e','KAFKA_PROCESS_ROLES=broker,controller','-e','KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
            '-e','CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk','-e','KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093',
            '-e','KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092','-e','KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',
            '-e','KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER','-e','KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT','-e','KAFKA_AUTO_CREATE_TOPICS_ENABLE=false',
            '-e','KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1','-e','KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1','-e','KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1',
            '-e','KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0',$brokerImage)|Out-Null
        $owned += $broker;$watch=[Diagnostics.Stopwatch]::StartNew();$ready=$false
        while($watch.Elapsed.TotalSeconds -lt 180){
            $old=$ErrorActionPreference;$ErrorActionPreference='Continue';$answer=& $Docker exec $broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1;$code=$LASTEXITCODE;$ErrorActionPreference=$old
            if($code -eq 0){$ready=$true;break};Start-Sleep -Seconds 2
        }
        if(!$ready){throw 'Kafka readiness exceeded 180 seconds'}
        DockerCommand @('run','-d','--pull','never','--platform','linux/amd64','--name',$schema,'--network',"container:$broker",'--label','brighter.validation=4170-remaining',
            '-e','SCHEMA_REGISTRY_HOST_NAME=localhost','-e','SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://localhost:9092',
            '-e','SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081','-e','SCHEMA_REGISTRY_HEAP_OPTS=-Xms128m -Xmx256m',$schemaImage)|Out-Null
        $owned += $schema;$watch.Restart();$ready=$false
        while($watch.Elapsed.TotalSeconds -lt 180){
            $old=$ErrorActionPreference;$ErrorActionPreference='Continue';$answer=& $Docker exec $schema curl -fsS http://localhost:8081/subjects 2>&1;$code=$LASTEXITCODE;$ErrorActionPreference=$old
            if($code -eq 0){$ready=$true;break};Start-Sleep -Seconds 2
        }
        if(!$ready){throw 'Schema registry readiness exceeded 180 seconds'}
        $runtimeImage=if($fw -eq 'net9.0'){$net9Image}else{$net10Image}
        DockerCommand @('run','--rm','--pull','never','--platform','linux/amd64','--label','brighter.validation=4170-remaining',$runtimeImage,'dotnet','--info')|Set-Content "$directory/runtime.txt"
        $assembly="/workspace/tests/Paramore.Brighter.Kafka.Tests/bin/Release/$fw/Paramore.Brighter.Kafka.Tests.dll"
        $testArguments=@('run','-d','--pull','never','--platform','linux/amd64','--cap-add','SYS_PTRACE','--ulimit','core=0','--name',$test,'--network',"container:$broker",'--label','brighter.validation=4170-remaining',
            '-v',"${directory}:/capture",'-v',"${work}:/workspace:ro",'-v',"${hook}/bin/Release/net9.0:/hook:ro",'-w','/workspace',
            '-e','DOTNET_STARTUP_HOOKS=/hook/NativeCapture.dll','-e','BRIGHTER_NATIVE_CAPTURE=/capture',$runtimeImage,'dotnet','vstest',$assembly,
            '/Logger:trx;LogFileName=results.trx','/Logger:console;verbosity=normal','/ResultsDirectory:/capture','/Blame:CollectHangDump;TestTimeout=5m;HangDumpType=mini')
        if($selection){$testArguments += '/Settings:/capture/selection.runsettings'}
        DockerCommand $testArguments|Out-Null
        $owned += $test;$watch.Restart();$seen=@{};$pending=@{};$closed=0;$healthyCaptured=$PassiveOnly.IsPresent;$stallCaptured=$false
        while((DockerCommand @('inspect',$test,'--format','{{.State.Running}}')) -eq 'true') {
            if($watch.Elapsed.TotalSeconds -gt 2100){$result.deadlineExceeded=$true;DockerCommand @('stop','-t','10',$test)|Out-Null;break}
            foreach($file in @(Get-ChildItem $directory -Filter 'native-stderr-*.txt')) {
                $lines=@(Get-Content $file.FullName);$offset=if($seen.ContainsKey($file.Name)){$seen[$file.Name]}else{0}
                for($n=$offset;$n -lt $lines.Count;$n++){
                    if($lines[$n] -match '^%\d\|[^|]+\|CLOSE\|([^|]+)\|.*: (Closing consumer|Consumer closed)$'){
                        $client=$Matches[1];if($Matches[2] -eq 'Closing consumer'){if(!$pending.ContainsKey($client)){$pending[$client]=$watch.Elapsed.TotalSeconds}}else{$pending.Remove($client);$closed++}
                    }
                }
                $seen[$file.Name]=$lines.Count
            }
            $overdue=@($pending.Keys|Where-Object{$watch.Elapsed.TotalSeconds-$pending[$_] -ge 60})
            $hostFiles=@(Get-ChildItem $directory -Filter 'testhost-*.json')
            $nativeFiles=@(Get-ChildItem $directory -Filter 'native-stderr-*.txt')
            $captureLabel=$null
            if($overdue.Count -gt 0 -and !$stallCaptured){$captureLabel='pending-close';$stallCaptured=$true}
            elseif(!$healthyCaptured -and $hostFiles.Count -eq 1 -and $nativeFiles.Count -eq 1 -and $nativeFiles[0].Length -gt 2048){$captureLabel='healthy-reference';$healthyCaptured=$true}
            if($captureLabel -and $hostFiles.Count -eq 1){
                $hostInfo=Get-Content $hostFiles[0].FullName -Raw|ConvertFrom-Json;$targetPid=$hostInfo.pid
                DockerCommand @('exec',$test,'cat',"/proc/$targetPid/maps")|Set-Content "$directory/$captureLabel-maps.txt"
                $captureDestination=if($PassiveOnly){"/tmp/$captureLabel.core"}else{"/capture/$captureLabel.core"}
                $captureArguments=@('exec',$test,'timeout','150s',"$($hostInfo.runtime)createdump",'--crashreport','--name',$captureDestination)
                if($captureLabel -eq 'healthy-reference'){$captureArguments += '--crashreportonly'}else{$captureArguments += '--full'}
                $captureArguments += "$targetPid"
                $result.pendingCloses=$pending;$result.completedCloses=$closed
                SaveJson $result "$directory/results.json"
                try {
                    $capture=RunBounded $Docker $captureArguments "$directory/$captureLabel-createdump.log" 180
                    $result.captures += @{label=$captureLabel;run=$capture;pendingClients=$overdue;destination=$captureDestination}
                }catch{$result.captures += @{label=$captureLabel;error="$_";pendingClients=$overdue;destination=$captureDestination}}
                SaveJson $result "$directory/results.json"
            }
            Start-Sleep -Seconds 5
        }
        $pending=@{};$closed=0
        foreach($file in @(Get-ChildItem $directory -Filter 'native-stderr-*.txt')) {
            foreach($line in Get-Content $file.FullName) {
                if($line -match '^%\d\|[^|]+\|CLOSE\|([^|]+)\|.*: (Closing consumer|Consumer closed)$') {
                    $client=$Matches[1];if($Matches[2] -eq 'Closing consumer'){$pending[$client]=$line}else{$pending.Remove($client);$closed++}
                }
            }
        }
        $state=DockerCommand @('inspect',$test,'--format','{{json .State}}')|ConvertFrom-Json
        $result.exitCode=$state.ExitCode;$result.seconds=$watch.Elapsed.TotalSeconds;$result.pendingCloses=$pending;$result.completedCloses=$closed
        if($PassiveOnly -and $stallCaptured){
            $coreRoot=Join-Path $env:TEMP 'brighter-4170-docker-native-captures'
            New-Item -ItemType Directory -Force $coreRoot|Out-Null
            $core=Join-Path $coreRoot "$OutputName-$fw-pending-close.core"
            $copy=RunBounded $Docker @('cp',"${test}:/tmp/pending-close.core",$core) "$directory/core-copy.log" 1200
            $result.coreCopy=@{run=$copy;path=$core}
            $copy=RunBounded $Docker @('cp',"${test}:/tmp/pending-close.core.crashreport.json","$directory/pending-close.core.crashreport.json") "$directory/report-copy.log" 120
        }
        if(Test-Path "$directory/results.trx"){
            [xml]$trx=Get-Content "$directory/results.trx" -Raw
            foreach($row in @($trx.TestRun.Results.UnitTestResult)){$result.cases += @{name=[string]$row.testName;outcome=[string]$row.outcome;message=[string]$row.Output.ErrorInfo.Message;stack=[string]$row.Output.ErrorInfo.StackTrace}}
        }
        Write-Output "$fw Kafka: $(@($result.cases|Where-Object outcome -eq Passed).Count) passed, $(@($result.cases|Where-Object outcome -eq Failed).Count) failed; $closed native closes observed"
    } catch {$result.error="$_";Write-Output "$fw Kafka setup/execution error: $_"}
    finally {
        [array]::Reverse($owned);foreach($name in $owned){$result.containers += StopOwned $name $directory}
        # Raw TRX is authoritative. Avoid serializing XML-adapted row objects,
        # which caused unbounded PowerShell JSON traversal after earlier runs.
        $summary=@{framework=$fw;exitCode=$result.exitCode;seconds=$result.seconds;completedCases=$result.cases.Count;error=$result.error;containers=$result.containers;rawTrx="$directory/results.trx"}
        SaveJson $summary "$directory/runner-summary.json";$all += $summary;SaveJson $all "$out/results.json"
    }
}
