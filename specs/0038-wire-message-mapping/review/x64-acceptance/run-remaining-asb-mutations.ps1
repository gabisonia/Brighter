. "$PSScriptRoot/common.ps1"
$meta=Get-Content "$PSScriptRoot/docker-remaining/workspace.json" -Raw|ConvertFrom-Json
$work=$meta.workspace;$out="$($meta.output)/asb-mutations"
if(Test-Path $out){throw 'Refusing mutation evidence overwrite'}
$completed=Get-Content "$($meta.output)/asb/net10.0-wire-20/results.json" -Raw|ConvertFrom-Json
if($completed.containers.Count -ne 2 -or @($completed.containers|Where-Object runningAfterStop -ne 'false').Count){throw 'Broad ASB matrix must finish and stop before mutating isolated binaries'}
New-Item -ItemType Directory $out|Out-Null
$source="$work/src/Paramore.Brighter.MessagingGateway.AzureServiceBus/AzureServiceBusConsumer.cs"
$original=[IO.File]::ReadAllBytes($source);$originalText=[Text.Encoding]::UTF8.GetString($original);$utf8=New-Object Text.UTF8Encoding($false)
[IO.File]::WriteAllBytes("$out/AzureServiceBusConsumer.original.cs",$original)
$originalHash=(Get-FileHash $source -Algorithm SHA256).Hash
$project="$work/tests/Paramore.Brighter.AzureServiceBus.Tests/Paramore.Brighter.AzureServiceBus.Tests.csproj"
$assembly="$work/tests/Paramore.Brighter.AzureServiceBus.Tests/bin/Release/net10.0/Paramore.Brighter.AzureServiceBus.Tests.dll"
$scenarios=(Get-Content "$PSScriptRoot/asb-recovery-isolated/plan.json" -Raw|ConvertFrom-Json).scenarios
$variants=@('reader-exception-escapes','null-reader-not-fallback','ordinary-event-control','discard-fallback-body','empty-fallback-id','empty-lock-token','negative-sequence','omit-dead-letter','forget-reader-after-failure','omit-completion')
function ReplaceOne([string]$Text,[string]$Before,[string]$After){if(([regex]::Matches($Text,[regex]::Escape($Before))).Count -ne 1){throw "Nonunique mutation target: $Before"};return $Text.Replace($Before,$After)}
function Mutate([int]$Index){
    $text=$originalText
    switch($Index){
        0 {$text=ReplaceOne $text 'Message message;' ("var selectedBeforeCatch = reader.ReadMessage(nativeDelivery.NativeMessage);`n        Message message;");$text=ReplaceOne $text 'message = reader.ReadMessage(nativeDelivery.NativeMessage)' 'message = selectedBeforeCatch'}
        1 {$text=ReplaceOne $text '?? throw new InvalidOperationException("The wire message reader returned null.");' '?? null!;'}
        2 {$text=ReplaceOne $text 'message = Message.FailureMessage(new RoutingKey(Topic), new Id(delivery.Id));' 'message = new Message(new MessageHeader(delivery.Id, new RoutingKey(Topic), MessageType.MT_EVENT), new MessageBody(Array.Empty<byte>()));'}
        3 {$text=ReplaceOne $text 'message.Body = new MessageBody(delivery.MessageBodyMemory);' 'message.Body = new MessageBody(Array.Empty<byte>());'}
        4 {$text=ReplaceOne $text 'Message.FailureMessage(new RoutingKey(Topic), new Id(delivery.Id))' 'Message.FailureMessage(new RoutingKey(Topic), Id.Empty)'}
        5 {$text=ReplaceOne $text 'message.Header.Bag[ASBConstants.LockTokenHeaderBagKey] = delivery.LockToken;' 'message.Header.Bag[ASBConstants.LockTokenHeaderBagKey] = Guid.Empty.ToString();'}
        6 {$text=ReplaceOne $text 'message.Header.Bag[ASBConstants.SequenceNumberBagKey] = delivery.SequenceNumber;' 'message.Header.Bag[ASBConstants.SequenceNumberBagKey] = -1L;'}
        7 {$text=ReplaceOne $text 'await ServiceBusReceiver!.DeadLetterAsync(lockToken, reasonString, description);' 'await Task.CompletedTask;'}
        8 {$text=ReplaceOne $text 'message.Body = new MessageBody(delivery.MessageBodyMemory);' ("message.Body = new MessageBody(delivery.MessageBodyMemory);`n            Subscription.WireMessageReader = null;")}
        9 {$text=ReplaceOne $text 'await ServiceBusReceiver!.CompleteAsync(lockToken);' 'await Task.CompletedTask;'}
    }
    return $text
}
function RunCases([string]$Phase,[string]$Directory){
    $cases=@()
    foreach($scenario in $scenarios){
        $index=$scenario.index;$prefix=if($Phase -eq 'mutated'){'m'}else{'b'}
        $env:BrighterTestsASBWireQueue="$prefix-q-$index";$env:BrighterTestsASBWireTopic="$prefix-t-$index";$env:BrighterTestsASBWireSubscription='wire-observer'
        $filter="FullyQualifiedName~AsbReaderFailureRecoveryTests&DisplayName~useQueue: $($scenario.useQueue)&DisplayName~useAsync: $($scenario.useAsync)&DisplayName~returnNull: $($scenario.returnNull)"
        $run=RunBounded 'C:/Program Files/dotnet/dotnet.exe' @('vstest',$assembly,"/TestCaseFilter:$filter","/Logger:trx;LogFileName=$Phase-$index.trx","/ResultsDirectory:$Directory",'/Blame:CollectHangDump;TestTimeout=90s;HangDumpType=mini') "$Directory/$Phase-$index.log" 150 $work
        [xml]$trx=Get-Content "$Directory/$Phase-$index.trx" -Raw;$rows=@($trx.TestRun.Results.UnitTestResult)
        if($rows.Count -ne 1){throw 'Mutation filter did not execute exactly one case'}
        $row=$rows[0];$cases += [PSCustomObject]@{scenario=$scenario;name=$row.testName;outcome=$row.outcome;message=$row.Output.ErrorInfo.Message;stack=$row.Output.ErrorInfo.StackTrace;run=$run}
    }
    return $cases
}
$all=@()
try {
    for($variant=0;$variant -lt $variants.Count;$variant++){
        $label=$variants[$variant];$directory="$out/$variant-$label";New-Item -ItemType Directory $directory|Out-Null
        $result=[ordered]@{head=$Head;framework='net10.0';variant=$label;index=$variant;sourceBefore=$originalHash;baseline=@();mutated=@();containers=@();valid=$false};$owned=@();$mutated=$false
        try {
            if((Get-FileHash $source -Algorithm SHA256).Hash -ne $originalHash){throw 'Isolated production input not restored'}
            $config=Get-Content "$PSScriptRoot/../asb-sdk-dlq/Config.json" -Raw|ConvertFrom-Json;$ns=$config.UserConfig.Namespaces[0]
            $qt=$ns.Queues[0]|ConvertTo-Json -Depth 12;$tt=$ns.Topics[0]|ConvertTo-Json -Depth 12;$ns.Queues=@();$ns.Topics=@()
            foreach($phase in @('b','m')){foreach($i in 0..7){$q=$qt|ConvertFrom-Json;$q.Name="$phase-q-$i";$t=$tt|ConvertFrom-Json;$t.Name="$phase-t-$i";$ns.Queues += $q;$ns.Topics += $t}}
            SaveJson $config "$directory/Config.json"
            $network="brighter-4170-rem-mutation-$variant";$db="$network-sql";$broker="$network-broker"
            if((DockerCommand @('network','ls','--format','{{.Name}}')) -contains $network){throw 'Mutation fixture exists'}
            DockerCommand @('network','create','--label','brighter.validation=4170-remaining',$network)|Out-Null
            $env:ACCEPT_EULA='Y';$env:MSSQL_PID='Developer';$env:MSSQL_MEMORY_LIMIT_MB='2048';$env:MSSQL_SA_PASSWORD='Local-'+[Guid]::NewGuid().ToString('N')+'!';$env:SQL_SERVER='sql';$env:SQL_WAIT_INTERVAL='15'
            $fe=@('-e','ACCEPT_EULA','-e','MSSQL_PID','-e','MSSQL_MEMORY_LIMIT_MB','-e','MSSQL_SA_PASSWORD','-e','SQL_SERVER','-e','SQL_WAIT_INTERVAL')
            DockerCommand (@('run','-d','--pull','never','--platform','linux/amd64','--name',$db,'--network',$network,'--network-alias','sql','--label','brighter.validation=4170-remaining')+$fe+@($SqlImage))|Out-Null
            $owned += $db;WaitSql $db
            DockerCommand (@('run','-d','--pull','never','--platform','linux/amd64','--name',$broker,'--network',$network,'--label','brighter.validation=4170-remaining','-p','127.0.0.1::5672','-v',"${directory}/Config.json:/ServiceBus_Emulator/ConfigFiles/Config.json:ro")+$fe+@($AsbImage))|Out-Null
            $owned += $broker;$watch=[Diagnostics.Stopwatch]::StartNew();$ready=$false
            while($watch.Elapsed.TotalSeconds -lt 120){if(((DockerCommand @('logs',$broker))-join "`n") -match 'Emulator Service is Successfully Up|Emulator is running'){$ready=$true;break};Start-Sleep -Seconds 2}
            if(!$ready){throw 'Mutation emulator readiness exceeded 120 seconds'}
            $port=((DockerCommand @('port',$broker,'5672/tcp'))-split ':')[-1]
            $env:BrighterTestsASBConnectionString="Endpoint=sb://127.0.0.1:$port;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
            $env:BrighterTestsASBAdministrationConnectionString=$env:BrighterTestsASBConnectionString
            Remove-Item Env:BrighterTestsASBNameSpace -ErrorAction SilentlyContinue
            $result.baseline=@(RunCases 'baseline' $directory)
            if(@($result.baseline|Where-Object outcome -ne Passed).Count){throw 'Fixture did not establish eight healthy original cases; mutation campaign stops without retry'}
            $text=Mutate $variant;[IO.File]::WriteAllText($source,$text,$utf8);$mutated=$true
            [IO.File]::WriteAllText("$directory/AzureServiceBusConsumer.mutated.cs",$text,$utf8)
            $result.mutatedSourceSha256=(Get-FileHash $source -Algorithm SHA256).Hash
            $result.build=RunBounded 'C:/Program Files/dotnet/dotnet.exe' @('build',$project,'-c','Release','-f','net10.0','--no-restore','--verbosity','minimal') "$directory/build.log" 600 $work
            if($result.build.exitCode -ne 0){throw 'Mutation compile error is not behavioral RED'}
            $result.mutated=@(RunCases 'mutated' $directory)
            $invalid=@()
            $brokerLines=@(DockerCommand @('logs',$broker))
            foreach($case in $result.mutated){
                $shouldFail=if($variant -eq 0){!$case.scenario.returnNull}elseif($variant -eq 1){$case.scenario.returnNull}else{$true}
                $pattern=switch($variant){0{'Deliberate wire reader failure'}1{'NullReferenceException'}2{'MT_UNACCEPTABLE'}3{'Assert.Equal'}4{'Assert.Equal'}5{'Assert.Single|LockToken'}6{'-1'}7{'Assert.IsType'}8{'mapped-'}9{'Assert.Null'}}
                if($shouldFail){if($case.outcome -ne 'Failed' -or $case.message -notmatch $pattern){$invalid += $case.name}}
                elseif($case.outcome -ne 'Passed'){$invalid += $case.name}
                if($variant -eq 7){
                    $entity=if($case.scenario.useQueue){"m-q-$($case.scenario.index)"}else{"m-t-$($case.scenario.index)"}
                    if(@($brokerLines|Where-Object{$_ -match 'Cannot add the same item' -and $_.Contains($entity)}).Count){$invalid += $case.name}
                }
            }
            $result.unattributed=$invalid;$result.valid=($invalid.Count -eq 0)
            if(!$result.valid){throw 'Mutation did not produce only attributable expected failures; stop without weakening assertions'}
            Write-Output "$label : $(@($result.mutated|Where-Object outcome -eq Failed).Count) intended failures verified"
        } catch {$result.error="$_";Write-Output "Mutation $label stopped: $_"}
        finally {
            [IO.File]::WriteAllBytes($source,$original);$result.restoredSourceSha256=(Get-FileHash $source -Algorithm SHA256).Hash
            if($mutated){
                $result.restoreBuild=RunBounded 'C:/Program Files/dotnet/dotnet.exe' @('build',$project,'-c','Release','-f','net10.0','--no-restore','--verbosity','minimal') "$directory/restore-build.log" 600 $work
                if($result.restoreBuild.exitCode -eq 0 -and @($result.baseline|Where-Object outcome -ne Passed).Count -eq 0){
                    $result.restoredCases=@(RunCases 'restored' $directory)
                    if(@($result.restoredCases|Where-Object outcome -ne Passed).Count){$result.valid=$false;$result.error='Restored source did not return all eight cases to green; no retry'}
                }else{$result.valid=$false;$result.error='Restoration build did not succeed'}
            }
            [array]::Reverse($owned);foreach($name in $owned){$result.containers += StopOwned $name $directory}
            SaveJson $result "$directory/results.json";$all += $result;SaveJson $all "$out/results.json"
        }
        if($result.error){break}
    }
} finally {[IO.File]::WriteAllBytes($source,$original)}
