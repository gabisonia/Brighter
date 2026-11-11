. "$PSScriptRoot/common.ps1"
$meta=Get-Content "$PSScriptRoot/docker-remaining/workspace.json" -Raw|ConvertFrom-Json
$work=$meta.workspace;$out="$($meta.output)/gcp"
if(Test-Path $out){throw 'Refusing GCP evidence overwrite'}
New-Item -ItemType Directory $out|Out-Null
$image='gcr.io/google.com/cloudsdktool/cloud-sdk@sha256:3797a874fae6b75aa25260b5bd40fa0397c2f1102f18ae3ac7dc887732fbe449'
$filter='FullyQualifiedName~Paramore.Brighter.Gcp.Tests.MessagingGateway.'
SaveJson @{head=$Head;filter=$filter;image=$image;hypothesis='Native x64 Pub/Sub lifecycle coverage with correlated stream input/deadline-zero/replacement evidence. One full matrix per framework, no green-seeking retry.';excluded='Firestore, Spanner and GCS are different adapters, outside the Pub/Sub wire-mapping gate; no hosted credentials used';productionChanges=@();assertionsChanged=$false} "$out/plan.json"
$all=@()
foreach($fw in @('net9.0','net10.0')) {
    $directory="$out/$fw";New-Item -ItemType Directory $directory|Out-Null
    $name='brighter-4170-rem-gcp-'+$fw.Replace('.','-');$owned=$false
    $result=[ordered]@{head=$Head;framework=$fw;image=$image;cases=@()}
    try {
        $existing=DockerCommand @('ps','-a','--format','{{.Names}}');if($existing -contains $name){throw 'Fixture already exists'}
        DockerCommand @('run','-d','--pull','never','--platform','linux/amd64','--name',$name,'--label','brighter.validation=4170-remaining','-p','127.0.0.1::8085',$image,'gcloud','beta','emulators','pubsub','start','--project=brighter-docker4170','--host-port=0.0.0.0:8085','--quiet')|Out-Null
        $owned=$true;$watch=[Diagnostics.Stopwatch]::StartNew();$ready=$false
        while($watch.Elapsed.TotalSeconds -lt 120){if(((DockerCommand @('logs',$name))-join "`n") -match 'Server started, listening on'){$ready=$true;break};if((DockerCommand @('inspect',$name,'--format','{{.State.Running}}')) -ne 'true'){throw 'GCP emulator exited'};Start-Sleep -Seconds 2}
        if(!$ready){throw 'GCP readiness exceeded 120 seconds'}
        $port=((DockerCommand @('port',$name,'8085/tcp'))-split ':')[-1]
        $env:PUBSUB_EMULATOR_HOST="127.0.0.1:$port";$env:GOOGLE_CLOUD_PROJECT='brighter-docker4170';$env:BRIGHTER_GCP_NATIVE_TRACE="$directory/stream-native.log"
        Remove-Item Env:GOOGLE_APPLICATION_CREDENTIALS -ErrorAction SilentlyContinue
        $hostDotnet=if($fw -eq 'net9.0'){Join-Path $env:TEMP 'brighter-x64-dotnet/dotnet.exe'}else{'C:/Program Files/dotnet/dotnet.exe'}
        $assembly="$work/tests/Paramore.Brighter.Gcp.Tests/bin/Release/$fw/Paramore.Brighter.Gcp.Tests.dll"
        $result.assemblySha256=(Get-FileHash $assembly -Algorithm SHA256).Hash
        $result.run=RunBounded $hostDotnet @('vstest',$assembly,"/TestCaseFilter:$filter",'/Logger:trx;LogFileName=results.trx','/Logger:console;verbosity=normal',"/ResultsDirectory:$directory",'/Blame:CollectHangDump;TestTimeout=180s;HangDumpType=mini') "$directory/console.log" 1800 $work
        [xml]$trx=Get-Content "$directory/results.trx" -Raw
        foreach($row in @($trx.TestRun.Results.UnitTestResult)){$result.cases += @{name=$row.testName;outcome=$row.outcome;message=$row.Output.ErrorInfo.Message;stack=$row.Output.ErrorInfo.StackTrace}}
        Write-Output "$fw Pub/Sub: $(@($result.cases|Where-Object outcome -eq Passed).Count) passed, $(@($result.cases|Where-Object outcome -eq Failed).Count) failed, $(@($result.cases|Where-Object outcome -eq NotExecuted).Count) skipped"
    } catch {$result.error="$_";Write-Output "$fw GCP setup/execution error: $_"}
    finally {
        if($owned){$env:MSSQL_SA_PASSWORD='nonexistent-local-secret';$result.container=StopOwned $name $directory}
        SaveJson $result "$directory/results.json";$all += $result;SaveJson $all "$out/results.json"
        foreach($variable in @('PUBSUB_EMULATOR_HOST','GOOGLE_CLOUD_PROJECT','BRIGHTER_GCP_NATIVE_TRACE')){Remove-Item "Env:$variable" -ErrorAction SilentlyContinue}
    }
}
