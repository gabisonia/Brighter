. "$PSScriptRoot/common.ps1"
$meta=Get-Content "$PSScriptRoot/docker-remaining/workspace.json" -Raw|ConvertFrom-Json
$work=$meta.workspace;$out=$meta.output;$utf8=New-Object Text.UTF8Encoding($false)
$provider="$work/tests/Paramore.Brighter.Gcp.Tests/MessagingGateway/GcpStreamMessageGatewayProvider.cs"
$text=[IO.File]::ReadAllText($provider)
$text=$text.Replace('public GcpStreamMessageGatewayProvider()' + "`n    {",'public GcpStreamMessageGatewayProvider()' + "`n    {`n        var diagnosticProvider = Guid.NewGuid().ToString(""N"");`n        var diagnosticGeneration = 0;")
$needle="StreamConfiguration = cfg =>`n            {`n                cfg.EmulatorDetection = EmulatorDetection.EmulatorOrProduction;"
$addition=@'
StreamConfiguration = cfg =>
            {
                cfg.EmulatorDetection = EmulatorDetection.EmulatorOrProduction;
                var generation = System.Threading.Interlocked.Increment(ref diagnosticGeneration);
                cfg.ApiSettings ??= new Google.Cloud.PubSub.V1.SubscriberServiceApiSettings();
                cfg.ApiSettings.Interceptor = new GcpStreamRpcObserver(line =>
                {
                    lock (typeof(GcpStreamMessageGatewayProvider))
                    {
                        System.IO.File.AppendAllText(Environment.GetEnvironmentVariable("BRIGHTER_GCP_NATIVE_TRACE")!,
                            "provider=" + diagnosticProvider + " generation=" + generation + " " + line + Environment.NewLine);
                    }
                });
'@
if(!$text.Contains($needle)){throw 'GCP diagnostic insertion target not found'}
$text=$text.Replace($needle,$addition)
[IO.File]::WriteAllText($provider,$text,$utf8)
$credential="$work/tests/Paramore.Brighter.Gcp.Tests/Helper/GatewayFactory.cs"
$text=[IO.File]::ReadAllText($credential).Replace('GoogleCredential.GetApplicationDefault()','GoogleCredential.FromAccessToken("docker-emulator-only")')
[IO.File]::WriteAllText($credential,$text,$utf8)
$kafka="$work/tests/Paramore.Brighter.Kafka.Tests/MessagingGateway/Reactor/When_committing_offsets_during_revoke_should_not_race.cs"
$text=[IO.File]::ReadAllText($kafka).Replace('Name = "Kafka Consumer Test",','Name = "Kafka Consumer Test",' + "`n                    Debug = ""cgrp,consumer,topic"",")
[IO.File]::WriteAllText($kafka,$text,$utf8)
New-Item -ItemType Directory -Force "$out/diagnostic-sources","$out/builds"|Out-Null
Copy-Item $provider,$credential,$kafka "$out/diagnostic-sources"
$hook=Join-Path $env:TEMP 'brighter-4170-docker-hook'
if(Test-Path $hook){throw 'Hook directory already exists'}
New-Item -ItemType Directory $hook|Out-Null
Copy-Item "$PSScriptRoot/../kafka-live-capture/StartupHook.cs","$PSScriptRoot/../kafka-live-capture/NativeCapture.csproj" $hook
foreach($project in @('Paramore.Brighter.Gcp.Tests','Paramore.Brighter.Kafka.Tests')) {
    foreach($fw in @('net9.0','net10.0')) {
        $run=RunBounded 'C:/Program Files/dotnet/dotnet.exe' @('build',"$work/tests/$project/$project.csproj",'-c','Release','-f',$fw,'--verbosity','minimal') "$out/builds/$project-$fw.log" 900 $work
        if($run.exitCode -ne 0){throw "Build failed: $project $fw"}
        SaveJson $run "$out/builds/$project-$fw.json"
        Write-Output "$project $fw built"
    }
}
$run=RunBounded 'C:/Program Files/dotnet/dotnet.exe' @('build',"$hook/NativeCapture.csproj",'-c','Release','--verbosity','minimal') "$out/builds/native-hook.log" 300 $hook
if($run.exitCode -ne 0){throw 'Native capture hook build failed'}
SaveJson @{workspace=$work;hook=$hook;productionChanges=@();fixtureOnlyChanges=@('passive GCP stream RPC observer through existing public hook','GCP emulator-only credential fixture','native librdkafka debug in original concurrency test');assertionsChanged=$false} "$out/diagnostics.json"
