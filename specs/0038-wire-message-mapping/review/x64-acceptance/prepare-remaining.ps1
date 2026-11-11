. "$PSScriptRoot/common.ps1"
$out = "$PSScriptRoot/docker-remaining"
$work = Join-Path $env:TEMP 'brighter-4170-docker-remaining'
if ((Test-Path $out) -or (Test-Path $work)) { throw 'One-shot workspace/evidence destination already exists' }
New-Item -ItemType Directory $out,$work | Out-Null
$archive=Join-Path $env:TEMP 'brighter-4170-docker-remaining.zip'
git -c core.autocrlf=false -c core.eol=lf archive --format=zip "--output=$archive" HEAD
if($LASTEXITCODE -ne 0){throw 'Source archive failed'}
tar.exe -xf $archive -C $work
if($LASTEXITCODE -ne 0){throw 'Source extraction failed'}
Remove-Item -LiteralPath $archive
$utf8=New-Object Text.UTF8Encoding($false)
$asb="$work/tests/Paramore.Brighter.AzureServiceBus.Tests"
$creds="$asb/MessagingGateway/ASBCreds.cs"
$original=[IO.File]::ReadAllText($creds)
$needle='if (!string.IsNullOrEmpty(connString)) return new ServiceBusConnectionStringClientProvider(connString);'
if(!$original.Contains($needle)){throw 'Unexpected ASB credentials fixture source'}
$replacement='if (!string.IsNullOrEmpty(connString)) return new DockerAcceptanceClientProvider(connString, Environment.GetEnvironmentVariable("BrighterTestsASBAdministrationConnectionString") ?? connString);'
[IO.File]::WriteAllText($creds,$original.Replace($needle,$replacement),$utf8)
$adapter=@'
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Paramore.Brighter.MessagingGateway.AzureServiceBus.ClientProvider;
namespace Paramore.Brighter.AzureServiceBus.Tests.MessagingGateway;
internal sealed class DockerAcceptanceClientProvider : ServiceBusClientProvider
{
    public DockerAcceptanceClientProvider(string messaging, string administration)
    {
        Client = new ServiceBusClient(messaging);
        AdminClient = new ServiceBusAdministrationClient(administration, new ServiceBusAdministrationClientOptions
        {
            Retry = { MaxRetries = 0, NetworkTimeout = System.TimeSpan.FromSeconds(15) }
        });
    }
}
'@
[IO.File]::WriteAllText("$asb/MessagingGateway/DockerAcceptanceClientProvider.cs",$adapter,$utf8)
foreach($name in @('InMemoryLockingServiceBusReceiver.cs','InMemoryLockingServiceBusSessionReceiver.cs')) {
    Copy-Item "$PSScriptRoot/../asb-session-lock-failure-diagnostics/$name" "$asb/TestDoubles/$name"
}
Copy-Item "$PSScriptRoot/../asb-cancellation-abandon-failure/AsbAbandonFailureWireReader.cs" "$asb/TestDoubles/AsbAbandonFailureWireReader.cs"
Copy-Item "$PSScriptRoot/../asb-cancellation-abandon-failure/When_cancellation_cleanup_loses_one_asb_lock_should_release_remaining_deliveries_and_recover.cs" "$asb/MessagingGateway/When_cancellation_cleanup_loses_one_asb_lock_should_release_remaining_deliveries_and_recover.cs"
Copy-Item $creds,"$asb/MessagingGateway/DockerAcceptanceClientProvider.cs" $out
SaveJson @{ head=$Head; workspace=$work; output=$out; isolatedOnlyChanges=@('ASBCreds management endpoint routing','DockerAcceptanceClientProvider real SDK adapter','two retained passive in-memory lock diagnostic helpers','two retained approved lost-lock test/helper files'); productionChanges=@(); assertionsChanged=$false } "$out/workspace.json"
Write-Output "Prepared isolated workspace: $work"
