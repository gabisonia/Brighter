$ErrorActionPreference = 'Stop'
$directory = "$PSScriptRoot/asb-sdk"
$results = Get-Content "$directory/results.json" -Raw | ConvertFrom-Json
$summary = @()
foreach ($item in $results) {
    $broker = @($item.containers | Where-Object name -like '*-broker')[0]
    $text = Get-Content "$directory/$($broker.name).log" -Raw
    $summary += [ordered]@{
        head = $item.head
        framework = $item.framework
        runtime = $item.probe.framework
        azureSdk = $item.probe.sdk
        topicPassed = @($item.probe.results | Where-Object { !$_.queue -and !$_.error }).Count
        topicFailed = @($item.probe.results | Where-Object { !$_.queue -and $_.error }).Count
        queuePassed = @($item.probe.results | Where-Object { $_.queue -and !$_.error }).Count
        queueFailed = @($item.probe.results | Where-Object { $_.queue -and $_.error }).Count
        duplicateSequenceFault = $text.Contains('S.SP-SequenceNumber:1')
        clockDiscontinuity = $item.run.clockDiscontinuity
        allOwnedContainersStopped = @($item.containers | Where-Object runningAfterStop -ne 'false').Count -eq 0
        anyContainerOomKilled = @($item.containers | Where-Object { $_.stateBeforeStop.OOMKilled }).Count -gt 0
    }
}
$probe = Join-Path $env:TEMP 'brighter-4170-x64-sdk-probe-20261111'
$assets = Get-Content "$probe/obj/project.assets.json" -Raw | ConvertFrom-Json
$libraries = @($assets.libraries.PSObject.Properties.Name)
$brighter = @($libraries | Where-Object { $_ -match 'Brighter|Paramore' })
if ($brighter.Count) { throw 'Unexpected Brighter dependency in standalone reproduction' }
[ordered]@{ samples = $summary; dependencies = $libraries; brighterDependencies = $brighter; conclusion = 'A native amd64 emulator with native amd64 SQL Server 2022 reproduces the SDK-only topic DLQ duplicate-sequence fault. ARM64/emulated SQL is not necessary for this reproduction. The precise SDK/emulator/backend cause remains unproven; no Brighter acceptance/mutation gate is closed.' } | ConvertTo-Json -Depth 10 | Set-Content "$directory/comparison.json" -Encoding UTF8
$summary | ForEach-Object { [PSCustomObject]$_ } | Format-Table framework,runtime,topicPassed,topicFailed,queuePassed,queueFailed,duplicateSequenceFault,allOwnedContainersStopped
