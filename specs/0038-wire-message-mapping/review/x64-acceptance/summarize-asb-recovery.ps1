$ErrorActionPreference='Stop'
$directory="$PSScriptRoot/asb-recovery-isolated"
$results=Get-Content "$directory/results.json" -Raw | ConvertFrom-Json
[xml]$old=Get-Content "$PSScriptRoot/../asb-docker-retry/net9.0.trx" -Raw
$oldNames=@($old.TestRun.Results.UnitTestResult | ForEach-Object testName)
$summary=@()
foreach($sample in $results) {
    $broker=@($sample.containers|Where-Object name -like '*-broker')[0]
    $lines=Get-Content "$directory/$($broker.name).log"
    $cases=@()
    foreach($case in $sample.cases) {
        $entity=if($case.scenario.useQueue){"recovery-queue-$($case.scenario.index)"}else{"recovery-topic-$($case.scenario.index)"}
        $duplicates=@($lines|Where-Object{$_ -match 'Cannot add the same item' -and $_.Contains($entity)})
        $cases += [PSCustomObject]@{ index=$case.scenario.index; name=$case.name; outcome=$case.outcome; entity=$entity; error=$case.message; duplicateSequenceFault=($duplicates.Count -gt 0); duplicateFaultRecords=$duplicates.Count; startedUtc=$case.run.startedUtc; finishedUtc=$case.run.finishedUtc; clockDiscontinuity=$case.run.clockDiscontinuity }
    }
    $differences=@(Compare-Object ($oldNames|Sort-Object) @($cases.name|Sort-Object))
    $summary += [PSCustomObject]@{ framework=$sample.framework; head=$sample.head; total=$cases.Count; passed=@($cases|Where-Object outcome -eq Passed).Count; failed=@($cases|Where-Object outcome -eq Failed).Count; historicalIdentityDifferences=$differences; allOwnedContainersStopped=(@($sample.containers|Where-Object runningAfterStop -ne 'false').Count -eq 0); cases=$cases }
}
$summary|ConvertTo-Json -Depth 12|Set-Content "$directory/verification.json" -Encoding UTF8
$summary|Format-Table framework,total,passed,failed,allOwnedContainersStopped
foreach($sample in $summary){if($sample.total -ne 8 -or $sample.historicalIdentityDifferences.Count){throw 'Unexpected recovery case identity inventory'}}
