. "$PSScriptRoot/common.ps1"
$out="$PSScriptRoot/docker-remaining"
[xml]$expected=Get-Content "$PSScriptRoot/../kafka-full-capture/full/results.trx" -Raw
[xml]$observed=Get-Content "$out/kafka-passive/net9.0/results.trx" -Raw
$passed=@($observed.TestRun.Results.UnitTestResult|Where-Object outcome -eq Passed|ForEach-Object{[string]$_.testName})
$methods=@{};foreach($definition in $expected.TestRun.TestDefinitions.UnitTest){$methods[[string]$definition.id]=[string]$definition.TestMethod.className+'.'+[string]$definition.TestMethod.name}
$wanted=@();$blocked=@()
foreach($row in $expected.TestRun.Results.UnitTestResult){
    if($passed -contains [string]$row.testName){continue}
    if([string]$row.testName -match 'KafkaMessageConsumerCommitRevokeConcurrency'){$blocked += [string]$row.testName;continue}
    $wanted += [PSCustomObject]@{name=[string]$row.testName;method=$methods[[string]$row.testId]}
}
$selectedMethods=@($wanted.method|Sort-Object -Unique)
SaveJson @{head=$Head;reference='review/kafka-full-capture/full/results.trx';referenceIdentities=@($expected.TestRun.Results.UnitTestResult).Count;alreadyPassed=$passed.Count;requestedIdentities=$wanted;blockedConcurrencyIdentities=$blocked;filter=($selectedMethods|ForEach-Object{"FullyQualifiedName=$_"}) -join '|';purpose='Run only methods containing identities not reached before the independently captured native-close abort. The two concurrency rows remain explicitly blocked; no assertion changes or silent skips.'} "$out/kafka-remainder-plan.json"
"Remainder: $($wanted.Count) identities in $($selectedMethods.Count) methods; $($blocked.Count) explicitly blocked concurrency identities"
