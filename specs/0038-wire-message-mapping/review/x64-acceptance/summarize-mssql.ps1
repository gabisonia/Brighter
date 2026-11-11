$ErrorActionPreference = 'Stop'
$directory = "$PSScriptRoot/mssql-native"
$frameworks = @()
foreach ($fw in @('net9.0', 'net10.0')) {
    [xml]$trx = Get-Content "$directory/$fw.trx" -Raw
    $cases = @($trx.TestRun.Results.UnitTestResult | ForEach-Object { [ordered]@{ name = $_.testName; outcome = $_.outcome } } | Sort-Object { $_.name })
    $frameworks += [ordered]@{ framework = $fw; head = (git rev-parse HEAD).Trim(); total = $cases.Count; passed = @($cases | Where-Object outcome -eq Passed).Count; failed = @($cases | Where-Object outcome -eq Failed).Count; skipped = @($cases | Where-Object outcome -eq NotExecuted).Count; cases = $cases }
}
$difference = @(Compare-Object @($frameworks[0].cases | ForEach-Object { "$($_.name)|$($_.outcome)" }) @($frameworks[1].cases | ForEach-Object { "$($_.name)|$($_.outcome)" }))
$result = [ordered]@{ frameworks = $frameworks; frameworkIdentityAndOutcomeDifferences = $difference; historicalSqlEdgeReportedPerFramework = @{ passed = 396; skipped = 4 }; historicalIdentityComparison = 'Unavailable: transferred mssql review packages retain reports and source copies but no SQL Edge TRX or per-case outcome inventory. Current two-framework identities/outcomes compared in full; historical count equality does not prove historical identity parity.' }
$result | ConvertTo-Json -Depth 12 | Set-Content "$directory/identity-comparison.json" -Encoding UTF8
$frameworks | ForEach-Object { "{0}: {1} passed, {2} failed, {3} skipped, {4} total" -f $_.framework,$_.passed,$_.failed,$_.skipped,$_.total }
"Framework identity/outcome differences: $($difference.Count)"
