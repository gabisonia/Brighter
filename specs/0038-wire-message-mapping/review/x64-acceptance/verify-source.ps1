param([string]$ReportName = 'source-verification.json')
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression.FileSystem
$root = (Get-Location).Path
$manifest = Get-Content specs/0038-wire-message-mapping/review/completion-source-manifest.json -Raw | ConvertFrom-Json
$archive = Join-Path $env:TEMP ('brighter-4170-source-' + [Guid]::NewGuid().ToString('N') + '.zip')
git -c core.autocrlf=false -c core.eol=lf archive --format=zip "--output=$archive" HEAD
if ($LASTEXITCODE -ne 0) { throw 'Could not archive HEAD for source verification' }
$zip = [IO.Compression.ZipFile]::OpenRead($archive)
$sha = [Security.Cryptography.SHA256]::Create()
function HashBytes([byte[]]$bytes) { ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant() }
$mismatches = @(); $rawMatches = 0; $checkoutLineEndingOnly = 0
try {
    foreach ($entry in $manifest.PSObject.Properties) {
        $item = $zip.GetEntry($entry.Name)
        if (!$item) { $mismatches += @{ path = $entry.Name; reason = 'Missing from HEAD' }; continue }
        $stream = $item.Open(); $memory = New-Object IO.MemoryStream
        $stream.CopyTo($memory); $stream.Dispose()
        $bytes = $memory.ToArray(); $memory.Dispose()
        $blobHash = HashBytes $bytes
        $local = [IO.File]::ReadAllBytes((Join-Path $root $entry.Name))
        $localHash = HashBytes $local
        if ($blobHash -ne $entry.Value) { $mismatches += @{ path = $entry.Name; reason = 'HEAD differs from manifest'; expected = $entry.Value; actual = $blobHash }; continue }
        if ($localHash -eq $blobHash) { $rawMatches++; continue }
        $localLf = [Text.Encoding]::UTF8.GetBytes([Text.Encoding]::UTF8.GetString($local).Replace("`r`n", "`n"))
        $blobLf = [Text.Encoding]::UTF8.GetBytes([Text.Encoding]::UTF8.GetString($bytes).Replace("`r`n", "`n"))
        if ((HashBytes $localLf) -eq (HashBytes $blobLf)) { $checkoutLineEndingOnly++ }
        else { $mismatches += @{ path = $entry.Name; reason = 'Working file content differs from HEAD' } }
    }
} finally { $zip.Dispose(); $sha.Dispose(); Remove-Item -LiteralPath $archive }
$result = [ordered]@{ head = (git rev-parse HEAD); branch = (git branch --show-current); utc = [DateTime]::UtcNow.ToString('o'); manifestEntries = @($manifest.PSObject.Properties).Count; headMatches = $rawMatches + $checkoutLineEndingOnly; rawWorkingMatches = $rawMatches; checkoutLineEndingOnly = $checkoutLineEndingOnly; mismatches = @($mismatches); method = 'git -c core.autocrlf=false -c core.eol=lf archive HEAD; SHA256 of HEAD ZIP entries against saved manifest; working files raw or CRLF/LF-only comparison. Checkout core.autocrlf=true.' }
$result | ConvertTo-Json -Depth 6 | Set-Content "$PSScriptRoot/$ReportName" -Encoding UTF8
$result | ConvertTo-Json -Depth 6
if ($mismatches.Count) { exit 1 }
