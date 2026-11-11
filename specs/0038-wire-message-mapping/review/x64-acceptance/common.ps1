$ErrorActionPreference = 'Stop'
$Docker = 'C:/Users/IrakliGabisonia/AppData/Local/Programs/DockerDesktop/resources/bin/docker.exe'
$SqlImage = 'mcr.microsoft.com/mssql/server@sha256:cf8f0ab2ebe61feec2eed2629061a07d247f791c2a8ddda74a4ac9f06a291718'
$AsbImage = 'mcr.microsoft.com/azure-messaging/servicebus-emulator@sha256:5a96d893b245031740f7d46e0fe5ff282d24b78c4b7d761dd57590f3f010a9b3'
$Head = (git rev-parse HEAD).Trim()
function DockerCommand([string[]]$Arguments) {
    $previous = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
    $output = & $Docker @Arguments 2>&1
    $code = $LASTEXITCODE; $ErrorActionPreference = $previous
    if ($code -ne 0) { throw "Docker command failed ($code): $($output -join "`n")" }
    return ($output | ForEach-Object { "$_" })
}
function SaveJson($Value, [string]$Path) { $Value | ConvertTo-Json -Depth 8 | Set-Content $Path -Encoding UTF8 }
function RunBounded([string]$File, [string[]]$Arguments, [string]$Log, [int]$Seconds, [string]$WorkingDirectory = (Get-Location).Path) {
    $startedUtc = [DateTime]::UtcNow
    $quoted = ($Arguments | ForEach-Object { '"' + $_.Replace('"', '\"') + '"' }) -join ' '
    $process = Start-Process -FilePath $File -ArgumentList $quoted -WorkingDirectory $WorkingDirectory -RedirectStandardOutput $Log -RedirectStandardError "$Log.stderr" -PassThru -WindowStyle Hidden
    $processHandle = $process.Handle
    $watch = [Diagnostics.Stopwatch]::StartNew()
    if (!$process.WaitForExit($Seconds * 1000)) { Stop-Process -Id $process.Id -Force; throw "Process deadline exceeded: $File ($Seconds seconds). See $Log" }
    $process.WaitForExit()
    $finishedUtc = [DateTime]::UtcNow
    return @{ exitCode = $process.ExitCode; seconds = $watch.Elapsed.TotalSeconds; log = $Log; startedUtc = $startedUtc.ToString('o'); finishedUtc = $finishedUtc.ToString('o'); clockDiscontinuity = ([Math]::Abs(($finishedUtc - $startedUtc).TotalSeconds - $watch.Elapsed.TotalSeconds) -gt 5) }
}
function WaitSql([string]$Name) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    while ($watch.Elapsed.TotalSeconds -lt 120) {
        $logs = DockerCommand @('logs', $Name)
        if (($logs -join "`n") -match 'SQL Server is now ready for client connections') { return }
        if ((DockerCommand @('inspect', $Name, '--format', '{{.State.Running}}')) -ne 'true') { throw 'SQL exited before readiness' }
        Start-Sleep -Seconds 2
    }
    throw 'SQL readiness exceeded 120 seconds'
}
function StopOwned([string]$Name, [string]$Directory) {
    $logs = DockerCommand @('logs', $Name)
    ($logs -join "`n").Replace($env:MSSQL_SA_PASSWORD, '[local fixture password redacted]') | Set-Content "$Directory/$Name.log" -Encoding UTF8
    $before = DockerCommand @('inspect', $Name, '--format', '{{json .State}}') | ConvertFrom-Json
    DockerCommand @('stop', '-t', '10', $Name) | Out-Null
    return @{ name = $Name; stateBeforeStop = $before; runningAfterStop = (DockerCommand @('inspect', $Name, '--format', '{{.State.Running}}')) }
}
