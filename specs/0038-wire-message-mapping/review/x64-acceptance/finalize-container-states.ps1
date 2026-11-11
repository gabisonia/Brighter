. "$PSScriptRoot/common.ps1"
$out="$PSScriptRoot/docker-remaining"
$path="$out/container-states-final.jsonl"
if(Test-Path $path){Move-Item -LiteralPath $path -Destination "$out/container-states-native-format.txt"}
$names=DockerCommand @('ps','-a','--filter','label=brighter.validation=4170-remaining','--format','{{.Names}}')
foreach($name in $names){
    $state=DockerCommand @('inspect',$name,'--format','{{json .State}}')|ConvertFrom-Json
    if($state.Running){throw "Owned fixture still running: $name"}
    @{name=$name;state=$state}|ConvertTo-Json -Depth 4 -Compress|Add-Content $path -Encoding UTF8
}
"Verified $($names.Count) stopped fixtures; machine-readable final states saved"
