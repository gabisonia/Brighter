. "$PSScriptRoot/common.ps1"
$out="$PSScriptRoot/docker-remaining/images"
New-Item -ItemType Directory -Force $out|Out-Null
$images=[ordered]@{
    gcp='gcr.io/google.com/cloudsdktool/cloud-sdk@sha256:3797a874fae6b75aa25260b5bd40fa0397c2f1102f18ae3ac7dc887732fbe449'
    kafka='apache/kafka:4.0.2'
    schema='confluentinc/cp-schema-registry:8.0.6'
    net9='mcr.microsoft.com/dotnet/sdk:9.0'
}
$results=@()
foreach($name in $images.Keys){
    $image=$images[$name]
    $run=RunBounded $Docker @('pull','--platform','linux/amd64',$image) "$out/$name-pull.log" 900
    if($run.exitCode -ne 0){throw "Image pull failed: $name"}
    $info=(DockerCommand @('image','inspect',$image,'--format','{{json .}}'))|ConvertFrom-Json
    $results += @{name=$name;requested=$image;id=$info.Id;digests=$info.RepoDigests;architecture=$info.Architecture;run=$run}
    SaveJson $results "$out/manifest.json"
    Write-Output "Cached $name : $($info.Id)"
}
DockerCommand @('image','inspect','mcr.microsoft.com/dotnet/sdk:10.0','--format','{{json .}}')|Set-Content "$out/net10-cached-image.json"
