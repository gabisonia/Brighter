#!/bin/sh

# Read-only host checks; no installation, containers or credentials are changed.
set -u

printf '\nHost operating system and architecture\n'
uname -sm
printf '\nAvailable filesystem space\n'
df -h .
if command -v free >/dev/null 2>&1; then
    printf '\nHost memory\n'
    free -h
fi

printf '\nDocker client/server\n'
if command -v docker >/dev/null 2>&1; then
    docker version --format 'Client={{.Client.Version}} Server={{.Server.Version}}'
    docker info --format 'OS={{.OSType}} Architecture={{.Architecture}} CPUs={{.NCPU}} MemoryBytes={{.MemTotal}}'
    printf '\nExisting containers and published ports\n'
    docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Ports}}'
else
    printf 'Docker command is unavailable.\n'
fi

printf '\nInstalled .NET SDKs\n'
if command -v dotnet >/dev/null 2>&1; then
    dotnet --list-sdks
else
    printf '.NET command is unavailable.\n'
fi
