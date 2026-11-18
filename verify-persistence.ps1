param([switch] $Crash)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
function Invoke-Compose([string[]] $Arguments) {
    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $($Arguments -join ' ')" }
}
$marker = [guid]::NewGuid().ToString('N')
Invoke-Compose -Arguments @('up', '-d', '--wait', 'facade')
Invoke-Compose -Arguments @('run', '--rm', 'tests', '-Dtest=ContainerTest', '-Dchat.phase=prepare', "-Dchat.marker=$marker")
if ($Crash) {
    Invoke-Compose -Arguments @('kill', '-s', 'SIGKILL', 'database')
} else {
    Invoke-Compose -Arguments @('stop', 'database')
}
Invoke-Compose -Arguments @('up', '-d', '--wait', 'facade')
Invoke-Compose -Arguments @('run', '--rm', 'tests', '-Dtest=ContainerTest', '-Dchat.phase=verify', "-Dchat.marker=$marker")
# Том и существующие данные не удаляются. Пользователей теста отличает уникальный marker.
