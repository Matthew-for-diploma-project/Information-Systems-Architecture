$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
New-Item -ItemType Directory -Force benchmarks | Out-Null
$output = Join-Path $PSScriptRoot 'benchmarks/benchmark-results.csv'
$last = Join-Path $PSScriptRoot 'benchmarks/benchmark-last.csv'
'layer,replicas,threads,requests,completed,errors,rps,p50_ms,p95_ms' | Set-Content $output -Encoding ascii
function Invoke-Compose([string[]] $Arguments) {
    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $($Arguments -join ' ')" }
}
function Assert-Replicas([string] $Layer, [int] $Expected) {
    $ids = @(& docker compose ps -q $Layer)
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect $Layer" }
    $actual = @($ids | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
    if ($actual -ne $Expected) { throw "Expected $Expected $Layer containers, found $actual" }
}
Invoke-Compose -Arguments @('build', 'facade')
try {
    foreach ($layer in @('facade', 'business', 'storage')) {
        foreach ($replicas in @(1, 2, 3)) {
            $scales = @{facade = 1; business = 1; storage = 1}
            $scales[$layer] = $replicas
            Invoke-Compose -Arguments @('up', '-d', '--wait', '--scale', "facade=$($scales.facade)",
                '--scale', "business=$($scales.business)", '--scale', "storage=$($scales.storage)", 'facade')
            foreach ($name in @('facade', 'business', 'storage')) { Assert-Replicas $name ([int]$scales[$name]) }
            Remove-Item $last -Force -ErrorAction SilentlyContinue
            # --no-deps сохраняет явно выставленное количество реплик.
            Invoke-Compose -Arguments @('run', '--no-deps', '--rm', '-T', '-e', "BENCH_LAYER=$layer",
                '-e', "BENCH_REPLICAS=$replicas", 'benchmark')
            foreach ($name in @('facade', 'business', 'storage')) { Assert-Replicas $name ([int]$scales[$name]) }
            if (-not (Test-Path $last)) { throw 'Benchmark did not create its result file' }
            $row = (Get-Content $last -Raw).Trim()
            if (-not $row.StartsWith("$layer,$replicas,")) { throw 'Unexpected benchmark result' }
            $row | Add-Content $output -Encoding ascii
        }
    }
} finally {
    Invoke-Compose -Arguments @('up', '-d', '--wait', '--scale', 'facade=1', '--scale', 'business=1', '--scale', 'storage=1', 'facade')
    Remove-Item $last -Force -ErrorAction SilentlyContinue
}
$rows = Import-Csv $output
$report = @('# Сравнение измеренных результатов', '',
    '| Слой | 1 реплика, запросов/с | 2 реплики | 3 реплики |', '|---|---:|---:|---:|')
foreach ($layer in @('facade', 'business', 'storage')) {
    $one = ($rows | Where-Object { $_.layer -eq $layer -and $_.replicas -eq '1' }).rps
    $two = ($rows | Where-Object { $_.layer -eq $layer -and $_.replicas -eq '2' }).rps
    $three = ($rows | Where-Object { $_.layer -eq $layer -and $_.replicas -eq '3' }).rps
    $report += "| $layer | $one | $two | $three |"
}
$report += ''; $report += 'Нагрузка: чтение METRICS. Ошибка прерывает замер. Рабочая БД и уведомления не изменяются.'
$report | Set-Content (Join-Path $PSScriptRoot 'benchmarks/benchmark-comparison.md') -Encoding utf8
