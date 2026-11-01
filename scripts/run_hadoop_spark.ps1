param(
    [string]$CsvPath = (Join-Path $PSScriptRoot "..\data\sample\sample_traffic.csv"),
    [string]$WslDistro = "Ubuntu-24.04"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
if (-not [System.IO.Path]::IsPathRooted($CsvPath)) {
    $CsvPath = Join-Path $ProjectRoot $CsvPath
}
$ResolvedCsv = (Resolve-Path -LiteralPath $CsvPath).Path
if ([System.IO.Path]::GetExtension($ResolvedCsv).ToLowerInvariant() -ne ".csv") {
    throw "Expected a CSV file: $ResolvedCsv"
}

$WslProjectRoot = (& wsl.exe -d $WslDistro --exec wslpath -a -- $ProjectRoot).Trim()
if ($LASTEXITCODE -ne 0 -or -not $WslProjectRoot) {
    throw "Could not resolve the project folder inside WSL distribution '$WslDistro'."
}
$WslCsvPath = (& wsl.exe -d $WslDistro --exec wslpath -a -- $ResolvedCsv).Trim()
if ($LASTEXITCODE -ne 0 -or -not $WslCsvPath) {
    throw "Could not resolve the CSV path inside WSL distribution '$WslDistro'."
}

$WslScript = "$WslProjectRoot/scripts/run_hadoop_spark_wsl.sh"
& wsl.exe -d $WslDistro --exec bash $WslScript $WslCsvPath
if ($LASTEXITCODE -ne 0) {
    throw "The native Hadoop + Spark run failed with exit code $LASTEXITCODE."
}
