param(
    [Parameter(Mandatory = $true)]
    [string]$CsvPath
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $ProjectRoot

$ResolvedPath = (Resolve-Path -LiteralPath $CsvPath).Path
if ([System.IO.Path]::GetExtension($ResolvedPath).ToLowerInvariant() -ne ".csv") {
    throw "Expected a CSV file: $ResolvedPath"
}
$Leaf = [System.IO.Path]::GetFileName($ResolvedPath)
$ContainerPath = "/tmp/$Leaf"

docker compose cp $ResolvedPath "namenode:$ContainerPath"
if ($LASTEXITCODE -ne 0) { throw "Could not copy the CSV into the NameNode container." }

docker compose exec -T namenode hdfs dfs -mkdir -p /user/cybershield/raw
if ($LASTEXITCODE -ne 0) { throw "Could not create the HDFS input directory. Check that NameNode is ready." }

docker compose exec -T namenode hdfs dfs -put -f $ContainerPath "/user/cybershield/raw/$Leaf"
if ($LASTEXITCODE -ne 0) { throw "HDFS upload failed for $Leaf." }

Write-Host "Uploaded $Leaf to /user/cybershield/raw/$Leaf"
docker compose exec -T namenode hdfs dfs -ls /user/cybershield/raw
