param(
    [string]$Database = "meio_optimizer_db",
    [string]$User = "postgres",
    [string]$HostName = "localhost",
    [int]$Port = 5432,
    [string]$PsqlPath = "psql",
    [string]$RawDataPath = ""
)

$ErrorActionPreference = "Stop"

if (-not $RawDataPath) {
    $RawDataPath = Join-Path $PSScriptRoot "data\raw"
}

$ResolvedRawDataPath = (Resolve-Path -LiteralPath $RawDataPath).Path.Replace("\", "/")
$DataFiles = [ordered]@{
    locations_csv           = "$ResolvedRawDataPath/locations.csv"
    sku_master_csv          = "$ResolvedRawDataPath/sku_master.csv"
    service_policy_csv      = "$ResolvedRawDataPath/service_policy.csv"
    lane_costs_csv          = "$ResolvedRawDataPath/lane_costs.csv"
    sales_orders_csv        = "$ResolvedRawDataPath/sales_orders.csv"
    shipments_csv           = "$ResolvedRawDataPath/shipments.csv"
    inventory_snapshots_csv = "$ResolvedRawDataPath/inventory_snapshots.csv"
}

foreach ($DataFile in $DataFiles.GetEnumerator()) {
    if (-not (Test-Path -LiteralPath $DataFile.Value)) {
        throw "Raw data file not found: $($DataFile.Value)"
    }
}

function Invoke-PsqlFile {
    param(
        [string]$FilePath
    )

    if (-not (Test-Path $FilePath)) {
        throw "SQL file not found: $FilePath"
    }

    Write-Host ""
    Write-Host "Running $FilePath" -ForegroundColor Cyan

    $args = @(
        "-v", "ON_ERROR_STOP=1",
        "-h", $HostName,
        "-p", "$Port",
        "-U", $User,
        "-d", $Database,
        "-f", $FilePath
    )

    foreach ($DataFile in $DataFiles.GetEnumerator()) {
        $args += @("-v", "$($DataFile.Key)=$($DataFile.Value)")
    }

    & $PsqlPath @args
    if ($LASTEXITCODE -ne 0) {
        throw "Failed while running $FilePath"
    }
}

Write-Host "MEIO PostgreSQL pipeline" -ForegroundColor Green
Write-Host "Database: $Database"
Write-Host "User:     $User"
Write-Host "Host:     $HostName"
Write-Host "Port:     $Port"
Write-Host "Raw data: $ResolvedRawDataPath"
Write-Host ""
Write-Host "Assumption: database '$Database' already exists and user '$User' can create/drop tables." -ForegroundColor Yellow

Invoke-PsqlFile (Join-Path $PSScriptRoot "schema.sql")
Invoke-PsqlFile (Join-Path $PSScriptRoot "ingest.sql")
Invoke-PsqlFile (Join-Path $PSScriptRoot "cleaning.sql")
Invoke-PsqlFile (Join-Path $PSScriptRoot "marts.sql")
Invoke-PsqlFile (Join-Path $PSScriptRoot "verify_marts.sql")

Write-Host ""
Write-Host "Pipeline completed successfully." -ForegroundColor Green
