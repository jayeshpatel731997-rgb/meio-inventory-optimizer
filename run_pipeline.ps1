param(
    [string]$Database = "meio_optimizer_db",
    [string]$User = "postgres",
    [string]$HostName = "localhost",
    [int]$Port = 5432,
    [string]$PsqlPath = "psql"
)

$ErrorActionPreference = "Stop"

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
Write-Host ""
Write-Host "Assumption: database '$Database' already exists and user '$User' can create/drop tables." -ForegroundColor Yellow

Invoke-PsqlFile ".\schema.sql"
Invoke-PsqlFile ".\ingest.sql"
Invoke-PsqlFile ".\cleaning.sql"
Invoke-PsqlFile ".\marts.sql"
Invoke-PsqlFile ".\verify_marts.sql"

Write-Host ""
Write-Host "Pipeline completed successfully." -ForegroundColor Green
