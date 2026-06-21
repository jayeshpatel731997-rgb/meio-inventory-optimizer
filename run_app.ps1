param(
    [int]$Port = 8504
)

$ErrorActionPreference = "Stop"

$AppDir = Join-Path $PSScriptRoot "meio-optimizer"
$VenvActivate = Join-Path $AppDir ".venv\Scripts\Activate.ps1"
$PythonExe = Join-Path $AppDir ".venv\Scripts\python.exe"
$DatabaseUser = "postgres"
$DatabaseHost = "127.0.0.1"
$DatabasePort = "5432"
$DatabaseName = "meio_optimizer_db"

function Get-MaskedDatabaseUrl {
    param([string]$DatabaseUrl)

    if (-not $DatabaseUrl) {
        return ""
    }

    if ($DatabaseUrl -notlike "*@*") {
        return $DatabaseUrl
    }

    $scheme = ""
    $rest = $DatabaseUrl
    if ($DatabaseUrl -like "*://*") {
        $parts = $DatabaseUrl -split "://", 2
        $scheme = "$($parts[0])://"
        $rest = $parts[1]
    }

    $afterAt = $rest.Substring($rest.LastIndexOf("@") + 1)
    return "$scheme****@$afterAt"
}

function Get-DatabaseUrlSummary {
    param([string]$DatabaseUrl)

    $summary = @{
        Host = "not available"
        Database = "not available"
    }

    if (-not $DatabaseUrl -or $DatabaseUrl -notlike "*@*") {
        return $summary
    }

    $afterAt = $DatabaseUrl.Substring($DatabaseUrl.LastIndexOf("@") + 1)
    $hostPortAndDb = $afterAt -split "/", 2
    $summary.Host = ($hostPortAndDb[0] -split ":", 2)[0]
    if ($hostPortAndDb.Count -gt 1) {
        $summary.Database = ($hostPortAndDb[1] -split "\?", 2)[0]
    }
    return $summary
}

if (-not (Test-Path $AppDir)) {
    throw "App directory not found: $AppDir"
}

if (-not (Test-Path $VenvActivate)) {
    throw "Virtual environment not found. Expected: $VenvActivate"
}

if (-not (Test-Path $PythonExe)) {
    throw "Python executable not found. Expected: $PythonExe"
}

if ($env:DATABASE_URL -and ($env:DATABASE_URL.Contains("USER") -or $env:DATABASE_URL.Contains("DB_NAME"))) {
    Write-Host "Existing DATABASE_URL contains placeholder values and will be replaced." -ForegroundColor Yellow
}
elseif ($env:DATABASE_URL) {
    Write-Host "Existing DATABASE_URL will be replaced for this MEIO session." -ForegroundColor Yellow
}

$pgPassword = Read-Host "Enter PostgreSQL password for user postgres"
if ([string]::IsNullOrWhiteSpace($pgPassword)) {
    throw "PostgreSQL password cannot be empty."
}

$pgPassword = $pgPassword.Trim()
$encodedPassword = [System.Uri]::EscapeDataString($pgPassword)
$pgPassword = $null
$env:DATABASE_URL = "postgresql+psycopg2://postgres:$encodedPassword@127.0.0.1:5432/meio_optimizer_db"

$summary = Get-DatabaseUrlSummary -DatabaseUrl $env:DATABASE_URL
Write-Host "DATABASE_URL set for this Streamlit session." -ForegroundColor Green
Write-Host "postgresql+psycopg2://postgres:***@127.0.0.1:5432/meio_optimizer_db" -ForegroundColor Green
Write-Host "Database host: $($summary.Host)" -ForegroundColor Green
Write-Host "Database name: $($summary.Database)" -ForegroundColor Green

Set-Location $AppDir
. $VenvActivate

Write-Host "Running PostgreSQL smoke test before launching Streamlit..." -ForegroundColor Cyan
& ".\.venv\Scripts\python.exe" test_db_connection.py
if ($LASTEXITCODE -ne 0) {
    throw "Database smoke test failed. Streamlit was not launched."
}

& ".\.venv\Scripts\python.exe" -m streamlit run app.py --server.port $Port
