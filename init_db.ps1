# ============================================================
#  boxoffice.int - one-time database initialisation
#
#  Requires:
#    - psql on PATH  (PostgreSQL client tools)
#    - BOXOFFICE_DB_URL set in .env (or already in the environment)
#
#  Usage:
#    .\init_db.ps1
# ============================================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --- Load .env -----------------------------------------------
$envFile = Join-Path $PSScriptRoot ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | Where-Object { $_ -match '^\s*[^#]\S+=\S*' } | ForEach-Object {
        $key, $value = $_ -split '=', 2
        [System.Environment]::SetEnvironmentVariable($key.Trim(), $value.Trim())
    }
    Write-Host "[env] Loaded $envFile" -ForegroundColor Green
} else {
    Write-Warning "[env] .env not found — BOXOFFICE_DB_URL must already be set"
}

$dbUrl = $env:BOXOFFICE_DB_URL
if (-not $dbUrl) {
    Write-Error "BOXOFFICE_DB_URL is not set. Aborting."
    exit 1
}

# --- Helper --------------------------------------------------
function Invoke-Psql {
    param([string]$File)
    $filePath = Join-Path $PSScriptRoot $File
    Write-Host "[psql] Applying $File ..." -ForegroundColor Cyan
    psql $dbUrl -f $filePath
    if ($LASTEXITCODE -ne 0) {
        Write-Error "psql failed on $File (exit $LASTEXITCODE). Aborting."
        exit $LASTEXITCODE
    }
}

# --- 1. Create all tables ------------------------------------
Invoke-Psql "schema/schema.sql"

# --- 2. Populate lookup dimensions ---------------------------
Invoke-Psql "schema/seed_dim_genre.sql"
Invoke-Psql "schema/seed_dim_distributor.sql"

# --- 3. Populate dim_date (2015-2035) ------------------------
Write-Host "[seed] Populating dim_date (2015-2035) ..." -ForegroundColor Cyan
boxoffice-int seed
if ($LASTEXITCODE -ne 0) {
    Write-Error "boxoffice-int seed failed (exit $LASTEXITCODE). Aborting."
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "[done] Database initialised successfully." -ForegroundColor Green
