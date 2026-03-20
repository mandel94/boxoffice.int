# ============================================================
#  boxoffice.int - dev CLI dashboard
#  Usage: . .\dev.ps1
# ============================================================

# --- Load .env into the current session ----------------------
$envFile = Join-Path $PSScriptRoot ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | Where-Object { $_ -match '^\s*[^#]\S+=\S*' } | ForEach-Object {
        $key, $value = $_ -split '=', 2
        [System.Environment]::SetEnvironmentVariable($key.Trim(), $value.Trim())
    }
    Write-Host "[env] Loaded $envFile" -ForegroundColor Green
} else {
    Write-Warning "[env] .env file not found at $envFile - skipping"
}

# --- Helper aliases ------------------------------------------
function ingest {
    param(
        [string]$Start,
        [string]$End,
        [switch]$Yesterday,
        [switch]$ThisWeek,
        [switch]$LastWeek,
        [switch]$ThisMonth,
        [switch]$LastMonth
    )
    $args_list = @("ingest")
    if ($Yesterday)  { $args_list += "--yesterday" }
    elseif ($ThisWeek)  { $args_list += "--this-week" }
    elseif ($LastWeek)  { $args_list += "--last-week" }
    elseif ($ThisMonth) { $args_list += "--this-month" }
    elseif ($LastMonth) { $args_list += "--last-month" }
    elseif ($Start -and $End) { $args_list += "--start", $Start, "--end", $End }
    else { Write-Error "Specify a date preset or --Start/--End"; return }
    py -3.12 -m boxoffice_int.pipeline @args_list
}

function enrich {
    param([Parameter(Mandatory)][string]$Path)
    py -3.12 -m boxoffice_int.pipeline enrich --input $Path
}

function build {
    param(
        [Parameter(Mandatory)][string]$Path,
        [string]$Metadata
    )
    $args_list = @("build", "--input", $Path)
    if ($Metadata) { $args_list += "--metadata", $Metadata }
    py -3.12 -m boxoffice_int.pipeline @args_list
}

function run-tests {
    py -3.12 -m pytest tests/ -v
}

function seed-dim-date {
    py -3.12 -c "
from boxoffice_int.warehouse.loader import get_connection, seed_dim_date
conn = get_connection()
n = seed_dim_date(conn)
print(f'seed_dim_date: {n} rows inserted')
conn.close()
"
}

function load-warehouse {
    param(
        [Parameter(Mandatory)][string]$CsvPath,
        [int]$SourceKey = 1
    )
    py -3.12 -c "
from pathlib import Path
from boxoffice_int.warehouse.loader import load_box_office_raw
n = load_box_office_raw(Path(r'$CsvPath'), source_key=$SourceKey)
print(f'load_box_office_raw: {n} rows inserted')
"
}

# --- Summary -------------------------------------------------
Write-Host ""
Write-Host "  boxoffice.int - commands available:" -ForegroundColor Cyan
Write-Host "    ingest  -Yesterday | -ThisWeek | -LastWeek | -ThisMonth | -LastMonth"
Write-Host "    ingest  -Start <YYYY-MM-DD> -End <YYYY-MM-DD>"
Write-Host "    enrich  -Path <path/to/raw.csv>"
Write-Host "    build   -Path <path/to/raw.csv> [-Metadata <path/to/metadata.csv>]"
Write-Host "    run-tests"
Write-Host "    load-warehouse  -CsvPath <path/to/raw.csv> [-SourceKey <int>]"
Write-Host "    seed-dim-date"
Write-Host ""
Write-Host "  env vars required:" -ForegroundColor DarkGray
Write-Host "    TMDB_API_KEY       (enrich step)"
Write-Host "    BOXOFFICE_DB_URL   (warehouse steps, e.g. postgresql://user:pass@host:5432/db)"
Write-Host ""
