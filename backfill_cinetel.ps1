# ============================================================
#  backfill_cinetel.ps1
#  Scraping + caricamento DB da Cinetel per gli ultimi 3 mesi.
#
#  Uso:
#    .\backfill_cinetel.ps1
#    .\backfill_cinetel.ps1 -StartDate 2026-02-01 -EndDate 2026-04-30
#    .\backfill_cinetel.ps1 -DryRun         # stampa i giorni senza eseguire
# ============================================================
param(
    [string]$StartDate = "",
    [string]$EndDate   = "",
    [switch]$DryRun
)

# --- Carica .env nella sessione corrente ---------------------
$envFile = Join-Path $PSScriptRoot ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | Where-Object { $_ -match '^\s*[^#]\S+=\S*' } | ForEach-Object {
        $key, $value = $_ -split '=', 2
        [System.Environment]::SetEnvironmentVariable($key.Trim(), $value.Trim())
    }
    Write-Host "[env] .env caricato" -ForegroundColor DarkGray
}

# --- Parametri -----------------------------------------------
$CINETEL_URL = "https://www.cinetel.it/homepage"
$DATA_DIR    = Join-Path $PSScriptRoot "data\raw\box_office_raw"

$today = [datetime]::Today
if ($StartDate -eq "") {
    # Tre mesi fa (stesso giorno, 3 mesi indietro)
    $start = $today.AddMonths(-3)
} else {
    $start = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
}
if ($EndDate -eq "") {
    $end = $today.AddDays(-1)   # ieri (il bollettino odierno potrebbe non essere ancora pubblicato)
} else {
    $end = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null)
}

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host "  Backfill Cinetel: $($start.ToString('yyyy-MM-dd')) → $($end.ToString('yyyy-MM-dd'))" -ForegroundColor Cyan
Write-Host "=====================================================" -ForegroundColor Cyan
if ($DryRun) { Write-Host "  [DRY-RUN] nessun comando verra' eseguito" -ForegroundColor Yellow }
Write-Host ""

# --- Loop sui giorni -----------------------------------------
$ok    = [System.Collections.Generic.List[string]]::new()
$skipped = [System.Collections.Generic.List[string]]::new()
$failed  = [System.Collections.Generic.List[string]]::new()

$current = $start
while ($current -le $end) {
    $dateStr = $current.ToString("yyyy-MM-dd")
    # Cinetel pubblica bollettini giornalieri anche il sabato — nessun giorno viene saltato.
    $csvPath = Join-Path $DATA_DIR "cinetel_$dateStr.csv"

    # ---- Step 1: ingest-cinetel ----
    Write-Host "[$dateStr] ingest-cinetel..." -ForegroundColor White -NoNewline

    if ($DryRun) {
        Write-Host " [DRY-RUN]" -ForegroundColor Yellow
        $current = $current.AddDays(1)
        continue
    }

    $ingestArgs = @("ingest-cinetel", "--date", $dateStr, "--url", $CINETEL_URL)
    & boxoffice-int @ingestArgs *>$null
    $ingestExit = $LASTEXITCODE
    if ($ingestExit -ne 0) {
        Write-Host " ERRORE (ingest)" -ForegroundColor Red
        $failed.Add($dateStr)
        $current = $current.AddDays(1)
        continue
    }

    # Verifica che il CSV esista prima di procedere al caricamento
    if (-not (Test-Path $csvPath)) {
        Write-Host " ERRORE (CSV non trovato: $csvPath)" -ForegroundColor Red
        $failed.Add($dateStr)
        $current = $current.AddDays(1)
        continue
    }

    # ---- Step 2: load nel DB ----
    Write-Host " OK | load DB..." -ForegroundColor DarkGreen -NoNewline

    $loadArgs = @("load-cinetel", "--input", $csvPath)
    & boxoffice-int @loadArgs *>$null
    $loadExit = $LASTEXITCODE
    if ($loadExit -ne 0) {
        Write-Host " ERRORE (load)" -ForegroundColor Red
        $failed.Add($dateStr)
        $current = $current.AddDays(1)
        continue
    }

    Write-Host " OK" -ForegroundColor Green
    $ok.Add($dateStr)

    $current = $current.AddDays(1)
}

# --- Riepilogo -----------------------------------------------
Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host "  Riepilogo backfill" -ForegroundColor Cyan
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host "  Completati : $($ok.Count)" -ForegroundColor Green
Write-Host "  Falliti    : $($failed.Count)" -ForegroundColor $(if ($failed.Count -gt 0) { "Red" } else { "Green" })
if ($failed.Count -gt 0) {
    Write-Host ""
    Write-Host "  Giorni falliti:" -ForegroundColor Red
    $failed | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
}
Write-Host ""
