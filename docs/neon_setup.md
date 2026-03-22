# Connessione al database Neon

## Prerequisiti

- Account su [neon.tech](https://neon.tech) con accesso al progetto `boxoffice.int`
- Python con le dipendenze warehouse installate:
  ```bash
  pip install -e ".[warehouse]"
  ```

---

## 1. Ottenere la connection string

1. Vai su **Neon Console → Projects → boxoffice.int → Dashboard**
2. Clicca **Connection Details** (angolo in alto a destra)
3. Seleziona il branch (`main`) e il ruolo (`owner` o il ruolo dedicato)
4. Copia la stringa in formato **psycopg2 / libpq**:
   ```
   postgresql://<user>:<password>@<host>.neon.tech/<dbname>?sslmode=require
   ```

---

## 2. Configurazione locale

Crea un file `.env` nella root del progetto (già in `.gitignore`):

```env
BOXOFFICE_DB_URL=postgresql://<user>:<password>@<host>.neon.tech/<dbname>?sslmode=require
TMDB_API_KEY=<la-tua-chiave-tmdb>
```

Poi esporta la variabile prima di usare la CLI:

```powershell
# PowerShell
$env:BOXOFFICE_DB_URL = "postgresql://..."
```

```bash
# bash / zsh
export BOXOFFICE_DB_URL="postgresql://..."
```

Oppure usa un tool come [`python-dotenv`](https://pypi.org/project/python-dotenv/) o [`direnv`](https://direnv.net/).

---

## 3. Inizializzare lo schema (prima volta)

```bash
# Crea tutte le tabelle del star schema
psql $BOXOFFICE_DB_URL -f src/boxoffice_int/warehouse/schema.sql

# Popola le tabelle di lookup
psql $BOXOFFICE_DB_URL -f schema/seed_dim_genre.sql
psql $BOXOFFICE_DB_URL -f schema/seed_dim_distributor.sql

# Popola dim_date (2015–2035)
boxoffice-int seed
```

---

## 4. Comandi CLI warehouse

| Comando | Descrizione |
|---------|-------------|
| `boxoffice-int seed` | Popola `dim_date` (idempotente) |
| `boxoffice-int load --input <csv>` | Carica un CSV raw in `fact_box_office_daily` |
| `boxoffice-int enrich-db` | Arricchisce `dim_film` con dati TMDB |

---

## 5. Configurazione GitHub Actions

I workflow CI leggono la connection string dai repository secrets. Per configurarli:

1. **GitHub → Settings → Secrets and variables → Actions → New repository secret**
2. Aggiungi:
   - `BOXOFFICE_DB_URL` — la connection string Neon (con `sslmode=require`)
   - `TMDB_API_KEY` — la chiave API TMDB

---

## Note

- La connection string include la password in chiaro: non commitarla mai.
- Neon mette in pausa il database dopo inattività sul piano gratuito; la prima query può richiedere 1–2 secondi di cold start.
- Per ambienti separati (dev/prod) usa branch Neon distinti e connection string diverse.
