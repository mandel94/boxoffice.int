# Connecting to the Neon Database

## Prerequisites

- Account on [neon.tech](https://neon.tech) with access to the `boxoffice.int` project
- Python with warehouse dependencies installed:
  ```bash
  pip install -e ".[warehouse]"
  ```

---

## 1. Retrieve the connection string

1. Go to **Neon Console → Projects → boxoffice.int → Dashboard**
2. Click **Connection Details** (top-right corner)
3. Select the branch (`main`) and role (`owner` or a dedicated role)
4. Copy the string in **psycopg2 / libpq** format:
   ```
   postgresql://<user>:<password>@<host>.neon.tech/<dbname>?sslmode=require
   ```

---

## 2. Local configuration

Create a `.env` file in the project root (already in `.gitignore`):

```env
BOXOFFICE_DB_URL=postgresql://<user>:<password>@<host>.neon.tech/<dbname>?sslmode=require
TMDB_API_KEY=<your-tmdb-key>
```

Then export the variable before using the CLI:

```powershell
# PowerShell
$env:BOXOFFICE_DB_URL = "postgresql://..."
```

```bash
# bash / zsh
export BOXOFFICE_DB_URL="postgresql://..."
```

Or use a tool like [`python-dotenv`](https://pypi.org/project/python-dotenv/) or [`direnv`](https://direnv.net/).

---

## 3. Initialize the schema (first time)

```bash
# Create all star schema tables
psql $BOXOFFICE_DB_URL -f schema/schema.sql

# Populate lookup tables
psql $BOXOFFICE_DB_URL -f schema/seed_dim_genre.sql
psql $BOXOFFICE_DB_URL -f schema/seed_dim_distributor.sql

# Populate dim_date (2015–2035)
boxoffice-int seed
```

---

## 4. Warehouse CLI commands

| Command | Description |
|---------|-------------|
| `boxoffice-int seed` | Populate `dim_date` (idempotent) |
| `boxoffice-int load --input <csv>` | Load a raw CSV into `fact_box_office_daily` |
| `boxoffice-int enrich-db` | Enrich `dim_film` with TMDB data |

---

## 5. GitHub Actions configuration

CI workflows read the connection string from repository secrets. To configure them:

1. **GitHub → Settings → Secrets and variables → Actions → New repository secret**
2. Add:
   - `BOXOFFICE_DB_URL` — the Neon connection string (with `sslmode=require`)
   - `TMDB_API_KEY` — the TMDB API key

---

## Notes

- The connection string includes the password in plain text: never commit it.
- Neon pauses the database after inactivity on the free plan; the first query may take 1–2 seconds of cold start.
- For separate environments (dev/prod) use distinct Neon branches and different connection strings.
