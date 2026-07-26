# MEIO Decision Intelligence Dashboard

This project now supports two runtime modes:

- PostgreSQL marts connected
- CSV demo fallback

The app will try PostgreSQL first through `DATABASE_URL`. If that is missing or the marts cannot be read, it will safely fall back to `sample_data.csv` so the dashboard still opens.

## App launch

From repo root:

```powershell
cd meio-optimizer
.\.venv\Scripts\activate
streamlit run app.py --server.port 8504
```

You can also run the root helper:

```powershell
cd <repository-root>
.\run_app.ps1
```

The helper prompts for the PostgreSQL password, URL-encodes it with PowerShell, sets `DATABASE_URL`, runs `test_db_connection.py`, and launches Streamlit only when the mart smoke test passes.

## Database configuration

Set `DATABASE_URL` before launching the app:

The app uses the local PostgreSQL database at `127.0.0.1:5432/meio_optimizer_db`.

Example in PowerShell:

```powershell
$env:DATABASE_URL="postgresql+psycopg2://postgres:ENCODED_POSTGRES_PASSWORD@127.0.0.1:5432/meio_optimizer_db"
```

If your password contains `@`, encode it as `%40` inside `DATABASE_URL`.
For example, password `Jayesh@73` becomes `Jayesh%4073`.

Shell `DATABASE_URL` has highest priority. A `.env` file will not override it.

## Database smoke test

```powershell
cd meio-optimizer
$env:DATABASE_URL="postgresql+psycopg2://postgres:ENCODED_POSTGRES_PASSWORD@127.0.0.1:5432/meio_optimizer_db"
.\.venv\Scripts\python.exe test_db_connection.py
.\.venv\Scripts\python.exe -m streamlit run app.py --server.port 8504
```

## What the app reads

The app expects these PostgreSQL objects when database mode is enabled:

- `mart_demand_stats`
- `mart_inventory_position`
- `mart_cost_to_serve`
- `mart_network_flow`
- `mart_data_quality_report`
- `dim_service_policy`

## SQL pipeline

The repo root SQL files are the source of truth for the pipeline:

- `schema.sql`
- `ingest.sql`
- `cleaning.sql`
- `marts.sql`

Run order:

```text
schema.sql -> ingest.sql -> cleaning.sql -> marts.sql
```

Important:

- `run_pipeline.ps1` supplies portable absolute CSV paths from `data/raw` to
  `ingest.sql`; use `-RawDataPath` to override the source.
- This repo does not assume a running local PostgreSQL database unless you provide one.

Root helper command:

```powershell
cd <repository-root>
.\run_pipeline.ps1 -Database meio_optimizer_db -User postgres
```

If PostgreSQL requires a password in your shell:

```powershell
$env:PGPASSWORD="YOUR_POSTGRES_PASSWORD"
.\run_pipeline.ps1 -Database meio_optimizer_db -User postgres
```

## SQL verification

After running the pipeline, you can verify the expected marts with:

```powershell
psql -d meio_optimizer_db -f verify_marts.sql
```

Or run the scripts one by one:

```powershell
psql -d meio_optimizer_db -f schema.sql
psql -d meio_optimizer_db -f ingest.sql
psql -d meio_optimizer_db -f cleaning.sql
psql -d meio_optimizer_db -f marts.sql
psql -d meio_optimizer_db -f verify_marts.sql
```

## Tests

From `meio-optimizer`:

```powershell
.\.venv\Scripts\python.exe -m compileall app.py src tests test_db_connection.py
.\.venv\Scripts\python.exe -m pytest
```

## Project structure

```text
MEIO/
|-- schema.sql
|-- ingest.sql
|-- cleaning.sql
|-- marts.sql
|-- verify_marts.sql
`-- meio-optimizer/
    |-- app.py
    |-- sample_data.csv
    |-- requirements.txt
    |-- src/
    |   |-- db.py
    |   |-- data_access.py
    |   |-- optimizer.py
    |   |-- scenario_engine.py
    |   |-- recommendations.py
    |   `-- data_quality.py
    `-- tests/
```
