# MEIO PostgreSQL Planning System

This repository contains a PostgreSQL-backed multi-echelon inventory optimization pipeline and a Streamlit planning dashboard.

## Prerequisites

- PostgreSQL installed locally
- `psql` available on `PATH`
- PowerShell
- Python virtual environment already present at `meio-optimizer\.venv`

## Create the database

```powershell
createdb -U postgres meio_optimizer_db
```

If `createdb` is not on your `PATH`, use pgAdmin to create a database named `meio_optimizer_db`.

## Run the SQL pipeline

From repo root:

```powershell
cd C:\Users\jayes\Desktop\MEIO
.\run_pipeline.ps1 -Database meio_optimizer_db -User postgres
```

If your local PostgreSQL user requires a password, set it for the current PowerShell session first:

```powershell
$env:PGPASSWORD="YOUR_POSTGRES_PASSWORD"
.\run_pipeline.ps1 -Database meio_optimizer_db -User postgres
```

If your PostgreSQL account is different:

```powershell
.\run_pipeline.ps1 -Database meio_optimizer_db -User postgres
```

The runner executes:

```text
schema.sql -> ingest.sql -> cleaning.sql -> marts.sql -> verify_marts.sql
```

`ingest.sql` loads CSV files from:

```text
C:\Users\jayes\Desktop\MEIO\data\raw
```

## Set DATABASE_URL

```powershell
$env:DATABASE_URL="postgresql+psycopg2://postgres:ENCODED_POSTGRES_PASSWORD@127.0.0.1:5432/meio_optimizer_db"
```

If your password contains `@`, encode it as `%40` inside `DATABASE_URL`.
For example, password `Jayesh@73` becomes `Jayesh%4073`.

If your local PostgreSQL user has no password:

```powershell
$env:DATABASE_URL="postgresql://postgres@localhost:5432/meio_optimizer_db"
```

Shell `DATABASE_URL` has highest priority. A `.env` file will not override it.

## Test the app database connection

```powershell
cd C:\Users\jayes\Desktop\MEIO\meio-optimizer
$env:DATABASE_URL="postgresql+psycopg2://postgres:ENCODED_POSTGRES_PASSWORD@127.0.0.1:5432/meio_optimizer_db"
.\.venv\Scripts\python.exe test_db_connection.py
.\.venv\Scripts\python.exe -m streamlit run app.py --server.port 8504
```

## Run the Streamlit app

The helper prompts for the PostgreSQL password, URL-encodes it safely, sets `DATABASE_URL`, runs `test_db_connection.py`, and launches Streamlit only if the mart smoke test passes.

```powershell
cd C:\Users\jayes\Desktop\MEIO
.\run_app.ps1
```

Direct command:

```powershell
cd C:\Users\jayes\Desktop\MEIO\meio-optimizer
$env:DATABASE_URL="postgresql+psycopg2://postgres:ENCODED_POSTGRES_PASSWORD@127.0.0.1:5432/meio_optimizer_db"
.\.venv\Scripts\python.exe test_db_connection.py
.\.venv\Scripts\python.exe -m streamlit run app.py --server.port 8504
```

## Run tests

```powershell
cd C:\Users\jayes\Desktop\MEIO\meio-optimizer
.\.venv\Scripts\python.exe -m compileall app.py src tests test_db_connection.py
.\.venv\Scripts\python.exe -m pytest
```

## Expected mart tables

- `mart_demand_stats`
- `mart_inventory_position`
- `mart_cost_to_serve`
- `mart_network_flow`
- `mart_data_quality_report`

Use this verification query after the pipeline:

```powershell
psql -U postgres -d meio_optimizer_db -f verify_marts.sql
```
