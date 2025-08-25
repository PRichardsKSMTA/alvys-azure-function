# Deploying the Alvys Weekly Ingestion as an Azure Function

This project is ready to run as an **Azure Functions (Python)** app with a **Timer Trigger** that fires **every Monday at 00:00 UTC**. It will fetch the most recently completed Sunday->Saturday week from the Alvys API and insert into SQL Server for every client in your credentials table.

## What changed

- `weekly_ingest/function.json`: fixed to a 6‑field NCRONTAB schedule: `0 0 0 * * 1` (run Mondays at 00:00).
- `main.py`: date range calculation now uses your local timezone via `zoneinfo`:
  - Uses `LOCAL_TZ` env var (`America/Denver` by default) to compute **two Sundays ago 00:00** -> **yesterday (Sunday) 05:59:59.999Z** (UTC) when converted for the API.
- `local.settings.json` (template added): includes `WEBSITE_TIME_ZONE` and `LOCAL_TZ` so the timer and date ranges align with Mountain Time in both local and cloud environments.

## Prerequisites

- Python 3.11
- Azure Functions Core Tools v4
- An Azure Storage account
- A SQL Server reachable from the Function App (firewall/VNet as needed)
- The Microsoft ODBC Driver for SQL Server available in your hosting environment
  - **Windows plan**: ODBC driver is typically available.
  - **Linux plan**: ensure `msodbcsql18` and `unixodbc` are installed (custom container or a build that installs these packages).

## Local run

1. Copy `local.settings.json` and fill in the `ALVYS_SQL_CONN_STR` value. Example:
   ```text
   Driver={ODBC Driver 18 for SQL Server};Server=yourserver.database.windows.net;Database=yourdb;Uid=youruser;Pwd=yourpassword;Encrypt=yes;TrustServerCertificate=yes;
   ```
2. (Optional) Set `LOCAL_TZ` to another IANA timezone if not America/Denver.
3. Install Python deps:
   ```bash
   python -m venv .venv
   . .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
4. Start the Functions host:
   ```bash
   func start
   ```
   You should see the `weekly_ingest` timer scheduled for Mondays at 00:00 (America/Denver). You can also run on‑demand from the **Functions Core Tools** prompt: press **'a'** to run all triggers once.

## Azure deployment

### Create the Function App (one‑time)

You can use the Azure Portal, VS Code, or CLI. Example using **VS Code** Azure Functions extension:
1. Sign in to Azure.
2. Create a Function App (Python 3.11) on your preferred plan.
3. Select your Storage account when prompted.
4. Deploy this folder (the one containing `host.json`) to the Function App.

### Configure App Settings

In **Configuration -> Application settings**, add/update:

- `FUNCTIONS_WORKER_RUNTIME=python`
- `WEBSITE_TIME_ZONE` **not set** (Azure default UTC)   ← ensures the CRON runs at local midnight year‑round
- `LOCAL_TZ (not used)`            ← used by the Python code to compute local week boundaries
- `ALVYS_SQL_CONN_STR=Driver={ODBC Driver 18 for SQL Server};Server=...`
- `ALVYS_BLOB_CONN_STR=DefaultEndpointsProtocol=https;AccountName=...`
- `ALVYS_BLOB_CONTAINER=container-name`

Restart the Function App after saving.

### Verify schedule

After deployment, check **Monitor** for the `weekly_ingest` function. It will show the next schedule. You can also trigger a run immediately from the **Code + Test** blade or via `func azure functionapp run <appName> weekly_ingest` from Core Tools.

## Data window

Each run processes the **most recently completed Sunday->Saturday week** in **Mountain Time**:
- **Start**: two Sundays ago at `00:00:00.000` (America/Denver), converted to UTC for the API.
- **End**: yesterday (Sunday) `05:59:59.999Z` (UTC), which corresponds to `Saturday 23:59:59.999` in America/Denver.
- The code passes this range to the exporter and then inserts the exported files into SQL Server.

## Multi‑tenant

`weekly_ingest/__init__.py` reads `ALVYS_SQL_CONN_STR` and queries the credentials table to iterate all SCACs. For each SCAC it sets:
`ALVYS_TENANT_ID`, `ALVYS_CLIENT_ID`, `ALVYS_CLIENT_SECRET`, `ALVYS_GRANT_TYPE`, then runs export and insert.

Make sure your table contains the required columns (`SCAC`, `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`, `GRANT_TYPE`).

## Notes

- The in‑repo `requirements.txt` already includes `azure-functions`, `pyodbc`, `SQLAlchemy`, and others used by this app.
- No features were removed. The CLI still works; the Function path calls the same `run_export`/`run_insert` code as the CLI, with the only addition being explicit local timezone handling.
