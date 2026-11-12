# Local PostgreSQL Runbook

## Start a fresh database

Install dependencies and start the local PostgreSQL service:

~~~sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
docker compose up -d
docker compose ps
~~~

A new Docker volume applies migrations 001–007 in filename order. The default password in Compose is for local development only; set POSTGRES_PASSWORD and optionally POSTGRES_PORT in an ignored .env file for local overrides.

## Phase 1 synthetic demo

~~~sh
python -m scripts.ingest_fixture
python -m scripts.reconcile
python -m scripts.query_state PC-DEMO-001
~~~

The expected trusted ETA is 2026-01-15T15:40:00Z from COORD-DELAYED-01. Rerunning ingestion and reconciliation is safe. The fixture summary is synthetic.

## Phase 2 Portnet V1 demo

The supported real-source adapter is scripts/ingest_portnet; it calls only Portnet V1:

~~~sh
python -m scripts.ingest_portnet --date 2026-11-10
~~~

Add --port FIHEL to filter to one port. Set DATABASE_URL to select a PostgreSQL database; otherwise the local development connection is used. Accepted observations, rejected records, and duplicate/collision issues use dedicated portnet_eta_* tables.

For an existing database already migrated through 006, apply migration 007 once:

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse < migrations/007_create_portnet_eta_tables.sql
~~~

Do not replay migrations 001–006 against an existing database. They remain part of applied migration history. Migrations 004 and 006 preserve legacy source rows. Existing raw_event rows and tables are retained; the reconciler keeps legacy visit-status handling for payloads already stored there. No V2/AIS adapter creates new records in the supported workflow, and no tables or data are dropped.

## Inspect Portnet tables

~~~sh
docker compose exec db psql -U portpulse -d portpulse -c '\dt portnet_eta_*'
docker compose exec db psql -U portpulse -d portpulse -c 'SELECT event_id, port_call_id, port_area_code, event_time_utc, received_at_utc, ingested_at_utc, eta_utc FROM portnet_eta_observation ORDER BY port_call_id, port_area_code, event_time_utc;'
docker compose exec db psql -U portpulse -d portpulse -c 'SELECT reason_code, count(*) FROM portnet_eta_rejected_event GROUP BY reason_code ORDER BY reason_code;'
docker compose exec db psql -U portpulse -d portpulse -c 'SELECT issue_code, count(*) FROM portnet_eta_quality_issue GROUP BY issue_code ORDER BY issue_code;'
~~~

## Build dbt analytics

The dbt project uses the pinned `dbt-postgres` version in `requirements-dbt.txt`. From the repository root, create an environment and install the application and dbt dependencies:

~~~sh
python -m venv .venv-dbt
source .venv-dbt/bin/activate
python -m pip install -r requirements.txt -r requirements-dbt.txt
~~~

Start PostgreSQL with `docker compose up -d db`. A fresh Docker volume applies migrations 001–007 automatically. For an existing database, follow the migration guidance above and make sure migration 007 has been applied.

The project reads a `portpulse_analytics` profile from `~/.dbt/profiles.yml`. If you do not already have a profiles file, copy the safe example; if you do, add its `portpulse_analytics` entry without replacing other profiles:

~~~sh
mkdir -p ~/.dbt
cp dbt/profiles.example.yml ~/.dbt/profiles.yml
~~~

Set connection values in your shell. Use the same password configured for the Compose database; the shown password is only Compose's local development default:

~~~sh
export DBT_HOST=127.0.0.1
export DBT_PORT=5432
export DBT_USER=portpulse
export DBT_PASSWORD=portpulse_local_only
export DBT_DATABASE=portpulse
export DBT_SCHEMA=analytics
~~~

From the dbt project directory, check the connection and build all three models and their data tests:

~~~sh
cd dbt
dbt debug
dbt build
~~~

Inspect the staging view and analytics tables from the repository root:

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.stg_portnet_eta LIMIT 5;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_portnet_latest_eta ORDER BY port_call_id, port_area_code;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_pipeline_quality;'
~~~

The CI fixture rows are synthetic and are not loaded by local dbt commands. No live maritime API call is needed to build the models.

## Tests

With a disposable PostgreSQL database running, set PORTPULSE_TEST_DATABASE_URL and run:

~~~sh
docker compose exec -T db createdb -U portpulse portpulse_test
PORTPULSE_TEST_DATABASE_URL=postgresql://portpulse:portpulse_local_only@127.0.0.1:5432/portpulse_test \
  python -m unittest discover -s tests -v
ruff check .
~~~

Without that variable, PostgreSQL integration tests are skipped. GitHub Actions configures its own disposable PostgreSQL service.

## Historical evidence

See reports/ for dated aggregate live-run reports and the separate synthetic injection report. The November 9 multi-source report is historical evidence, not the supported ingestion flow.
