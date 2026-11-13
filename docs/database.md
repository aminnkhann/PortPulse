# Local Compose Runbook

## Start the complete project

From the repository root, build and start PostgreSQL and Airflow together:

~~~sh
docker compose up --build -d
docker compose ps
~~~

On a fresh database volume, PostgreSQL applies migrations 001–007 in filename order. The Airflow UI is at <http://localhost:8080> with local demo credentials `admin` / `admin`. The DAG starts paused. PostgreSQL uses the local development user `portpulse` and password `admin`.

Compose keeps PostgreSQL data in `postgres_data` and Airflow's separate SQLite metadata in `airflow_data`. `docker compose down` stops the services without deleting data. Do not use `docker compose down -v` unless you intentionally want to delete both volumes.

### Existing PostgreSQL volume from the previous local setup

Changing `POSTGRES_PASSWORD` in Compose does not rotate the password of an already initialized PostgreSQL volume. To keep existing rows and switch the local database password to `admin`, run this one-time update through Compose:

~~~sh
docker compose up -d db
docker compose exec -T db psql -U portpulse -d portpulse -c "ALTER ROLE portpulse WITH PASSWORD 'admin';"
docker compose up --build -d
~~~

This changes the login password only; it does not remove or recreate the database volume.

## Phase 1 synthetic demo

~~~sh
docker compose exec airflow python -m scripts.ingest_fixture
docker compose exec airflow python -m scripts.reconcile
docker compose exec airflow python -m scripts.query_state PC-DEMO-001
~~~

The expected trusted ETA is `2026-01-15T15:40:00Z` from `COORD-DELAYED-01`. Rerunning ingestion and reconciliation is safe. This fixture is synthetic.

## Phase 2 Portnet V1 demo

The supported real-source adapter calls Portnet V1:

~~~sh
docker compose exec airflow python -m scripts.ingest_portnet --date 2026-11-10
~~~

Add `--port FIHEL` to filter to one port. The service is configured with the Compose database connection. Accepted observations, rejected records, and duplicate/collision issues use dedicated `portnet_eta_*` tables.

For an existing database already migrated through 006, apply migration 007 once:

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse -f /docker-entrypoint-initdb.d/007_create_portnet_eta_tables.sql
~~~

Do not replay migrations 001–006 against an existing database. Migrations 004 and 006 preserve legacy source rows. Existing `raw_event` rows and tables remain intact; no supported workflow creates new V2/AIS adapter records.

## Inspect Portnet and analytics tables

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse -c '\dt portnet_eta_*'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT event_id, port_call_id, port_area_code, event_time_utc, received_at_utc, ingested_at_utc, eta_utc FROM portnet_eta_observation ORDER BY port_call_id, port_area_code, event_time_utc;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT reason_code, count(*) FROM portnet_eta_rejected_event GROUP BY reason_code ORDER BY reason_code;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT issue_code, count(*) FROM portnet_eta_quality_issue GROUP BY issue_code ORDER BY issue_code;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.stg_portnet_eta LIMIT 5;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_portnet_latest_eta ORDER BY port_call_id, port_area_code;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_pipeline_quality;'
~~~

## dbt analytics

The Airflow image includes the pinned `dbt-postgres` dependency and a profile configured from the Compose environment. The DAG runs `dbt build` after ingestion. To inspect or run dbt directly inside that same container:

~~~sh
docker compose exec airflow dbt debug --project-dir /opt/portpulse/dbt
docker compose exec airflow dbt build --project-dir /opt/portpulse/dbt
~~~

The CI fixture rows are synthetic and are not loaded by this local dbt command. No live maritime API call is needed for the dbt build.

## Tests

Run lint and Python tests inside the Airflow image:

~~~sh
docker compose exec airflow ruff check /opt/portpulse
docker compose exec airflow python -m unittest discover -s tests -v
~~~

GitHub Actions runs the full PostgreSQL integration suite and dbt build with deterministic fixtures against a disposable database. CI does not call the live API.

## Stop services

~~~sh
docker compose down
~~~

This leaves both named data volumes in place.

## Historical evidence

See `reports/` for dated live-run summaries and separate synthetic injection evidence. The November 9 multi-source report is historical, not the supported ingestion flow.
