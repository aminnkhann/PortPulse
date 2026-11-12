# PortPulse

PortPulse is a small data engineering capstone that ingests port-call ETA updates, validates and stores them, then produces queryable analytics. Its supported real-data path is **Digitraffic Portnet V1 → Python/Pydantic → PostgreSQL → dbt → analytics marts**, orchestrated by Apache Airflow.

## Architecture and data

- **Phase 1 (synthetic):** deterministic JSONL fixtures exercise validation, quarantine, duplicate and ID-collision handling, and trusted ETA reconciliation across three simulated sources. The golden ETA is `2026-01-15T15:40:00Z` from `COORD-DELAYED-01`.
- **Phases 2–3 (Portnet):** the existing Python CLI fetches Portnet V1 records, validates and stores observations/rejections/quality issues in PostgreSQL, and dbt builds staging and analytics marts. Real Portnet observations are not reconciled by the Phase 1 synthetic engine.
- **Orchestration:** one Airflow DAG runs ingestion, `dbt build`, then database and metric verification. Airflow adds orchestration only; PostgreSQL remains the application store.

No Kafka, MQTT, AIS, other API provider, cloud deployment, or dashboard is part of the supported workflow. The old V2/AIS experiment remains historical only.

## Deployment approach

Airflow is installed and runs locally on Ubuntu. Docker Compose runs PostgreSQL only; it does not define Airflow containers or mounts. The Airflow process uses the same `.venv-airflow` that contains the ingestion dependencies and dbt. The DAG locates the project from its own file, invokes ingestion with Airflow's `sys.executable`, and invokes dbt from the sibling executable in that same virtual environment. Both tasks inherit the Airflow process environment: `DATABASE_URL` configures ingestion and verification, while matching `DBT_*` values configure dbt. Use `127.0.0.1:5432` for PostgreSQL from local Airflow. Airflow metadata stays in SQLite under `.airflow/`; it is separate from PortPulse data.

## Local prerequisites and installation

Use Ubuntu with Python 3.12, Docker Compose, and a working local PostgreSQL Docker service. From the repository root:

~~~sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt -r requirements-dbt.txt
docker compose up -d db
docker compose ps
~~~

A fresh PostgreSQL volume applies migrations 001–007 in order. For an existing database, apply any missing migration only; see [database runbook](docs/database.md). Do not replay already-applied migrations.

## Run the ingestion CLI independently

The supported adapter remains usable without Airflow. Provide a UTC collection date:

~~~sh
python -m scripts.ingest_portnet --date 2026-11-12
~~~

Add `--port FIHEL` to filter by port. The CLI uses `DATABASE_URL` when set, otherwise the local Compose development connection. Repeated runs use the existing duplicate, collision, and quarantine behavior.

## Build dbt analytics

Create a project-local profile directory (ignored by Git) so you do not overwrite a personal dbt profile:

~~~sh
mkdir -p .dbt-profile
cp dbt/profiles.example.yml .dbt-profile/profiles.yml
export DBT_PROFILES_DIR="$PWD/.dbt-profile"
export DATABASE_URL='postgresql://portpulse:portpulse_local_only@127.0.0.1:5432/portpulse'
export DBT_HOST=127.0.0.1
export DBT_PORT=5432
export DBT_USER=portpulse
export DBT_PASSWORD=portpulse_local_only
export DBT_DATABASE=portpulse
export DBT_SCHEMA=analytics
cd dbt
dbt debug
dbt build
cd ..
~~~

Replace the local development password with the value configured for your database. Do not commit passwords, `.env` files, or personal dbt profiles. `dbt build` builds all three models and runs their data tests.

## Run the Airflow pipeline

The local setup pins Apache Airflow 2.10.5. Its [official quick start](https://airflow.apache.org/docs/apache-airflow/2.10.5/start.html) supports Python 3.12 and recommends pip with the versioned constraints file. Install Airflow with those constraints, then install the project/dbt requirements while repeating the Airflow pin to prevent accidental upgrade or downgrade.

~~~sh
python3.12 -m venv .venv-airflow
source .venv-airflow/bin/activate
python -m pip install -r requirements-airflow.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
python -m pip install -r requirements-airflow.txt -r requirements.txt -r requirements-dbt.txt
mkdir -p .dbt-profile
cp dbt/profiles.example.yml .dbt-profile/profiles.yml
export DBT_PROFILES_DIR="$PWD/.dbt-profile"
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export DATABASE_URL='postgresql://portpulse:portpulse_local_only@127.0.0.1:5432/portpulse'
export DBT_HOST=127.0.0.1 DBT_PORT=5432 DBT_USER=portpulse
export DBT_PASSWORD=portpulse_local_only DBT_DATABASE=portpulse DBT_SCHEMA=analytics
airflow standalone
~~~

This runs Airflow locally with a separate SQLite metadata database; the PortPulse application still uses the existing PostgreSQL service. Open [http://localhost:8080](http://localhost:8080) and use the admin credentials printed by `airflow standalone`. The `.airflow/` directory is ignored by Git. Keep the local-only credentials private.

The DAG tasks execute in this same virtual environment and inherit these PostgreSQL credentials. Set the matching database and dbt environment values before starting Airflow. The DAG is paused when first created, so the scheduler will not begin live ingestion until you explicitly unpause it. If this DAG already exists in local Airflow metadata, pause it before startup with `airflow dags pause portpulse_daily_pipeline`; Airflow preserves the existing pause state.

The DAG ID is `portpulse_daily_pipeline`. It runs daily at 06:00 UTC, with catch-up disabled and at most one active run. Its tasks run sequentially: `ingest_portnet` → `build_dbt_analytics` → `verify_pipeline`. To trigger from another terminal, use the same Airflow environment and repository root:

~~~sh
source .venv-airflow/bin/activate
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
airflow dags trigger --conf '{"run_date":"2026-11-12"}' portpulse_daily_pipeline
airflow dags list-runs -d portpulse_daily_pipeline
~~~

Omit `--conf` to use the run's logical date converted to UTC. The UI Graph view and each task's Logs tab show the execution order, command output, dbt tests, and final health summary.

To enable its daily schedule, run `airflow dags unpause portpulse_daily_pipeline` (or toggle the pause control in the UI). To run it once while keeping the schedule paused, use the CLI `airflow dags trigger --conf '{"run_date":"2026-11-12"}' portpulse_daily_pipeline`, or open the DAG in the UI, select **Trigger DAG**, and optionally enter `{"run_date":"2026-11-12"}`. Manual triggers are allowed while a DAG is paused.

## Query analytics outputs

Run these from the repository root after `dbt build`:

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.stg_portnet_eta LIMIT 5;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT port_call_id, port_area_code, event_id, provider_updated_at_utc, eta_utc FROM analytics.mart_portnet_latest_eta ORDER BY port_call_id, port_area_code;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_pipeline_quality;'
~~~

## Phase 1 synthetic reconciliation

~~~sh
python -m scripts.ingest_fixture
python -m scripts.reconcile
python -m scripts.query_state PC-DEMO-001
~~~

## Tests and checks

Create a separate test database once, then use it for the integration suite. This keeps the `portpulse` application database untouched:

~~~sh
docker compose exec -T db createdb -U portpulse portpulse_test
PORTPULSE_TEST_DATABASE_URL=postgresql://portpulse:portpulse_local_only@127.0.0.1:5432/portpulse_test \
  python -m unittest discover -s tests -v
ruff check .
cd dbt && dbt debug && dbt build
~~~

Airflow parse and task checks are included in the Python suite when the Airflow dependencies are installed. GitHub Actions runs Python tests, Ruff, DAG import checks, and dbt build against deterministic fixtures; it does not call the live API.

## Project guide

- [Product requirements](PRD.md)
- [Architecture](ARCHITECTURE.md)
- [Database runbook](docs/database.md)
- [Data dictionary](docs/data_dictionary.md)
- [Final demo runbook](docs/final_demo.md)
- [Presentation outline](docs/presentation_outline.md)
- [Progress](progress.md)
- [Dated reports](reports/)
