# PortPulse final demo (5–7 minutes)

Run commands from the repository root unless a `cd` is shown. **Deployment: Apache Airflow is installed and runs locally; Docker Compose runs PostgreSQL only.** Airflow is not containerized. Its SQLite metadata is stored separately under `.airflow/`; the DAG, ingestion CLI, and dbt execute from the same `.venv-airflow` environment. The host Airflow process connects to Compose PostgreSQL at `127.0.0.1:5432`. API availability and records for the selected UTC date can vary; an empty valid poll is acceptable.

## 1. Start PostgreSQL and Airflow

In terminal 1:

~~~sh
docker compose up -d db
docker compose ps
docker compose exec -T db createdb -U portpulse portpulse_test
source .venv-airflow/bin/activate
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

The ingestion task uses the active environment's Python executable and project-root working directory. The dbt task calls the sibling `dbt` executable in that same virtual environment and runs from `dbt/`. Both inherit the configured environment; `DATABASE_URL` and the matching `DBT_*` settings must point to the same PostgreSQL database. The DAG is paused upon first creation. If it already exists in your local Airflow metadata, pause it before starting standalone with `airflow dags pause portpulse_daily_pipeline`; existing pause state is retained.

Open <http://localhost:8080>. Use the admin credentials printed by `airflow standalone` (local only). Airflow metadata is local SQLite; application data stays in the Compose PostgreSQL database.

## 2. Trigger and inspect the DAG

In terminal 2, from the same repository root:

~~~sh
source .venv-airflow/bin/activate
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
airflow dags trigger --conf '{"run_date":"2026-11-12"}' portpulse_daily_pipeline
airflow dags list-runs -d portpulse_daily_pipeline
~~~

The manual trigger works while the DAG is paused and leaves its daily schedule disabled. To enable the 06:00 UTC schedule, run `airflow dags unpause portpulse_daily_pipeline` or toggle the pause control in the UI. Avoid unpausing until database credentials are configured.

In the UI, open `portpulse_daily_pipeline`, show the Graph view and then task logs for `ingest_portnet`, `build_dbt_analytics`, and `verify_pipeline`. Ingestion calls the existing CLI; dbt builds the models and runs tests; verification logs counts and checks consistency. A failed command fails its task and the DAG retries it.

## 3. Show stored API observations

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS stored_observations FROM public.portnet_eta_observation;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT event_id, port_call_id, port_area_code, event_time_utc, received_at_utc, ingested_at_utc, eta_utc FROM public.portnet_eta_observation ORDER BY ingested_at_utc DESC LIMIT 5;'
~~~

These are cumulative stored rows, not the number fetched by this run. The task log's ingestion summary is the evidence for this run's API poll.

## 4. Show analytics and quality

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT port_call_id, port_area_code, event_id, provider_updated_at_utc, eta_utc FROM analytics.mart_portnet_latest_eta ORDER BY port_call_id, port_area_code LIMIT 10;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_pipeline_quality;'
~~~

Explain that the latest mart selects one eligible ETA per call and area and excludes collision IDs. The quality mart reports accepted observations, rejected observations, duplicate issues, collision issues, and latest ETA rows. Zero rejections is a valid result.

## 5. Explain tests and synthetic reconciliation

Run or show the test evidence:

~~~sh
ruff check .
PORTPULSE_TEST_DATABASE_URL=postgresql://portpulse:portpulse_local_only@127.0.0.1:5432/portpulse_test python -m unittest discover -s tests -v
cd dbt && dbt debug && dbt build
~~~

Python tests cover the Phase 1 fixture/reconciliation, Phase 2 normalization and reliability, and Airflow date handling, failure propagation, dependency order, and health-check behavior. dbt tests cover schema expectations, deterministic latest-row choice, collision exclusion, and one latest ETA per call/area. CI seeds deterministic synthetic observations and never calls the live API.

Demonstrate Phase 1 as a separate synthetic workflow (not the real Portnet path):

~~~sh
cd ..
python -m scripts.ingest_fixture
python -m scripts.reconcile
python -m scripts.query_state PC-DEMO-001
~~~

Point out the deterministic golden result: ETA `2026-01-15T15:40:00Z` from `COORD-DELAYED-01`. Clarify that real Portnet observations are not reconciled through this synthetic three-source engine.

## Suggested timing

| Segment | Time |
| --- | ---: |
| Architecture and Airflow setup | 1 minute |
| Trigger, graph, and task logs | 2 minutes |
| PostgreSQL and dbt marts | 1.5 minutes |
| Tests and separate synthetic demo | 1.5 minutes |
