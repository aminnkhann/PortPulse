# PortPulse

PortPulse is a small data engineering capstone that ingests port-call ETA updates, validates and stores them, then produces queryable analytics. Its supported real-data path is **Digitraffic Portnet V1 → Python/Pydantic → PostgreSQL → dbt → analytics marts**, orchestrated by Apache Airflow.

## Architecture and data

- **Phase 1 (synthetic):** deterministic JSONL fixtures exercise validation, quarantine, duplicate and ID-collision handling, and trusted ETA reconciliation across three simulated sources. The golden ETA is `2026-01-15T15:40:00Z` from `COORD-DELAYED-01`.
- **Phases 2–3 (Portnet):** the existing Python CLI fetches Portnet V1 records, validates and stores observations/rejections/quality issues in PostgreSQL, and dbt builds staging and analytics marts. Real Portnet observations are not reconciled by the Phase 1 synthetic engine.
- **Orchestration:** one Airflow DAG runs ingestion, `dbt build`, then database and metric verification. Airflow adds orchestration only; PostgreSQL remains the application store.

No Kafka, MQTT, AIS, other API provider, cloud deployment, or dashboard is part of the supported workflow. The old V2/AIS experiment remains historical only.

## Deployment approach

Run the complete local demo with Docker Compose. Compose starts PostgreSQL and one custom Airflow container; the Airflow container includes the PortPulse Python dependencies and dbt, then runs the UI, scheduler, ingestion task, and dbt task. Inside Compose, the DAG connects to PostgreSQL using hostname `db`. PostgreSQL keeps PortPulse application data in the existing `postgres_data` volume; Airflow metadata uses a separate `airflow_data` volume. This is a local learning setup, not a production deployment.

## Local prerequisites and installation

Install Docker Engine and Docker Compose v2. From the repository root, build and start everything with one command:

~~~sh
docker compose up --build -d
docker compose ps
~~~

On a fresh database volume, PostgreSQL applies migrations 001–007 in order. Existing `postgres_data` is preserved; `docker compose down` does not delete it. Do not use `docker compose down -v` if you want to keep your data.

Open [http://localhost:8080](http://localhost:8080) and sign in with **admin / admin**. These simple credentials are for local development only. The DAG is created paused so it cannot start a live API poll before you choose to run it.

To stop the containers while keeping both database volumes, run:

~~~sh
docker compose down
~~~

### Existing PostgreSQL volume

PostgreSQL does not change an existing database user's password just because the Compose environment changes. If you are upgrading the previous local setup, start the database and run this one-time password update through Compose; it preserves all database rows and volumes:

~~~sh
docker compose up -d db
docker compose exec -T db psql -U portpulse -d portpulse -c "ALTER ROLE portpulse WITH PASSWORD 'admin';"
docker compose up --build -d
~~~

Fresh volumes already use the local `admin` password. Migrations run automatically on first initialization.

## Run the ingestion CLI independently

The ingestion CLI is also available inside the Airflow container. Provide a UTC collection date:

~~~sh
docker compose exec airflow python -m scripts.ingest_portnet --date 2026-11-12
~~~

Add `--port FIHEL` to filter by port. Repeated runs use the existing duplicate, collision, and quarantine behavior.

## Build dbt analytics

The Airflow image includes dbt and a profile that reads credentials from the Compose environment. dbt builds automatically when you trigger the DAG. To run the same build manually inside the container:

~~~sh
docker compose exec airflow dbt debug --project-dir /opt/portpulse/dbt
docker compose exec airflow dbt build --project-dir /opt/portpulse/dbt
~~~

`dbt build` builds all three models and runs their data tests. The local example uses `admin` as the PostgreSQL password; change it consistently in `docker-compose.yml` if you want a different local password.

## Run the Airflow pipeline

The Dockerfile pins Apache Airflow 2.10.5 and installs the project and dbt dependencies with the matching official constraints. The DAG ID is `portpulse_daily_pipeline`. It runs daily at 06:00 UTC after you unpause it, with catch-up disabled and at most one active run. The tasks run sequentially: `ingest_portnet` → `build_dbt_analytics` → `verify_pipeline`.

Check container health and DAG discovery with:

~~~sh
docker compose ps
docker compose exec airflow airflow dags list
~~~

To run the DAG once, leave it paused and select **Trigger DAG** in the UI. To enable its daily schedule, toggle the pause control in the UI. You can also use the Airflow CLI inside the Compose container:

~~~sh
docker compose exec airflow airflow dags trigger --conf '{"run_date":"2026-11-12"}' portpulse_daily_pipeline
docker compose exec airflow airflow dags unpause portpulse_daily_pipeline
~~~

Omit `--conf` to use the run's logical date converted to UTC. The UI Graph view and task Logs tabs show task status, dbt tests, and the final health summary.

## Query analytics outputs

Run these from the repository root after `dbt build`:

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.stg_portnet_eta LIMIT 5;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT port_call_id, port_area_code, event_id, provider_updated_at_utc, eta_utc FROM analytics.mart_portnet_latest_eta ORDER BY port_call_id, port_area_code;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_pipeline_quality;'
~~~

## Phase 1 synthetic reconciliation

~~~sh
docker compose exec airflow python -m scripts.ingest_fixture
docker compose exec airflow python -m scripts.reconcile
docker compose exec airflow python -m scripts.query_state PC-DEMO-001
~~~

## Tests and checks

Run lint and unit checks inside the Airflow container; the database-backed suite and full dbt build also run in GitHub Actions against a disposable PostgreSQL service:

~~~sh
docker compose exec airflow ruff check /opt/portpulse
docker compose exec airflow python -m unittest discover -s tests -v
~~~

GitHub Actions runs Python tests, Ruff, DAG import checks, and dbt build against deterministic fixtures; it does not call the live API.

## Project guide

- [Product requirements](PRD.md)
- [Architecture](ARCHITECTURE.md)
- [Database runbook](docs/database.md)
- [Data dictionary](docs/data_dictionary.md)
- [Final demo runbook](docs/final_demo.md)
- [Presentation outline](docs/presentation_outline.md)
- [Progress](progress.md)
- [Dated reports](reports/)
