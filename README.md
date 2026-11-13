# PortPulse

PortPulse ingests Finnish maritime port-call ETA updates, validates them, stores them in PostgreSQL, and builds analytics with dbt. Airflow orchestrates the real-data workflow.

## What it does

PortPulse keeps two data paths separate:

- **Synthetic reconciliation:** deterministic fixtures exercise the Phase 1 three-source ETA rules, duplicate and collision handling, and quarantine. Its golden ETA is `2026-01-15T15:40:00Z` from `COORD-DELAYED-01`.
- **Portnet V1 ingestion:** a Python CLI fetches Digitraffic Portnet V1 data, validates each record with Pydantic, and stores accepted observations, rejected records, and quality issues in PostgreSQL. This source provides Finnish maritime data; it is not operational port data for Hamburg.
- **dbt analytics:** dbt stages Portnet observations and builds latest-ETA and pipeline-quality models. It does not run Phase 1 reconciliation over Portnet records.
- **Airflow orchestration:** the `portpulse_daily_pipeline` DAG runs ingestion, `dbt build`, and pipeline verification in sequence.

## Architecture and stack

`Digitraffic Portnet V1 → Python / Pydantic → PostgreSQL → dbt analytics`

Apache Airflow schedules and monitors the Portnet workflow. Docker Compose starts PostgreSQL and the custom Airflow image; the root Dockerfile builds the Airflow environment with the project and dbt dependencies. See [ARCHITECTURE.md](ARCHITECTURE.md) for the workflow and [the data dictionary](docs/data_dictionary.md) for fields and tables.

## Quick start

Prerequisites: Docker Engine and Docker Compose v2. From the repository root:

```sh
docker compose up --build -d
docker compose ps
```

This starts PostgreSQL and Airflow. The services publish their ports on the local machine only. Open Airflow at [http://localhost:8080](http://localhost:8080) and sign in with the local demo credentials `admin` / `admin`. These defaults are for local demonstration only; do not expose this setup to the public internet or use it as a production deployment. The DAG starts paused so it will not poll the live API until you trigger it or enable its schedule.

PostgreSQL applies migrations 001–007 when its data volume is initialized for the first time. The named `postgres_data` and `airflow_data` volumes persist across `docker compose down`. To stop the services while retaining data:

```sh
docker compose down
```

Avoid `docker compose down -v` unless you intend to remove both volumes. See [docs/database.md](docs/database.md) for existing-volume migration and troubleshooting details.

## Run the Portnet pipeline

Trigger `portpulse_daily_pipeline` from the Airflow UI. To run it from the container CLI with a specific UTC date:

```sh
docker compose exec airflow airflow dags trigger \
  --conf '{"run_date":"2026-11-12"}' portpulse_daily_pipeline
```

The DAG is scheduled daily at 06:00 UTC after it is unpaused. It runs `ingest_portnet`, `build_dbt_analytics`, and `verify_pipeline`. To inspect discovery and task output:

```sh
docker compose exec airflow airflow dags list
docker compose logs --tail 100 airflow
```

To run ingestion independently, use a UTC collection date and optionally filter by port:

```sh
docker compose exec airflow python -m scripts.ingest_portnet --date 2026-11-12
# Optional port filter:
docker compose exec airflow python -m scripts.ingest_portnet --date 2026-11-12 --port FIHEL
```

Portnet V1 is provided by Digitraffic and covers Finnish maritime data. API availability and date eligibility are external; an empty valid poll is possible. See [Digitraffic maritime data](https://www.digitraffic.fi/en/marine-traffic/) for source information.

## Inspect PostgreSQL and dbt outputs

The Airflow image includes dbt and a Compose-configured PostgreSQL profile. To build the models manually:

```sh
docker compose exec airflow dbt debug --project-dir /opt/portpulse/dbt
docker compose exec airflow dbt build --project-dir /opt/portpulse/dbt
```

Inspect the resulting marts with `psql`:

```sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.stg_portnet_eta LIMIT 5;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT port_call_id, port_area_code, event_id, provider_updated_at_utc, eta_utc FROM analytics.mart_portnet_latest_eta ORDER BY port_call_id, port_area_code LIMIT 10;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_pipeline_quality;'
```

For cumulative stored observations and quarantined records, see the queries in [docs/database.md](docs/database.md).

## Run the synthetic Phase 1 example

This deterministic fixture flow is separate from Portnet ingestion and dbt:

```sh
docker compose exec airflow python -m scripts.ingest_fixture
docker compose exec airflow python -m scripts.reconcile
docker compose exec airflow python -m scripts.query_state PC-DEMO-001
```

## Tests and limitations

Inside the running Airflow container:

```sh
docker compose exec airflow ruff check /opt/portpulse
docker compose exec -e PORTPULSE_TEST_DATABASE_URL=postgresql://portpulse:admin@db:5432/portpulse airflow python -m unittest discover -s tests -v
```

GitHub Actions also runs Python tests, Ruff, Airflow DAG import checks, and a PostgreSQL-backed dbt build using deterministic test data. CI does not call the live API. These checks verify the tested code paths; they are not performance benchmarks or a production-readiness assessment.

The Compose setup uses local demo credentials and SQLite for Airflow metadata. Credentials are not configurable through a supported setup workflow yet. The ports bind to loopback by default; keep them local and do not expose them to the public internet. The project has no dashboard or cloud deployment, and the quality mart describes Portnet ingestion rather than synthetic reconciliation.

## Technical documentation

- [Architecture](ARCHITECTURE.md)
- [Database operations and troubleshooting](docs/database.md)
- [Data dictionary](docs/data_dictionary.md)
- [Agent and maintenance guidance](AGENTS.md)

## License

No license has been selected. Until one is added, reuse and redistribution rights are not explicitly granted. See the repository owner’s licensing decision before reusing the project.
