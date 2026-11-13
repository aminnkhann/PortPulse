# PortPulse

PortPulse collects estimated arrival times (ETAs) for ships from the Finnish Portnet API. It validates and stores updates in PostgreSQL, uses dbt to build analytics tables, and runs the pipeline with Apache Airflow.

The project runs locally with Docker Compose.

## What it does

- Fetches port-call ETA data from Digitraffic Portnet V1.
- Validates incoming records with Python and Pydantic.
- Stores observations, rejected records, and data-quality issues in PostgreSQL.
- Uses dbt to select the latest valid ETA for each port call and area and summarize data quality.
- Uses Airflow to schedule ingestion, build dbt models, and verify the results.

## Tech stack

Python · Pydantic · PostgreSQL · dbt · Apache Airflow · Docker

**Data flow:**

`Portnet V1 API → Python → PostgreSQL → dbt → Analytics`

Airflow orchestrates the three pipeline tasks:

`ingest_portnet → build_dbt_analytics → verify_pipeline`

See [ARCHITECTURE.md](ARCHITECTURE.md) for more details.

## Getting started

You need Docker Engine and Docker Compose v2 installed.

Clone the repository:

```bash
git clone https://github.com/aminnkhann/portpulse.git
cd portpulse
```

Build and start the services:

```bash
docker compose up --build -d
```

Check that the containers are running:

```bash
docker compose ps
```

Open the Airflow UI:

**http://localhost:8080**

Default local demo credentials:

- Username: `admin`
- Password: `admin`

The DAG starts paused, so it will not automatically fetch data.

> This configuration is intended for local development only. Do not expose the Airflow UI or PostgreSQL to the public internet.

## Run the pipeline

In Airflow, find the DAG named `portpulse_daily_pipeline` and click **Trigger DAG**.

You can also trigger it from the terminal:

```bash
docker compose exec airflow airflow dags trigger portpulse_daily_pipeline
```

To process a specific UTC date:

```bash
docker compose exec airflow airflow dags trigger \
  --conf '{"run_date":"2026-11-12"}' \
  portpulse_daily_pipeline
```

Use the Airflow Graph view to follow each task and inspect its logs.

The DAG is scheduled for 06:00 UTC daily when unpaused.

## View the results

After the pipeline finishes, you can query the analytics tables in PostgreSQL.

**Latest ETA for each port call and area:**

```bash
docker compose exec -T db psql -U portpulse -d portpulse \
  -c "SELECT port_call_id, port_area_code, eta_utc
      FROM analytics.mart_portnet_latest_eta
      LIMIT 10;"
```

**Pipeline data quality:**

```bash
docker compose exec -T db psql -U portpulse -d portpulse \
  -c "SELECT * FROM analytics.mart_pipeline_quality;"
```

The quality mart contains counts of accepted observations, rejected records, duplicate issues, collision issues, and latest ETA records.

To rebuild the analytics tables manually:

```bash
docker compose exec airflow dbt build \
  --project-dir /opt/portpulse/dbt
```

## Tests

Run the Python unit tests and Ruff:

```bash
docker compose exec airflow python -m unittest discover -s tests -v
docker compose exec airflow ruff check /opt/portpulse
```

GitHub Actions also runs PostgreSQL integration tests, Airflow DAG checks, and dbt models and tests against deterministic fixture data.

The CI workflow does not call the live Portnet API.

## Project scope

PortPulse uses real ETA observations from [Digitraffic's Finnish maritime data](https://www.digitraffic.fi/en/marine-traffic/).

The repository also includes a separate synthetic example for testing ETA reconciliation across three simulated sources. This example is independent of the real Portnet pipeline.

PortPulse is a local Data Engineering project, not a production maritime tracking service. API availability and ETA coverage depend on the data provider.

## Documentation

- [Architecture](ARCHITECTURE.md)
- [Database setup and troubleshooting](docs/database.md)
- [Data dictionary](docs/data_dictionary.md)

## Stopping the application

Stop the containers with:

```bash
docker compose down
```

This keeps the PostgreSQL and Airflow data volumes. Avoid `docker compose down -v` unless you intend to delete the stored data.

## License

No software license has been selected yet.
