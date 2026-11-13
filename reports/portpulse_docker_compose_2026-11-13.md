# PortPulse Docker Compose verification — 2026-11-13

## Deployment

The local project runs PostgreSQL and Apache Airflow in Docker Compose. The Airflow 2.10.5 / Python 3.12 image contains the PortPulse ingestion dependencies and dbt-postgres. The UI is mapped to `http://localhost:8080`, with the local demo login `admin` / `admin`. PostgreSQL and Airflow metadata use separate named volumes. The DAG `portpulse_daily_pipeline` is paused on first creation.

## Checks completed

- `docker compose config --quiet`: passed.
- Docker image build: passed.
- Ruff: passed.
- Airflow health endpoint inside the container: HTTP 200.
- Airflow import-error check: no import errors; DAG discovered and paused.
- `dbt debug`: passed against the Compose PostgreSQL service.
- `dbt build`: passed, 12/12 operations, including all 9 data tests.
- Airflow `build_dbt_analytics` task test: passed, including dbt build and data tests.
- Airflow `verify_pipeline` task test: passed; zero observations, rejected records, duplicate/collision issues, and latest-mart rows were handled as a healthy empty state.
- Python unit tests without database access: 35 passed; 11 database integration tests skipped in that run.

The container's Airflow login was reset to `admin` / `admin` and its login form returned the home page. The host browser connection could not be independently confirmed from this restricted shell; use `http://localhost:8080` on the Ubuntu desktop.

## Database and live API limits

The expected Compose volume `portpulse-main_postgres_data` did not exist before this start, so Docker created a new, empty application database and applied migrations. No Docker volumes were removed, reset, or overwritten. Other PortPulse volumes were left untouched. As a result, the successful dbt build and verifier checked the zero-observation case; they do not establish current live API counts or populated mart results.

The full ingestion task and a live API poll were not run in this verification. The November 12 report documents an earlier live run in a separate local setup; those counts must not be attributed to this Compose database. For a live demo, confirm the correct data volume first, then trigger the paused DAG manually.
