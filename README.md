# PortPulse

PortPulse is a local data engineering project that demonstrates deterministic ETA reconciliation and a real single-source ETL pipeline.

## Supported scope

- **Phase 1:** process a fixed synthetic port-call fixture, validate and quarantine records, detect duplicates and ID collisions, reconcile a trusted ETA, and query its history.
- **Phase 2:** fetch Digitraffic Portnet V1 observations, validate them with Pydantic, store accepted and rejected observations in PostgreSQL, and report pipeline-health metrics.

The supported live ingestion command is python -m scripts.ingest_portnet. Digitraffic V2 and AIS ingestion are not included in the supported workflow. Their old migrations and the November 9 comparison report remain as history. Existing generic raw_event rows are not dropped. Kafka, FastAPI, cloud services, and dbt are not implemented.

## Run Phase 1 locally

Install the dependencies and start PostgreSQL:

~~~sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
docker compose up -d
~~~

Load and reconcile the fixture, then query the result:

~~~sh
python -m scripts.ingest_fixture
python -m scripts.reconcile
python -m scripts.query_state PC-DEMO-001
~~~

The golden result selects COORD-DELAYED-01 with ETA 2026-01-15T15:40:00Z. The fixture and expected result are deterministic; they contain synthetic data only.

## Run Phase 2 Portnet V1

The Portnet command uses DATABASE_URL, or the local PostgreSQL default:

~~~sh
python -m scripts.ingest_portnet --date 2026-11-10
~~~

Use --port FIHEL to filter by port. The date is required. New Docker database volumes apply migrations 001–007 in order. An existing database already through migration 006 needs migration 007 applied once; see the database runbook.

Portnet records retain provider update time, API collection time, and database ingestion time separately. A stable identity includes port call, port area, and provider update time, so a legitimate ETA revision becomes a new observation. Invalid observations are quarantined with reasons; exact repeats and same-ID payload collisions are reported separately.

## Run checks

With PostgreSQL running, point integration tests to a disposable database:

~~~sh
PORTPULSE_TEST_DATABASE_URL=postgresql://portpulse:portpulse_local_only@127.0.0.1:5432/portpulse \
  python -m unittest discover -s tests -v
ruff check .
~~~

Without PORTPULSE_TEST_DATABASE_URL, PostgreSQL integration tests are skipped. GitHub Actions supplies a disposable PostgreSQL service.

## Project guide

- [Product requirements](PRD.md)
- [Architecture](ARCHITECTURE.md)
- [User stories](USER_STORIES.md)
- [Progress and deadline](progress.md)
- [Database runbook](docs/database.md)
- [Data dictionary and dbt readiness](docs/data_dictionary.md)
- [Historical live observations](reports/)
