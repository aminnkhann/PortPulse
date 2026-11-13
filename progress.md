# PortPulse Progress

**Updated:** 2026-11-13
**Final project deadline:** Monday, November 16, 2026

## Implemented before final orchestration

- **Phase 1:** deterministic synthetic JSONL fixtures, Pydantic validation, PostgreSQL persistence, quarantine, duplicate/collision handling, trusted ETA reconciliation, query CLI, and golden acceptance tests.
- **Phase 2:** supported Digitraffic Portnet V1 ingestion via `scripts.ingest_portnet`, Pydantic validation, PostgreSQL observations, rejected-record quarantine, duplicate/collision tracking, and integration/reliability tests.
- **Phase 3:** dbt staging, latest-ETA, and pipeline-quality models with source declarations and schema/custom SQL tests. PR #15 was merged to `main` at commit `3fa4d01`.
- **Final orchestration work:** branch `feat/airflow-final-pipeline` adds a Docker Compose image with Airflow 2.10.5, Python 3.12, the ingestion CLI, and dbt; services start with `docker compose up --build -d`. The three-task DAG is `portpulse_daily_pipeline`. On 2026-11-13, the image build, Compose configuration, Ruff, Airflow DAG import, dbt debug/build (12/12 operations), and the dbt and verification tasks passed in the container. See [the Compose verification report](reports/portpulse_docker_compose_2026-11-13.md). A live ingestion task was not run in this Compose verification.

## Data evidence and limits

The November 9 report describes an earlier Portnet/AIS experiment and is historical only. The November 10 reports describe separate Portnet-only live runs. Injected failures and CI rows are synthetic tests, not provider defects or fresh live observations. The November 12 report separates live API observations from deterministic CI fixtures and cumulative repeat-poll issue counts.

The current real Portnet path does not use the Phase 1 synthetic multi-source reconciler. API date eligibility and availability vary. The Compose deployment uses SQLite metadata and `admin` / `admin` for local learning only, not production operation.

The November 12 local standalone UI check is historical: it inserted 170 observations in an earlier local database. On November 13 the expected Compose PostgreSQL volume was absent, so Compose created a fresh empty volume; no existing Docker volumes were deleted or reset. Other named PortPulse volumes remain untouched. See the dated report for exact verification and limits.
