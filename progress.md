# PortPulse Progress

**Updated:** 2026-11-12
**Final project deadline:** Monday, November 16, 2026

## Implemented before final orchestration

- **Phase 1:** deterministic synthetic JSONL fixtures, Pydantic validation, PostgreSQL persistence, quarantine, duplicate/collision handling, trusted ETA reconciliation, query CLI, and golden acceptance tests.
- **Phase 2:** supported Digitraffic Portnet V1 ingestion via `scripts.ingest_portnet`, Pydantic validation, PostgreSQL observations, rejected-record quarantine, duplicate/collision tracking, and integration/reliability tests.
- **Phase 3:** dbt staging, latest-ETA, and pipeline-quality models with source declarations and schema/custom SQL tests. PR #15 was merged to `main` at commit `3fa4d01`.
- **Final orchestration work:** branch `feat/airflow-final-pipeline` was created from `origin/main` at `3fa4d01`. Apache Airflow 2.10.5 runs the single three-task DAG `portpulse_daily_pipeline`. Verified locally: Ruff passed; all 34 Python tests passed against disposable PostgreSQL; dbt parse/debug passed and full build passed all 12 operations (9 data tests); Airflow metadata migration and DAG import/list checks passed; the end-to-end Airflow test completed all tasks successfully against a real Portnet V1 poll and dbt marts. Detailed dated evidence is in [the live pipeline report](reports/portpulse_live_pipeline_2026-11-12.md). Commit and PR are not yet complete.

## Data evidence and limits

The November 9 report describes an earlier Portnet/AIS experiment and is historical only. The November 10 reports describe separate Portnet-only live runs. Injected failures and CI rows are synthetic tests, not provider defects or fresh live observations. The November 12 report separates live API observations from deterministic CI fixtures and cumulative repeat-poll issue counts.

The current real Portnet path does not use the Phase 1 synthetic multi-source reconciler. API date eligibility and availability vary. Airflow standalone with SQLite metadata is intended for local learning and demonstration, not production operation.

During a separate standalone UI startup check without the demo environment variables, Airflow ran the due schedule against the existing local application database and inserted 170 valid Portnet observations for 2026-11-11. Its dbt task failed authentication against the existing local profile. After correcting the environment, dbt build and the DAG verifier both passed against that database; the full three-task Airflow run had passed against the disposable database. See the dated report for the distinction.
