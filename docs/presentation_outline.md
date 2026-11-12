# PortPulse presentation outline

Target length: 5–7 minutes. Replace the team placeholders with verified details; do not imply work by a team member without confirming it.

## Slides

1. **Business problem** — ETA updates change over time; explain why collecting history, selecting the latest valid estimate, and tracking data quality matter.
2. **Data source** — Digitraffic Portnet V1 API; explain the observation date and UTC timestamps. Mention that the live provider can return no eligible records for a date.
3. **System architecture** — API → Python/Pydantic → PostgreSQL → dbt → analytics marts, with Airflow orchestrating ingestion, build, and verification.
4. **Python ingestion** — existing CLI, schema validation, accepted/rejected records, idempotent duplicate handling, and event-ID collision quarantine.
5. **PostgreSQL and dbt** — dedicated Portnet tables, staging model, latest-ETA mart, and pipeline-quality mart; explain that collisions are excluded from trusted latest ETA.
6. **Airflow orchestration** — show the three sequential tasks, UTC schedule/manual run, retries, task logs, and fail-fast behavior.
7. **Data-quality tests and results** — show verified Python/Ruff/dbt/Airflow results and distinguish deterministic CI fixtures from observations retrieved live. Fill in metrics only from the dated execution report.
8. **Limitations and next steps** — local Docker Compose Airflow with SQLite metadata and demo credentials, API availability and date eligibility, no production deployment/dashboard, and real Portnet data kept separate from synthetic trusted-ETA reconciliation.

## Team-specific details to fill in

- **Team members and roles:** [fill in from the team]
- **Individual contributions:** [confirm with each team member]
- **Demo run date and evidence:** [link the verified report under `reports/`]
- **Final test and PR status:** [fill in after review]
