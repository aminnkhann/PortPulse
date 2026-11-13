# PortPulse Architecture

PortPulse demonstrates two related but separate data workflows: deterministic synthetic ETA reconciliation (Phase 1) and real single-source Portnet V1 ingestion with analytics (Phases 2–3). Airflow orchestrates the latter; it is not an additional storage or processing layer.

## End-to-end Portnet workflow

~~~mermaid
flowchart LR
    API[Digitraffic Portnet V1 API] --> A[scripts.ingest_portnet]
    A --> V[Pydantic validation]
    V -->|accepted| O[(portnet_eta_observation)]
    V -->|rejected with reason| R[(portnet_eta_rejected_event)]
    O --> Q[(portnet_eta_quality_issue)]
    O --> S[dbt stg_portnet_eta]
    R --> H[dbt mart_pipeline_quality]
    Q --> H
    S --> L[dbt mart_portnet_latest_eta]
    S --> H
    DAG[Airflow: ingest >> dbt build >> verify] --> A
    DAG --> S
    DAG --> H
~~~

Airflow task `ingest_portnet` calls the existing CLI for the UTC run date. `build_dbt_analytics` runs `dbt build`, which builds all three models and executes their tests. `verify_pipeline` checks the source tables, model relations, source-to-model counts, quality metrics, and uniqueness of the latest ETA per port call and area. A task failure fails the DAG run; Airflow retries transient task failures twice with a five-minute delay. Runs do not overlap and historical catch-up is disabled. The schedule is 06:00 UTC daily; users can trigger a run manually and supply an optional `run_date`.

## Validation, identity, and clocks

The Portnet adapter validates provider data with Pydantic. Accepted observations, rejected records with reasons, and duplicate/collision quality issues are stored in dedicated PostgreSQL tables. Identity is scoped by provider port-call ID, area, and provider update timestamp. An ETA revision becomes a new observation; exact repeats and conflicting payloads do not silently change trusted records. Provider event time, API collection time, and database ingestion time are stored separately in UTC.

The adapter is independent of Phase 1 synthetic reconciliation. Real Portnet rows are not passed through the three-source trusted-ETA engine. Phase 1 retains its fixed synthetic fixture, legacy `raw_event` tables, and deterministic golden output.

## PostgreSQL and dbt

Migrations 001–007 are retained and run in order on a fresh Compose database. Migration 007 adds the Portnet tables. Existing generic tables and legacy rows remain intact.

dbt reads the Portnet source tables in `public` and builds:

- `analytics.stg_portnet_eta`: validated source projection.
- `analytics.mart_portnet_latest_eta`: one latest eligible ETA per port call and area, excluding event-ID collisions.
- `analytics.mart_pipeline_quality`: observation, rejection, duplicate, collision, and latest-row counts.

dbt uses the same PostgreSQL application database as ingestion, with credentials supplied through the Compose environment. Airflow metadata uses SQLite in the separate `airflow_data` Compose volume.

## Local Docker Compose deployment

The repository builds a small image from Apache Airflow 2.10.5 and Python 3.12, then installs the PortPulse and dbt requirements. One `airflow` Compose service runs `airflow standalone`, which supplies the local UI and scheduler. A startup script initializes Airflow metadata and sets the demo UI login to `admin` / `admin`. The DAG is paused when first created.

The Airflow container reaches PostgreSQL at hostname `db`; it does not use `localhost` for inter-container traffic. PostgreSQL migrations 001–007 run automatically on a fresh `postgres_data` volume. The named `postgres_data` volume stores PortPulse rows, and the separate `airflow_data` volume stores orchestration metadata. Run `docker compose up --build -d` from the repository root to start the demo. This setup is for local learning, not production deployment. See [README setup](README.md#local-prerequisites-and-installation) and the [final demo runbook](docs/final_demo.md).

## Current limitations

- Portnet API availability and date eligibility are external; an empty valid poll is healthy.
- Airflow's SQLite metadata and `admin` / `admin` demo login are for local development only.
- The quality mart describes this Portnet ingestion path, not the separate synthetic Phase 1 reconciler.
- The project does not include a dashboard, cloud deployment, or additional providers.
