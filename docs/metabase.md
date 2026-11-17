# Metabase BI dashboards

Metabase reads PortPulse's existing dbt marts in PostgreSQL. It does not ingest data. The Portnet dashboard contains only real Portnet data. Synthetic ETA analytics and Kafka CDC each have their own clearly labeled simulated-data dashboards.

## Start

From the repository root, run the same command as the rest of the local stack:

```bash
docker compose up -d
```

Compose starts PostgreSQL, Airflow, Metabase, Kafka, and Kafka Connect. One-shot setup containers build the dbt models, configure the read-only account, create or update the dashboards, verify saved questions, and register the Debezium connector. Check their output with `docker compose logs bi-setup cdc-setup`. If setup fails, the relevant container exits with an error visible in its logs; fix the issue and rerun `docker compose up -d`.

Open Metabase at [http://localhost:3000](http://localhost:3000). The local default login is `admin@example.test` / `portpulse-local-demo-2026`. These are intentionally simple local demo credentials and must never be used for a public deployment.

No `.env` file or host Python installation is needed. To override the local demo credentials, define `POSTGRES_PASSWORD`, `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_EMAIL`, `METABASE_ADMIN_EMAIL`, `METABASE_ADMIN_PASSWORD`, and `METABASE_DB_PASSWORD` in your environment or optional `.env` file before starting Compose. `.env.example` lists the variables. Metabase connects to PostgreSQL as `metabase_reader`, which is limited to SELECT on analytics relations.

Metabase settings are stored in the `metabase_data` volume using H2. PostgreSQL is recommended for Metabase's own application database in production.

## Dashboards

- **PortPulse — Portnet Overview (REAL API DATA)** uses only captured Portnet observations. It shows accepted/latest counts, quality totals, a chart grouped by **provider update date**, the top 10 observed port areas, ETA lead-time buckets, and recent ETA records. The update-date chart represents the records PortPulse captured, **not** a complete time series of Finnish port traffic. Quality counts are cumulative snapshots; without repeated real observations, a real ETA revision trend would be misleading.
- **PortPulse — Synthetic ETA Analytics (SIMULATED DATA)** uses the separate `synthetic_demo` pipeline and its dbt marts. It shows fictional calls, daily ETA updates, signed ETA revisions, source contributions, latest ETAs, and ingestion-quality totals. Positive ETA revisions mean a later estimate, negative revisions mean an earlier estimate; **neither represents actual arrival delay**. This dataset has no port-area code, so the synthetic ETA dashboard does not invent one.
- **PortPulse — Kafka CDC Demo (SIMULATED DATA)** uses only the separate `stream_demo` Kafka/Debezium workflow. Its charts show CDC operation counts and active fictional calls by area.

Metabase reads finished analytics models and does not need to run as an Airflow task. The real Portnet, synthetic ETA and CDC workflows can be run independently; their dashboards do not combine source records. The setup script saves and updates questions in the `PortPulse` collection and is designed to be safe to run again.

## Apply dashboard updates to an existing local installation

First ensure the code and dbt models from this PR are available inside the rebuilt application image. PostgreSQL, Metabase and Airflow volumes must be retained.

```sh
docker compose up -d --build
docker compose exec -T airflow dbt build --project-dir /opt/portpulse/dbt
docker compose run --rm bi-setup
```

The `bi-setup` job refreshes the read-only Metabase database account and updates the saved dashboard questions. It does not ingest real or synthetic data. To populate the synthetic analytics dashboard, run the `portpulse_synthetic_demo` Airflow DAG as described in [the synthetic guide](synthetic_eta.md), then refresh Metabase. The CDC dashboard requires its separate simulation and consumer workflow.

If you previously created the manual question **Captured ETA Observations — Daily Trend**, the setup script may reuse and update that saved question. Review any manual edits to that card before running setup. It does not delete unrelated saved questions.

On a fresh Compose volume, the normal one-shot `bi-setup` initialization also creates all three dashboards after building the dbt models.

## Troubleshooting

- **Setup did not finish:** inspect `docker compose logs bi-setup`; the service waits for both dependencies and exits nonzero on dbt, permission, API, or question-query errors.
- **Metabase is starting:** inspect `docker compose logs metabase` and wait for its health check.
- **Cannot connect to PostgreSQL:** Metabase uses the Compose hostname `db`, not `localhost`.
- **A dashboard is empty:** run the real Portnet ingestion for Portnet data, or follow the [CDC guide](streaming_cdc.md) to generate simulated events and trigger the paused streaming DAG.
- **Settings disappear:** retain `metabase_data`; `docker compose down` keeps it. Do not use `docker compose down -v` if you want to preserve data.

The Metabase port binds only to `127.0.0.1:3000`. Do not expose the local demo to the public internet.
