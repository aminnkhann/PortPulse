# Metabase BI dashboards

Metabase reads PortPulse's existing dbt marts in PostgreSQL. It does not ingest data. The Portnet dashboard contains only real Portnet data; the separate CDC dashboard is labeled as simulated.

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

- **PortPulse — Portnet Overview** uses `mart_portnet_latest_eta` and `mart_pipeline_quality`. The quality mart is a current cumulative snapshot, not a historical trend. The latest-ETA mart has one current row per port call and area, not a full ETA history. No source rows means the dashboard is honestly empty.
- **PortPulse — Kafka CDC Demo (SIMULATED DATA)** uses `mart_stream_latest_eta`. It remains separate from the real Portnet data and is populated only after running the optional CDC simulation and Airflow DAG.

Airflow or dbt refreshes the marts; Metabase displays the latest committed results. The setup job runs dbt at startup, so the questions have their required models on a fresh database and remain aligned after an explicit restart.

## Troubleshooting

- **Setup did not finish:** inspect `docker compose logs bi-setup`; the service waits for both dependencies and exits nonzero on dbt, permission, API, or question-query errors.
- **Metabase is starting:** inspect `docker compose logs metabase` and wait for its health check.
- **Cannot connect to PostgreSQL:** Metabase uses the Compose hostname `db`, not `localhost`.
- **A dashboard is empty:** run the real Portnet ingestion for Portnet data, or follow the [CDC guide](streaming_cdc.md) to generate simulated events and trigger the paused streaming DAG.
- **Settings disappear:** retain `metabase_data`; `docker compose down` keeps it. Do not use `docker compose down -v` if you want to preserve data.

The Metabase port binds only to `127.0.0.1:3000`. Do not expose the local demo to the public internet.
