# Metabase BI dashboards

Metabase reads the existing dbt marts in PostgreSQL. It does not fetch source data. The Portnet dashboard contains only Portnet marts; the CDC dashboard is labeled as simulated data.

## Start and finish the one-time setup

From the repository root, start the normal services and Metabase:

```bash
docker compose --profile bi up --build -d
```

To run Metabase alongside the optional CDC services, enable both profiles: `docker compose --profile bi --profile streaming up --build -d`.

Metabase stores its configuration in the `metabase_data` Docker volume. The demo uses a persistent H2 application database; use PostgreSQL for Metabase's own application data in a production deployment.

Copy `.env.example` to `.env`, replace the example values, load them in the shell, and run the setup script. It initializes the first local admin through Metabase's setup API when needed, then creates the database connection and dashboards:

```bash
cp .env.example .env
# Edit .env with local passwords first.
set -a; . ./.env; set +a
```

Before creating saved questions, build the marts so their PostgreSQL relations exist, then configure Metabase:

```bash
docker compose exec airflow dbt build --project-dir /opt/portpulse/dbt
python scripts/setup_metabase.py
```

The script creates `metabase_reader`, grants it SELECT access only to `analytics`, connects Metabase to PostgreSQL as that role, and creates or updates the `PortPulse` collection and dashboards. The script expects the default local PostgreSQL account at `localhost:5432`; set `PORTPULSE_ADMIN_DATABASE_URL` if yours differs. Credentials are supplied through environment variables and are not saved in the repository.

## Open the dashboards

- **PortPulse — Portnet Overview**: real Portnet latest ETA rows and the current data-quality snapshot. An empty latest-ETA table means no Portnet observations have been ingested; the dashboard does not create sample Portnet rows.
- **PortPulse — Kafka CDC Demo (SIMULATED DATA)**: active rows from `analytics.mart_stream_latest_eta`, with the scenario ID visible. To populate it, follow the [CDC walkthrough](streaming_cdc.md) and run the streaming DAG after the connector captures the simulated changes.

The quality mart is one current snapshot of cumulative counts, not a time series. The latest-ETA mart has one current row per port call and area, not a history of all ETA revisions. dbt refreshes these results when its build runs; use the Airflow DAG or run `docker compose exec airflow dbt build --project-dir /opt/portpulse/dbt` to refresh.

## Manual database setup

If you do not use the setup script, complete the Metabase first-run wizard, add a PostgreSQL database with host `db`, port `5432`, database `portpulse`, and user `metabase_reader`, then choose the `analytics` schema. Create a `PortPulse` collection, save questions based on the three marts listed above, and add the questions to the two dashboards. Do not grant this account access to `public` or `stream_demo`.

## Troubleshooting

- **Metabase is still starting:** check `docker compose --profile bi logs metabase` and wait for the health check to pass.
- **Cannot connect to PostgreSQL:** use hostname `db` from Metabase, not `localhost`. Confirm the database container is healthy.
- **Login/setup script fails:** check the local admin email and password in the environment. The script uses the one-time setup API on a fresh Metabase volume.
- **No rows appear:** run the relevant ingestion and dbt workflow. Portnet remains empty until actual API observations are ingested; CDC remains empty until its separate simulation and DAG run.
- **Permission denied:** rerun the setup script after dbt creates or replaces models. Default privileges cover future analytics tables created by the `portpulse` role.
- **Dashboard settings disappeared:** keep the `metabase_data` volume. `docker compose down` preserves it; `docker compose down -v` deletes it.

The optional BI service binds only to `127.0.0.1:3000`. Do not expose it to the public internet.

Metabase references: [Docker installation](https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker), [application database configuration](https://www.metabase.com/docs/latest/installation-and-operation/configuring-application-database), and [API setup guide](https://www.metabase.com/articles/set-up-metabase-from-code-with-the-api).
