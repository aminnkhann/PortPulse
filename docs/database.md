# Database operations

## PostgreSQL and migrations

Compose runs PostgreSQL 16. PortPulse uses database `portpulse`, user `portpulse`, and the local Compose password `admin`. Keep the default ports bound to loopback; do not expose the demo database to the public internet.

On a fresh `postgres_data` volume, the official PostgreSQL image applies SQL files from `migrations/` in filename order (001–008). Initialization scripts run only when the data directory is empty. Changing `POSTGRES_PASSWORD` later does not rotate a role password inside an existing volume.

Migration 008 creates the separate `stream_demo` source and CDC event tables and a publication limited to `stream_demo.port_eta_state`. On a fresh volume it is applied during initialization. For an existing volume, start Compose with the updated PostgreSQL settings and apply only migration 008 once:

```sh
docker compose up -d db kafka kafka-connect
docker compose exec -T db psql -U portpulse -d portpulse \
  -f /docker-entrypoint-initdb.d/008_create_streaming_cdc_tables.sql
```

PostgreSQL starts with `wal_level=logical`, `max_replication_slots=4`, and `max_wal_senders=4`. Changing `wal_level` requires a PostgreSQL restart; `docker compose up` applies the Compose command change while preserving `postgres_data`. Check settings and CDC objects with:

```sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SHOW wal_level;'
docker compose exec -T db psql -U portpulse -d portpulse -c "SELECT pubname FROM pg_publication WHERE pubname = 'portpulse_stream_publication';"
docker compose exec -T db psql -U portpulse -d portpulse -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots WHERE slot_name = 'portpulse_stream_slot';"
```

Debezium creates the logical replication slot on connector startup. The local demo uses the existing `portpulse` account; production should use a dedicated least-privilege replication account rather than the Compose superuser. An inactive slot retains WAL until it resumes or is deliberately removed; monitor disk use and do not drop an active slot.

For an existing database migrated through 006, apply migration 007 once:

```sh
docker compose exec -T db psql -U portpulse -d portpulse \
  -f /docker-entrypoint-initdb.d/007_create_portnet_eta_tables.sql
```

Do not replay migrations 001–006 against an initialized database. Migrations 004 and 006 preserve legacy rows; they are not part of the supported Portnet ingestion route.

## Persistent volumes

`postgres_data` stores PortPulse application rows. `airflow_data` stores Airflow's SQLite metadata. `docker compose down` stops services but keeps both volumes. `docker compose down -v` deletes them; avoid it when the data must be retained.

Airflow 3 migrates its metadata database on startup. Before upgrading an existing Airflow 2 installation, stop the stack and back up `airflow_data`; Airflow's [upgrade guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html) recommends a metadata-database backup before migration.

For an old local volume whose database role still has the previous password, update the role in place without deleting data:

```sh
docker compose up -d db
docker compose exec -T db psql -U portpulse -d portpulse \
  -c "ALTER ROLE portpulse WITH PASSWORD 'admin';"
docker compose up --build -d
```

## Inspect stored data

```sh
docker compose exec -T db psql -U portpulse -d portpulse -c '\dt portnet_eta_*'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) FROM public.portnet_eta_observation;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT reason_code, count(*) FROM public.portnet_eta_rejected_event GROUP BY reason_code ORDER BY reason_code;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT issue_code, count(*) FROM public.portnet_eta_quality_issue GROUP BY issue_code ORDER BY issue_code;'
```

Observation counts are cumulative stored records, not per-run API totals. Rejection and quality issue details are retained in their respective tables.

## dbt connection and troubleshooting

The Airflow image includes `dbt-postgres`. Its profile uses the Compose environment and connects to PostgreSQL at hostname `db` on port 5432; `localhost` inside the Airflow container refers to that container, not the database service.

```sh
docker compose exec airflow dbt debug --project-dir /opt/portpulse/dbt
docker compose exec airflow dbt build --project-dir /opt/portpulse/dbt
```

If dbt cannot connect, check service health and the configured Compose environment:

```sh
docker compose ps
docker compose logs --tail 100 db
docker compose logs --tail 100 airflow
```

A valid Portnet poll may return no eligible records. That does not by itself indicate a database or dbt failure.

For Kafka Connect status, inspect `docker compose logs --tail 100 kafka-connect` and `http://localhost:8083/connectors/portpulse-postgres-cdc/status`. If the connector cannot start, check `wal_level`, publication membership, the database account's replication privileges, and that migration 008 was applied. The full procedure is in [the streaming CDC guide](streaming_cdc.md).
