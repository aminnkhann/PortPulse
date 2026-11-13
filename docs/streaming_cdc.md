# Simulated PostgreSQL CDC demo

This local demo sends fixed, fictional port-call changes through PostgreSQL WAL, Debezium, Kafka, an Airflow micro-batch, and dbt. It does not use the live Portnet API and does not write to Portnet tables.

## Start the stack

Install Docker Engine and Compose v2, then run from the repository root:

```sh
docker compose up -d
docker compose ps
```

The command starts PostgreSQL, Airflow, Metabase, a single-node KRaft broker, and Kafka Connect. A one-shot Compose service registers the Debezium connector and waits for its task to become healthy. Kafka is available to containers as `kafka:9092`; its broker port is not published on the host. Kafka Connect REST is bound to `http://localhost:8083`.

The demo pins Apache Kafka `4.1.1` and Debezium Connect `3.7.1.Final`. It uses Airflow Kafka provider `3.0.0`, whose official package requirements support Airflow 2.11 and later; this project runs Airflow 3.4.0.

On a fresh `postgres_data` volume, the PostgreSQL image applies migration 008 at initialization. On an existing volume, Compose preserves the database and Airflow volumes. Apply only the new migration once:

```sh
docker compose exec -T db psql -U portpulse -d portpulse \
  -f /docker-entrypoint-initdb.d/008_create_streaming_cdc_tables.sql
```

Confirm logical replication is active and the dedicated publication includes only the simulator table:

```sh
docker compose exec -T db psql -U portpulse -d portpulse \
  -c "SHOW wal_level; SELECT pubname, schemaname, tablename FROM pg_publication_tables WHERE pubname = 'portpulse_stream_publication';"
```

Expected: `wal_level` is `logical` and the only publication table is `stream_demo.port_eta_state`. `wal_level` takes effect after PostgreSQL restarts; `docker compose up --build -d` recreates the service with the new setting while keeping `postgres_data`.

## Check Debezium

Compose registers the version-controlled connector automatically. Check its status:

```sh
curl -fsS http://localhost:8083/connectors/portpulse-postgres-cdc/status
```

The connector uses `pgoutput`, the one-table publication, a dedicated slot, and `snapshot.mode=no_data`. Wait for its task to reach `RUNNING` before creating simulator events; with `no_data`, the connector does not backfill pre-existing source rows. If registration needs to be retried, run `docker compose up -d --force-recreate cdc-setup`.

Check the database slot:

```sh
docker compose exec -T db psql -U portpulse -d portpulse \
  -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots WHERE slot_name = 'portpulse_stream_slot';"
```

## Generate port traffic

Choose a new scenario ID for each run. Each source change is its own committed transaction, with deterministic UTC source timestamps. The default delay is two seconds; use zero to run quickly:

```sh
docker compose exec airflow python scripts/simulate_port_traffic.py \
  --scenario-id demo-01 --delay-seconds 0
```

The simulator performs five database operations: `SIM-001` is inserted and updated twice; `SIM-002` is inserted and deleted. Reusing a scenario ID fails on its first duplicate primary key. Choose another ID rather than clearing history.

Inspect current source rows:

```sh
docker compose exec -T db psql -U portpulse -d portpulse \
  -c "SELECT scenario_id, port_call_id, port_area_code, eta_utc, updated_at_utc FROM stream_demo.port_eta_state ORDER BY port_call_id;"
```

`SIM-001` should have ETA `2026-11-13 14:50:00+00`; `SIM-002` is absent after its delete.

## Inspect Kafka CDC events

For a clean demo topic, print the key and five JSON change records:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic portpulse_stream.stream_demo.port_eta_state \
  --from-beginning --max-messages 5 --property print.key=true
```

The Debezium JSON envelope has operation codes `c`, `u`, and `d`; key fields keep all events for the same scenario/call/area together in a Kafka partition. For a topic with earlier demos, the first five records may belong to a previous scenario. Use `--max-messages` accordingly or inspect the scenario ID in each before/after row.

## Process the micro-batch in Airflow

The streaming DAG is paused on first creation. Unpause it and trigger a run:

```sh
docker compose exec airflow airflow dags unpause portpulse_streaming_cdc
docker compose exec airflow airflow dags trigger portpulse_streaming_cdc
```

It polls no more than 100 records per run, in batches of up to 50, with a five-second poll timeout. Each database batch commits before the Kafka operator commits offsets. If a retry sees a previously stored topic/partition/offset, PostgreSQL ignores that duplicate. Airflow runs dbt and verification only after the consumer task succeeds. The DAG also runs every minute while unpaused.

Inspect stored events and ETAs:

```sh
docker compose exec -T db psql -U portpulse -d portpulse \
  -c "SELECT kafka_topic, kafka_partition, kafka_offset, operation, scenario_id, port_call_id, source_updated_at_utc, processed_at_utc FROM stream_demo.port_eta_cdc_event ORDER BY kafka_partition, kafka_offset;"
docker compose exec -T db psql -U portpulse -d portpulse \
  -c "SELECT scenario_id, port_call_id, port_area_code, eta_utc, cdc_operation FROM analytics.mart_stream_latest_eta ORDER BY scenario_id, port_call_id;"
```

For the example scenario, the expected final state is `SIM-001` at `2026-11-13 14:50:00+00`, with no active `SIM-002`. The local end-to-end run verified CDC operations `c,u,u,c,d`, five stored history rows, and the expected dbt result. Reprocessing the same Kafka offsets leaves one row per topic/partition/offset.

To build/check the models manually:

```sh
docker compose exec airflow dbt build --project-dir /opt/portpulse/dbt \
  --select stg_stream_eta_changes mart_stream_latest_eta
```

## Stop and troubleshoot

Stop all services without deleting volumes:

```sh
docker compose down
```

Do not add `-v` if you want to preserve PostgreSQL, Airflow, or Kafka data. To remove only the connector, stop it first through Kafka Connect; the slot and its retained WAL remain until deliberately removed:

```sh
curl -X DELETE http://localhost:8083/connectors/portpulse-postgres-cdc
```

Only after the connector is stopped, and only if discarding its unconsumed change history is intended, drop its slot:

```sh
docker compose exec -T db psql -U portpulse -d portpulse \
  -c "SELECT pg_drop_replication_slot('portpulse_stream_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'portpulse_stream_slot' AND NOT active);"
```

Common checks:

- Connector is `FAILED`: inspect `curl -fsS http://localhost:8083/connectors/portpulse-postgres-cdc/status` and `docker compose logs --tail 100 kafka-connect`; verify the migration, password, publication, and replication privileges.
- PostgreSQL reports `wal_level` is not `logical`: rerun `docker compose up -d db` and check `SHOW wal_level` after it restarts.
- Kafka Connect cannot reach PostgreSQL or Kafka: check `docker compose ps` and logs for `db`, `kafka`, and `kafka-connect`.
- Source table changes but no Kafka record appears: verify the connector reached `RUNNING` before running the simulator and inspect the configured publication and slot.
- WAL usage grows while the connector is stopped: replication slots retain WAL until they advance or are intentionally dropped. Restart the connector or remove its inactive slot only when its retained changes are no longer needed.
- Airflow's streaming DAG does not consume events automatically: the DAG is intentionally paused; unpause or trigger it in Airflow. The daily Portnet DAG continues to ingest real Portnet data separately.

## What the synthetic data tests

The original three-source JSONL fixture tests ETA reconciliation. API-shaped Portnet fixtures test validation, quarantine, and dbt behavior with mocked API responses. This new simulator creates PostgreSQL row changes to prove actual CDC; it never writes fabricated rows into Portnet observations.
