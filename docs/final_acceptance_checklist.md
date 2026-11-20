# Final clean-clone acceptance checklist

**Windows Docker demonstration: VERIFIED for the recorded main pipeline and five-change CDC workflow. Overall final acceptance status: NOT RUN.** See [Windows Docker results](windows_docker_results.md), including its evidence limits. The complete checklist below has not been reported as finished: repeat-run and other acceptance-record checks remain unverified. Run this procedure against the latest `main` branch after the documentation PR is merged. It uses an isolated Compose project name and host ports so it does not attach to or remove another PortPulse project's volumes. Do not use `docker compose down -v`.

## 1. Clone and configure isolation

The repositories mirror the same tracked files. Use the personal repository below, or substitute `https://github.com/dci-fbw-dataeng-python-25-a/amin-final-project.git` in the clone command to test the DCI mirror.

```sh
git clone https://github.com/aminnkhann/portpulse.git portpulse-acceptance
cd portpulse-acceptance
git switch main
git pull --ff-only origin main

export COMPOSE_PROJECT_NAME=portpulse-acceptance
export POSTGRES_PORT=15432
export AIRFLOW_PORT=18080
export METABASE_PORT=13000
export KAFKA_CONNECT_PORT=18083
```

Optionally set local credentials in the shell before starting Compose. These are local demo services; do not publish the ports or reuse demo credentials outside this isolated machine.

## 2. Start and inspect the stack

```sh
docker compose up --build -d
docker compose ps -a
```

Expected: `db`, `metabase`, `kafka`, and `kafka-connect` become healthy; `airflow` remains running. `bi-setup`, `cdc-topic-setup`, and `cdc-setup` should exit successfully after their one-time jobs finish. `kafka-volume-init` also exits successfully. If a service is still starting, inspect `docker compose logs --tail 100 <service>`; do not delete volumes to repair startup.

## 3. Run the real and synthetic pipeline

Open `http://localhost:18080`, sign in with the local credentials, trigger `portpulse_full_pipeline`, and wait for `fetch_real_portnet`, `generate_synthetic_eta`, `ingest_synthetic_eta`, and `build_analytics` to succeed. The real task processes one UTC day at a time; earlier daily ingestion transactions may commit even if a later day fails. A successful dbt task does not prove source freshness.

The independent synthetic DAG is `portpulse_synthetic_demo`. It is initially paused, so unpause it before triggering the test:

```sh
docker compose exec -T airflow airflow dags unpause portpulse_synthetic_demo
```

For exact seed assertions, trigger it with `{"port_calls":1000,"seed":42}` and wait for all three tasks to succeed. The expected deterministic dataset is 1,000 fictional calls, 3,496 accepted ETA observations, 90 rejected events, 142 duplicate-delivery issues, and zero ID collisions. These synthetic records must remain in `synthetic_demo`, not the real Portnet or CDC tables.

## 4. Check database assertions

```sh
docker compose exec -T db psql -U portpulse -d portpulse -c \
  "SELECT count(*) AS observations FROM portnet_eta_observation;"
docker compose exec -T db psql -U portpulse -d portpulse -c \
  "SELECT count(*) AS rejected FROM portnet_eta_rejected_event;"
docker compose exec -T db psql -U portpulse -d portpulse -c \
  "SELECT issue_code, count(*) FROM portnet_eta_quality_issue GROUP BY issue_code ORDER BY issue_code;"
docker compose exec -T db psql -U portpulse -d portpulse -c \
  "SELECT count(*) AS synthetic_observations FROM synthetic_demo.eta_observation;"
docker compose exec -T db psql -U portpulse -d portpulse -c \
  "SELECT count(*) AS real_rows_with_synthetic_ids FROM portnet_eta_observation WHERE port_call_id LIKE 'SYNTH-%';"
docker compose exec -T db psql -U portpulse -d portpulse -c \
  "SELECT count(*) AS synthetic_rows_with_real_ids FROM synthetic_demo.eta_observation WHERE port_call_id LIKE 'portnet:%';"
```

Expected: the two cross-source counts are zero. `mart_portnet_latest_eta` has no rows whose `event_id` is in `mart_portnet_id_collisions`; the collision detail mart retains every conflicting stored version. The pipeline quality mart's rejected, duplicate, and collision values count unique stored problems, not poll attempts. The collection age is based on the latest recorded accepted or rejected payload and may be old even when dbt has just refreshed.

## 5. Review all three dashboards

Open `http://localhost:13000` and check **Portnet Overview** (real captured data only), **Synthetic ETA Analytics** (fictional events only), and **Kafka CDC Demo** (fictional CDC only). Check that the Portnet dashboard shows both latest recorded collection age and dbt refresh time with distinct labels, and that collision versions can be inspected. The live API may return no eligible observations, so do not require a fixed real-data count.

## 6. Run a fresh five-change CDC scenario

Check that Kafka Connect and the Debezium connector are running before creating changes:

```sh
curl -fsS http://localhost:18083/connectors/portpulse-postgres-cdc/status
```

Expected: the connector and its task report `RUNNING`. Generate a unique scenario ID for this acceptance run; the simulator commits five fictional changes and refuses to reuse an existing ID:

```sh
SCENARIO_ID="acceptance-$(python -c 'import uuid; print(uuid.uuid4().hex[:12])')"
docker compose exec -T airflow python scripts/simulate_port_traffic.py \
  --scenario-id "$SCENARIO_ID" --delay-seconds 0
```

In Airflow, unpause and manually trigger `portpulse_streaming_cdc`. Wait for both `consume_cdc_batch` and `build_streaming_dbt_models` to succeed. The consumer is a bounded, manually triggered micro-batch, not a continuously running consumer.

Check the new scenario's operation history and final mart state:

```sh
docker compose exec -T db psql -U portpulse -d portpulse -c \
  "SELECT operation, count(*) FROM stream_demo.port_eta_cdc_event WHERE scenario_id = '$SCENARIO_ID' GROUP BY operation ORDER BY operation;"
docker compose exec -T db psql -U portpulse -d portpulse -c \
  "SELECT port_call_id, eta_utc FROM analytics.mart_stream_latest_eta WHERE scenario_id = '$SCENARIO_ID' ORDER BY port_call_id;"
```

Expected history: two creates (`c`), two updates (`u`), and one delete (`d`). Expected latest state: only `SIM-001`, with ETA `2026-11-13 14:50:00+00`; deleted `SIM-002` is absent. Confirm this directly if needed:

```sh
docker compose exec -T db psql -U portpulse -d portpulse -c \
  "SELECT count(*) AS deleted_call_in_latest_state FROM analytics.mart_stream_latest_eta WHERE scenario_id = '$SCENARIO_ID' AND port_call_id = 'SIM-002';"
```

The result should be zero. Do not clear Kafka history or database rows to make the counts match; use the new scenario ID to isolate this run.

The task is bounded to 100 messages per DAG run. Inspect pending work and repeat the DAG only if `LAG` remains above zero:

Inspect the configured consumer group's pending work:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group portpulse-streaming-cdc
```

If `LAG` remains above zero, trigger the CDC DAG again and recheck until zero. The history table's `(kafka_topic, kafka_partition, kafka_offset)` uniqueness makes database replay safe. Delete tombstones do not create history rows; Debezium delete envelopes do. Offsets are not comparable across partitions. Check one call key's partition history rather than treating partition numbers as global sequence values.

## 7. Repeat-run and data-quality assertions

Trigger `portpulse_synthetic_demo` again with the same seed. The accepted observation count and deterministic latest ETAs should remain unchanged; unique rejected and quality-problem counts must not grow from replay alone. The run file path must be isolated per Airflow run. For Portnet, use the CLI's repeated poll summary and verify `new_observation_rows_inserted` is zero when the provider payload is unchanged; unique rejection and quality counts should remain stable. The live API can legitimately change between polls.

For CDC, trigger another micro-batch after the consumer has caught up. With no new source changes, no additional CDC rows should be stored. If offsets are deliberately replayed in a controlled test, the history count must remain unchanged because topic/partition/offset is unique.

## 8. Final manual outcome

Record the commit tested, date, service states, Airflow run IDs, synthetic assertions, dashboard review, CDC lag before/after catch-up, and any failures here after completing the procedure. Until then the result remains **NOT RUN**.
