# Synthetic maritime ETA demonstration

The deterministic generator creates fictional PortPulse v1 ETA events in
`fixtures/synthetic_eta_v1.jsonl`. The ingestion command validates them with the
existing `PortCallEvent` reconciliation contract and stores accepted events in
the separate `synthetic_demo` PostgreSQL schema. Invalid events and duplicate
delivery details are recorded in separate tables in that schema.

Synthetic records never enter `portnet_eta_observation` or the Kafka CDC tables.
The data represents ETA updates, not actual vessel arrivals or measured arrival
delays. The v1 event contract has no port-area code, so the dbt models report
activity and ETA history per fictional call without inventing an area.

## Generate and ingest

Start a fresh local stack with Docker Compose:

```bash
docker compose up --build -d
```

For an existing PostgreSQL volume, apply migration 010 once before ingestion;
instructions are in [Database operations](database.md#applying-migration-010-to-an-existing-volume).

Generate a small 50-call dataset, then ingest it:

```bash
docker compose exec airflow python -m scripts.generate_synthetic_eta --port-calls 50 --seed 42
docker compose exec airflow python -m scripts.ingest_synthetic_eta
```

Generate and ingest the full 1,000-call demonstration dataset:

```bash
docker compose exec airflow python -m scripts.generate_synthetic_eta --port-calls 1000 --seed 42
docker compose exec airflow python -m scripts.ingest_synthetic_eta --input fixtures/synthetic_eta_v1.jsonl
```

The second load adds events not present in the first 50 calls. Repeating either
ingestion is safe: accepted observations and quality records do not accumulate
on reruns. The command reports accepted, rejected, duplicate, collision, and
newly inserted counts. `DATABASE_URL` is supplied by the Compose `airflow`
service; the CLI exits with a clear error if it is missing.

Call and event IDs include the seed, so separate seeded scenarios can coexist
in `synthetic_demo` without ID collisions. Continue using seed 42 when loading
the 50-call sample followed by the 1,000-call dataset; the larger dataset then
adds the remaining calls and remains idempotent on reruns.

To ingest a different JSONL file, pass its path with `--input`. The path must be
visible inside the `airflow` container.

## Rebuild and inspect dbt models

Rebuild the analytics models after ingestion:

```bash
docker compose exec airflow dbt build --project-dir /opt/portpulse/dbt
```

The three synthetic marts are:

- `mart_synthetic_eta_daily_activity`: valid ETA update and call counts by UTC
  source-event date.
- `mart_synthetic_eta_revision_history`: each valid update, its previous ETA,
  and the signed ETA change in minutes. Positive changes move the estimate
  later; negative changes move it earlier. Ordering uses source event time.
- `mart_synthetic_eta_latest_eta`: the most recent ETA update per fictional
  port call, selected by source event time.

All three are built from validated, unique observations. Invalid events and
duplicate deliveries do not inflate the marts. Calls with conflicting event
IDs are excluded from analytics and retained in the quality table. Source
event time (`event_time_utc`), receipt/collection time (`received_at_utc`), and
database ingestion time (`ingested_at_utc`) remain separate fields.

Check stored counts and the isolated schema:

```bash
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS accepted_observations FROM synthetic_demo.eta_observation;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT reason_code, count(*) FROM synthetic_demo.eta_rejected_event GROUP BY reason_code ORDER BY reason_code;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT issue_code, count(*) FROM synthetic_demo.eta_quality_issue GROUP BY issue_code ORDER BY issue_code;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_synthetic_eta_daily_activity ORDER BY event_date_utc;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT port_call_id, event_id, previous_eta_utc, eta_utc, eta_change_minutes FROM analytics.mart_synthetic_eta_revision_history ORDER BY port_call_id, event_time_utc;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_synthetic_eta_latest_eta ORDER BY port_call_id LIMIT 10;'
```

With the default seed and 50 calls, generation reports 170 valid ETA updates,
181 event occurrences, 7 duplicate deliveries, and 4 invalid events. A fresh
schema then has 170 accepted observations, 4 rejected events, and 7
`EXACT_DUPLICATE` quality issues. For 1,000 calls, the expected totals are
3,496 accepted ETA updates, 3,728 event occurrences, 90 rejected events, and
142 duplicate deliveries. Loading 1,000 calls after the first 50 inserts 3,326
new observations; rerunning it inserts 0.

After loading the full dataset, dbt produces 28 daily activity rows covering
3,496 updates, 3,496 revision-history rows, and 1,000 latest-call rows. These
counts include only valid, unique observations.

## Next integration step

The contract has no `port_area_code`. If the next visualization needs area
filters, the smallest backward-compatible change is to define a v2 event with
an optional nullable `port_area_code`, retain v1 validation, and add a nullable
database column. Do not infer an area in dbt. A future visualization PR can
then decide whether to chart the new synthetic marts in Metabase; this PR does
not create dashboards or feed the Kafka CDC pipeline.

## Orchestration boundary

The intended architecture is for Airflow to orchestrate both the real API and
synthetic demo pipelines: source or generator → Python validation and
normalization → separate PostgreSQL schemas → dbt transformations and tests.
The real API and CDC DAGs remain independent. This PR adds no Airflow DAG; a
small dedicated synthetic-demo DAG is the next step and can call the existing
commands:

```sh
python -m scripts.generate_synthetic_eta --port-calls 1000 --seed 42
python -m scripts.ingest_synthetic_eta --input fixtures/synthetic_eta_v1.jsonl
dbt build --project-dir /opt/portpulse/dbt
```

Metabase reads the refreshed PostgreSQL analytics models as the presentation
layer. It does not need to run as an Airflow task for every pipeline run.
