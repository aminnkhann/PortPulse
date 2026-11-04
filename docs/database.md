# Local database

This project uses one local PostgreSQL service. The migrations create the source, port-call, event-history, rejection, issue, and trusted-state tables, then seed the three synthetic sources from `rules/eta_rules_v1.yaml`.

## Start and inspect

```sh
docker compose up -d
docker compose ps
docker compose exec db psql -U portpulse -d portpulse -c '\dt'
docker compose exec db psql -U portpulse -d portpulse -c 'SELECT * FROM source_system ORDER BY priority DESC;'
```

PostgreSQL listens on `127.0.0.1:5432`. The default password is `portpulse_local_only` for local development only. Set `POSTGRES_PASSWORD` and, if needed, `POSTGRES_PORT` in a local `.env` file to override the defaults; `.env` is ignored by Git.

Migrations are automatically applied in filename order when Docker initializes a new database volume. Apply a new migration to an existing database with:

```sh
docker compose exec -T db psql -U portpulse -d portpulse < migrations/002_rejected_event_details.sql
docker compose exec -T db psql -U portpulse -d portpulse < migrations/003_raw_event_ingested_at.sql
```

Migration 003 adds `raw_event.ingested_at_utc`, which PostgreSQL assigns when it stores an event. This is distinct from the fixture's simulated `received_at_utc`. Existing rows receive the migration's application time because their original ingestion time cannot be recovered.

## Ingest the fixture

Install the two runtime dependencies and run the validator/loader:

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/ingest_fixture.py
```

The loader checks the JSON Schema, stores one row per unique event ID and payload hash, records each exact duplicate attempt as a quality issue, stores collision variants, and quarantines invalid payloads. It can be rerun without adding the same events or issues again. If you changed the database password or port, set `DATABASE_URL` for the loader, for example `postgresql://portpulse:your_password@127.0.0.1:5433/portpulse`.

Run the focused automated checks with `python -m unittest discover -s tests -v`. To run the PostgreSQL integration checks, also set `PORTPULSE_TEST_DATABASE_URL` to a disposable PostgreSQL database; the tests create and drop a uniquely named schema in it.

For the current fixture, the first run should leave 28 rows in `raw_event`, 1 in `rejected_event`, 1 in `port_call`, and quality issue counts of 2 `EXACT_DUPLICATE` plus 1 `ID_COLLISION`. Check with:

```sh
docker compose exec db psql -U portpulse -d portpulse -c "SELECT (SELECT count(*) FROM raw_event) AS raw_events, (SELECT count(*) FROM rejected_event) AS rejected_events, (SELECT count(*) FROM port_call) AS port_calls;"
docker compose exec db psql -U portpulse -d portpulse -c 'SELECT issue_code, count(*) FROM quality_issue GROUP BY issue_code ORDER BY issue_code;'
```

`docker compose down` stops the service and keeps its data volume. Do not use `docker compose down -v` unless you intentionally want to delete the local database.

`raw_event` allows different payload hashes for the same `event_id`, so a collision's variants can be retained. Its composite uniqueness rule prevents storing an exact event payload twice; the duplicate attempt belongs in `quality_issue`. Reconciliation must exclude every `raw_event` row whose ID has an `ID_COLLISION` issue. Invalid payloads belong in `rejected_event` and do not enter trusted state.

To check the generated ingestion timestamps:

```sh
docker compose exec db psql -U portpulse -d portpulse -c 'SELECT event_id, received_at_utc, ingested_at_utc FROM raw_event ORDER BY ingested_at_utc LIMIT 5;'
```
