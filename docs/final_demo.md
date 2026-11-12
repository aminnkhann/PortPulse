# PortPulse final demo (5–7 minutes)

Run every command from the repository root. Docker Compose runs PostgreSQL and the Airflow container, which includes the Python ingestion CLI and dbt. PostgreSQL data and Airflow metadata use separate persistent volumes. The local demo login is `admin` / `admin`.

## 1. Start the project

~~~sh
docker compose up --build -d
docker compose ps
docker compose logs --tail 30 airflow
~~~

If upgrading the existing database volume from the previous local password, follow the one-time password update in [the Compose runbook](database.md#existing-postgresql-volume-from-the-previous-local-setup). It preserves existing rows and volumes.

Open <http://localhost:8080> and sign in with `admin` / `admin`. In the DAG list, find `portpulse_daily_pipeline`. It starts paused to prevent an unexpected live API request.

## 2. Trigger and inspect the DAG

In the UI, select `portpulse_daily_pipeline` and **Trigger DAG**. Leave the optional configuration blank to use the current logical date, or specify a UTC date such as `{"run_date":"2026-11-13"}`. Manual runs work while the schedule remains paused.

Show the Graph view and the logs for `ingest_portnet`, `build_dbt_analytics`, and `verify_pipeline`. Explain that the first task runs the existing CLI, the second builds dbt models and runs their tests, and the third checks source tables, marts, counts, and latest-row uniqueness. A task failure fails the DAG run; transient failures are retried twice.

To enable the daily 06:00 UTC schedule, unpause the DAG with its UI toggle. Do this only when ready for scheduled live API polling.

## 3. Show stored API observations

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS stored_observations FROM public.portnet_eta_observation;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT event_id, port_call_id, port_area_code, event_time_utc, received_at_utc, ingested_at_utc, eta_utc FROM public.portnet_eta_observation ORDER BY ingested_at_utc DESC LIMIT 5;'
~~~

These are cumulative stored rows, not the number fetched by this run. Use the ingestion task log for that run's API summary.

## 4. Show analytics and quality

~~~sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT port_call_id, port_area_code, event_id, provider_updated_at_utc, eta_utc FROM analytics.mart_portnet_latest_eta ORDER BY port_call_id, port_area_code LIMIT 10;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT * FROM analytics.mart_pipeline_quality;'
~~~

The latest mart chooses one eligible ETA per call and area and excludes collision IDs. The quality mart reports accepted observations, rejected observations, duplicate issues, collision issues, and latest ETA rows. Zero rejected records is valid.

## 5. Explain tests and synthetic reconciliation

GitHub Actions runs Ruff, all Python tests, Airflow DAG import checks, PostgreSQL migrations, deterministic fixtures, `dbt debug`, and the full dbt build. CI does not call the live API. From a running Compose stack, lint and unit tests can also be run inside the image:

~~~sh
docker compose exec airflow ruff check /opt/portpulse
docker compose exec airflow python -m unittest discover -s tests -v
~~~

Demonstrate Phase 1 as a separate synthetic flow:

~~~sh
docker compose exec airflow python -m scripts.ingest_fixture
docker compose exec airflow python -m scripts.reconcile
docker compose exec airflow python -m scripts.query_state PC-DEMO-001
~~~

Its deterministic golden ETA is `2026-01-15T15:40:00Z` from `COORD-DELAYED-01`. Real Portnet observations are not reconciled through this synthetic three-source engine.

## Suggested timing

| Segment | Time |
| --- | ---: |
| Compose startup and architecture | 1 minute |
| Trigger, graph, and task logs | 2 minutes |
| PostgreSQL and dbt marts | 1.5 minutes |
| Tests and separate synthetic demo | 1.5 minutes |
