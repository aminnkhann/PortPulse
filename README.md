# PortPulse

PortPulse is a local data engineering demo for Finnish port-call ETAs. It collects live Portnet observations, generates a deterministic synthetic ETA dataset, stores both in PostgreSQL, builds dbt analytics, and displays results in Metabase. Kafka CDC is a separate simulated workflow.

## Install and start

Prerequisites are Git, Docker Engine or Docker Desktop with Linux containers, and Docker Compose v2. On Windows, use PowerShell with Docker Desktop and Linux containers; WSL 2 is also supported with Docker Desktop integration. A Bash-compatible terminal is required only for the Bash instructions below. The project has no published minimum CPU or memory requirement; Docker needs resources for Airflow, PostgreSQL, Metabase, Kafka, and Kafka Connect together. Internet access is needed to clone the private personal repository, pull images, and fetch live data from Digitraffic. Services bind to localhost on Airflow 8080, Metabase 3000, PostgreSQL 5432, and Kafka Connect 8083. Kafka is internal to Compose.

The personal GitHub repository is private. You need authorized access to clone it. The DCI mirror is an alternative repository and may also require access.

### Windows PowerShell with Docker Desktop

Open PowerShell, start Docker Desktop, select Linux containers, then run:

```powershell
git --version
docker --version
docker compose version
docker info

git clone https://github.com/aminnkhann/portpulse.git portpulse
Set-Location portpulse
```

The Compose version should be v2 or later, and docker info should show a reachable server. If you use the DCI mirror, replace the clone command with:

```powershell
git clone https://github.com/dci-fbw-dataeng-python-25-a/amin-final-project.git portpulse
Set-Location portpulse
```

Optional: copy .env.example to .env and edit it to override local credentials or host ports:

```powershell
Copy-Item .env.example .env
```

Skip that command to use the local demo defaults.

Validate and start:

```powershell
docker compose config -q
docker compose up --build -d
docker compose ps -a
```

### Bash: WSL 2 or Linux

In WSL 2, enable Docker Desktop integration for your distribution. On Linux, ensure the Docker Engine is running and your account can use Docker. Then run:

```bash
git --version
docker --version
docker compose version
docker info

git clone https://github.com/aminnkhann/portpulse.git portpulse
cd portpulse
```

The personal repository is private and needs authorized access. For the DCI mirror, use this clone command instead:

```bash
git clone https://github.com/dci-fbw-dataeng-python-25-a/amin-final-project.git portpulse
cd portpulse
```

Optional: copy .env.example to .env and edit it to override local credentials or host ports:

```bash
cp .env.example .env
```

Skip that command to use the local demo defaults.

Validate and start:

```bash
docker compose config -q
docker compose up --build -d
docker compose ps -a
```

The first build can take several minutes. Re-run docker compose ps -a while services initialize. The db, metabase, kafka, and kafka-connect services should become healthy; airflow should be running. The one-shot services bi-setup, cdc-topic-setup, cdc-setup, and kafka-volume-init should show Exited (0). If any service or setup job fails, use the diagnostics in [Troubleshooting](#troubleshooting).

Open [Airflow](http://localhost:8080) and sign in with admin / admin unless changed in .env. Open the DAG list, find portpulse_full_pipeline, and use the play/trigger action. Wait for all four tasks to show Success:

- fetch_real_portnet
- generate_synthetic_eta
- ingest_synthetic_eta
- build_analytics

The default Portnet window is seven dates: the DAG run's UTC date and the preceding six dates. Digitraffic responses vary, and a run may have no eligible ETA observations. The synthetic branch uses the default 1,000-call seed-42 dataset. dbt runs after both ingestion branches complete. Open [Metabase](http://localhost:3000); default local sign-in is admin@example.test / portpulse-local-demo-2026 unless changed in .env.

## Running the Complete Test Suite with uv (86 Tests)

These commands are verified on Linux/Ubuntu with Bash. The complete integration run uses a separate Docker Compose project and PostgreSQL volume; tests do not connect to the `portpulse-main` database. The committed `uv.lock` keeps the resolved Python 3.12 dependency set reproducible.

### A. Prerequisites

Install Git, Docker Engine or Docker Desktop with Linux containers, Docker Compose v2, and uv. Install Python through uv and sync the project dependencies, including pytest and Ruff:

```bash
uv python install 3.12
uv sync --python 3.12
```

Run these commands from the repository root. Airflow is pinned at 3.4.0. Avoid upgrading Airflow or its providers independently of the lockfile; Docker and GitHub Actions continue to install Airflow with its official Python 3.12 constraints.

### B. Run unit tests without PostgreSQL

With the integration-test variables unset, run:

```bash
unset PORTPULSE_TEST_DATABASE_URL METABASE_DB_PASSWORD
uv run pytest -v
```

The verified baseline was **68 passed, 18 skipped**. The skipped tests need the separate PostgreSQL integration environment; the Metabase permission test also needs the `metabase_reader` role configured below. These counts describe the verified project version and can change as tests are added.

### C. Start an isolated PostgreSQL test database

Set the test-only port and local credentials. The defaults match the repository's local demo credentials; set these variables to your chosen test credentials before continuing if you override them:

```bash
export POSTGRES_PORT=15432
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-admin}"
export METABASE_DB_PASSWORD="${METABASE_DB_PASSWORD:-portpulse-local-reader}"
```

Start only the database under a separate Compose project:

```bash
docker compose -p portpulse-itest up -d db
docker compose -p portpulse-itest exec -T db pg_isready -U portpulse -d portpulse
```

Wait until `pg_isready` reports that PostgreSQL is accepting connections. On a fresh `portpulse-itest` volume, the official PostgreSQL image applies the SQL files mounted from `migrations/` in filename order. It runs those initialization files only when the database volume is empty. If you need a fresh test database later, use another project name and unused host port instead of deleting a volume.

`portpulse-main` is the normal application stack and has its own database volume. `portpulse-itest` is the separate integration-test stack and uses `portpulse-itest_postgres_data`; the main stack uses `portpulse-main_postgres_data`. Keep the integration-test URL on port `15432` so tests cannot target the main database on port `5432`.

### D. Prepare dbt models and the Metabase reader role

Build the project image used by `bi-setup`:

```bash
docker compose -p portpulse-itest build bi-setup
```

Build dbt models in the isolated database. The Airflow image has an Airflow entrypoint, so invoke dbt from Python with `subprocess.run(..., check=True)`:

```bash
docker compose -p portpulse-itest run --no-deps --rm bi-setup python -c "import subprocess; subprocess.run(['dbt', 'build', '--project-dir', '/opt/portpulse/dbt'], check=True)"
```

Configure `metabase_reader` by calling the existing `configure_reader()` function. This runs inside the `bi-setup` image and uses the test database service credentials:

```bash
docker compose -p portpulse-itest run --no-deps --rm bi-setup python -c "import os, psycopg; from scripts.setup_metabase import configure_reader; conn=psycopg.connect(host='db', dbname=os.environ['POSTGRES_DB'], user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD']); configure_reader(conn, os.environ['METABASE_DB_PASSWORD']); conn.commit(); conn.close()"
```

The role can read analytics marts but cannot read source tables or modify data. The full suite verifies these permissions. `METABASE_DB_PASSWORD` in the test command must match the value used when configuring the role.

### E. Run all 86 tests

With the local demo credentials, point both integrations at the isolated test server and run pytest:

```bash
PORTPULSE_TEST_DATABASE_URL="postgresql://portpulse:admin@127.0.0.1:15432/portpulse" \
METABASE_TEST_HOST=127.0.0.1 \
METABASE_TEST_PORT=15432 \
METABASE_DB_PASSWORD=portpulse-local-reader \
uv run pytest -v
```

If you override the local test credentials or host port, change the corresponding values in this command too. `METABASE_DB_PASSWORD` must match the password set for `metabase_reader`. The recorded result was:

```text
86 passed, 21 subtests passed in 17.41s
```

Runtime varies by machine. Integration tests create isolated schemas and a temporary database inside `portpulse-itest`; their cleanup does not use or remove the `portpulse-main` database.

### F. Troubleshooting

| Problem | Fix |
|---|---|
| `ModuleNotFoundError: No module named 'scripts'` or `dags` | Run `uv run pytest -v` from the repository root. `pyproject.toml` configures pytest's import path; do not set `PYTHONPATH` manually. |
| Missing or incompatible Python | Run `uv python install 3.12`, then `uv sync --python 3.12`. The project requires Python 3.12. |
| Airflow dependency conflict | Use the committed `uv.lock` with `uv sync`; do not independently upgrade Airflow or providers. Docker and CI install Airflow 3.4.0 using the official 3.12 constraints. |
| PostgreSQL connection failure | Check `docker compose -p portpulse-itest ps` and `docker compose -p portpulse-itest exec -T db pg_isready -U portpulse -d portpulse`. Confirm the URL uses `127.0.0.1:15432`, not the main stack's port `5432`. |
| PostgreSQL tests are skipped | Set `PORTPULSE_TEST_DATABASE_URL` to the isolated `portpulse-itest` database as shown above. Do not point it at `portpulse-main`. |
| Metabase reader authentication fails | Use the same `METABASE_DB_PASSWORD` in the role-configuration command and pytest environment. The test connects as `metabase_reader`. |
| `analytics` schema or marts are missing | Run the dbt build command in section D against the isolated project. A fresh Docker volume runs migrations automatically; migrations do not rerun on an existing volume. |
| Port `15432` is occupied | Set `POSTGRES_PORT=15433`, use a distinct project such as `portpulse-itest-alt`, and use that port in the readiness command, database URL, and `METABASE_TEST_PORT`. |
| `dbt build` is interpreted as an Airflow command | Run it through `bi-setup python -c` with `subprocess.run(..., check=True)` as shown in section D; do not pass `dbt` directly to the Airflow entrypoint. |

**Windows distinction:** this procedure is verified on Linux/Ubuntu, not native Windows PowerShell. On Windows, use WSL 2 with Docker Desktop Linux-container integration and run these Bash commands inside the WSL distribution.

### G. Stop the isolated test environment safely

Stop and remove only the `portpulse-itest` containers and network while preserving its named volume:

```bash
docker compose -p portpulse-itest down
```

This leaves `portpulse-main` running and preserves the test database volume. Do not add `-v` and do not remove the main application's database volume.

## Reproduce and Verify the Results

This section reproduces the main Airflow run and then the optional CDC demo. Docker Compose and psql commands shown in SQL blocks work in both PowerShell and Bash. Do not reset or remove volumes to reproduce results; use a fresh unique CDC scenario ID.

### Main pipeline and dashboards

In Airflow, trigger portpulse_full_pipeline. Confirm all four task instances listed above are Success. To inspect a failure, open that task instance and its Logs tab; docker compose logs --tail 100 airflow is also a safe diagnostic.

Check service and one-shot setup status with:

```sh
docker compose ps -a
```

Expected setup jobs are bi-setup, cdc-topic-setup, cdc-setup, and kafka-volume-init, each exited with code 0. db, metabase, kafka, and kafka-connect should report healthy; airflow should be running.

Run these read-only SQL checks:

```sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT accepted_observations, unique_rejected_records, unique_duplicate_delivery_problems, identity_collision_groups, latest_eta_records FROM analytics.mart_pipeline_quality;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS portnet_observations FROM public.portnet_eta_observation;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS latest_portnet_etas FROM analytics.mart_portnet_latest_eta;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS synthetic_calls FROM analytics.mart_synthetic_eta_latest_eta;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS accepted_synthetic_observations FROM synthetic_demo.eta_observation;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) FILTER (WHERE previous_eta_utc IS NOT NULL) AS eta_revisions FROM analytics.mart_synthetic_eta_revision_history;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT rejected_events, duplicate_issues, collision_issues FROM analytics.mart_synthetic_eta_quality;'
```

After the default synthetic seed-42 dataset is ingested, the expected synthetic results are 1,000 latest calls, 3,496 accepted observations, 2,496 revisions, 90 rejected events, 142 duplicate-delivery issues, and zero identity collisions. These results depend on the default input and successful ingestion. They are deterministic and can be checked with the queries above.

The main images show a Windows run captured on 20 November 2026:

- Airflow: [full pipeline success](docs/images/windows-docker-2026-11-20/airflow-full-pipeline.png). Trigger portpulse_full_pipeline in Airflow; all four task states are verified in the Airflow UI, not by SQL.
- Real data: [Portnet dashboard](docs/images/windows-docker-2026-11-20/metabase-portnet-overview.png). Open the dashboard named **PortPulse — Portnet Overview**. The capture showed 569 accepted observations, 569 latest ETAs, and zero unique rejected, duplicate, and collision records. The first SQL query above verifies these displayed metrics. These real API counts vary and can be zero; they are not guaranteed on another run.
- An [earlier empty Portnet capture](docs/images/portnet_overview.png) is also a valid live-data snapshot. It came from a different run; the same live-data SQL checks above show what the current database contains.
- Synthetic data: [Synthetic ETA dashboard](docs/images/windows-docker-2026-11-20/metabase-synthetic-eta.png). Open **PortPulse — Synthetic ETA Analytics (SIMULATED DATA)**. The synthetic SQL queries above verify its counts. Counts are deterministic for the default seed-42 run after successful ingestion.

Synthetic ETA revisions describe changes to estimates; they do not measure actual arrival delays or prediction accuracy. For the full original run context and its recorded values, see [Windows Docker results](docs/windows_docker_results.md).

### Optional Kafka CDC demonstration

CDC is separate from portpulse_full_pipeline. It writes only fictional changes to stream_demo.port_eta_state and its CDC tables. The streaming DAG is initially paused. Follow these steps from the project directory after the main Compose stack is running.

First check service health and the Debezium connector/task status. In PowerShell:

```powershell
docker compose ps -a
$Connector = Invoke-RestMethod -Uri http://localhost:8083/connectors/portpulse-postgres-cdc/status
$Connector.connector.state
$Connector.tasks | Format-Table id, state
```

In Bash:

```bash
docker compose ps -a
curl -fsS http://localhost:8083/connectors/portpulse-postgres-cdc/status
```

Continue only when kafka and kafka-connect are healthy and both the connector and its task report RUNNING.

Create a new scenario ID for each run. PowerShell:

```powershell
$ScenarioId = "windows-$([guid]::NewGuid().ToString('N'))"
$ScenarioId
docker compose exec -T airflow python scripts/simulate_port_traffic.py --scenario-id $ScenarioId --delay-seconds 0
```

Bash:

```bash
SCENARIO_ID="linux-$(date +%s)-$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')"
printf '%s\n' "$SCENARIO_ID"
docker compose exec -T airflow python scripts/simulate_port_traffic.py --scenario-id "$SCENARIO_ID" --delay-seconds 0
```

The simulator prints five committed changes in this order: create SIM-001, update SIM-001 twice, create SIM-002, delete SIM-002. Check the selected scenario's source state before triggering CDC:

```powershell
docker compose exec -T db psql -U portpulse -d portpulse -c "SELECT port_call_id, port_area_code, eta_utc FROM stream_demo.port_eta_state WHERE scenario_id = '$ScenarioId' ORDER BY port_call_id;"
```

```bash
docker compose exec -T db psql -U portpulse -d portpulse -c "SELECT port_call_id, port_area_code, eta_utc FROM stream_demo.port_eta_state WHERE scenario_id = '$SCENARIO_ID' ORDER BY port_call_id;"
```

Unpause and trigger the bounded CDC DAG:

```sh
docker compose exec -T airflow airflow dags unpause portpulse_streaming_cdc
docker compose exec -T airflow airflow dags trigger portpulse_streaming_cdc
```

In Airflow, open portpulse_streaming_cdc and confirm consume_cdc_batch and build_streaming_dbt_models both show Success. The consumer processes at most 100 Kafka records per run; trigger it again only if lag remains. Open the task logs from the Airflow task instance when a task fails.

Use the same scenario variable in the following checks.

PowerShell:

```powershell
docker compose exec -T db psql -U portpulse -d portpulse -c "SELECT operation, count(*) FROM stream_demo.port_eta_cdc_event WHERE scenario_id = '$ScenarioId' GROUP BY operation ORDER BY operation;"
docker compose exec -T db psql -U portpulse -d portpulse -c "SELECT port_call_id, port_area_code, eta_utc FROM analytics.mart_stream_latest_eta WHERE scenario_id = '$ScenarioId' ORDER BY port_call_id;"
```

Bash:

```bash
docker compose exec -T db psql -U portpulse -d portpulse -c "SELECT operation, count(*) FROM stream_demo.port_eta_cdc_event WHERE scenario_id = '$SCENARIO_ID' GROUP BY operation ORDER BY operation;"
docker compose exec -T db psql -U portpulse -d portpulse -c "SELECT port_call_id, port_area_code, eta_utc FROM analytics.mart_stream_latest_eta WHERE scenario_id = '$SCENARIO_ID' ORDER BY port_call_id;"
```

For the selected scenario, the operation query should show two c (create), two u (update), and one d (delete). The latest-state query should return only SIM-001, area VUOS, with ETA 2026-11-13 14:50:00+00. SIM-002 was deleted and should not appear.

Check consumer lag:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group portpulse-streaming-cdc
```

The demonstration topic has three partitions. Confirm LAG is 0 for every listed partition; if a partition still has lag, trigger the CDC DAG again and recheck.

Open the **PortPulse — Kafka CDC Demo (SIMULATED DATA)** dashboard in Metabase. These read-only queries verify its cumulative cards and rows:

```sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS cdc_events FROM analytics.stg_stream_eta_changes;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS active_simulated_calls FROM analytics.mart_stream_latest_eta;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT scenario_id, port_call_id, port_area_code, eta_utc FROM analytics.mart_stream_latest_eta ORDER BY scenario_id, port_call_id;'
```

With one completed scenario on an otherwise empty CDC database, expect five stored CDC operations and one active simulated call. With two completed scenarios, expect ten cumulative events and two active calls, assuming no other scenarios were stored. Existing databases can contain other scenarios, so dashboard totals are cumulative and are not fixed expected counts.

The repository's [CDC Airflow screenshot](docs/images/portpulse-streaming-cdc-airflow-success.png) shows the DAG run and both tasks successful; task state is verified in Airflow, not with a SQL query. The event and latest-state queries above verify its data. The [verified Windows CDC Metabase screenshot](docs/images/windows-docker-2026-11-20/windows-cdc-metabase-2026-11-20.png) shows ten cumulative CDC events and two active simulated calls across two scenarios. The older [CDC Metabase screenshot](docs/images/cdc_demo.png) shows a separate earlier capture with one active call. Per-scenario results are verified using the scenario-filtered SQL queries above; the 10/2 dashboard totals are cumulative and are not guaranteed for a fresh or existing database.

![Windows Kafka CDC Metabase dashboard: ten cumulative events and two active simulated calls](docs/images/windows-docker-2026-11-20/windows-cdc-metabase-2026-11-20.png) See the [CDC guide](docs/streaming_cdc.md) for connector details and [Windows Docker results](docs/windows_docker_results.md) for recorded findings.

## Troubleshooting

| Problem | Safe diagnostic |
|---|---|
| Docker is unavailable or stopped | docker info and docker compose version; start Docker Engine/Desktop. |
| A required host port is occupied | docker compose ps -a; set a different AIRFLOW_PORT, METABASE_PORT, POSTGRES_PORT, or KAFKA_CONNECT_PORT in .env and recreate with docker compose up -d. |
| A service is unhealthy or setup failed | docker compose ps -a and docker compose logs --tail 100 db metabase kafka kafka-connect airflow bi-setup cdc-topic-setup cdc-setup. |
| Airflow is unavailable or a task fails | docker compose logs --tail 100 airflow; inspect the failed task's Logs tab. |
| PostgreSQL or dbt connection errors | docker compose exec -T db pg_isready -U portpulse -d portpulse and docker compose logs --tail 100 db airflow bi-setup. |
| Metabase dashboards are missing | docker compose logs bi-setup metabase; confirm bi-setup exited successfully. |
| Digitraffic returns no eligible ETAs | This can be a valid live result. Review the fetch_real_portnet task log; do not expect a fixed live count. |
| Kafka Connect or CDC is not ready | docker compose ps -a, docker compose logs --tail 100 kafka kafka-connect cdc-setup, and the connector status endpoint above. |

See [Metabase dashboards](docs/metabase.md), [Database operations](docs/database.md), and [CDC operations](docs/streaming_cdc.md) for detailed troubleshooting.

## Stop safely

Stop containers while retaining data:

```sh
docker compose stop
```

Restart later with docker compose up -d. docker compose down also retains named volumes. Do not use docker compose down -v unless you intend to delete local PostgreSQL and service data.

## Acceptance status

**Windows Docker demonstration: VERIFIED** for the owner-reported main pipeline and five-change CDC workflow ([Windows Docker results](docs/windows_docker_results.md)). **Existing-stack repeat-run evidence: VERIFIED on 22 November 2026** for unchanged seed-42 synthetic summary counts, a successful CDC micro-batch with no new stored events and zero lag, and two Real/Synthetic ID-pattern separation checks ([22 November acceptance evidence](docs/acceptance_evidence_2026-11-22.md)). **Full isolated clean-clone acceptance: NOT COMPLETE / NOT YET RUN as a complete checklist.** The successful checks above were on the existing Windows stack; the isolated latest-`main` run, full repeat-run assertions, and final signed-off record remain pending ([acceptance checklist](docs/final_acceptance_checklist.md)).
