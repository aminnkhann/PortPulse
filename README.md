# PortPulse

PortPulse is an educational data engineering project that collects Finnish port-call ETA observations, stores them in PostgreSQL, and builds analytics with dbt. It also includes a deterministic synthetic ETA demo and an optional, separate PostgreSQL CDC demonstration.

## Install and run locally

These steps use a Bash-compatible terminal and Docker Compose v2. Docker Engine on Linux or Docker Desktop with Linux containers is required. On Windows, use WSL 2 with Docker Desktop integration or another Bash-compatible environment. The repository does not specify a minimum CPU or memory size; Docker must have enough resources to build and run Airflow, PostgreSQL, Metabase, Kafka, and Kafka Connect together. A network connection is needed to clone the private GitHub repository, download container images, and reach Digitraffic for live data.

### 1. Verify prerequisites

Check Git, Docker, and the Compose plugin:

```sh
git --version
docker --version
docker compose version
```

Compose should report version 2 or later. Confirm Docker Engine is running:

```sh
docker info
```

If `docker info` cannot connect to the daemon, start Docker Engine or Docker Desktop and run the command again. The GitHub repository is private, so your GitHub account must have authorized access before cloning.

### 2. Clone and open the project

```sh
git clone https://github.com/aminnkhann/portpulse.git
cd portpulse
```

Run all following commands from this project directory. Compose binds its web interfaces and PostgreSQL to localhost only. Default host ports are `8080` (Airflow), `3000` (Metabase), `5432` (PostgreSQL), and `8083` (Kafka Connect). Kafka itself is only available to the other containers. If one of these host ports is already in use, see [Troubleshooting](#troubleshooting).

### 3. Configure optional local credentials

The defaults in Compose are for a local demo. To use your own local passwords, copy the example file and edit `.env` before starting the containers:

```sh
cp .env.example .env
```

Set the variables you want to override in `.env`; leave other values unset to use the defaults. Compose reads `.env` automatically. Do not publish these local services or reuse demo passwords on an internet-facing system. You do not need to create `.env` to continue.

### 4. Validate configuration and start the stack

```sh
docker compose config -q
docker compose up --build -d
```

The first build downloads images and can take several minutes. PostgreSQL applies the included migrations when its data volume is first created. The `bi-setup` one-shot service builds the dbt models, creates the Metabase read-only database account, and sets up the dashboards. Other setup jobs prepare Kafka and the CDC connector.

### 5. Wait for initialization and check service status

```sh
docker compose ps -a
```

Wait until `db`, `metabase`, `kafka`, and `kafka-connect` show `healthy` and `airflow` is running. `bi-setup`, `cdc-topic-setup`, and `cdc-setup` are one-shot jobs: they should show `Exited (0)` after completing successfully. `kafka-volume-init` also exits after setting up Kafka storage. If a job is still running, check it again shortly. For a failure, see [Troubleshooting](#troubleshooting).

### 6. Run the full pipeline in Airflow

Open [Airflow](http://localhost:8080) and sign in with `admin` / `admin`, unless you changed `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` in `.env`. Find `portpulse_full_pipeline` and click **Trigger DAG**. It is manual-only and unpaused on first installation. Wait for all four tasks to show **Success**:

1. `fetch_real_portnet`
2. `generate_synthetic_eta`
3. `ingest_synthetic_eta`
4. `build_analytics`

The first task requests seven UTC dates from Digitraffic Portnet V1: the DAG run's UTC date and the preceding six days. The synthetic branch generates and ingests 1,000 fictional port calls with seed 42. The final task builds the dbt models after both branches finish. A successful run means the requests, ingestion steps, and dbt checks completed; it does not establish ETA prediction accuracy or guarantee fresh source data.

### 7. Verify Metabase dashboards

Open [Metabase](http://localhost:3000) and sign in with `admin@example.test` / `portpulse-local-demo-2026`, unless you changed `METABASE_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD`. In the `PortPulse` collection, look for these three dashboards:

- **PortPulse — Portnet Overview** shows captured real Portnet observations. Counts vary with the live API and can be zero if no eligible observations are returned.
- **PortPulse — Synthetic ETA Analytics (SIMULATED DATA)** shows the separate fictional seed-42 data. Its expected dataset has 3,496 accepted ETA observations, 90 rejected events, and 142 duplicate-delivery issues. These are repeatable seed results, not real vessel calls.
- **PortPulse — Kafka CDC Demo (SIMULATED DATA)** shows the separate optional simulated CDC path. It can be empty until you run the [CDC demonstration](docs/streaming_cdc.md).

The three dashboards keep live Portnet, synthetic ETA, and simulated CDC data separate. The synthetic ETA changes are not actual vessel delays or measured arrival outcomes.

### 8. Run read-only PostgreSQL checks

These queries count stored rows and do not modify the database. They may return zero for live Portnet data; synthetic counts are expected after the full DAG succeeds. If you have rerun the pipeline before, stored counts are cumulative across distinct observations.

```sh
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS portnet_observations FROM portnet_eta_observation;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS synthetic_observations FROM synthetic_demo.eta_observation;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS latest_synthetic_etas FROM analytics.mart_synthetic_eta_latest_eta;'
docker compose exec -T db psql -U portpulse -d portpulse -c 'SELECT count(*) AS current_portnet_etas FROM analytics.mart_portnet_latest_eta;'
```

After the default seed-42 dataset is ingested, it contains 3,496 accepted synthetic observations and 1,000 latest synthetic ETAs. The live Portnet and latest real ETA counts depend on the API response. For more database checks and migration guidance, see [Database operations](docs/database.md).

### 9. Stop and restart safely

Stop the containers while keeping your data:

```sh
docker compose stop
```

Start them again later:

```sh
docker compose up -d
```

`docker compose down` also stops and removes the containers but retains the named data volumes. Do not run `docker compose down -v` unless you deliberately want to delete the stored PostgreSQL, Airflow, Kafka, and Metabase data.

## Optional: simulate Kafka CDC

CDC is a separate manual demonstration and is not part of the beginner pipeline above. Follow the [CDC guide](docs/streaming_cdc.md) for the complete procedure. In brief, wait for Kafka Connect and the Debezium connector to report `RUNNING`, create a new scenario ID, write five fictional changes, and trigger `portpulse_streaming_cdc` in Airflow. The expected operations are `c,u,u,c,d`; `stream_demo.port_eta_cdc_event` stores the history and `analytics.mart_stream_latest_eta` should retain `SIM-001` at `2026-11-13 14:50:00+00` while the deleted `SIM-002` is absent. Use a fresh scenario ID each time; the guide has the commands and exact columns.

From the project directory, this command starts a fresh five-change scenario after connector setup is ready:

```sh
SCENARIO_ID="demo-$(date +%s)"
docker compose exec -T airflow python scripts/simulate_port_traffic.py --scenario-id "$SCENARIO_ID" --delay-seconds 0
```

Then trigger `portpulse_streaming_cdc` in Airflow and use the verification queries in the [CDC guide](docs/streaming_cdc.md#process-the-micro-batch-in-airflow).

## Troubleshooting

Start with these safe diagnostics. Detailed procedures are in [Database operations](docs/database.md), [Metabase dashboards](docs/metabase.md), and the [CDC guide](docs/streaming_cdc.md).

| Problem | Diagnostic and next step |
|---|---|
| Docker is unavailable or not running | Run `docker info` and `docker compose version`. Start Docker Engine/Desktop, then retry. |
| A required host port is occupied | Check `docker compose ps -a` and `docker compose logs --tail 100 airflow db metabase kafka-connect`. Change the corresponding port in `.env` (`AIRFLOW_PORT`, `METABASE_PORT`, `POSTGRES_PORT`, or `KAFKA_CONNECT_PORT`) and recreate with `docker compose up -d`. |
| A service stays unhealthy or a setup job fails | Run `docker compose ps -a` and `docker compose logs --tail 100 db metabase kafka kafka-connect airflow bi-setup cdc-topic-setup cdc-setup`. Fix the reported issue and rerun `docker compose up -d`. |
| Airflow is unavailable or a task fails | Run `docker compose logs --tail 100 airflow` and `docker compose ps -a`. In Airflow, open the failed task's log for the specific error. |
| PostgreSQL or dbt reports a connection error | Run `docker compose logs --tail 100 db airflow bi-setup` and `docker compose exec -T db pg_isready -U portpulse -d portpulse`. See [Database operations](docs/database.md). |
| Metabase dashboards are missing | Run `docker compose logs bi-setup metabase` and check that `bi-setup` exited with code 0. See [Metabase dashboards](docs/metabase.md). |
| Digitraffic returns no eligible ETA observations | This can be a valid live result. Review the `fetch_real_portnet` task log and `docker compose logs --tail 100 airflow`; do not expect a fixed live count. |
| Kafka Connect or CDC is not ready | Run `docker compose ps -a`, `docker compose logs --tail 100 kafka kafka-connect cdc-setup`, and `curl -fsS http://localhost:8083/connectors/portpulse-postgres-cdc/status`. Follow the [CDC guide](docs/streaming_cdc.md). |

Do not delete Docker volumes as a troubleshooting step; they contain persistent data.

## Documentation

- [Architecture](ARCHITECTURE.md)
- [Database operations and migration notes](docs/database.md)
- [Data dictionary](docs/data_dictionary.md)
- [Metabase dashboards](docs/metabase.md)
- [Synthetic ETA demo](docs/synthetic_eta.md)
- [CDC guide and catch-up procedure](docs/streaming_cdc.md)
- [Final clean-clone acceptance checklist](docs/final_acceptance_checklist.md)
