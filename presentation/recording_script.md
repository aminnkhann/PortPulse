# PortPulse Metabase walkthrough

Target length: 3–5 minutes. Record the running application and code, not only these slides. Save the recording as `PortPulse_Metabase_Walkthrough.mp4` and share it with the mentor on Slack yourself.

## Speaking and screen plan

### 1. Introduce PortPulse (about 30 seconds)

**Show:** Slide 1 or the repository README.

“PortPulse collects estimated arrival times from Finland’s Portnet API. Python validates the data, PostgreSQL stores it, dbt creates analytics tables, and Airflow coordinates the workflow. Metabase gives us a visual way to inspect those finished tables.”

### 2. Walk through the code (about 60 seconds)

**Show:** `scripts/ingest_portnet.py`, `dags/portpulse_daily_pipeline.py`, then the mart SQL files under `dbt/models/marts/`.

“The Python command fetches and validates Portnet records. The Airflow DAG runs ingestion, builds dbt models, and verifies the results. The mart models select the current ETA and summarize data quality. Metabase reads these marts; it does not replace the pipeline.”

### 3. Show Airflow (about 30 seconds)

**Show:** Airflow at `http://localhost:8080`, the `portpulse_daily_pipeline` graph, and task logs if a run is available.

“Airflow shows the task sequence and whether each step completed. The DAG is paused by default, so it runs only when enabled or triggered.”

### 4. Show the Portnet dashboard (about 45 seconds)

**Show:** The actual `PortPulse — Portnet Overview` dashboard in Metabase.

“This dashboard uses real Portnet mart rows only. It shows latest records, their port-area distribution, and quality counts. The quality values are a current cumulative snapshot. If there are no API observations, an empty result is the accurate result.”

### 5. Show Kafka CDC dashboard (about 60 seconds)

**Show:** `PortPulse — Kafka CDC Demo (SIMULATED DATA)`, then optionally the streaming DAG or CDC guide.

“This second dashboard is deliberately separate and labeled simulated. The simulator changes a PostgreSQL table. Debezium reads those changes from PostgreSQL’s write-ahead log and sends them to Kafka. Airflow consumes a bounded batch, stores the events, and runs dbt. The mart shows SIM-001 at its final simulated ETA and leaves SIM-002 out after its delete, when the demo has been run.”

### 6. Close (about 20 seconds)

**Show:** Slide 5 or the README’s BI section.

“This project demonstrates API ingestion, validation, orchestration, CDC, transformations, and BI in one local learning environment. It is an educational demo, not a production maritime tracking service.”

## Recording checklist

- Start the services and confirm the dashboards load before recording.
- Use genuine live application screens. Do not present empty data as populated data.
- Keep the Portnet and simulated CDC dashboards clearly separate.
- Avoid showing passwords, `.env`, or private account information.
- Save as `PortPulse_Metabase_Walkthrough.mp4`; review the video before sending it to the mentor on Slack.
