# PortPulse live pipeline run — 2026-11-12

## Environment and execution

- Source: real Digitraffic Portnet V1 API, all ports, requested UTC date `2026-11-12`.
- Application database: disposable local PostgreSQL database on port 5433, initialized with migrations 001–007. The existing PortPulse database and volume were not used.
- Airflow metadata: separate temporary SQLite database under `/tmp/portpulse-airflow-run`.
- Workflow: `portpulse_daily_pipeline`, executed with `airflow dags test`; all three tasks completed successfully in order.
- Successful execution used `run_date=2026-11-12`; the Airflow test's logical date was `2026-11-13` to create a distinct run record.

## Real API observations

The first live poll returned 320 Portnet call records. Of these, 139 completed calls were skipped, 174 active ETA observations were valid and inserted, and 7 active calls had no ETA and were skipped. The API reported zero rejected call records, zero rejected ETA details, zero duplicate attempts, and zero ID-collision groups.

Two subsequent idempotency polls returned 320 call records each. The first repeat had 174 active ETA observations, all unchanged; the final poll had 173 active ETA observations, all unchanged, after one call moved from active to completed. Neither repeat inserted new rows, so the stored live observation count remained 174.

## Database and analytics results

The disposable test database also contained four deterministic dbt CI fixture observations, kept separate from the live API rows in these counts:

| Measure | Result |
| --- | ---: |
| Live API observations stored | 174 |
| Synthetic CI fixture observations | 4 |
| Total accepted source observations | 178 |
| Rejected observations | 0 |
| Duplicate issue rows accumulated during repeat-poll checks | 347 |
| Collision issue rows | 1 (deterministic CI fixture) |
| Latest-ETA mart rows total | 176 |
| Latest-ETA rows from live observations | 174 |

The dbt build completed with `PASS=12 WARN=0 ERROR=0 SKIP=0`, comprising three model builds and nine data tests. The latest mart selected the expected current live ETA rows and excluded the fixture collision ID. Airflow's `verify_pipeline` task reported `observations=178 staging=178 rejected=0 duplicates=347 collisions=1 latest_eta=176` and succeeded. The 347 duplicate issue rows are the accumulated results of the two repeat-poll checks.

These are results from the disposable demo database. The duplicate issue total includes repeated polls from the run verification and the one collision is synthetic; neither is a current live API rejection/collision count. No result here claims that Portnet observations passed through Phase 1's synthetic three-source trusted-ETA reconciler.

## Airflow execution evidence

The successful Airflow CLI run marked `ingest_portnet`, `build_dbt_analytics`, `verify_pipeline`, and the DAG run as successful. An earlier invocation included `--show-dagrun`; all tasks and the DAGRun succeeded, but the CLI returned an error after completion because the optional graph export needs the Graphviz Python package. The recorded successful CLI run omitted that optional rendering flag.

Airflow standalone initialized its metadata database and logged Gunicorn listening on `0.0.0.0:8080`. An HTTP probe from this restricted execution environment could not reach the local listener, so browser access itself remains to be confirmed on the user's Ubuntu session.

## Separate scheduled startup run against the existing local database

While checking standalone startup, its scheduler immediately picked up a due scheduled interval. That startup was intentionally not configured with the isolated test database environment. Its ingestion task called Portnet V1 for UTC date `2026-11-11` and inserted 170 valid live observations into the existing local PostgreSQL database on port 5432. A read-only query confirmed 170 observations with `ingested_at_utc` at or after 22:03 UTC. No rows were deleted. The following dbt task failed authentication against the pre-existing local profile, and the Airflow process was stopped before the retry; this startup run did not complete dbt or verification. The fully successful three-task run above was on the disposable port 5433 database.

Afterward, dbt was configured with the same local connection values as ingestion. `dbt debug` passed and `dbt build` passed all 12 operations against the primary database. A direct call to the DAG's verifier then passed with `observations=207 staging=207 rejected=0 duplicates=37 collisions=0 latest_eta=207`. The 207 primary-database observations comprise the 170 inserted during the scheduled startup and 37 that were already stored. This brought the primary database's marts current, but the scheduled Airflow run itself was not restarted through verification after its dbt authentication failure.
