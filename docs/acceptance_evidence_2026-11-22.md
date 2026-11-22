# Acceptance evidence — 22 November 2026 (existing Windows stack)

**Status: PASS for the specific observed checks listed below; full isolated clean-clone acceptance remains NOT COMPLETE.** A separate manual Main DAG run failed on the tested revision; the date-handling fix and regression tests are recorded below, and isolated Docker retesting remains pending.

This records results supplied by the project owner from their existing Windows Docker environment, reported on 22 November 2026. These tests were **not performed in a separate `portpulse-acceptance` Compose project**, and this documentation commit did not rerun Docker. The author reviewed the owner-provided terminal output and Airflow screenshots in conversation. Those particular 22 November screenshots and full terminal transcripts are **not yet committed as files** in this repository; the observations are transcribed below with their evidence limits.

**Reference revision:** personal GitHub `main` was at `79db664d9f7451464db77e4f1b02f234da5cfb75` when this record was prepared. The exact commit checked out on the owner's Windows machine was **not captured**, so do not treat the GitHub reference revision as a verified host revision.

## Manual DAG run date failure and fix — 22 November 2026

- Tested revision: `55879bed5bd827f19647c44cc98d4b56df3a5ec6`
- DAG run: `portpulse_full_pipeline`, `manual__2026-11-22T11:07:08.013480+00:00`
- Failed task: `fetch_real_portnet`
- Observed exception: `AttributeError: 'NoneType' object has no attribute 'astimezone'`
- Root cause: the task assumed every `dag_run.logical_date` was set. In this manual run it was `None`, and the task called `.astimezone()` on it.

The installed Airflow SDK is 3.4.0. Its `DagRun.run_after` column is a timezone-aware `TIMESTAMP`; a constructed `DagRun` with `logical_date=None` retained its supplied timezone-aware `run_after`. The fix uses `logical_date` when present and otherwise `run_after`, converts the selected value to UTC, and raises a clear `ValueError` if the timestamp is missing or naive. The daily DAG uses the same fallback. Explicit `run_date`, `lookback_days`, and `portnet_dates()` validation are preserved.

**Checks run for the fix branch:**

- **PASS:** focused Main and Daily DAG unit tests — 14 tests passed, including manual `run_after` fallback, logical-date UTC boundary conversion, explicit `run_date`, lookback windows, and missing/naive timestamps.
- **PASS:** `python -m compileall` for both DAGs and their tests; `git diff --check`.
- **LIMIT:** the full two-module unit-test run had one unrelated import failure in the Airflow `DagBag` test because this local virtualenv lacks `airflow.providers.apache.kafka` (the dependency is specified in `requirements.txt`). The other 14 tests passed.
- **PENDING:** no Docker/Airflow integration test was run for the fix. In the isolated acceptance project, manually trigger `portpulse_full_pipeline` with no `run_date` override and verify `fetch_real_portnet` succeeds using the run's UTC `run_after` date. Do not remove or overwrite the existing acceptance environment or its volumes.

The overall acceptance status remains **NOT COMPLETE** until this isolated manual-trigger retest and the remaining checklist items pass.

## 1. Real / Synthetic ID-pattern separation — PASS

Read-only SQL executed on the existing Windows stack:

```sql
SELECT count(*) AS synthetic_records_in_real
FROM portnet_eta_observation
WHERE port_call_id LIKE 'SYNTH-%';

SELECT count(*) AS real_records_in_synthetic
FROM synthetic_demo.eta_observation
WHERE port_call_id LIKE 'portnet:%';
```

| Query | Observed | Expected | Result |
|---|---:|---:|---|
| Synthetic-looking IDs in real Portnet observation table | 0 | 0 | PASS |
| Portnet-looking IDs in synthetic observation table | 0 | 0 | PASS |

**Limit:** These are ID-pattern checks, **not a complete proof of row-level source isolation** or a test of CDC cross-contamination.

## 2. Synthetic same-seed repeat-run — PASS for recorded counts

**Baseline before manual re-trigger:**

| Metric | Before run | After successful run | Expected | Result |
|---|---:|---:|---:|---|
| `synthetic_demo.eta_observation` rows | 3,496 | 3,496 | 3,496 | PASS |
| `analytics.mart_synthetic_eta_latest_eta` rows | 1,000 | 1,000 | 1,000 | PASS |
| `analytics.mart_synthetic_eta_quality.rejected_events` | 90 | 90 | 90 | PASS |
| `analytics.mart_synthetic_eta_quality.duplicate_issues` | 142 | 142 | 142 | PASS |
| `analytics.mart_synthetic_eta_quality.collision_issues` | 0 | 0 | 0 | PASS |

- DAG: `portpulse_synthetic_demo`
- Run ID: `manual__2026-11-22T10:31:31.496301+00:00`
- Manual run configuration: `{"port_calls":1000,"seed":42}`
- Airflow screenshot: entire run **Success**, all three tasks (`generate_synthetic_eta`, `ingest_synthetic_eta`, `build_synthetic_eta_analytics`) **Success**; approximately 22.5 seconds
- The owner ran the SQL again **after** task success, and all five values above remained unchanged.

SQL used for the counts:

```sql
SELECT count(*) AS accepted FROM synthetic_demo.eta_observation;
SELECT count(*) AS latest FROM analytics.mart_synthetic_eta_latest_eta;
SELECT rejected_events, duplicate_issues, collision_issues
FROM analytics.mart_synthetic_eta_quality;
```

**Limit:** The user did not submit before/after equality checks for the **contents** of each latest ETA row or the revision-history count. Thus the observation proves stable recorded counts after the successful same-seed run, **not byte-for-byte identity across all tables**. The host's local commit SHA was not captured.

## 3. CDC no-new-events repeat-run — PASS

**Baseline before triggering CDC again:** `stream_demo.port_eta_cdc_event` contained **10** events. Consumer group `portpulse-streaming-cdc` had zero lag:

| Kafka partition | Current offset | Log-end offset | LAG before | LAG after |
|---|---:|---:|---:|---:|
| 0 | 3 | 3 | 0 | 0 |
| 1 | 3 | 3 | 0 | 0 |
| 2 | 6 | 6 | 0 | 0 |

The group displayed `has no active members` at rest, consistent with a manually invoked bounded micro-batch consumer.

- No new simulated source changes were reported before this re-trigger.
- DAG: `portpulse_streaming_cdc`
- Run ID: `manual__2026-11-22T10:37:06.415675+00:00`
- Airflow screenshot: run **Success**; both `consume_cdc_batch` and `build_streaming_dbt_models` **Success**; approximately 13.2 seconds
- After task completion, `stream_demo.port_eta_cdc_event` still contained **10** rows and Kafka lag remained **0** on all three partitions.

SQL and command:

```sql
SELECT count(*) AS cdc_events FROM stream_demo.port_eta_cdc_event;
```

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group portpulse-streaming-cdc
```

**Limit:** This confirms that a successful re-trigger without reported new changes did not increase stored CDC event rows in that instance. It is **not** a forced offset-replay or crash-recovery test.

## 4. Evidence carried forward from 20 November

The previous [Windows Docker result report](windows_docker_results.md) records the successful four-task main Airflow pipeline, deterministic synthetic seed-42 dataset, manual five-change CDC scenario, final-state verification, Kafka lag checks, and committed dashboard screenshots. The 22 November evidence above adds repeatability checks on the **existing** environment; it does not replace the older report or claim all steps were repeated after the latest merge.

## 5. Checks still required for full isolated acceptance

1. Record the actual Windows checkout commit SHA using `git rev-parse HEAD` and confirm alignment with the intended `main` revision.
2. Create a **fresh independent checkout with a distinct Compose project name, isolated ports and fresh named volumes**; verify startup, health checks and one-shot setup jobs.
3. In that new isolated environment, run the main DAG, synthetic seed-42 DAG and separate five-change CDC scenario; record all run IDs, task statuses, SQL assertions, dashboards and Kafka lag.
4. Check repeat-run equality for **latest ETA values/revisions**, not only row counts. Perform the Portnet identical-input replay where the provider payload can be held constant, and capture the CLI `new_observation_rows_inserted` summary without assuming live data cannot change.
5. Validate detailed collision exclusion/visibility and unique-problem counters where applicable; record findings instead of inferring them from zero collisions in a particular live snapshot.
6. Record a final signed-off acceptance outcome for each checklist item in [final_acceptance_checklist.md](final_acceptance_checklist.md). Do not mark the overall checklist `PASSED` until these outstanding checks complete.

**Repository synchronization:** `AGENTS.md` requires matching documented changes in the private DCI course mirror. The DCI repository was not accessible to the connected GitHub account when this evidence PR was prepared. Keep the personal PR unmerged until the same changes can be reviewed and verified against the DCI mirror.
