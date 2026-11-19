# PortPulse Engineering Readiness Assessment

**Date:** 2026-11-19 (Europe/Berlin)  
**Reviewed repository:** `aminnkhann/portpulse`, `main` at `143aaf6e472b864dc53860941589af3df31eba52`  
**Audience:** portfolio reviewers, internship interviewers, instructors, and future contributors  
**Classification:** local, reproducible-by-design educational platform; **not production-ready**  
**Review method:** inspect versioned source, configuration, tests, workflows and documentation; compare against project acceptance goals and relevant official Airflow, dbt, and Digitraffic guidance. Prior local runtime output is identified separately. No new clean-clone execution was performed during this review.

## Executive conclusion

The core platform is **demonstrably functional in an existing local environment**. A single Airflow run successfully fetched real Portnet observations, generated and ingested deterministic synthetic ETA data, built dbt analytics, and exposed dashboards through Metabase. A separate Debezium/Kafka CDC scenario captured the expected create/update/delete events, and its final dbt mart matched the expected surviving ETA. Following fix PR #34, the user's local Compose startup showed `cdc-topic-setup` and `cdc-setup` exit successfully; `bi-setup` reported **75/75** dbt nodes passing and the three dashboards ready.

These observations prove **working component integration on the user's machine**. They do **not yet prove reproducibility after cloning into a completely independent environment**. The next acceptance test must be a new checkout, isolated Compose project, fresh PostgreSQL/Kafka/Metabase/Airflow state, one DAG trigger, and objective assertions.

The highest-priority product gap is **API interchangeability**. Current provider-specific request, parsing, event identity, and source code are coupled to Digitraffic Port Call V1, which the provider says will stop receiving new data on **2027-01-01** and be removed on **2027-07-01**. The V2 API has different data content. Build a small provider adapter boundary and validate it using a second test provider/fixture *without changing the database and dbt contracts*.

## 1. Evidence and status, not assumptions

| Area | Evidence in repo / previous local run | Rating | Qualification |
|---|---|---|---|
| GitHub `main` | At SHA `143aaf6e...`, latest default `tests.yml` run succeeded | **Pass** | CI does not prove clean Compose launch |
| One-trigger Airflow | `dags/portpulse_full_pipeline.py`; 4 tasks, parallel real/synthetic branches, dbt after both | **Pass, demonstrated locally** | Manual trigger; no live upstream guarantee |
| Docker startup | `docker-compose.yml` with DB/Kafka/Kafka Connect/Metabase healthchecks, one-shot setup jobs | **Pass on existing stack** | Fresh checkout acceptance not executed yet |
| Portnet ingestion | `scripts/ingest_portnet.py`, Pydantic, PostgreSQL inserts, rejections, quality issues | **Pass, source-specific** | Live counts change; no generic adapter |
| Synthetic ETA | `scripts/generate_synthetic_eta.py` and `scripts/ingest_synthetic_eta.py`; seed 42 test fixtures | **Pass** | Repeated scenario adds no accepted duplicate rows |
| dbt | 3 staging and 7 mart SQL models; 75/75 local `dbt build` outcomes | **Pass** | No explicit source freshness SLA |
| Kafka CDC | Publication + Debezium + topic setup + Airflow consumer + history and mart | **Pass on existing stack** | Optional demo; no evidence of automated fresh-clone CDC smoke after #34 |
| Metabase | `scripts/setup_metabase.py` initializes 3 dashboards, analytics reader | **Pass on existing stack** | Distinguish accessible UI from fully automated dashboard assertions |
| CI integration | Python/Pydantic/Postgres/dbt tests on push/PR; smoke workflow files present | **Partial** | `bi-smoke.yml` and `cdc-smoke.yml` are `workflow_dispatch` only |
| API portability | Fixed V1 URL, `locode`/`date` query, `portCalls`/`etaTimestamp` fields, hardcoded `source_id` | **Not met** | Merely changing API URL does not satisfy original goal |
| Clean-clone portability | No independent fresh-volume acceptance evidence recorded | **Not verified** | Required before "works for any clone" claim |
| Production operations | Single Kafka node; Airflow SQLite metadata; local-only passwords | **Not targeted** | Do not present as production-ready |
| Portfolio visibility | GitHub repository is currently private | **Partial** | External reviewers need access or a sanitized public showcase |

**Repository state as of audit:** PRs #32 (one-run DAG), #33 (automatic topic/manual CDC), #34 (topic CLI argv fix) are merged. PR #31 (collection lookback/backup) is **open and draft**. Changes from #31 are not part of `main`; no seven-day backfill exists in the *separate* `portpulse_daily_pipeline` DAG. The full DAG already has its own seven-day recheck logic. Do not merge #31 merely to satisfy the one-run workflow.

**Review boundary:** The DCI course repository is not accessible through this GitHub integration, so matching file contents, CI and PR status there could not be independently audited.

## 2. Acceptance criteria for the next clean-clone test

Use a **new checkout, unique Compose project name, and alternate local ports** to avoid touching or competing with a known-good stack. Do not delete or prune existing volumes. Check what a genuinely clean project provides:

| ID | Criterion | Objective evidence | Pass rule |
|---|---|---|---|
| A1 | Fresh setup | `docker compose config`, `docker compose up -d --build`, `docker compose ps -a` | DB/Kafka/Kafka Connect/Metabase healthy; topic/CDC/BI setup exited 0; Airflow running |
| A2 | Airflow DAG discovery | `airflow dags list`, `airflow dags list-import-errors` | Full and CDC DAGs load, no errors |
| A3 | One-trigger execution | Trigger `portpulse_full_pipeline` once | All four tasks succeed; dbt runs only after both loads |
| A4 | Pydantic quarantine | Use existing deterministic invalid-input tests | Invalid events stored as rejected; not accepted |
| A5 | Synthetic reproducibility | SQL counts on **fresh** database | 1,000 latest calls; 3,496 valid updates; 90 rejected; 142 duplicate issues; 0 collisions |
| A6 | Real API ingestion | Per-day CLI logs + database counts | API requests/reporting succeed; valid observations persist (do **not** demand exactly 580/643) |
| A7 | dbt correctness | `dbt build` and data assertions | All applicable tests pass; marts have expected identity uniqueness |
| A8 | Metabase | `bi-setup` logs + UI or API inspection | Three dashboards exist; source separation is explicit |
| A9 | CDC connector/topic | `cdc-topic-setup` `cdc-setup` status and connector REST status | Startup automatic, connector/task RUNNING; no manual topic creation |
| A10 | CDC end-to-end | New scenario; Kafka messages; manual CDC DAG; SQL state | 2 creates, 2 updates, 1 delete; final SIM-001 ETA; deleted SIM-002 absent |
| A11 | Repeatability | Rerun full DAG on same project | Accepted synthetic count unchanged; duplicate handling not destructive |
| A12 | Isolation | Compare old vs new Compose project/volume names | Fresh test does not mutate or remove known-good stack |

**Non-goals:** identical live API row counts, replaying historical records no longer served by the provider, production HA, zero downtime, full security certification.

Suggested **minimal setup commands**:

```bash
git clone https://github.com/aminnkhann/portpulse.git portpulse-acceptance
cd portpulse-acceptance

# Use a unique Compose project; choose unused host ports if the old stack is still up.
# For example, launch with env overrides:
COMPOSE_PROJECT_NAME=portpulse-acceptance \
  POSTGRES_PORT=15432 AIRFLOW_PORT=18080 \
  METABASE_PORT=13000 KAFKA_CONNECT_PORT=18083 \
  docker compose up -d --build

COMPOSE_PROJECT_NAME=portpulse-acceptance docker compose ps -a
COMPOSE_PROJECT_NAME=portpulse-acceptance docker compose logs --tail=40 bi-setup cdc-setup cdc-topic-setup

COMPOSE_PROJECT_NAME=portpulse-acceptance \
  docker compose exec -T airflow airflow dags list-import-errors

COMPOSE_PROJECT_NAME=portpulse-acceptance \
  docker compose exec -T airflow airflow dags trigger portpulse_full_pipeline
```

Use the corresponding project name in **every subsequent command**. A DAG trigger returning `queued` is not a successful pipeline run; verify all task states before checking final output. Default setup credentials remain local demo credentials. Do not publish the test deployment.

## 3. Review of engineering domains

### 3.1 Source ingestion and API adaptability — **highest risk**

**Current implementation:** `scripts/ingest_portnet.py` owns the URL, HTTP headers, date/port filters, the provider's `portCalls` response envelope, Portnet field selection, normalized event ID, Pydantic validation, database writes, and run metrics. The normalized model `MarineEtaEvent` accepts only `source_id="portnet"`. The Portnet payload model retains the provider's field names.

**Why this matters:** A second ETA provider may use different pagination, timestamps, area names, identifiers, authentication, update semantics and completeness rules. The pipeline is currently **not** plug-and-play. Port Call V2 is a real looming test case, not a hypothetical one.

**Recommended minimum implementation (no framework):**

1. Define a stable **canonical ETA input contract** already understood by PostgreSQL and dbt: event identity, source name, call/area identity, provider update timestamp, ETA timestamp, collection timestamp, version. Distinguish *required* from optional attributes and document missing-field behavior.
2. Keep provider-specific **fetch + normalize** functions in one small adapter module per actual source (Port Call V1, Port Call V2 when studied). Let a config/env selector choose an adapter; never try to parse every imaginable provider in one generic class hierarchy.
3. Keep **one** shared Pydantic validation and persistence step after normalization; retain source-qualified event IDs so independent providers do not collide.
4. Test both providers with committed **fixed JSON fixtures**, including empty responses, duplicates, invalid ETA/time zone, missing area, revisions and partial failure.
5. Demonstrate swapping the configured provider on a fresh setup **without changing SQL models or dashboard definitions**.

**Acceptance:** A test double or real second provider that returns semantically equivalent data produces identical **schema/analytics behavior**, not identical row counts.

**Do not change:** the existing real/synthetic separation or the synthetic deterministic event contract merely for architectural symmetry.

### 3.2 Data contracts and quality — **strong foundation with gaps**

Strengths: `MarineEtaEvent`, `PortnetEtaPayload` and `PortCallEvent` Pydantic validation; generated JSON Schema in `contracts/`; separate accepted, rejected and quality-issue tables; canonical hashes and unique keys; UTC timestamps; explicit latest-ETA logic; dbt unique/not-null tests.

Gaps and actions:

- Provider-level parsing uses a mix of hand-coded checks and Pydantic. Document that **not every raw API record is validated by one Pydantic model**; only eligible normalized ETA observations use the Pydantic models.
- CDC messages are explicitly validated in `scripts/streaming_cdc.py` but **do not use Pydantic**; this is fine for a small demonstration if stated correctly.
- Existing `mart_pipeline_quality` reports cumulative counts. Real repeated polling can add `EXACT_DUPLICATE` quality rows without adding accepted observations, making the headline quality metric grow even when nothing bad changes. Label this **repeat-poll attempts** or implement one clean poll ledger later.
- No persisted per-poll attempt ledger (`request date`, `fetched`, `eligible`, `inserted`, `rejected`, `duration`, `status`). Add a small `ingestion_run` table **only after clean clone + API adapter** if a run-level dashboard/operations report is required.
- Add dbt **source freshness** checks and distinguish "API returned zero eligible rows" from "API request failed". A source freshness check is separate from `dbt build`.
- Put representative contract field semantics and source mappings in the data dictionary.

### 3.3 Orchestration — **correct primary graph, scheduling semantics need attention**

**Full DAG:** independent `fetch_real_portnet` and synthetic generation, synthetic validation/ingestion after generation, and `dbt build` only after both ingestion paths succeed. That is understandable and suitable for a portfolio demonstration.

**Potential issues:**

- `fetch_real_portnet` defaults to `datetime.now(timezone.utc).date()`. Retrying the **same DAG run across a UTC date boundary can change the requested date range**. Airflow guidance favors run-bound deterministic/partitioned parameters. Capture a fixed date via DAG-run metadata or validated config.
- The real path makes seven synchronous HTTP requests. `fetch_json` has a 20-second timeout but no explicit exponential retry/backoff for HTTP 429/5xx, and a later request failure can leave earlier date inserts committed. That's not corrupt data, but the run's partial ingestion should be visible and retry-safe.
- Synthetic file handoff uses a run-ID-dependent **local temporary file**. Safe in the observed single-container executor, but not a shared location if executor topology changes.
- The optional daily and synthetic DAGs coexist with the full DAG. Clearly mark the full DAG as the recommended demonstration. Avoid overlapping DBT builds on the same marts when running multiple DAGs simultaneously.
- The Kafka DAG is manual-only (`schedule=None`); an unpaused CDC DAG alone will **not** generate simulator changes or populate its dashboard.

**Recommendation:** Do not introduce Celery/Kubernetes just for an internship portfolio. Add a fixed run-date test, bounded retry behavior, clear error/reporting, and optional guard against concurrent dbt builds.

### 3.4 Kafka and CDC — **working demo; reason about limits**

Strengths: real PostgreSQL WAL change capture (not a mocked Kafka producer); Debezium connector configured for a single source table; automatic idempotent topic creation; connector readiness check; bounded Airflow consumption; PostgreSQL topic/partition/offset uniqueness; delete tombstones ignored; separate simulated data.

**Limits and risks:**

- Single Kafka broker with replication factor 1 is a **development demo**, not high availability.
- CDC operator consumes up to **100 messages per manual DAG run**; backlog may require repeated runs. Show backlog explicitly before calling a dashboard "caught up".
- Consumer commits offsets after successful database batch; PostgreSQL storage can safely deduplicate retries by topic/partition/offset. This gives **at-least-once processing with idempotent storage**, not exactly-once delivery.
- `mart_stream_latest_eta.sql` ranks by partition then offset. Kafka offset order is comparable **within a partition only**. The same key normally stays on one partition, but if the topic partition count or key-mapping changes, a database query using cross-partition sort order can give incorrect latest state. Document the invariant; strengthen ordering using Debezium source LSN or source sequence metadata before changing partitioning.
- Connector RUNNING is a readiness signal, not proof that every emitted message reached the mart. The 5-event scenario is the evidence.

**Target:** Fresh setup + deterministic 5-event smoke + correct mart. Avoid unrelated Kafka infrastructure expansion.

### 3.5 PostgreSQL and dbt — **good educational layer**

Strengths: explicit SQL migrations, normalized source tables, dedicated synthetic and CDC schemas, dbt staging/marts, analytics reader, models tested for identity and nullability.

Improvements:

- Treat **schema migrations** as versioned operations. A fresh volume initializes 001–010, but modifying an existing mounted database requires documented forward migrations; file edits in `migrations/` do not auto-apply to pre-existing volumes.
- **dbt source freshness** and business-level assertions would improve trust: ETA timestamp is valid UTC, source event time/ETA order follows assumptions, no invalid synthetic events in marts, excluded collisions.
- Preserve distinction between **ETA revision** and **actual arrival delay**. Current source data do not justify real arrival-delay claims.
- Add one data-run lineage reference through the pipeline only if needed for traceability; avoid redundant metadata frameworks.

### 3.6 Metabase and stakeholder experience — **functional but presentation could improve**

Strengths: three purpose-separated dashboards; real versus fictional data labels; saved questions and dashboards set up automatically; local credentials; documented read-only analytics role.

Improvements:

- Show **last successful full-pipeline run**, **last source collection date**, and a warning when source data are stale. A dashboard can be reachable but show old data.
- Label quality counts by semantics (duplicate attempts versus distinct rejected records). Add "ETA updates ≠ arrival delays" wording near synthetic charts.
- Improve table widths and technical column labels, ideally demonstrate in a clean browser session with screenshots.
- CDC demo should explain its expected empty state until a scenario and Airflow CDC trigger occur; do not display an "always healthy" statement based only on zero rows.
- Version-control a concise dashboard acceptance check with expected card names and sources.

### 3.7 Testing, CI, and automation — **default CI good, integration CI incomplete**

**Observed default CI:** `.github/workflows/tests.yml` triggers on push and PR. It installs pinned Airflow/other dependencies, runs Ruff, parses DAGs, applies migrations into a test PostgreSQL instance, runs Python tests, and builds/tests dbt. Latest `main` action succeeded.

**Important gaps:**

- `.github/workflows/bi-smoke.yml` and `.github/workflows/cdc-smoke.yml` are **manual dispatch**; they are not run on every PR or default-branch commit. Fresh Compose success is therefore not part of normal merge gating.
- `cdc-smoke.yml` includes an old `docker compose --profile streaming up --build -d` command although the current Compose file has no `streaming` profiles, and its direct host invocation of `scripts/register_cdc_connector.py` expects Compose networking. Review and modernize this workflow **before claiming it validates the current stack**.
- Add a single **fresh-stack acceptance job** that performs the main DAG and CDC end-to-end with bounded timeouts, and checks counts and dashboard readiness. This can initially be a manual release gate rather than every-commit CI to control resources.
- Add a test of `docker compose config` / actual one-shot topic creation so CLI-argument errors cannot escape as in #33.
- Pin dependencies deliberately; current Python requirement ranges for some libraries mean installs can change over time. The intended level of reproducibility is deterministic pipeline logic, not bit-identical package resolutions unless lock/constraint files are used.

### 3.8 Security, operations and portfolio — **demo acceptable, label accurately**

- The repository is **private**. For internships, create a sanitized public project or give reviewers controlled repository access.
- `.env.example` contains well-known demo credentials; they are acceptable for loopback-only local demos, not hosted deployments. Make safe deployment restrictions prominent.
- Docker port bindings target loopback and Kafka broker port is internal; keep them that way.
- Airflow metadata defaults to a local SQLite data file. Official Airflow docs regard it as development/test, not production. Do not describe the deployment as scalable Airflow production.
- A single-node broker and no external alerting/telemetry are clear scope limitations, not a reason to add Kubernetes now.
- Consider adding a public-safe `LICENSE`, concise About/description, architecture screenshot, demo GIF/video, badges, and a "what I built and learned" section **after** the clean-clone test. Confirm rights for all screenshots/data before making the repository public.
- No secrets or real API payload exports should be committed.

## 4. Prioritized improvement backlog

| Priority | Work item | Expected artifact | Acceptance / done criterion | Estimated complexity |
|---|---|---|---|---|
| **P0** | Run isolated clean-clone acceptance | Recorded commands, CI logs, metrics, dashboard screenshots | A1–A12 all pass; no manual Kafka/migration patches | Medium |
| **P0** | Stabilize user-visible README and architecture | README + accurate diagrams + data semantics | Clone/run/verify steps match actual code; no false readiness claims | Small |
| **P0** | Review and update obsolete CDC smoke CI workflow | Tested GitHub workflow | Dispatch succeeds from current `main`; no deprecated compose profile | Medium |
| **P1** | Implement explicit API provider adapter (V1 + second fixture/V2) | Adapter module, config, Pydantic model/tests | API swap needs config only; downstream DB/dbt untouched | Medium |
| **P1** | Prepare for Digitraffic V1 sunset | V2 adapter migration plan | Date-driven transition before 2027-01-01 new-data cutoff | Medium |
| **P1** | Deterministic Airflow run-date and HTTP retry design | Small DAG/fetch changes + unit tests | Same run fetches same dates on retry; bounded 429/5xx handling | Small/Medium |
| **P1** | Add one clean-stack test as release gate | CI integration job or reproducible test script | Creates own isolated project and passes full smoke | Medium |
| **P2** | Add ingestion-run ledger and source freshness | Run table + dbt freshness config | Distinguish empty/upstream failure/stale data with no duplicate noise | Medium |
| **P2** | Clarify CDC ordering/offset assumptions | Explicit documented key/partition invariant and tests | Correct latest mart across replay / partition changes | Medium |
| **P2** | Polish Metabase and portfolio presentation | Updated dashboards/screenshots, README evidence | Reviewer can identify real/simulated data and trace ETA lineage | Small |
| **Later** | Production architecture | External Airflow DB, secrets manager, HA Kafka, observability stack | Only if deployment beyond local portfolio becomes a goal | Large — out of scope |

**Execution order:** Clean clone first → fix the **first actual failure** → fix CDC CI gate → API adapter + V2 preparation → freshness/observability → presentation. Avoid adding infrastructure or dashboards before proving the basic demo can be reproduced.

## 5. Definition of done for the user's original project goal

PortPulse can be called **complete as a portfolio-grade local demonstration** when:

- A reviewer can clone and start the repository using the documented commands without editing Python files or manually creating Kafka topics.
- Airflow lists the full DAG with zero import errors; a single manual trigger results in four green tasks and built dbt marts.
- A fresh seeded demo deterministically yields 1,000 synthetic latest calls, 3,496 valid ETA observations, 90 rejected synthetic events and 142 duplicate attempts.
- The real API pipeline correctly parses, validates and stores any eligible current data; empty API results are reported honestly, not treated as a promise of a fixed count.
- Metabase creates all three distinct dashboards and accurately labels real versus simulated data.
- A new CDC scenario produces Kafka create/update/delete events, an Airflow CDC run succeeds, and the latest-state dbt mart excludes the deleted call.
- Both source and downstream data contracts are tested; swapping to a second ETA provider requires only a provider adapter/configuration and no mart rewrites.
- The README, architecture, dictionary and demo procedure remain current; CI explicitly covers the critical startup path.
- All claims are framed as a **local development/learning project** with documented limitations.

## 6. Follow-through and evidence hygiene

Keep full source-of-truth code in the two mirrored repositories. Implementation PRs should have matching changes and successful checks before merging both per `AGENTS.md`. A **documentation-only** report/README update can stay in the personal repository, but the course README should be kept consistent when later implementing adapter or runtime changes.

A successful GitHub workflow verifies the code covered by that workflow only. A green `docker compose ps` verifies service startup only. A green Airflow run verifies the orchestrated tasks in that environment only. Separate these three claims in interviews and documentation.

## 7. Authoritative external references

- [Digitraffic: Port Call V1 APIs being deprecated](https://www.digitraffic.fi/en/news/2026/11/13/port-call-v1-en.html) — **2027-01-01** new-data cutoff; **2027-07-01** API removal; V2 content differs.
- [Apache Airflow 3.4: Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) — idempotent retries, run-partitioned inputs, task file handling and DAG simplicity.
- [Apache Airflow 3.4: Production Deployment](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html) — metadata database and operational deployment limitations.
- [dbt: Source Freshness](https://docs.getdbt.com/docs/deploy/source-freshness) — source recency monitoring is distinct from `dbt build`.
