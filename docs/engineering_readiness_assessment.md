# PortPulse Engineering Readiness Assessment

**Date:** 2026-11-19 (Europe/Berlin)
**Baseline reviewed:** `aminnkhann/portpulse`, `main` at `143aaf6e472b864dc53860941589af3df31eba52`
**Audience:** portfolio reviewers, internship interviewers, instructors, and future contributors
**Classification:** local, reproducible-by-design educational platform; **not production-ready**
**Review method:** inspect versioned source, configuration, tests, workflows and documentation; compare against project acceptance goals and relevant official Airflow, dbt, and Digitraffic guidance. Prior local runtime output is identified separately. No new clean-clone execution was performed during this review.

**Update:** This began as a baseline assessment of `143aaf6`. PR [#36](https://github.com/aminnkhann/portpulse/pull/36) addresses the verified ETA ordering, repeated quality counting, HTTP retries, run-date stability, handoff, freshness display, CDC ordering guard, smoke workflow, and README findings below. The PR's local checks and remaining unrun checks are reported in the PR; baseline descriptions are historical where superseded.

## Executive conclusion

The core platform is **demonstrably functional in an existing local environment**. A single Airflow run successfully fetched real Portnet observations, generated and ingested deterministic synthetic ETA data, built dbt analytics, and exposed dashboards through Metabase. A separate Debezium/Kafka CDC scenario captured the expected create/update/delete events, and its final dbt mart matched the expected surviving ETA. Following fix PR #34, the user's local Compose startup showed `cdc-topic-setup` and `cdc-setup` exit successfully; `bi-setup` reported **75/75** dbt nodes passing and the three dashboards ready.

These observations prove **working component integration on the user's machine**. They do **not yet prove reproducibility after cloning into a completely independent environment**. The next acceptance test must be a new checkout, isolated Compose project, fresh PostgreSQL/Kafka/Metabase/Airflow state, one DAG trigger, and objective assertions.

The highest-priority product gap is **API interchangeability**. Current provider-specific request, parsing, event identity, and source code are coupled to Digitraffic Port Call V1, which the provider says will stop receiving new data on **2027-01-01** and be removed on **2027-07-01**. The V2 API has different data content. Build a small provider adapter boundary and validate it using a second test provider/fixture *without changing the database and dbt contracts*.

## 1. Evidence and status, not assumptions

| Area | Evidence in repo / previous local run | Rating | Qualification |
|---|---|---|---|
| GitHub `main` | Baseline SHA `143aaf6e...` had a successful default `tests.yml` run | **Historical evidence** | PR #36 checks are pending; CI does not prove clean Compose launch |
| One-trigger Airflow | `dags/portpulse_full_pipeline.py`; 4 tasks, parallel real/synthetic branches, dbt after both | **Pass, demonstrated locally** | Manual trigger; no live upstream guarantee |
| Docker startup | `docker-compose.yml` with DB/Kafka/Kafka Connect/Metabase healthchecks, one-shot setup jobs | **Pass on existing stack** | Fresh checkout acceptance not executed yet |
| Portnet ingestion | `scripts/ingest_portnet.py`, Pydantic, PostgreSQL inserts, rejections, quality issues | **Pass, source-specific** | Live counts change; no generic adapter |
| Synthetic ETA | `scripts/generate_synthetic_eta.py` and `scripts/ingest_synthetic_eta.py`; seed 42 test fixtures | **Pass** | Repeated scenario adds no accepted duplicate rows |
| dbt | Baseline had 3 staging and 7 mart SQL models; PR #36 adds 2 marts and assertions | **Changed** | Local `dbt parse` passed; dbt build and a formal freshness SLA remain unverified |
| Kafka CDC | Publication + Debezium + topic setup + Airflow consumer + history and mart | **Pass on existing stack** | Optional demo; no evidence of automated fresh-clone CDC smoke after #34 |
| Metabase | `scripts/setup_metabase.py` initializes 3 dashboards, analytics reader | **Pass on existing stack** | Distinguish accessible UI from fully automated dashboard assertions |
| CI integration | Default CI covers Python/PostgreSQL/dbt; PR #36 removes the obsolete CDC Compose profile | **In progress** | PR checks are pending; the CDC smoke workflow remains manual `workflow_dispatch` |
| API portability | Fixed V1 URL, `locode`/`date` query, `portCalls`/`etaTimestamp` fields, hardcoded `source_id` | **Not met** | Merely changing API URL does not satisfy original goal |
| Clean-clone portability | No independent fresh-volume acceptance evidence recorded | **Not verified** | Required before "works for any clone" claim |
| Production operations | Single Kafka node; Airflow SQLite metadata; local-only passwords | **Not targeted** | Do not present as production-ready |
| Portfolio visibility | GitHub repository is currently private | **Partial** | External reviewers need access or a sanitized public showcase |

**Repository state at baseline:** PRs #32–#34 were merged. The seven-day date recheck belongs to the full DAG; each daily ingestion can commit before a later day fails. PR #36 adds a run-date anchor and explicit partial-commit guidance.

**Review boundary:** This task intentionally changes only the private `aminnkhann/portpulse` repository; the DCI course repository is out of scope.

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
- PR #36 changes the real quality mart to count unique semantic rejection payloads, duplicate event/payload identities, and collision event IDs. Repeat polling still leaves legacy issue rows available for audit; no schema migration is needed.
- There is still no persisted per-poll attempt ledger (`request date`, `fetched`, `eligible`, `inserted`, `rejected`, `duration`, `status`). The Portnet dashboard shows the latest recorded accepted/rejected payload collection and its age separately from dbt refresh. Empty successful polls are not recorded, and this is not a formal source freshness SLA.
- Put representative contract field semantics and source mappings in the data dictionary.

### 3.3 Orchestration — **correct primary graph, scheduling semantics need attention**

**Full DAG:** independent `fetch_real_portnet` and synthetic generation, synthetic validation/ingestion after generation, and `dbt build` only after both ingestion paths succeed. That is understandable and suitable for a portfolio demonstration.

**Verified behavior and remaining limits:**

- The full DAG now defaults the window to the DAG run's UTC logical date, with a validated `run_date` override.
- Portnet requests use bounded retries for 429/5xx and network failures. Daily requests remain sequential; if a later day fails, earlier daily transactions may already have committed. Replays are idempotent.
- Synthetic handoff now uses a run-ID-specific path under the shared `AIRFLOW_HOME` volume and atomically replaces the completed JSONL file. The documented single Compose Airflow service shares that volume.
- The optional daily and synthetic DAGs coexist with the full DAG. Clearly mark the full DAG as the recommended demonstration. Avoid overlapping DBT builds on the same marts when running multiple DAGs simultaneously.
- The Kafka DAG is manual-only (`schedule=None`); an unpaused CDC DAG alone will **not** generate simulator changes or populate its dashboard.

**Recommendation:** Do not introduce Celery/Kubernetes just for an internship portfolio. The run-date, retry, and handoff fixes are covered by focused tests; do not claim concurrent task execution was observed.

### 3.4 Kafka and CDC — **working demo; reason about limits**

Strengths: real PostgreSQL WAL change capture (not a mocked Kafka producer); Debezium connector configured for a single source table; automatic idempotent topic creation; connector readiness check; bounded Airflow consumption; PostgreSQL topic/partition/offset uniqueness; delete tombstones ignored; separate simulated data.

**Limits and risks:**

- Single Kafka broker with replication factor 1 is a **development demo**, not high availability.
- CDC operator consumes up to **100 messages per manual DAG run**; backlog may require repeated runs. Show backlog explicitly before calling a dashboard "caught up".
- Consumer commits offsets after successful database batch; PostgreSQL storage can safely deduplicate retries by topic/partition/offset. This gives **at-least-once processing with idempotent storage**, not exactly-once delivery.
- `mart_stream_latest_eta.sql` ranks first by the existing Debezium row's source update timestamp; partition and offset only break ties. Kafka offsets are comparable **within a partition only**. A dbt data test fails if one source key appears in multiple partitions. Changing topic partition count or key mapping still requires review; the demo does not persist Debezium source LSN.
- Connector RUNNING is a readiness signal, not proof that every emitted message reached the mart. The 5-event scenario is the evidence.

**Target:** Fresh setup + deterministic 5-event smoke + correct mart. Avoid unrelated Kafka infrastructure expansion.

### 3.5 PostgreSQL and dbt — **good educational layer**

Strengths: explicit SQL migrations, normalized source tables, dedicated synthetic and CDC schemas, dbt staging/marts, analytics reader, models tested for identity and nullability.

Improvements:

- Treat **schema migrations** as versioned operations. A fresh volume initializes 001–010, but modifying an existing mounted database requires documented forward migrations; file edits in `migrations/` do not auto-apply to pre-existing volumes.
- PR #36 adds dbt assertions for late-arriving Portnet ETAs, collision visibility/exclusion, unique semantic quality counts, latest recorded collection, and CDC key partitioning. The latest collection age is not a provider SLA and does not observe empty polls.
- Preserve distinction between **ETA revision** and **actual arrival delay**. Current source data do not justify real arrival-delay claims.
- Add one data-run lineage reference through the pipeline only if needed for traceability; avoid redundant metadata frameworks.

### 3.6 Metabase and stakeholder experience — **functional but presentation could improve**

Strengths: three purpose-separated dashboards; real versus fictional data labels; saved questions and dashboards set up automatically; local credentials; documented read-only analytics role.

Improvements:

- The Portnet dashboard now separates latest recorded collection age from dbt mart refresh and labels unique rejected records, duplicate-delivery problems, and identity collision groups. It does not show last successful full-DAG run or infer freshness from empty polls.
- ETA revision charts still describe estimate changes, not actual arrival delays; no actual arrival timestamp is persisted for prediction-accuracy analysis.
- Improve table widths and technical column labels, ideally demonstrate in a clean browser session with screenshots.
- CDC demo should explain its expected empty state until a scenario and Airflow CDC trigger occur; do not display an "always healthy" statement based only on zero rows.
- Version-control a concise dashboard acceptance check with expected card names and sources.

### 3.7 Testing, CI, and automation — **default CI good, integration CI incomplete**

**Observed default CI:** `.github/workflows/tests.yml` triggers on push and PR. It installs pinned Airflow/other dependencies, runs Ruff, parses DAGs, applies migrations into a test PostgreSQL instance, runs Python tests, and builds/tests dbt. Latest `main` action succeeded.

**Important gaps:**

- `.github/workflows/bi-smoke.yml` and `.github/workflows/cdc-smoke.yml` are **manual dispatch**; they are not run on every PR or default-branch commit. Fresh Compose success is therefore not part of normal merge gating.
- PR #36 removes the obsolete `--profile streaming` argument from `cdc-smoke.yml`. The workflow is still manual and has not been executed for this PR; do not claim it passed.
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

| Priority | Work item | Expected artifact | Acceptance / done criterion | Current status |
|---|---|---|---|---|
| **P0** | Run isolated clean-clone acceptance | Recorded commands, service/task states, SQL assertions | A1–A12 pass without deleting existing data | **NOT RUN; checklist added** |
| **P0** | Update README, architecture, and acceptance steps | Concise README, current diagrams, acceptance checklist | Steps match current code and no false readiness claims | **Addressed in PR #36** |
| **P0** | Correct CDC smoke workflow | Current Compose invocation | Workflow dispatch succeeds against the current stack | **Profile fixed; workflow not run** |
| **P1** | Implement explicit API provider adapter (V1 + second fixture/V2) | Adapter module, config, Pydantic model/tests | API swap needs config only; downstream DB/dbt untouched | **Future work; no unverified V2 adapter added** |
| **P1** | Prepare for Digitraffic V1 sunset | Verified V2 transition plan | Date-driven transition before the provider cutoff | **Future work** |
| **P1** | Deterministic Airflow run-date and HTTP retries | DAG/fetch changes + regression tests | Same run uses same UTC dates; bounded transient retries | **Addressed; local unit checks pass** |
| **P1** | Full-stack acceptance gate | Manual reproducible procedure | Runs on an isolated project and records actual outcomes | **Procedure added; execution reserved for owner** |
| **P2** | Persist poll-attempt ledger/formal freshness SLA | Run table or source SLA | Observe successful empty polls and provider availability | **Not implemented; current collection age is record-based** |
| **P2** | Clarify CDC ordering and partition assumptions | Source-time ordering, guard test, operations guide | Same key remains in one partition; lag can be checked | **Addressed in PR #36; DB/CDC CI checks pending** |
| **P2** | Evidence-based presentation finding | Read-only query and verified result | Report only after reproducing on the owner's database | **Query added; result awaiting verification** |
| **Later** | Production architecture | External Airflow DB, secrets manager, HA Kafka, observability stack | Only if deployment beyond local portfolio becomes a goal | **Out of scope** |

**Execution order:** Review PR #36 → run the owner-controlled isolated acceptance checklist → fix any observed failure → plan V2 adapter work before the provider cutoff. Do not treat the manual CDC workflow as passed until it is actually dispatched and completed.

## 5. Definition of done for the user's original project goal

PortPulse can be called **complete as a portfolio-grade local demonstration** when:

- A reviewer can clone and start the repository using the documented commands without editing Python files or manually creating Kafka topics.
- Airflow lists the full DAG with zero import errors; a single manual trigger results in four green tasks and built dbt marts.
- A fresh seeded demo deterministically yields 1,000 synthetic latest calls, 3,496 valid ETA observations, 90 rejected synthetic events and 142 duplicate attempts.
- The real API pipeline correctly parses, validates and stores any eligible current data; empty API results are reported honestly, not treated as a promise of a fixed count.
- Metabase creates all three distinct dashboards and accurately labels real versus simulated data.
- A new CDC scenario produces Kafka create/update/delete events, an Airflow CDC run succeeds, and the latest-state dbt mart excludes the deleted call.
- The current V1 adapter, event contracts, late-arrival/collision rules, and separate data paths are documented. Provider interchangeability and a V2 adapter remain future work and are not claimed.
- ETA prediction accuracy is not established because actual arrivals are not persisted for analysis.
- The README, architecture, dictionary, and demo procedure match the implementation; the owner's manual isolated acceptance verifies fresh-stack startup because default CI does not run the full Compose acceptance path.
- All claims are framed as a **local development/learning project** with documented limitations.

## 6. Follow-through and evidence hygiene

PR #36 intentionally targets only the private personal repository, as requested; it does not modify the DCI course mirror.

A successful GitHub workflow verifies the code covered by that workflow only. A green `docker compose ps` verifies service startup only. A green Airflow run verifies the orchestrated tasks in that environment only. Separate these three claims in interviews and documentation.

## 7. Authoritative external references

- [Digitraffic: Port Call V1 APIs being deprecated](https://www.digitraffic.fi/en/news/2026/11/13/port-call-v1-en.html) — **2027-01-01** new-data cutoff; **2027-07-01** API removal; V2 content differs.
- [Apache Airflow 3.4: Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) — idempotent retries, run-partitioned inputs, task file handling and DAG simplicity.
- [Apache Airflow 3.4: Production Deployment](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html) — metadata database and operational deployment limitations.
- [dbt: Source Freshness](https://docs.getdbt.com/docs/deploy/source-freshness) — source recency monitoring is distinct from `dbt build`.
