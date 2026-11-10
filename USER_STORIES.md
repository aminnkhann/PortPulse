# User Stories and Acceptance

## Phase 1 — synthetic reconciliation

### US-01: Explain the trusted ETA

As an operations analyst, I want the current synthetic ETA, its winning event, and its source so I can explain the result.

**Acceptance:** scripts.query_state returns the golden ETA and lineage for PC-DEMO-001; the expected result is in fixtures/expected_output.json.

**Status:** Implemented and covered by Phase 1 reconciliation tests.

### US-02: Keep invalid and duplicate input from corrupting state

As a data engineer, I want malformed, duplicated, and colliding fixture events classified so I can trust the reconciled output.

**Acceptance:** Invalid input is quarantined with a reason; exact duplicates are counted; every variant of a colliding ID is excluded from trusted state.

**Status:** Implemented and covered by fixture and database tests.

### US-03: Make time and source rules visible

As a reviewer, I want late, stale, and conflicting synthetic events reported using explicit rules so I can reproduce the state.

**Acceptance:** Fixed as_of_utc, source priorities, freshness, conflict, delay, and ordering behavior match the golden result.

**Status:** Implemented and covered by deterministic tests.

## Phase 2 — Portnet V1 ingestion

### US-04: Ingest and validate Portnet ETA observations

As a data engineer, I want Portnet V1 records validated and stored so the project demonstrates a real-source ETL path.

**Acceptance:** Valid observations enter portnet_eta_observation; invalid or ambiguous records enter quarantine with a reason; the CLI reports pipeline-health counts.

**Status:** Implemented in scripts/ingest_portnet.py.

### US-05: Preserve revisions and make replay safe

As a reviewer, I want provider updates for each call and area preserved as history while exact repeats and ID collisions are visible.

**Acceptance:** A distinct etaTimestamp creates a new revision; an unchanged replay does not add another observation; changed content reusing an ID is recorded as a collision.

**Status:** Implemented and covered by deterministic injected and PostgreSQL tests.

## Evidence

The fixtures and injected reliability cases are synthetic. Live Portnet observations are documented in separate dated reports. The November 9 Portnet/AIS comparison is historical evidence only.
