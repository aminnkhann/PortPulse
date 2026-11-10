# PortPulse Product Requirements

**Status:** Implemented Phase 1 and Phase 2 scope
**Final project deadline:** Monday, November 16, 2026

## Purpose

PortPulse demonstrates two related data-engineering workflows:

1. Rebuild a trusted ETA from a controlled synthetic event history.
2. Ingest real ETA observations from one provider, Digitraffic Portnet V1, while preserving validation and quality outcomes.

PortPulse is an educational project. Synthetic thresholds are project assumptions, not port-industry policy. Live API observations are snapshots and do not establish provider reliability.

## Phase 1 — synthetic reconciliation

The pipeline reads fixtures/port_call_v1.jsonl, validates records with PortCallEvent, and writes accepted events, rejected records, and quality issues to PostgreSQL. It selects a trusted state using the versioned rules in rules/eta_rules_v1.yaml.

Required behavior:

- Reject malformed records with a reason.
- Distinguish exact duplicates from ID collisions; exclude collided IDs from trusted state.
- Preserve source update, simulated receipt, and database ingestion times.
- Select the latest eligible event per source, apply freshness and priority rules, and report conflicts, delayed delivery, stale sources, and out-of-order events.
- Produce the fixed golden result and the same result on replay.

Acceptance is covered by the Phase 1 tests in tests/test_step01_03.py and tests/test_step04_reconciliation.py.

## Phase 2 — Portnet V1 ETL

The only supported real-data route is:

**Digitraffic Portnet V1 → Python → Pydantic validation → accept or quarantine → PostgreSQL → deduplication and health reporting.**

The adapter is scripts/ingest_portnet.py and uses /api/port-call/v1/port-calls. It does not call AIS or Digitraffic V2 and does not depend on multi-source matching.

Required behavior:

- Validate call, area, ETA, and provider update timestamp data; quarantine invalid or ambiguous observations with reason codes.
- Identify an observation by port call, area, and etaTimestamp; a later provider timestamp is a legitimate revision.
- Store source update time, collection time, and database ingestion time separately.
- Persist accepted observations, quarantine rows, exact-repeat issues, and ID-collision issues in dedicated Portnet tables.
- Make repeat ingestion idempotent and report accepted, rejected, duplicate, collision, and inserted-row counts.

Focused deterministic failure injections are test inputs, not observed Portnet defects. Real API run reports are kept separately from those injected tests.

## Out of scope

AIS comparison, Digitraffic V2 ingestion, MQTT, Kafka, FastAPI, dashboards, cloud infrastructure, scheduling, and dbt are not part of the current implementation. dbt is a planned next step after repository cleanup and is not included in this project change.

## Data and compatibility

Phase 1 fixtures are synthetic. Portnet reports contain aggregate observations and no full raw API responses. Migrations 001–007 are retained in order. Migrations 004 and 006 include source rows from earlier experiments; these are retained for compatibility with applied databases and are not supported ingestion routes. Migration 007 creates dedicated Portnet tables without altering Phase 1 tables.
