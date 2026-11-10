# PortPulse Data Dictionary

## Phase 1 synthetic contract

contracts/port_call_event_v1.schema.json is generated from the Pydantic PortCallEvent model. The checked-in fixture contains synthetic event occurrences for PC-DEMO-001.

| Field | Meaning |
|---|---|
| event_id | Source event identity used for duplicate and collision detection. |
| port_call_id | Synthetic call identity; currently PC-DEMO-001. |
| source_id | One of the three synthetic sources in the v1 contract. |
| event_time_utc | Source update time; used for within-source ordering. |
| received_at_utc | Fixed simulated receipt time; used for freshness and delay rules. |
| ingested_at_utc | PostgreSQL storage time, separate from source and simulated times. |
| eta_utc | Predicted arrival time. |
| schema_version | Contract version, currently v1. |

rules/eta_rules_v1.yaml defines deterministic selection and quality thresholds. They are educational project assumptions, not operating policy.

## Phase 2 Portnet V1 observations

The official adapter reads Portnet V1 portCalls and normalizes ETA details.

| PortPulse value | Portnet value | Meaning |
|---|---|---|
| port_call_id | portCallId | Namespaced provider call identity. |
| port_area_code | portAreaCode | Call area; part of event identity when supplied. |
| event_time_utc | etaTimestamp | Provider ETA update time. |
| eta_utc | eta | Predicted arrival time. |
| received_at_utc | collection time | When this API poll collected the observation. |
| ingested_at_utc | PostgreSQL default | When the accepted observation was stored. |
| eta_source | etaSource | Provider-reported ETA source retained in the selected audit payload. |

The stable event ID combines the port-call ID, area code (or unspecified where unambiguous), and normalized etaTimestamp. A missing area is quarantined when multiple active ETA details make identity ambiguous. A distinct provider timestamp is a legitimate revision.

## Portnet PostgreSQL tables

| Table | Purpose |
|---|---|
| portnet_eta_observation | Accepted observations, normalized times, canonical payload hash, and selected audit fields. Unique (event_id, canonical_payload_sha256) allows changed variants to be retained and diagnosed. |
| portnet_eta_rejected_event | Rejected input with reason code, selected safe payload, details, and rejection time. |
| portnet_eta_quality_issue | EXACT_DUPLICATE and ID_COLLISION records with observation identity and details. |

These tables are independent of the Phase 1 raw_event and trusted-state tables. They are the stable source tables proposed for future dbt staging and quality summaries.

## dbt starting point (planned, not implemented)

Keep a future dbt project in a root-level dbt/ directory. Useful source columns are:

- Observations: event_id, port_call_id, port_area_code, event_time_utc, received_at_utc, ingested_at_utc, eta_utc, canonical_payload_sha256, and selected fields in raw_payload.
- Quarantine: reason_code, received_at_utc, rejected_at_utc, and details from portnet_eta_rejected_event.
- Quality issues: issue_code, event_id, port_call_id, received_at_utc, and details from portnet_eta_quality_issue.

A latest-ETA model should partition by port_call_id and port_area_code, exclude every event_id with an ID_COLLISION issue, and rank eligible observations by event_time_utc descending, then ingested_at_utc and event_id as deterministic tie-breakers. Quality summaries can aggregate rejected rows by reason_code and quality issues by issue_code.

The CLI reports per-poll totals such as calls received, completed calls skipped, and rows inserted, but those run-level counters are not persisted in a run ledger. Database models can summarize stored observations, rejections, and issues; full per-run trend reporting would need a persisted run table in a future approved scope.

## Migration history and legacy sources

Migrations 001–007 are retained in order. Migrations 004 and 006 seed digitraffic and ais source rows for older experiments; they remain for compatibility with existing databases. Those source IDs and old adapters are not part of the supported ingestion route. Historical API comparisons remain in the dated reports.
