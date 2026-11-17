# PortPulse data dictionary

## Synthetic ETA reconciliation contract

`contracts/port_call_event_v1.schema.json` is generated from the Pydantic `PortCallEvent` model. The checked-in JSONL fixture contains synthetic events for `PC-DEMO-001`.

| Field | Meaning |
|---|---|
| `event_id` | Source event identity used for duplicate and collision detection. |
| `port_call_id` | Synthetic call identity. |
| `source_id` | One of the three synthetic sources in the contract. |
| `event_time_utc` | Source update time used for within-source ordering. |
| `received_at_utc` | Fixed simulated receipt time used by freshness and delay rules. |
| `ingested_at_utc` | PostgreSQL storage time, separate from source and simulated times. |
| `eta_utc` | Predicted arrival time. |
| `schema_version` | Contract version, currently `v1`. |

`rules/eta_rules_v2.yaml` defines deterministic selection and quality thresholds. These are project assumptions, not maritime operating policy.

## Portnet V1 observations

The Portnet adapter reads Portnet V1 port-call records and normalizes ETA details from the provider’s REST API.

| PortPulse field | Portnet field or source | Meaning |
|---|---|---|
| `port_call_id` | `portCallId` | Namespaced provider call identity. |
| `port_area_code` | `portAreaCode` | Call area; part of observation identity when supplied. |
| `event_time_utc` | `etaTimestamp` | Provider ETA update time. |
| `eta_utc` | `eta` | Predicted arrival time. |
| `received_at_utc` | API collection time | When the poll collected the observation. |
| `ingested_at_utc` | PostgreSQL default | When the observation was stored. |
| `eta_source` | `etaSource` | Provider-reported ETA source retained in the selected audit payload. |

Event identity combines port-call ID, area code (or an unambiguous unspecified area), and normalized provider update time. Ambiguous missing areas are rejected. Distinct provider timestamps represent revisions. All stored timestamps are UTC; collection and ingestion times are distinct from provider event time.

## PostgreSQL tables

| Table | Purpose |
|---|---|
| `portnet_eta_observation` | Accepted observations, normalized timestamps, canonical payload hash, and selected audit fields. |
| `portnet_eta_rejected_event` | Rejected input with reason code, selected safe payload, details, and rejection time. |
| `portnet_eta_quality_issue` | Duplicate and ID-collision issues with observation identity and details. |

These tables are separate from the synthetic reconciliation `raw_event` and trusted-state tables.

## dbt outputs

| Relation | Contents |
|---|---|
| `analytics.stg_portnet_eta` | Validated projection of accepted observations. |
| `analytics.mart_portnet_latest_eta` | One eligible latest ETA per call and area, excluding collided event IDs. |
| `analytics.mart_pipeline_quality` | Counts of accepted observations, rejected records, quality issues, and latest rows. |

The models use observation identity, provider event time, collection and ingestion times, ETA, canonical payload hash, and selected fields in `raw_payload`. Rejections contribute their reason, collection/rejection times, and details; quality issues contribute issue code, event identity, collection time, and details. Latest-row ordering uses provider update time followed by collection time, ingestion time, and observation ID.

Per-poll counters reported by the CLI are not persisted as a run ledger. The marts summarize stored records rather than providing per-run trends.

## Simulated streaming CDC

| Relation | Purpose |
|---|---|
| `stream_demo.port_eta_state` | Current simulated call state captured by the dedicated Debezium publication. Its primary key is scenario, call, and area. |
| `stream_demo.port_eta_cdc_event` | Idempotent history of Debezium events keyed by Kafka topic, partition, and offset. `before_values` and `after_values` hold the row images when available. |
| `analytics.stg_stream_eta_changes` | Typed projection of CDC history, including operation, source update time, Kafka identity, and processing time. |
| `analytics.mart_stream_latest_eta` | Latest active simulated row per scenario, call, and area; rows whose latest operation is delete are omitted. |

CDC operations use Debezium codes: `c` create, `u` update, `d` delete, and `r` snapshot/read. The simulator's `updated_at_utc` is the fixed simulated source time; `processed_at_utc` is when Airflow stores the event. Kafka topic/partition/offset identify the delivered message, not the simulated time. All synthetic rows use fictional `SIM-` call IDs and remain separate from Portnet observations.
