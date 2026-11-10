# PortPulse Architecture

## Supported workflows

### Phase 1: deterministic synthetic reconciliation

~~~mermaid
flowchart LR
    F[JSONL fixture] --> V[Pydantic validation]
    V -->|valid| R[(raw_event)]
    V -->|invalid| Q[(rejected_event)]
    R --> D[Deduplication and quality checks]
    D --> T[Trusted ETA reconciliation]
    T --> S[(port_call_state)]
    R --> H[(quality_issue)]
    S --> C[Query CLI]
~~~

The fixture, rules, and golden result are checked in. Replaying the same fixture produces the same trusted result.

### Phase 2: single-source Portnet V1 ETL

~~~mermaid
flowchart LR
    P[Digitraffic Portnet V1 API] --> I[scripts.ingest_portnet]
    I --> V[Pydantic validation]
    V -->|accepted| O[(portnet_eta_observation)]
    V -->|rejected| Q[(portnet_eta_rejected_event)]
    O --> D[Duplicate and collision accounting]
    D --> H[(portnet_eta_quality_issue)]
    D --> M[Pipeline-health report]
~~~

The Portnet adapter is independent of the Phase 1 reconciliation tables and does not call the historical V2/AIS adapters. It writes only to its dedicated observation, rejection, and quality-issue tables.

## Event identity and clocks

For Portnet, identity is scoped by provider port-call ID and port-area code; etaTimestamp distinguishes ETA revisions. The event time is the provider's update timestamp. received_at_utc records API collection time, and PostgreSQL assigns ingested_at_utc. This lets the pipeline distinguish source change, collection, and storage times.

Phase 1 uses its own synthetic event contract and fixed evaluation time. Its thresholds and source priorities are demonstrator assumptions.

## PostgreSQL and migrations

Migrations 001–007 are append-only project history. Migration 007 creates Portnet tables without changing Phase 1 tables. Migrations 004 and 006 seed source rows from older experiments; they remain so existing schemas and the Phase 1 source-parity check continue to work. The generic raw_event tables and their rows are not dropped. The reconciler retains visit-status handling for existing legacy payloads, but no old adapter creates new rows. The old adapters are no longer part of the supported repository workflow.

## Future dbt layer

After cleanup, a dbt project can be added at repository root under dbt/. It can source the Portnet observation, rejection, and quality-issue tables and build staging, latest-ETA, and health models. No dbt code or dependency is present yet.
