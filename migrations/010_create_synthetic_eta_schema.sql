CREATE SCHEMA IF NOT EXISTS synthetic_demo;

CREATE TABLE IF NOT EXISTS synthetic_demo.eta_observation (
    synthetic_observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT NOT NULL,
    port_call_id TEXT NOT NULL,
    source_id TEXT NOT NULL CHECK (
        source_id IN ('coordinator_sim', 'carrier_sim', 'terminal_sim')
    ),
    event_time_utc TIMESTAMPTZ NOT NULL,
    received_at_utc TIMESTAMPTZ NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    eta_utc TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL CHECK (schema_version = 'v1'),
    canonical_payload_sha256 CHAR(64) NOT NULL
        CHECK (canonical_payload_sha256 ~ '^[0-9a-f]{64}$'),
    raw_payload JSONB NOT NULL,
    UNIQUE (event_id, canonical_payload_sha256)
);

CREATE INDEX IF NOT EXISTS synthetic_eta_call_event_time_idx
    ON synthetic_demo.eta_observation (port_call_id, event_time_utc DESC);

CREATE TABLE IF NOT EXISTS synthetic_demo.eta_rejected_event (
    rejected_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT,
    port_call_id TEXT,
    source_id TEXT,
    event_time_utc TIMESTAMPTZ,
    received_at_utc TIMESTAMPTZ,
    reason_code TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    details JSONB NOT NULL,
    rejected_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (reason_code, raw_payload, details)
);

CREATE TABLE IF NOT EXISTS synthetic_demo.eta_quality_issue (
    quality_issue_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issue_code TEXT NOT NULL CHECK (
        issue_code IN ('EXACT_DUPLICATE', 'ID_COLLISION')
    ),
    event_id TEXT NOT NULL,
    port_call_id TEXT NOT NULL,
    received_at_utc TIMESTAMPTZ,
    raw_payload JSONB,
    details JSONB NOT NULL,
    detected_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (issue_code, event_id, details)
);

CREATE INDEX IF NOT EXISTS synthetic_eta_quality_call_code_idx
    ON synthetic_demo.eta_quality_issue (port_call_id, issue_code);
