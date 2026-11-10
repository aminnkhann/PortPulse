CREATE TABLE IF NOT EXISTS portnet_eta_observation (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT NOT NULL,
    port_call_id TEXT NOT NULL,
    port_area_code TEXT,
    event_time_utc TIMESTAMPTZ NOT NULL,
    received_at_utc TIMESTAMPTZ NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    eta_utc TIMESTAMPTZ NOT NULL,
    canonical_payload_sha256 CHAR(64) NOT NULL
        CHECK (canonical_payload_sha256 ~ '^[0-9a-f]{64}$'),
    raw_payload JSONB NOT NULL,
    UNIQUE (event_id, canonical_payload_sha256)
);

CREATE INDEX IF NOT EXISTS portnet_eta_observation_call_time_idx
    ON portnet_eta_observation (port_call_id, port_area_code, event_time_utc DESC);

CREATE TABLE IF NOT EXISTS portnet_eta_rejected_event (
    rejected_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT,
    port_call_id TEXT,
    received_at_utc TIMESTAMPTZ,
    reason_code TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    details JSONB NOT NULL,
    rejected_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS portnet_eta_quality_issue (
    quality_issue_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issue_code TEXT NOT NULL CHECK (
        issue_code IN ('EXACT_DUPLICATE', 'ID_COLLISION')
    ),
    event_id TEXT NOT NULL,
    port_call_id TEXT NOT NULL,
    received_at_utc TIMESTAMPTZ,
    raw_payload JSONB,
    details JSONB NOT NULL,
    detected_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS portnet_eta_quality_issue_call_code_idx
    ON portnet_eta_quality_issue (port_call_id, issue_code);
