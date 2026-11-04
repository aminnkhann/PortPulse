CREATE TABLE IF NOT EXISTS source_system (
    source_id TEXT PRIMARY KEY,
    priority INTEGER NOT NULL CHECK (priority > 0),
    freshness_minutes INTEGER NOT NULL CHECK (freshness_minutes > 0)
);

CREATE TABLE IF NOT EXISTS port_call (
    port_call_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS raw_event (
    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT NOT NULL,
    port_call_id TEXT NOT NULL REFERENCES port_call (port_call_id),
    source_id TEXT NOT NULL REFERENCES source_system (source_id),
    event_time_utc TIMESTAMPTZ NOT NULL,
    received_at_utc TIMESTAMPTZ NOT NULL,
    eta_utc TIMESTAMPTZ NOT NULL,
    schema_version TEXT NOT NULL,
    canonical_payload_sha256 CHAR(64) NOT NULL
        CHECK (canonical_payload_sha256 ~ '^[0-9a-f]{64}$'),
    raw_payload JSONB NOT NULL,
    UNIQUE (event_id, canonical_payload_sha256)
);

CREATE INDEX IF NOT EXISTS raw_event_call_source_time_idx
    ON raw_event (port_call_id, source_id, event_time_utc DESC);

CREATE TABLE IF NOT EXISTS rejected_event (
    rejected_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id TEXT,
    port_call_id TEXT,
    received_at_utc TIMESTAMPTZ,
    reason_code TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    rejected_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quality_issue (
    quality_issue_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issue_code TEXT NOT NULL,
    event_id TEXT,
    port_call_id TEXT,
    received_at_utc TIMESTAMPTZ,
    raw_payload JSONB,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    detected_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS quality_issue_call_code_idx
    ON quality_issue (port_call_id, issue_code);

CREATE TABLE IF NOT EXISTS port_call_state (
    port_call_id TEXT PRIMARY KEY REFERENCES port_call (port_call_id),
    status TEXT NOT NULL CHECK (status IN ('TRUSTED', 'UNAVAILABLE_STALE')),
    trusted_eta_utc TIMESTAMPTZ,
    source_id TEXT REFERENCES source_system (source_id),
    winning_event_id TEXT,
    conflict_flag BOOLEAN NOT NULL DEFAULT FALSE,
    as_of_utc TIMESTAMPTZ NOT NULL,
    rules_version TEXT NOT NULL,
    CHECK (
        (status = 'TRUSTED'
            AND trusted_eta_utc IS NOT NULL
            AND source_id IS NOT NULL
            AND winning_event_id IS NOT NULL)
        OR
        (status = 'UNAVAILABLE_STALE'
            AND trusted_eta_utc IS NULL
            AND source_id IS NULL
            AND winning_event_id IS NULL)
    )
);

INSERT INTO source_system (source_id, priority, freshness_minutes)
VALUES
    ('coordinator_sim', 3, 120),
    ('carrier_sim', 2, 120),
    ('terminal_sim', 1, 120)
ON CONFLICT (source_id) DO UPDATE
SET priority = EXCLUDED.priority,
    freshness_minutes = EXCLUDED.freshness_minutes;
