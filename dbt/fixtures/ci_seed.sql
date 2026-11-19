INSERT INTO portnet_eta_observation (
    event_id,
    port_call_id,
    port_area_code,
    event_time_utc,
    received_at_utc,
    ingested_at_utc,
    eta_utc,
    canonical_payload_sha256,
    raw_payload
)
VALUES
    (
        'portnet:dbt-ci-latest-old', 'portnet:dbt-ci-latest', 'VUOS',
        '2026-11-01T09:00:00Z', '2026-11-01T09:01:00Z', '2026-11-01T09:02:00Z',
        '2026-11-01T14:00:00Z', repeat('1', 64), '{}'::jsonb
    ),
    (
        'portnet:dbt-ci-latest-new', 'portnet:dbt-ci-latest', 'VUOS',
        '2026-11-01T10:00:00Z', '2026-11-01T10:01:00Z', '2026-11-01T10:02:00Z',
        '2026-11-01T15:00:00Z', repeat('2', 64), '{}'::jsonb
    ),
    (
        'portnet:dbt-ci-collision', 'portnet:dbt-ci-collision', 'KATA',
        '2026-11-01T10:00:00Z', '2026-11-01T10:01:00Z', '2026-11-01T10:02:00Z',
        '2026-11-01T14:30:00Z', repeat('3', 64), '{}'::jsonb
    ),
    (
        'portnet:dbt-ci-collision', 'portnet:dbt-ci-collision', 'KATA',
        '2026-11-01T10:00:00Z', '2026-11-01T11:01:00Z', '2026-11-01T11:02:00Z',
        '2026-11-01T16:00:00Z', repeat('4', 64), '{}'::jsonb
    );

INSERT INTO portnet_eta_quality_issue (
    issue_code,
    event_id,
    port_call_id,
    received_at_utc,
    raw_payload,
    details
)
VALUES (
    'ID_COLLISION',
    'portnet:dbt-ci-collision',
    'portnet:dbt-ci-collision',
    '2026-11-01T11:01:00Z',
    '{}'::jsonb,
    '{"fixture": "dbt-ci"}'::jsonb
);

INSERT INTO portnet_eta_quality_issue (
    issue_code, event_id, port_call_id, received_at_utc, raw_payload, details
)
VALUES
    ('EXACT_DUPLICATE', 'portnet:dbt-ci-duplicate', 'portnet:dbt-ci-duplicate',
     '2026-11-01T10:02:00Z', '{}'::jsonb,
     '{"payload_sha256":"duplicate-fixture"}'::jsonb),
    ('EXACT_DUPLICATE', 'portnet:dbt-ci-duplicate', 'portnet:dbt-ci-duplicate',
     '2026-11-01T10:30:00Z', '{}'::jsonb,
     '{"payload_sha256":"duplicate-fixture"}'::jsonb);

INSERT INTO portnet_eta_rejected_event (
    event_id, port_call_id, received_at_utc, reason_code, raw_payload, details
)
VALUES
    ('portnet:dbt-ci-rejected', 'portnet:dbt-ci-rejected', '2026-11-01T10:02:00Z',
     'INVALID_PORTNET_ETA', '{"eta":"bad"}'::jsonb, '{"message":"bad ETA"}'::jsonb),
    ('portnet:dbt-ci-rejected', 'portnet:dbt-ci-rejected', '2026-11-01T10:30:00Z',
     'INVALID_PORTNET_ETA', '{"eta":"bad"}'::jsonb, '{"message":"bad ETA"}'::jsonb);

INSERT INTO synthetic_demo.eta_observation (
    event_id,
    port_call_id,
    source_id,
    event_time_utc,
    received_at_utc,
    eta_utc,
    schema_version,
    canonical_payload_sha256,
    raw_payload
)
VALUES
    (
        'SYNTH-CI-ETA-001', 'SYNTH-CI-CALL-001', 'carrier_sim',
        '2026-01-01T09:00:00Z', '2026-01-01T09:01:00Z',
        '2026-01-01T14:00:00Z', 'v1', repeat('a', 64),
        '{"event_id":"SYNTH-CI-ETA-001","port_call_id":"SYNTH-CI-CALL-001","source_id":"carrier_sim","event_time_utc":"2026-01-01T09:00:00Z","received_at_utc":"2026-01-01T09:01:00Z","eta_utc":"2026-01-01T14:00:00Z","schema_version":"v1"}'::jsonb
    ),
    (
        'SYNTH-CI-ETA-002', 'SYNTH-CI-CALL-001', 'carrier_sim',
        '2026-01-01T10:00:00Z', '2026-01-01T10:01:00Z',
        '2026-01-01T15:00:00Z', 'v1', repeat('b', 64),
        '{"event_id":"SYNTH-CI-ETA-002","port_call_id":"SYNTH-CI-CALL-001","source_id":"carrier_sim","event_time_utc":"2026-01-01T10:00:00Z","received_at_utc":"2026-01-01T10:01:00Z","eta_utc":"2026-01-01T15:00:00Z","schema_version":"v1"}'::jsonb
    ),
    (
        'SYNTH-CI-ETA-LATE', 'SYNTH-CI-CALL-001', 'terminal_sim',
        '2026-01-01T09:30:00Z', '2026-01-01T11:30:00Z',
        '2026-01-01T14:30:00Z', 'v1', repeat('c', 64),
        '{"event_id":"SYNTH-CI-ETA-LATE","port_call_id":"SYNTH-CI-CALL-001","source_id":"terminal_sim","event_time_utc":"2026-01-01T09:30:00Z","received_at_utc":"2026-01-01T11:30:00Z","eta_utc":"2026-01-01T14:30:00Z","schema_version":"v1"}'::jsonb
    );

INSERT INTO portnet_eta_observation (
    event_id, port_call_id, port_area_code, event_time_utc, received_at_utc,
    ingested_at_utc, eta_utc, canonical_payload_sha256, raw_payload
)
VALUES
    ('portnet:dbt-ci-late-old', 'portnet:dbt-ci-late', 'VUOS',
     '2026-11-01T09:00:00Z', '2026-11-01T10:30:00Z', '2026-11-01T10:31:00Z',
     '2026-11-01T18:00:00Z', repeat('5', 64), '{}'::jsonb),
    ('portnet:dbt-ci-late-new', 'portnet:dbt-ci-late', 'VUOS',
     '2026-11-01T10:00:00Z', '2026-11-01T10:02:00Z', '2026-11-01T10:03:00Z',
     '2026-11-01T16:00:00Z', repeat('6', 64), '{}'::jsonb);
