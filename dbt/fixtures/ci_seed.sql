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
        'portnet:dbt-ci-collision-good', 'portnet:dbt-ci-collision', 'KATA',
        '2026-11-01T09:00:00Z', '2026-11-01T09:01:00Z', '2026-11-01T09:02:00Z',
        '2026-11-01T14:30:00Z', repeat('3', 64), '{}'::jsonb
    ),
    (
        'portnet:dbt-ci-collision-bad', 'portnet:dbt-ci-collision', 'KATA',
        '2026-11-01T11:00:00Z', '2026-11-01T11:01:00Z', '2026-11-01T11:02:00Z',
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
    'portnet:dbt-ci-collision-bad',
    'portnet:dbt-ci-collision',
    '2026-11-01T11:01:00Z',
    '{}'::jsonb,
    '{"fixture": "dbt-ci"}'::jsonb
);
