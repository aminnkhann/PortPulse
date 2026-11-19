with expected (port_call_id, port_area_code, event_id, eta_utc) as (
    values
        (
            'portnet:dbt-ci-latest',
            'VUOS',
            'portnet:dbt-ci-latest-new',
            timestamptz '2026-11-01T15:00:00Z'
        ),
        (
            'portnet:dbt-ci-late',
            'VUOS',
            'portnet:dbt-ci-late-new',
            timestamptz '2026-11-01T16:00:00Z'
        )
),
actual as (
    select port_call_id, port_area_code, event_id, eta_utc
    from {{ ref('mart_portnet_latest_eta') }}
    where port_call_id in (
        'portnet:dbt-ci-latest',
        'portnet:dbt-ci-late',
        'portnet:dbt-ci-collision'
    )
),
fixture_loaded as (
    select exists (
        select 1
        from {{ source('portnet', 'portnet_eta_observation') }}
        where event_id = 'portnet:dbt-ci-latest-old'
    ) as loaded
)

select
    expected.port_call_id,
    expected.port_area_code,
    expected.event_id as expected_event_id,
    actual.event_id as actual_event_id,
    expected.eta_utc as expected_eta_utc,
    actual.eta_utc as actual_eta_utc
from expected
full outer join actual using (port_call_id, port_area_code, event_id, eta_utc)
cross join fixture_loaded
where fixture_loaded.loaded
  and (expected.event_id is null or actual.event_id is null)
