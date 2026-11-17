with fixture_present as (

    select exists (
        select 1
        from {{ source('synthetic_demo', 'eta_observation') }}
        where event_id = 'SYNTH-CI-ETA-001'
    ) as loaded

),

expected (port_call_id, event_id, eta_utc, event_time_utc) as (

    values (
        'SYNTH-CI-CALL-001',
        'SYNTH-CI-ETA-002',
        timestamptz '2026-01-01T15:00:00Z',
        timestamptz '2026-01-01T10:00:00Z'
    )

),

actual as (

    select port_call_id, event_id, eta_utc, event_time_utc
    from {{ ref('mart_synthetic_eta_latest_eta') }}
    where port_call_id = 'SYNTH-CI-CALL-001'

)

select
    expected.port_call_id as expected_port_call_id,
    actual.port_call_id as actual_port_call_id
from expected
full outer join actual using (port_call_id)
cross join fixture_present
where fixture_present.loaded
  and (
      expected.port_call_id is null
      or actual.port_call_id is null
      or actual.event_id is distinct from expected.event_id
      or actual.eta_utc is distinct from expected.eta_utc
      or actual.event_time_utc is distinct from expected.event_time_utc
  )
