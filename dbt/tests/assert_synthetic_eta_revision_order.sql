with fixture_present as (

    select exists (
        select 1
        from {{ source('synthetic_demo', 'eta_observation') }}
        where event_id = 'SYNTH-CI-ETA-001'
    ) as loaded

),

expected (event_id, previous_eta_utc, eta_change_minutes) as (

    values
        (
            'SYNTH-CI-ETA-LATE',
            timestamptz '2026-01-01T14:00:00Z',
            30::numeric
        ),
        (
            'SYNTH-CI-ETA-002',
            timestamptz '2026-01-01T14:30:00Z',
            30::numeric
        )

),

actual as (

    select event_id, previous_eta_utc, eta_change_minutes
    from {{ ref('mart_synthetic_eta_revision_history') }}
    where port_call_id = 'SYNTH-CI-CALL-001'
      and event_id in ('SYNTH-CI-ETA-LATE', 'SYNTH-CI-ETA-002')

)

select
    expected.event_id as expected_event_id,
    actual.event_id as actual_event_id
from expected
full outer join actual using (event_id)
cross join fixture_present
where fixture_present.loaded
  and (
      expected.event_id is null
      or actual.event_id is null
      or actual.previous_eta_utc is distinct from expected.previous_eta_utc
      or actual.eta_change_minutes is distinct from expected.eta_change_minutes
  )
