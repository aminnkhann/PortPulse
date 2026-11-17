with ordered_updates as (

    select
        *,
        lag(eta_utc) over (
            partition by port_call_id
            order by event_time_utc, event_id
        ) as previous_eta_utc
    from {{ ref('stg_synthetic_eta') }}

)

select
    synthetic_observation_id,
    event_id,
    port_call_id,
    source_id,
    event_time_utc,
    received_at_utc,
    ingested_at_utc,
    eta_utc,
    previous_eta_utc,
    extract(epoch from (eta_utc - previous_eta_utc)) / 60.0
        as eta_change_minutes
from ordered_updates
