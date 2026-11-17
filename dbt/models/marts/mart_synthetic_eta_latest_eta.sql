with ranked_updates as (

    select
        *,
        row_number() over (
            partition by port_call_id
            order by event_time_utc desc, event_id desc
        ) as update_rank
    from {{ ref('stg_synthetic_eta') }}

)

select
    port_call_id,
    event_id,
    source_id,
    eta_utc,
    event_time_utc,
    received_at_utc,
    ingested_at_utc
from ranked_updates
where update_rank = 1
