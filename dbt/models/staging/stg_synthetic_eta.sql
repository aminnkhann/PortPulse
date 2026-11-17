with collided_ids as (

    select distinct event_id
    from {{ source('synthetic_demo', 'eta_quality_issue') }}
    where issue_code = 'ID_COLLISION'

)

select
    o.synthetic_observation_id,
    o.event_id,
    o.port_call_id,
    o.source_id,
    o.event_time_utc,
    o.received_at_utc,
    o.ingested_at_utc,
    o.eta_utc,
    o.canonical_payload_sha256
from {{ source('synthetic_demo', 'eta_observation') }} o
left join collided_ids c
    on o.event_id = c.event_id
where c.event_id is null
