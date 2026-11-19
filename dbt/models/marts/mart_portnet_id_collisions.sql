with collided_ids as (

    select distinct event_id
    from {{ source('portnet', 'portnet_eta_quality_issue') }}
    where issue_code = 'ID_COLLISION'

)

select
    observation.port_call_id,
    observation.port_area_code,
    observation.event_id,
    observation.eta_utc,
    observation.event_time_utc as provider_updated_at_utc,
    observation.received_at_utc as collected_at_utc,
    observation.canonical_payload_sha256
from {{ source('portnet', 'portnet_eta_observation') }} observation
join collided_ids using (event_id)
