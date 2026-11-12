
select
    observation_id,
    event_id,
    port_call_id,
    port_area_code,
    event_time_utc as provider_updated_at_utc,
    received_at_utc as collected_at_utc,
    ingested_at_utc,
    eta_utc,
    canonical_payload_sha256
from {{ source('portnet', 'portnet_eta_observation') }}
