with recorded_collections as (

    select received_at_utc as collected_at_utc
    from {{ source('portnet', 'portnet_eta_observation') }}

    union all

    select received_at_utc as collected_at_utc
    from {{ source('portnet', 'portnet_eta_rejected_event') }}
    where received_at_utc is not null

)

select max(collected_at_utc) as latest_recorded_collection_utc
from recorded_collections
